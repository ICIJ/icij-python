# pylint: disable=redefined-outer-name
import asyncio
import functools
import itertools
import json
from datetime import datetime
from typing import ClassVar, Dict, List, Optional, Type

import pytest
from aio_pika import (
    ExchangeType,
    Message as AMQPMessage,
    connect_robust,
)
from pydantic import Field

from icij_common.pydantic_utils import safe_copy
from icij_common.test_utils import async_true_after, fail_if_exception
from icij_worker import (
    AMQPTaskManager,
    AsyncApp,
    Message,
    ResultEvent,
    Task,
    TaskError,
    TaskState,
    Worker,
    WorkerConfig,
)
from icij_worker.objects import (
    CancelEvent,
    ErrorEvent,
    ManagerEvent,
    ProgressEvent,
    StacktraceItem,
    TaskResult,
    TaskUpdate,
)
from icij_worker.routing_strategy import RoutingStrategy
from icij_worker.tests.conftest import (
    DEFAULT_VHOST,
    RABBITMQ_MANAGEMENT_PORT,
    RABBITMQ_TEST_HOST,
    RABBITMQ_TEST_PASSWORD,
    RABBITMQ_TEST_PORT,
    RABBITMQ_TEST_USER,
    TestableAMQPPublisher,
    get_queue,
    get_queue_size,
)
from icij_worker.utils.amqp import AMQPManagementClient, AMQPMixin, RobustConnection
from icij_worker.worker.amqp import AMQPWorker, AMQPWorkerConfig
from icij_worker.worker.worker import WE


@WorkerConfig.register("test-amqp")
class TestableAMQPWorkerConfig(AMQPWorkerConfig):
    type: ClassVar[str] = Field(const=True, default="test-amqp")


@Worker.register("test-amqp")
class TestableAMQPWorker(AMQPWorker):

    def __init__(
        self,
        app: AsyncApp,
        management_client: AMQPManagementClient,
        worker_id: str,
        *,
        group: Optional[str],
        broker_url: str,
        inactive_after_s: Optional[float] = None,
        handle_signals: bool = True,
        teardown_dependencies: bool = False,
        **kwargs,
    ):
        super().__init__(
            app,
            management_client,
            worker_id,
            group=group,
            broker_url=broker_url,
            inactive_after_s=inactive_after_s,
            handle_signals=handle_signals,
            teardown_dependencies=teardown_dependencies,
            **kwargs,
        )
        self._declare_exchanges = True

    @property
    def publisher(self):
        return self._publisher

    @property
    def app(self) -> AsyncApp:
        return self._app

    @property
    def worker_events(self) -> Dict[Type[WE], Dict[str, WE]]:
        return self._worker_events

    @property
    def late_ack(self) -> bool:
        return self._late_ack

    def _create_publisher(self):
        return TestableAMQPPublisher(
            self._logger,
            broker_url=self._broker_url,
            connection_timeout_s=self._connection_timeout_s,
            reconnection_wait_s=self._reconnection_wait_s,
            is_qpid=self._is_qpid,
            app_id=self._app.name,
        )

    async def work_once(self):
        await self._work_once()


@pytest.fixture(scope="session")
def amqp_worker_config() -> TestableAMQPWorkerConfig:
    config = TestableAMQPWorkerConfig(
        rabbitmq_host=RABBITMQ_TEST_HOST,
        rabbitmq_port=RABBITMQ_TEST_PORT,
        rabbitmq_management_port=RABBITMQ_MANAGEMENT_PORT,
        rabbitmq_vhost=DEFAULT_VHOST,
        rabbitmq_user=RABBITMQ_TEST_USER,
        rabbitmq_password=RABBITMQ_TEST_PASSWORD,
        rabbitmq_is_qpid=False,
    )
    return config


@pytest.fixture(params=[{"app": "test_async_app"}, {"app": "test_async_app_late"}])
def amqp_worker(
    rabbit_mq: str, amqp_worker_config: TestableAMQPWorkerConfig, request
) -> TestableAMQPWorker:
    # pylint: disable=unused-argument
    params = getattr(request, "param", dict())
    params = params or dict()
    app = request.getfixturevalue(params.get("app", "test_async_app"))

    worker = Worker.from_config(
        amqp_worker_config,
        app=app,
        worker_id="test-worker",
        teardown_dependencies=True,
        group=params.get("group"),
    )
    return worker


@pytest.fixture
async def populate_tasks(rabbit_mq: str, request):
    connection = await connect_robust(rabbit_mq, connection_class=RobustConnection)
    routing_strategy = RoutingStrategy()
    group = getattr(request, "param", None)
    task_routing = routing_strategy.amqp_task_routing(group)
    tasks = [
        Task(
            id="task-0",
            name="hello_world",
            created_at=datetime.now(),
            state=TaskState.CREATED,
            args={"greeted": "world"},
        ),
        Task(
            id="task-1",
            name="hello_world",
            created_at=datetime.now(),
            state=TaskState.CREATED,
            args={"greeted": "goodbye"},
        ),
    ]
    async with connection:
        channel = await connection.channel(publisher_confirms=False)
        task_ex = await channel.declare_exchange(
            task_routing.exchange.name, durable=True
        )
        dl_ex = task_routing.dead_letter_routing.exchange.name
        dl_routing_key = task_routing.dead_letter_routing.routing_key
        arguments = {
            "x-dead-letter-exchange": dl_ex,
            "x-dead-letter-routing-key": dl_routing_key,
            "x-queue-type": "quorum",
        }
        task_queue = await channel.declare_queue(
            task_routing.queue_name, durable=True, arguments=arguments
        )
        await task_queue.bind(task_ex, routing_key=task_routing.routing_key)
        for task in tasks:
            msg = AMQPMessage(task.json().encode())
            await task_ex.publish(msg, task_routing.routing_key)
    return tasks


async def test_worker_work_forever(
    populate_tasks: List[Task], amqp_worker: TestableAMQPWorker, rabbit_mq: str
):
    # pylint: disable=protected-access,unused-argument
    # Given
    broker_url = rabbit_mq
    connection = await connect_robust(url=broker_url)
    channel = await connection.channel()
    res_routing = TestableAMQPWorker.manager_evt_routing()

    # When
    async with amqp_worker:
        asyncio.create_task(amqp_worker._work_forever())

        # Then
        res_queue = await channel.get_queue(res_routing.queue_name)
        receive_timeout = 1.0
        async with res_queue.iterator(timeout=receive_timeout) as messages:
            try:
                async for message in messages:
                    msg = Message.parse_raw(message.body)
                    if not isinstance(msg, ResultEvent):
                        continue
                    break
            except asyncio.TimeoutError:
                pytest.fail(f"Failed to receive result in less than {receive_timeout}")
        task = populate_tasks[0]
        expected_result = ResultEvent(
            task_id=task.id,
            result=TaskResult(value="Hello world !"),
            created_at=msg.created_at,
        )
        assert msg == expected_result


@pytest.mark.parametrize(
    "populate_tasks,amqp_worker",
    [
        (
            "some-ignored-group",
            {"group": "some-ignored-group"},
        ),
        (None, None),
    ],
    indirect=["populate_tasks", "amqp_worker"],
)
async def test_worker_consume_task(
    populate_tasks: List[Task], amqp_worker: TestableAMQPWorker, rabbit_mq: str
):
    # pylint: disable=protected-access,unused-argument
    # When
    async with amqp_worker:
        # Then
        consume_task = asyncio.create_task(amqp_worker.consume())
        consume_timeout = 2.0
        await asyncio.wait([consume_task], timeout=consume_timeout)
        expected_task = safe_copy(
            populate_tasks[0], update={"progress": 0.0, "state": "RUNNING"}
        )
        consumed = consume_task.result()
        assert consumed == expected_task


async def test_worker_should_nack_queue_unregistered_task(
    amqp_worker: TestableAMQPWorker,
    test_amqp_task_manager: AMQPTaskManager,
):
    # Given
    unknown = Task.create(task_id="some-id", task_name="im_unknown", args=dict())
    task_manager = test_amqp_task_manager
    task_routing = amqp_worker.default_task_routing()
    await task_manager.save_task_(unknown, None)
    async with amqp_worker:
        # When
        await task_manager.enqueue(unknown)

        # Then
        async def _assert_has_size(queue_name: str, n: int) -> bool:
            size = await get_queue_size(queue_name)
            return size == n

        t = asyncio.create_task(amqp_worker.consume())
        timeout = 10.0
        expected = functools.partial(
            _assert_has_size, queue_name=task_routing.queue_name, n=0
        )
        failure = f"Failed to consume task in less than {timeout} seconds."
        assert await async_true_after(expected, after_s=timeout), failure
        expected = functools.partial(
            _assert_has_size,
            queue_name=task_routing.dead_letter_routing.queue_name,
            n=1,
        )
        failure = f"Failed to DL-queue task in less than {timeout} seconds."
        assert await async_true_after(expected, after_s=timeout), failure
        t.cancel()


@pytest.mark.parametrize(
    "populate_tasks,amqp_worker",
    [("some-group", None), (None, {"group": "some-group"})],
    indirect=["populate_tasks", "amqp_worker"],
)
async def test_should_not_consume_task_from_other_group(
    populate_tasks: List[Task], amqp_worker: TestableAMQPWorker, rabbit_mq: str
):
    # pylint: disable=protected-access,unused-argument
    # When
    async with amqp_worker:
        # Then
        consume_task = asyncio.create_task(amqp_worker.consume())
        consume_timeout = 0.5
        done, _ = await asyncio.wait([consume_task], timeout=consume_timeout)
        if done:
            pytest.fail("groupd task was consumed!")


@pytest.mark.parametrize(
    "requeue,retries_left", list(itertools.product((False, True), (0, 1)))
)
async def test_worker_consume_cancel_events(
    test_amqp_task_manager: AMQPTaskManager,
    amqp_worker: TestableAMQPWorker,
    rabbit_mq: str,
    requeue,
    retries_left: int,
):
    # pylint: disable=protected-access,unused-argument
    # Given
    task_manager = test_amqp_task_manager
    amqp_worker._declare_exchanges = False
    created_at = datetime.now()
    duration = 100
    task = Task(
        id="some-id",
        name="sleep_for",
        created_at=created_at,
        state=TaskState.CREATED,
        args={"duration": duration},
        retries_left=retries_left,
    )
    await task_manager.save_task(task)
    after_s = 5

    async def _assert_has_state(state: TaskState) -> bool:
        saved = await task_manager.get_task(task_id=task.id)
        return saved.state is state

    async with amqp_worker:
        await task_manager.enqueue(task)
        t = asyncio.create_task(amqp_worker.work_once())
        amqp_worker._work_once_task = t
        failure = f"failed to consume task event in less than {after_s}s"
        is_running = functools.partial(_assert_has_state, TaskState.RUNNING)
        assert await async_true_after(is_running, after_s=after_s), failure
        # When
        await task_manager.cancel(task.id, requeue=requeue)

        # Then
        failure = f"failed to consume cancel event in less than {after_s}s"

        async def _received_event() -> bool:
            return bool(amqp_worker.worker_events[CancelEvent])

        assert await async_true_after(_received_event, after_s=after_s), failure
        received_event = amqp_worker.worker_events[CancelEvent].pop("some-id")
        expected_event = CancelEvent(
            task_id="some-id", requeue=requeue, created_at=datetime.now()
        ).dict(exclude={"created_at"})
        received_event = received_event.dict()
        created_at = received_event.pop("created_at")
        assert isinstance(created_at, datetime)
        assert received_event == expected_event


@pytest.mark.parametrize(
    "amqp_worker", [{"app": "test_async_app_late"}], indirect=["amqp_worker"]
)
async def test_worker_negatively_acknowledge(
    populate_tasks: List[Task], amqp_worker: TestableAMQPWorker
):
    # pylint: disable=protected-access,unused-argument
    # When
    async with amqp_worker:
        # Then
        task = await amqp_worker.consume()
        await amqp_worker._negatively_acknowledge_(task)
        task_routing = amqp_worker.task_routing(None)

        async def _processed_current() -> bool:
            q = await get_queue(task_routing.queue_name)
            # We expect the worker to ack the task (not to nack it), the TM will take
            # care of requeuing the task when receiving the ErrorEvent
            n_acks = q.get("message_stats", dict()).get("ack")
            return bool(n_acks)

        timeout = 10  # Stats can take long to refresh...
        failure = f"Failed to ack current task in less than {timeout} seconds."
        assert await async_true_after(_processed_current, after_s=timeout), failure


_CREATED_AT = datetime.now()


@pytest.mark.parametrize(
    "event,expected_json",
    [
        (
            ProgressEvent(task_id="some-id", progress=0.5, created_at=_CREATED_AT),
            f'{{"taskId": "some-id", "createdAt": "{_CREATED_AT.isoformat()}",'
            ' "progress": 0.5, "@type": "ProgressEvent"}',
        ),
        (
            ErrorEvent(
                task_id="some-id",
                retries_left=4,
                error=TaskError(
                    name="some-error",
                    message="some message",
                    stacktrace=[
                        StacktraceItem(
                            name="SomeError", file="some details", lineno=666
                        )
                    ],
                ),
                created_at=_CREATED_AT,
            ),
            '{"taskId": "some-id", '
            f'"createdAt": "{_CREATED_AT.isoformat()}", "retriesLeft": 4, '
            '"error": {'
            '"name": "some-error", "message": "some message", "stacktrace": [{"name": '
            '"SomeError", "file": "some details", "lineno": 666}],'
            ' "@type": "TaskError"}, "@type": "ErrorEvent"}',
        ),
    ],
)
async def test_publish_event(
    test_async_app: AsyncApp,
    amqp_worker: TestableAMQPWorker,
    rabbit_mq: str,
    event: ManagerEvent,
    expected_json: str,
):
    # pylint: disable=protected-access,unused-argument
    # Given
    broker_url = rabbit_mq
    # When
    async with amqp_worker:
        await amqp_worker.publish_event(event)

        # Then
        connection = await connect_robust(url=broker_url)
        channel = await connection.channel()
        event_routing = amqp_worker.manager_evt_routing()
        queue = await channel.get_queue(event_routing.queue_name)
        async with queue.iterator(timeout=2.0) as messages:
            async for message in messages:
                received_event_json = message.body.decode()
                break
        assert received_event_json == expected_json
        assert Message.parse_obj(json.loads(received_event_json)) == event


@pytest.mark.parametrize("retries_left", [0, 1])
async def test_publish_error(
    populate_tasks: List[Task],
    amqp_worker: TestableAMQPWorker,
    rabbit_mq: str,
    retries_left: int,
):
    # pylint: disable=unused-argument
    # Given
    broker_url = rabbit_mq
    task = populate_tasks[0]
    error = TaskError(
        name="someErrorTitle",
        message="with_details",
        stacktrace=[StacktraceItem(name="someErrorTitle", file="somefile", lineno=666)],
    )

    # When
    async with amqp_worker:
        await amqp_worker.publish_error_event(error, task, retries=retries_left)
        # Then
        connection = await connect_robust(url=broker_url)
        channel = await connection.channel()
        error_routing = amqp_worker.manager_evt_routing()
        error_queue = await channel.get_queue(error_routing.queue_name)
        async with error_queue.iterator(timeout=2.0) as messages:
            async for message in messages:
                received_event = json.loads(message.body)
                break
        expected_json = {
            "@type": "ErrorEvent",
            "error": {
                "@type": "TaskError",
                "message": "with_details",
                "name": "someErrorTitle",
                "stacktrace": [
                    {"file": "somefile", "lineno": 666, "name": "someErrorTitle"}
                ],
            },
            "retriesLeft": retries_left,
            "taskId": "task-0",
        }
        received_created_at = received_event.pop("createdAt")
        assert isinstance(datetime.fromisoformat(received_created_at), datetime)
        assert received_event == expected_json


async def test_publish_result_event(
    populate_tasks: List[Task], amqp_worker: TestableAMQPWorker, rabbit_mq: str
):
    # pylint: disable=unused-argument
    # Given
    broker_url = rabbit_mq
    task = populate_tasks[0]
    completed_at = datetime.now()
    task = safe_copy(
        task, update=TaskUpdate.done(completed_at).dict(exclude_unset=True)
    )
    result = "hello world !"

    # When
    async with amqp_worker:
        await amqp_worker.publish_result_event(result, task)
        # Then
        connection = await connect_robust(url=broker_url)
        channel = await connection.channel()
        result_routing = amqp_worker.manager_evt_routing()
        result_queue = await channel.get_queue(result_routing.queue_name)
        async with result_queue.iterator(timeout=2.0) as messages:
            async for message in messages:
                received_result = json.loads(message.body)
                break
        expected_json = {
            "@type": "ResultEvent",
            "result": {"@type": "TaskResult", "value": "hello world !"},
            "taskId": "task-0",
        }
        created_at = received_result.pop("createdAt")
        assert isinstance(datetime.fromisoformat(created_at), datetime)
        assert received_result == expected_json


async def test_amqp_config_uri():
    # Given
    config = AMQPWorkerConfig()
    # When
    url = config.broker_url
    # Then
    assert url == "amqp://guest:guest@127.0.0.1:5672/%2F"


async def test_worker_should_share_publisher_connection(amqp_worker: AMQPWorker):
    # pylint: disable=protected-access
    # When
    async with amqp_worker:
        assert amqp_worker._connection is amqp_worker._publisher._connection


class _RouteCreator(AMQPMixin):
    def __init__(self, broker_url: str):
        super().__init__(broker_url)

    async def __aenter__(self):
        self._connection_ = await connect_robust(
            self._broker_url,
            timeout=self._connection_timeout_s,
            reconnect_interval=self._reconnection_wait_s,
            connection_class=RobustConnection,
        )
        await self._exit_stack.enter_async_context(self._connection)
        self._channel_ = await self._connection.channel(
            publisher_confirms=self._publisher_confirms, on_return_raises=False
        )

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)


async def test_worker_connection_workflow(
    rabbit_mq: str,  # pylint: disable=unused-argument
    amqp_worker_config: AMQPWorker,
    test_async_app: AsyncApp,
):
    # pylint: disable=protected-access
    # Given
    route_creator = _RouteCreator(rabbit_mq)
    worker_id = "test-worker"
    worker = AMQPWorker._from_config(
        amqp_worker_config, app=test_async_app, worker_id=worker_id, group=None
    )
    async with route_creator:
        # These are supposed to be created by the TM
        await route_creator._create_routing(route_creator.default_task_routing())
        await route_creator._create_routing(route_creator.manager_evt_routing())
        await route_creator.channel.declare_exchange(
            route_creator.worker_evt_routing().exchange.name,
            durable=True,
            type=ExchangeType.FANOUT,
        )
        # When
        msg = "Failed to start worker"
        with fail_if_exception(msg):
            async with worker:
                # Ensure that the worker created it own queue
                expected_queue = "WORKER_EVENT-test-worker"
                await route_creator.channel.get_queue(expected_queue, ensure=True)
