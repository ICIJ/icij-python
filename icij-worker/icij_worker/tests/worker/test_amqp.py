# pylint: disable=redefined-outer-name
import asyncio
import functools
from datetime import datetime
from functools import lru_cache
from typing import ClassVar, Dict, List, Optional

import pytest
from aio_pika import ExchangeType, Message, connect_robust
from pydantic import Field

from icij_common.pydantic_utils import safe_copy
from icij_common.test_utils import (
    TEST_PROJECT,
    async_true_after,
    fail_if_exception,
)
from icij_worker import (
    AsyncApp,
    Task,
    TaskError,
    TaskEvent,
    TaskResult,
    TaskStatus,
    Worker,
    WorkerConfig,
)
from icij_worker.event_publisher.amqp import AMQPPublisher, Exchange, Routing
from icij_worker.task import CancelledTaskEvent
from icij_worker.tests.conftest import (
    DEFAULT_VHOST,
    RABBITMQ_TEST_HOST,
    RABBITMQ_TEST_PORT,
    TestableAMQPPublisher,
    get_queue_size,
)
from icij_worker.worker.amqp import AMQPWorker, AMQPWorkerConfig


@WorkerConfig.register("test-amqp")
class TestableAMQPWorkerConfig(AMQPWorkerConfig):
    type: ClassVar[str] = Field(const=True, default="test-amqp")


@Worker.register("test-amqp")
class TestableAMQPWorker(AMQPWorker):

    def __init__(
        self,
        app: AsyncApp,
        worker_id: str,
        *,
        broker_url: str,
        inactive_after_s: Optional[float] = None,
        handle_signals: bool = True,
        teardown_dependencies: bool = False,
        **kwargs,
    ):
        super().__init__(
            app,
            worker_id,
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

    @classmethod
    @lru_cache(maxsize=1)
    def _task_routing(cls) -> Routing:
        routing = super()._task_routing()
        dl_routing = Routing(
            exchange=Exchange(name="exchangeDLQTasks", type=ExchangeType.DIRECT),
            routing_key="routingKeyDLQTasks",
            default_queue="queueDLQTasks",
        )
        routing = safe_copy(routing, update={"dead_letter_routing": dl_routing})
        return routing

    @classmethod
    @property
    def task_routing(cls) -> Routing:
        return cls._task_routing()

    @classmethod
    @property
    def cancel_event_routing(cls) -> Routing:
        return cls._cancel_event_routing()

    @classmethod
    @property
    def result_routing(cls) -> Routing:
        return AMQPPublisher.res_routing()

    @classmethod
    @property
    def event_routing(cls) -> Routing:
        return AMQPPublisher.evt_routing()

    @classmethod
    @property
    def error_routing(cls) -> Routing:
        return AMQPPublisher.err_routing()

    @property
    def cancelled(self) -> Dict[str, CancelledTaskEvent]:
        return self._cancelled

    def _create_publisher(self):
        return TestableAMQPPublisher(
            self._logger,
            broker_url=self._broker_url,
            connection_timeout_s=self._connection_timeout_s,
            reconnection_wait_s=self._reconnection_wait_s,
            app_id=self._app.name,
        )


@pytest.fixture
def amqp_worker(test_async_app: AsyncApp, rabbit_mq: str) -> TestableAMQPWorker:
    # pylint: disable=unused-argument
    config = TestableAMQPWorkerConfig(
        rabbitmq_host=RABBITMQ_TEST_HOST,
        rabbitmq_port=RABBITMQ_TEST_PORT,
        rabbitmq_vhost=DEFAULT_VHOST,
        rabbitmq_user="guest",
        rabbitmq_password="guest",
    )
    worker = Worker.from_config(
        config,
        app=test_async_app,
        worker_id="test-worker",
        teardown_dependencies=True,
    )
    return worker


@pytest.fixture
async def populate_tasks(rabbit_mq: str):
    connection = await connect_robust(rabbit_mq)
    task_routing = TestableAMQPWorker.task_routing
    tasks = [
        Task(
            id="task-0",
            type="hello_world",
            created_at=datetime.now(),
            status=TaskStatus.CREATED,
            inputs={"greeted": "world"},
        ),
        Task(
            id="task-1",
            type="hello_world",
            created_at=datetime.now(),
            status=TaskStatus.CREATED,
            inputs={"greeted": "goodbye"},
        ),
    ]
    async with connection:
        channel = await connection.channel()
        task_ex = await channel.declare_exchange(
            task_routing.exchange.name, durable=True
        )
        dl_ex = task_routing.dead_letter_routing.exchange.name
        arguments = {"x-dead-letter-exchange": dl_ex}
        task_queue = await channel.declare_queue(
            task_routing.default_queue, durable=True, arguments=arguments
        )
        await task_queue.bind(task_ex, routing_key=task_routing.routing_key)
        for task in tasks:
            msg = Message(task.json().encode())
            await task_ex.publish(msg, task_routing.routing_key)
    return tasks


async def _publish_cancel_event(rabbit_mq: str, task_id: str):
    connection = await connect_robust(rabbit_mq)
    cancel_event_routing = TestableAMQPWorker.cancel_event_routing
    event = CancelledTaskEvent(task_id=task_id, created_at=datetime.now(), requeue=True)
    async with connection:
        channel = await connection.channel()
        cancel_event_ex = await channel.declare_exchange(
            cancel_event_routing.exchange.name, durable=True, type=ExchangeType.FANOUT
        )
        msg = Message(event.json().encode())
        await cancel_event_ex.publish(msg, cancel_event_routing.routing_key)
    return event


async def test_worker_work_forever(
    populate_tasks: List[Task], amqp_worker: TestableAMQPWorker, rabbit_mq: str
):
    # pylint: disable=protected-access,unused-argument
    # Given
    broker_url = rabbit_mq
    connection = await connect_robust(url=broker_url)
    channel = await connection.channel()
    res_routing = TestableAMQPWorker.result_routing

    # When
    async with amqp_worker:
        asyncio.create_task(amqp_worker._work_forever())

        # Then
        res_queue = await channel.get_queue(res_routing.default_queue)
        receive_timeout = 1.0
        async with res_queue.iterator(timeout=receive_timeout) as messages:
            try:
                async for message in messages:
                    result = TaskResult.parse_raw(message.body)
                    break
            except asyncio.TimeoutError:
                pytest.fail(f"Failed to receive result in less than {receive_timeout}")
        task = populate_tasks[0]
        expected_result = TaskResult(task_id=task.id, result="Hello world !")
        assert result == expected_result


async def test_worker_consume_task(
    populate_tasks: List[Task], amqp_worker: TestableAMQPWorker, rabbit_mq: str
):
    # pylint: disable=protected-access,unused-argument
    # When
    async with amqp_worker:
        # Then
        consume_task = asyncio.create_task(amqp_worker.consume())
        consume_timeout = 2.0
        with fail_if_exception(
            f"failed to consume task in less than {consume_timeout}s"
        ):
            await asyncio.wait([consume_task], timeout=consume_timeout)
        expected_task = safe_copy(populate_tasks[0], update={"progress": 0.0})
        consumed = consume_task.result()
        assert consumed == expected_task


async def test_worker_consume_cancel_events(
    amqp_worker: TestableAMQPWorker, rabbit_mq: str
):
    # pylint: disable=protected-access,unused-argument
    # Given
    async with amqp_worker:
        assert not amqp_worker.cancelled
        # When
        expected_event = await _publish_cancel_event(rabbit_mq, task_id="some-id")

        # Then
        cancel_timeout = 2.0
        failure = f"failed to consume cancel event in less than {cancel_timeout}s"

        async def _received_event() -> bool:
            return bool(amqp_worker.cancelled)

        assert await async_true_after(_received_event, after_s=cancel_timeout), failure
        assert len(amqp_worker.cancelled) == 1
        received_event = amqp_worker.cancelled.pop("some-id")
        assert received_event == expected_event


async def test_worker_negatively_acknowledge(
    populate_tasks: List[Task], amqp_worker: TestableAMQPWorker, rabbit_mq: str
):
    # pylint: disable=protected-access,unused-argument
    # When
    async with amqp_worker:
        # Then
        task = await amqp_worker.consume()
        await amqp_worker.negatively_acknowledge(task, requeue=False)

        dlq_name = amqp_worker.task_routing.dead_letter_routing.default_queue

        async def _dlqueued() -> bool:
            size = await get_queue_size(dlq_name)
            return bool(size)

        assert await async_true_after(_dlqueued, after_s=10.0)


async def test_worker_negatively_acknowledge_and_requeue(
    populate_tasks: List[Task], amqp_worker: TestableAMQPWorker, rabbit_mq: str
):
    # pylint: disable=protected-access,unused-argument
    # Given
    n_tasks = len(populate_tasks)
    # When
    async with amqp_worker:
        # Then
        task = await amqp_worker.consume()
        await amqp_worker.negatively_acknowledge(task, requeue=True)
        # Check that we can poll the task again
        task_ids = set()
        for _ in range(n_tasks):
            consume_task = asyncio.create_task(amqp_worker.consume())
            consume_timeout = 2.0
            with fail_if_exception(
                f"failed to consume task in less than {consume_timeout}s"
            ):
                await asyncio.wait([consume_task], timeout=consume_timeout)
            task_ids.add(consume_task.result().id)
        assert task.id in task_ids


@pytest.mark.parametrize("requeue", [True, False])
async def test_worker_negatively_acknowledge_and_cancel(
    populate_tasks: List[Task],
    amqp_worker: TestableAMQPWorker,
    rabbit_mq: str,
    requeue: bool,
):
    # pylint: disable=protected-access,unused-argument
    # Given
    # When
    async with amqp_worker:
        # Then
        task = await amqp_worker.consume()
        await amqp_worker.negatively_acknowledge(task, requeue=requeue, cancel=True)
        task_routing = amqp_worker.task_routing

        async def _requeued(queue_name: str, n: int) -> bool:
            size = await get_queue_size(queue_name)
            return size == n

        if requeue:
            expected = functools.partial(
                _requeued, queue_name=task_routing.default_queue, n=2
            )
        else:
            dlq_name = amqp_worker.task_routing.dead_letter_routing.default_queue
            expected = functools.partial(_requeued, queue_name=dlq_name, n=1)
        timeout = 5  # Stats can take long to refresh...
        failure = f"Failed to requeue job in less than {timeout} seconds."
        assert await async_true_after(expected, after_s=timeout), failure


async def test_publish_event(
    test_async_app: AsyncApp,
    amqp_worker: TestableAMQPWorker,
    rabbit_mq: str,
    hello_world_task: Task,
):
    # pylint: disable=protected-access,unused-argument
    # Given
    broker_url = rabbit_mq
    task = hello_world_task
    event = TaskEvent(task_id=task.id, progress=50.0)
    # When
    async with amqp_worker:
        await amqp_worker.publish_event(event, task)

        # Then
        connection = await connect_robust(url=broker_url)
        channel = await connection.channel()
        event_routing = amqp_worker.event_routing
        queue = await channel.get_queue(event_routing.default_queue)
        async with queue.iterator(timeout=2.0) as messages:
            async for message in messages:
                received_event = TaskEvent.parse_raw(message.body)
                break
        expected = safe_copy(event, update={"project_id": TEST_PROJECT})
        assert received_event == expected


async def test_publish_error(
    populate_tasks: List[Task], amqp_worker: TestableAMQPWorker, rabbit_mq: str
):
    # pylint: disable=unused-argument
    # Given
    broker_url = rabbit_mq
    task = populate_tasks[0]
    error = TaskError(
        id="error-id",
        task_id=task.id,
        title="someErrorTitle",
        detail="with_details",
        occurred_at=datetime.now(),
    )

    # When
    async with amqp_worker:
        await amqp_worker.save_error(error=error)
        # Then
        connection = await connect_robust(url=broker_url)
        channel = await connection.channel()
        error_routing = amqp_worker.error_routing
        error_queue = await channel.get_queue(error_routing.default_queue)
        async with error_queue.iterator(timeout=2.0) as messages:
            async for message in messages:
                received_error = TaskError.parse_raw(message.body)
                break
        assert received_error == error


async def test_publish_result(
    populate_tasks: List[Task], amqp_worker: TestableAMQPWorker, rabbit_mq: str
):
    # pylint: disable=unused-argument
    # Given
    broker_url = rabbit_mq
    task = populate_tasks[0]
    result = TaskResult(task_id=task.id, result="hello world !")

    # When
    async with amqp_worker:
        await amqp_worker.save_result(result)
        # Then
        connection = await connect_robust(url=broker_url)
        channel = await connection.channel()
        result_routing = amqp_worker.result_routing
        result_queue = await channel.get_queue(result_routing.default_queue)
        async with result_queue.iterator(timeout=2.0) as messages:
            async for message in messages:
                received_result = TaskResult.parse_raw(message.body)
                break
        assert received_result == result
