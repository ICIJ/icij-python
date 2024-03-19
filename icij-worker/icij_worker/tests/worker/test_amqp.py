# pylint: disable=redefined-outer-name
import asyncio
from datetime import datetime
from typing import ClassVar, List, Optional

import pytest
from aio_pika import Message, connect_robust
from pydantic import Field

from icij_common.pydantic_utils import safe_copy
from icij_common.test_utils import (
    TEST_PROJECT,
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
from icij_worker.event_publisher.amqp import AMQPPublisher, Routing
from icij_worker.tests.conftest import (
    DEFAULT_VHOST,
    RABBITMQ_TEST_HOST,
    RABBITMQ_TEST_PORT,
    TestableAMQPPublisher,
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
        self._declare_and_bind = True

    @property
    def publisher(self):
        return self._publisher

    @classmethod
    @property
    def task_routing(cls) -> Routing:
        return cls._task_routing()

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
        task_queue = await channel.declare_queue(
            task_routing.default_queue, durable=True
        )
        await task_queue.bind(task_ex, routing_key=task_routing.routing_key)
        for task in tasks:
            msg = Message(task.json().encode())
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
        expected_task = populate_tasks[0]
        consumed, _ = consume_task.result()
        assert consumed == expected_task


async def test_worker_negatively_acknowledge(
    populate_tasks: List[Task], amqp_worker: TestableAMQPWorker, rabbit_mq: str
):
    # pylint: disable=protected-access,unused-argument
    # Given
    project = TEST_PROJECT
    # When
    async with amqp_worker:
        # Then
        task, _ = await amqp_worker.consume()
        nacked = await amqp_worker.negatively_acknowledge(task, project, requeue=False)
        update = {"status": TaskStatus.ERROR}
        expected_nacked = safe_copy(task, update=update)
        assert nacked == expected_nacked


async def test_worker_negatively_acknowledge_and_requeue(
    populate_tasks: List[Task], amqp_worker: TestableAMQPWorker, rabbit_mq: str
):
    # pylint: disable=protected-access,unused-argument
    # Given
    project = TEST_PROJECT
    n_tasks = len(populate_tasks)
    # When
    async with amqp_worker:
        # Then
        task, _ = await amqp_worker.consume()
        await amqp_worker.negatively_acknowledge(task, project, requeue=True)
        # Check that we can poll the task again
        task_ids = set()
        for _ in range(n_tasks):
            consume_task = asyncio.create_task(amqp_worker.consume())
            consume_timeout = 2.0
            with fail_if_exception(
                f"failed to consume task in less than {consume_timeout}s"
            ):
                await asyncio.wait([consume_task], timeout=consume_timeout)
            task_ids.add(consume_task.result()[0].id)
        assert task.id in task_ids


async def test_publish_event(
    test_async_app: AsyncApp, amqp_worker: TestableAMQPWorker, rabbit_mq: str
):
    # pylint: disable=protected-access,unused-argument
    # Given
    broker_url = rabbit_mq
    project = TEST_PROJECT

    event = TaskEvent(task_id="some_task", progress=50.0)
    # When
    async with amqp_worker:
        await amqp_worker.publish_event(event, project)

        # Then
        connection = await connect_robust(url=broker_url)
        channel = await connection.channel()
        event_routing = amqp_worker.event_routing
        queue = await channel.get_queue(event_routing.default_queue)
        async with queue.iterator(timeout=2.0) as messages:
            async for message in messages:
                received_event = TaskEvent.parse_raw(message.body)
                break
        assert received_event == event


async def test_publish_error(
    populate_tasks: List[Task], amqp_worker: TestableAMQPWorker, rabbit_mq: str
):
    # pylint: disable=unused-argument
    # Given
    broker_url = rabbit_mq
    project = TEST_PROJECT
    task = populate_tasks[0]
    error = TaskError(
        id="error-id",
        title="someErrorTitle",
        detail="with_details",
        occurred_at=datetime.now(),
    )

    # When
    async with amqp_worker:
        await amqp_worker.save_error(error=error, task=task, project=project)
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
    project = TEST_PROJECT
    task = populate_tasks[0]
    result = TaskResult(task_id=task.id, result="hello world !")

    # When
    async with amqp_worker:
        await amqp_worker.save_result(result, project=project)
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
