import asyncio
import json
from datetime import datetime
from typing import List, Optional

import pytest
from aio_pika import DeliveryMode, Message

from icij_common.pydantic_utils import safe_copy
from icij_common.test_utils import async_true_after, fail_if_exception
from icij_worker import Task, TaskError, TaskState
from icij_worker.exceptions import TaskAlreadyExists, TaskQueueIsFull, UnknownTask
from icij_worker.objects import ErrorEvent, ProgressEvent, StacktraceItem, TaskResult
from icij_worker.tests.conftest import (
    TestableAMQPTaskManager,
    TestableFSKeyValueStorage,
)


async def test_task_manager_enqueue(
    hello_world_task: Task, test_amqp_task_manager: TestableAMQPTaskManager
) -> None:
    # Given
    task_manager = test_amqp_task_manager
    task = hello_world_task

    # When
    queued = await task_manager.enqueue(task, namespace=None)

    # Then
    assert queued.state is TaskState.QUEUED
    channel = task_manager.channel
    res_queue = await channel.get_queue("TASK")
    receive_timeout = 1.0
    async with res_queue.iterator(timeout=receive_timeout) as messages:
        try:
            async for message in messages:
                task_json = json.loads(message.body.decode())
                break
        except asyncio.TimeoutError:
            pytest.fail(f"Failed to receive result in less than {receive_timeout}")
    created_at = task_json.pop("createdAt")
    with fail_if_exception("failed to parse cancelled_at datetime"):
        datetime.fromisoformat(created_at)
    expected_json = {
        "@type": "TaskCreation",
        "id": "some-id",
        "state": "CREATED",
        "name": "hello_world",
        "arguments": {"greeted": "world"},
    }
    assert task_json == expected_json
    task_json["createdAt"] = created_at
    amqp_task = Task.parse_obj(task_json)
    assert amqp_task == task


async def test_task_manager_enqueue_with_namespace(
    hello_world_task: Task, test_amqp_task_manager: TestableAMQPTaskManager
):
    # Given
    namespace = "some-namespace"
    task_manager = test_amqp_task_manager
    task = hello_world_task

    # When
    queued = await task_manager.enqueue(task, namespace=namespace)

    # Then
    assert queued.state is TaskState.QUEUED
    channel = task_manager.channel
    res_queue = await channel.get_queue(f"TASK.{namespace}")
    receive_timeout = 1.0
    async with res_queue.iterator(timeout=receive_timeout) as messages:
        try:
            async for message in messages:
                task_json = json.loads(message.body.decode())
                break
        except asyncio.TimeoutError:
            pytest.fail(f"Failed to receive result in less than {receive_timeout}")
    created_at = task_json.pop("createdAt")
    with fail_if_exception("failed to parse cancelled_at datetime"):
        datetime.fromisoformat(created_at)
    expected_json = {
        "@type": "TaskCreation",
        "id": "some-id",
        "state": "CREATED",
        "name": "hello_world",
        "arguments": {"greeted": "world"},
    }
    assert task_json == expected_json
    task_json["createdAt"] = created_at
    amqp_task = Task.parse_obj(task_json)
    assert amqp_task == task


async def test_task_manager_enqueue_should_raise_for_existing_task(
    test_amqp_task_manager: TestableAMQPTaskManager, hello_world_task: Task
):
    # Given
    task = hello_world_task
    task_manager = test_amqp_task_manager
    await task_manager.enqueue(task, namespace=None)

    # When/Then
    with pytest.raises(TaskAlreadyExists):
        await task_manager.enqueue(task, namespace=None)


async def test_task_manager_enqueue_should_raise_when_queue_full(
    fs_storage: TestableFSKeyValueStorage, rabbit_mq: str, hello_world_task: Task
):
    # Given
    task_manager = TestableAMQPTaskManager(
        fs_storage, app_name="test-app", broker_url=rabbit_mq, max_task_queue_size=1
    )
    task = hello_world_task
    async with task_manager:
        await task_manager.enqueue(task, namespace=None)
        other_task = safe_copy(task, update={"id": "some-other-id"})
        # When/Then
        with pytest.raises(TaskQueueIsFull):
            await task_manager.enqueue(other_task, namespace=None)


async def test_task_manager_should_consume_events(
    test_amqp_task_manager: TestableAMQPTaskManager,
):
    # Given
    task_manager = test_amqp_task_manager
    channel = task_manager.channel
    task = Task(
        id="task-0",
        name="task-type-0",
        created_at=datetime.now(),
        state=TaskState.CREATED,
        arguments={"greeted": "world"},
    )
    await task_manager.save_task(task, namespace=None)
    event = ProgressEvent(task_id=task.id, progress=100.0)
    message = event.json().encode()

    # When
    exchange = await channel.get_exchange("exchangeMainEvents")
    message = Message(message, delivery_mode=DeliveryMode.PERSISTENT, app_id="test-app")
    await exchange.publish(message, "routingKeyMainEvents")
    # Then
    consume_timeout = 5.0
    msg = f"Failed to consume event in less than {consume_timeout}"

    async def _is_running() -> bool:
        try:
            t = await task_manager.get_task(task.id)
        except UnknownTask:
            return False
        return t.state is TaskState.RUNNING

    assert await async_true_after(_is_running, after_s=consume_timeout), msg
    stored_task = await task_manager.get_task(task.id)
    updates = {"progress": 100.0, "state": TaskState.RUNNING}
    expected = safe_copy(task, update=updates)
    assert stored_task == expected


async def test_task_manager_should_consume_errors(
    test_amqp_task_manager: TestableAMQPTaskManager,
):
    # Given
    task_manager = test_amqp_task_manager
    error = TaskError(
        id="some-id",
        task_id="some-task-id",
        name="error",
        message="with details",
        occurred_at=datetime.now(),
        stacktrace=[StacktraceItem(name="SomeError", file="somefile", lineno=666)],
        cause="some cause",
    )
    message = error.json().encode()
    channel = task_manager.channel
    # When
    exchange = await channel.get_exchange("exchangeTaskResults")
    message = Message(message, delivery_mode=DeliveryMode.PERSISTENT, app_id="test-app")
    await exchange.publish(message, "routingKeyMainTaskResults")
    # Then
    consume_timeout = 2.0
    msg = f"Failed to consume error in less than {consume_timeout}"

    async def _get_errors() -> List[TaskError]:
        try:
            return await task_manager.get_task_errors(error.task_id)
        except UnknownTask:
            return []

    assert await async_true_after(_get_errors, after_s=consume_timeout), msg
    errors = await task_manager.get_task_errors(error.task_id)
    assert errors == [error]


async def test_task_manager_should_consume_error_events(
    test_amqp_task_manager: TestableAMQPTaskManager,
):
    # Given
    task_manager = test_amqp_task_manager
    task = Task(
        id="some-id",
        name="some-task-name",
        created_at=datetime.now(),
        state=TaskState.RUNNING,
    )
    await task_manager.save_task(task, namespace=None)
    error = TaskError(
        id="error-id",
        task_id=task.id,
        name="error",
        message="with details",
        occurred_at=datetime.now(),
        stacktrace=[StacktraceItem(name="SomeError", file="somefile", lineno=666)],
        cause="some cause",
    )
    event = ErrorEvent.from_error(error, task_id=task.id)
    message = event.json().encode()
    channel = task_manager.channel
    # When
    exchange = await channel.get_exchange("exchangeMainEvents")
    message = Message(message, delivery_mode=DeliveryMode.PERSISTENT, app_id="test-app")
    await exchange.publish(message, "routingKeyMainEvents")
    # Then
    consume_timeout = 2.0
    msg = f"Failed to consume error event in less than {consume_timeout}"

    async def _is_error() -> bool:
        t = await task_manager.get_task(error.task_id)
        return t.state is TaskState.ERROR

    assert await async_true_after(_is_error, after_s=consume_timeout), msg


async def test_task_manager_should_consume_result(test_amqp_task_manager):
    # Given
    task_manager = test_amqp_task_manager
    result = TaskResult(
        task_id="some-task-id", result="some-result", completed_at=datetime.now()
    )
    message = result.json().encode()
    channel = task_manager.channel
    # When
    exchange = await channel.get_exchange("exchangeTaskResults")
    message = Message(message, delivery_mode=DeliveryMode.PERSISTENT, app_id="test-app")
    await exchange.publish(message, "routingKeyMainTaskResults")
    # Then
    consume_timeout = 2.0
    msg = f"Failed to consume result in less than {consume_timeout}"

    async def _get_result() -> Optional[TaskResult]:
        try:
            return await task_manager.get_task_result(result.task_id)
        except UnknownTask:
            return None

    assert await async_true_after(_get_result, after_s=consume_timeout), msg
    amqp_res = await task_manager.get_task_result(result.task_id)
    assert amqp_res == result


@pytest.mark.parametrize("requeue", [True, False])
async def test_task_manager_cancel(
    test_amqp_task_manager: TestableAMQPTaskManager,
    requeue: bool,
    hello_world_task: Task,
):
    # Given
    task_manager = test_amqp_task_manager
    task = hello_world_task
    await task_manager.save_task(task, namespace=None)

    # When
    await task_manager.cancel(task_id=task.id, requeue=requeue)

    # Then
    channel = task_manager.channel
    res_queue = await channel.get_queue("RUNNER_EVENT")
    receive_timeout = 1.0
    async with res_queue.iterator(timeout=receive_timeout) as messages:
        try:
            async for message in messages:
                cancel_evt_json = json.loads(message.body.decode())
                break
        except asyncio.TimeoutError:
            pytest.fail(f"Failed to receive result in less than {receive_timeout}")
    cancelled_at = cancel_evt_json.pop("cancelledAt")
    with fail_if_exception("failed to parse cancelled_at datetime"):
        datetime.fromisoformat(cancelled_at)
    expected_json = {"@type": "CancelEvent", "requeue": requeue, "taskId": "some-id"}
    assert cancel_evt_json == expected_json
