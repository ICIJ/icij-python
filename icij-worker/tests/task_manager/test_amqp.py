import asyncio
import json
from copy import deepcopy
from datetime import datetime

import pytest
from aio_pika import DeliveryMode, Message

from icij_common.pydantic_utils import safe_copy
from icij_common.test_utils import fail_if_exception, true_after
from icij_worker import AsyncApp, Task, TaskState
from icij_worker.app import AsyncAppConfig
from icij_worker.exceptions import TaskAlreadyQueued, TaskQueueIsFull
from icij_worker.objects import (
    ProgressEvent,
)
from icij_worker.utils.amqp import AMQPManagementClient
from ..conftest import TestableAMQPTaskManager, TestableFSKeyValueStorage


async def test_task_manager_enqueue(
    hello_world_task: Task, test_amqp_task_manager: TestableAMQPTaskManager
) -> None:
    # Given
    task_manager = test_amqp_task_manager
    task = hello_world_task
    await task_manager.save_task(task)

    # When
    queued = await task_manager.enqueue(task)

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
    with fail_if_exception("failed to parse createdAt datetime"):
        datetime.fromisoformat(created_at)
    expected_json = {
        "@type": "Task",
        "id": "some-id",
        "state": "CREATED",
        "name": "hello_world",
        "args": {"greeted": "world"},
    }
    assert task_json == expected_json
    task_json["createdAt"] = created_at
    amqp_task = Task.model_validate(task_json)
    assert amqp_task == task


async def test_task_manager_enqueue_with_group(
    grouped_hello_world_task: Task, test_amqp_task_manager: TestableAMQPTaskManager
):
    # Given
    group = "hello"
    task_manager = test_amqp_task_manager
    task = grouped_hello_world_task
    await task_manager.save_task(task)

    # When
    queued = await task_manager.enqueue(task)

    # Then
    assert queued.state is TaskState.QUEUED
    channel = task_manager.channel
    res_queue = await channel.get_queue(f"TASK.{group}")
    receive_timeout = 1.0
    async with res_queue.iterator(timeout=receive_timeout) as messages:
        try:
            async for message in messages:
                task_json = json.loads(message.body.decode())
                break
        except asyncio.TimeoutError:
            pytest.fail(f"Failed to receive result in less than {receive_timeout}")
    created_at = task_json.pop("createdAt")
    with fail_if_exception("failed to parse createdAt datetime"):
        datetime.fromisoformat(created_at)
    expected_json = {
        "@type": "Task",
        "id": "some-id",
        "state": "CREATED",
        "name": "grouped_hello_world",
        "args": {"greeted": "world"},
        "retriesLeft": 3,
        "maxRetries": 3,
    }
    assert task_json == expected_json
    task_json["createdAt"] = created_at
    expected_json["createdAt"] = created_at
    amqp_task = Task.model_validate(task_json)
    assert amqp_task == task.model_validate(expected_json)


async def test_task_manager_enqueue_should_raise_for_existing_task(
    test_amqp_task_manager: TestableAMQPTaskManager, hello_world_task: Task
):
    # Given
    task = hello_world_task
    task_manager = test_amqp_task_manager
    await task_manager.save_task(task)
    await task_manager.enqueue(task)

    # When/Then
    with pytest.raises(TaskAlreadyQueued):
        await task_manager.enqueue(task)


async def test_task_manager_enqueue_should_raise_when_queue_full(
    fs_storage: TestableFSKeyValueStorage,
    management_client: AMQPManagementClient,
    rabbit_mq: str,
    hello_world_task: Task,
    test_async_app: AsyncApp,
):
    # Given
    app = AsyncApp("test-app", config=AsyncAppConfig(max_task_queue_size=1))
    app._registry = deepcopy(  # pylint: disable=protected-access
        test_async_app.registry
    )
    task_manager = TestableAMQPTaskManager(
        app, fs_storage, management_client, broker_url=rabbit_mq
    )
    task = hello_world_task
    other_task = safe_copy(task, update={"id": "some-other-id"})
    yet_another_task = safe_copy(task, update={"id": "yet-another-id"})
    await task_manager.save_task(task)
    await task_manager.save_task(other_task)
    await task_manager.save_task(yet_another_task)
    async with task_manager:
        await task_manager.enqueue(task)
        # When/Then
        with pytest.raises(TaskQueueIsFull):
            await task_manager.enqueue(other_task)
            # This one is needed for quorum queue which have an approximate definition
            # of the limit
            await task_manager.enqueue(yet_another_task)


async def test_task_manager_requeue(
    test_amqp_task_manager: TestableAMQPTaskManager, hello_world_task: Task
):
    # Given
    task_manager = test_amqp_task_manager
    task = hello_world_task
    task = hello_world_task.as_resolved(
        ProgressEvent(task_id=task.id, progress=0.5, created_at=datetime.now())
    )

    # When
    await task_manager.save_task(task)
    await task_manager.requeue(task)

    # Then
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
    with fail_if_exception("failed to parse createdAt datetime"):
        datetime.fromisoformat(created_at)
    expected_json = {
        "@type": "Task",
        "args": {"greeted": "world"},
        "id": "some-id",
        "name": "hello_world",
        "progress": 0.0,
        "state": "QUEUED",
    }
    assert task_json == expected_json
    task_json["createdAt"] = created_at
    amqp_task = Task.model_validate(task_json)
    expected = safe_copy(task, update={"progress": 0.0, "state": TaskState.QUEUED})
    assert amqp_task == expected


async def test_task_manager_should_consume(
    test_amqp_task_manager: TestableAMQPTaskManager,
):
    # Given
    task_manager = test_amqp_task_manager
    event = ProgressEvent(
        task_id="some-task-id", progress=1.0, created_at=datetime.now()
    )
    message = event.model_dump_json().encode()
    channel = task_manager.channel
    # When
    exchange = await channel.get_exchange("exchangeManagerEvents")
    message = Message(message, delivery_mode=DeliveryMode.PERSISTENT)
    await exchange.publish(message, "routingKeyManagerEvents")
    # Then
    consume_timeout = 10.0
    msg = f"Failed to consume error in less than {consume_timeout}"
    assert true_after(lambda: task_manager.consumed, after_s=consume_timeout), msg
    consumed = task_manager.consumed[0]
    assert consumed == event


@pytest.mark.parametrize("requeue", [True, False])
async def test_task_manager_cancel(
    test_amqp_task_manager: TestableAMQPTaskManager,
    requeue: bool,
    hello_world_task: Task,
):
    # Given
    task_manager = test_amqp_task_manager
    task = hello_world_task
    await task_manager.save_task(task)

    # When
    await task_manager.cancel(task_id=task.id, requeue=requeue)

    # Then
    channel = task_manager.channel
    res_queue = await channel.get_queue("WORKER_EVENT")
    receive_timeout = 1.0
    async with res_queue.iterator(timeout=receive_timeout) as messages:
        try:
            async for message in messages:
                cancel_evt_json = json.loads(message.body.decode())
                break
        except asyncio.TimeoutError:
            pytest.fail(f"Failed to receive result in less than {receive_timeout}")
    created_at = cancel_evt_json.pop("createdAt")
    assert isinstance(datetime.fromisoformat(created_at), datetime)
    expected_json = {"@type": "CancelEvent", "requeue": requeue, "taskId": "some-id"}
    assert cancel_evt_json == expected_json


async def test_get_health(
    test_amqp_task_manager: TestableAMQPTaskManager,
):
    # Given
    task_manager = test_amqp_task_manager
    # When
    async with task_manager:
        health = await task_manager.get_health()
    # Then
    assert health == {"storage": True, "amqp": True}


async def test_get_health_should_fail(
    monkeypatch,
    management_client: AMQPManagementClient,
    fs_storage: TestableFSKeyValueStorage,
    rabbit_mq: str,
    test_async_app: AsyncApp,
):
    # Given
    task_manager = TestableAMQPTaskManager(
        test_async_app, fs_storage, management_client, broker_url=rabbit_mq
    )

    def _failing_len(self):
        raise OSError("failing...")

    monkeypatch.setattr("icij_worker.task_storage.fs.SqliteDict.__len__", _failing_len)
    # When
    health = await task_manager.get_health()
    # Then
    assert health == {"storage": False, "amqp": False}
