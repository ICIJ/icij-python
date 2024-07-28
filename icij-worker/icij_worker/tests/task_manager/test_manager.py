# pylint: disable=redefined-outer-name
import itertools
from datetime import datetime
from functools import partial
from typing import Optional

import pytest

from icij_common.pydantic_utils import safe_copy
from icij_common.test_utils import async_true_after
from icij_worker import Task, TaskError, TaskState
from icij_worker.objects import CancelEvent, ProgressEvent, StacktraceItem
from icij_worker.utils.tests import MockManager, MockWorker


@pytest.fixture
def mock_manager(mock_db, request) -> MockManager:
    app = getattr(request, "param", "test_async_app")
    app = request.getfixturevalue(app)
    task_manager = MockManager(app, mock_db)
    return task_manager


async def test_consume_progress_event(
    mock_manager: MockManager, mock_worker: MockWorker
):
    # Given
    task_manager = mock_manager
    worker = mock_worker
    task_name = "sleep_for"
    task = Task(
        id="some-id",
        name=task_name,
        created_at=datetime.now(),
        state=TaskState.RUNNING,
        progress=0.0,
    )
    await task_manager.save_task(task, namespace=None)
    event = ProgressEvent(task_id=task.id, progress=0.99)
    # When
    await worker.publish_event(event)
    # Then
    async with task_manager:
        consume_timeout = 20.0
        msg = f"Failed to consume error event in less than {consume_timeout}"

        async def _received_progress() -> bool:
            task = await task_manager.get_task(event.task_id)
            return task.progress > 0.0

        update = {"state": TaskState.RUNNING, "progress": 0.99}
        expected_task = safe_copy(task, update=update)
        assert await async_true_after(_received_progress, after_s=consume_timeout), msg
        db_task = await task_manager.get_task(task_id=task.id)
        assert db_task == expected_task


@pytest.mark.parametrize(
    "retries,mock_worker",
    list(zip((0, 1), itertools.repeat(None))),
    indirect=["mock_worker"],
)
async def test_task_manager_should_consume_error_events(
    mock_manager: MockManager, mock_worker: MockWorker, retries: Optional[int]
):
    # Given
    task_manager = mock_manager
    worker = mock_worker
    task_name = "sleep_for"
    max_retries = task_manager.app.registry[task_name].max_retries
    task = Task(
        id="some-id",
        name=task_name,
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
    # When
    await worker.publish_error_event(error, task, retries)
    # Then
    async with task_manager:
        consume_timeout = 20.0
        msg = f"Failed to consume error event in less than {consume_timeout}"

        async def _assert_has_state(state: TaskState) -> bool:
            saved = await task_manager.get_task(task_id=task.id)
            return saved.state is state

        if retries == max_retries:
            expected = partial(_assert_has_state, TaskState.ERROR)
            update = {"state": TaskState.ERROR, "retries": retries}
            expected_task = safe_copy(task, update=update)
        else:
            expected = partial(_assert_has_state, TaskState.QUEUED)
            update = {"state": TaskState.QUEUED, "retries": retries, "progress": 0.0}
            expected_task = safe_copy(task, update=update)
        assert await async_true_after(expected, after_s=consume_timeout), msg
        db_task = await task_manager.get_task(task_id=task.id)
        assert db_task == expected_task
        errors = await task_manager.get_task_errors(task.id)
        assert errors == [error]


@pytest.mark.parametrize(
    "requeue,mock_manager",
    list(itertools.product((True, False), ("test_async_app", "test_async_app_late"))),
    indirect=["mock_manager"],
)
async def test_consume_cancelled_event(
    mock_manager: MockManager,
    mock_worker: MockWorker,
    requeue: bool,
    hello_world_task: Task,
):
    # Given
    task_manager = mock_manager
    worker = mock_worker
    task = hello_world_task
    await task_manager.save_task(task, None)
    await task_manager.enqueue(task, None)
    await mock_worker.consume()
    worker.current = task
    await task_manager.save_task(task, namespace=None)

    # When
    cancel_event = CancelEvent.from_task(task, requeue=requeue)
    await worker.publish_cancelled_event(cancel_event=cancel_event)

    # Then
    async with task_manager:
        consume_timeout = 2.0
        msg = f"Failed to consume error event in less than {consume_timeout}"

        async def _assert_has_state(state: TaskState) -> bool:
            saved = await task_manager.get_task(task_id=task.id)
            return saved.state is state

        if requeue:
            expected = partial(_assert_has_state, TaskState.QUEUED)
            expected_task = safe_copy(
                task, update={"state": TaskState.QUEUED, "progress": 0.0}
            )
            expected_cancelled_at_type = type(None)
        else:
            expected = partial(_assert_has_state, TaskState.CANCELLED)
            expected_task = safe_copy(
                task, update={"state": TaskState.CANCELLED, "progress": 0.0}
            )
            expected_cancelled_at_type = datetime
        assert await async_true_after(expected, after_s=consume_timeout), msg
        db_task = await task_manager.get_task(task_id=task.id)
        assert isinstance(db_task.cancelled_at, expected_cancelled_at_type)
        db_task = db_task.dict(exclude_unset=True, exclude_none=True)
        expected_task = expected_task.dict(exclude_unset=True, exclude_none=True)
        db_task.pop("cancelled_at", None)
        expected_task.pop("cancelled_at", None)
        assert db_task == expected_task