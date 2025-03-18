# pylint: disable=redefined-outer-name
from datetime import datetime
from functools import partial

import itertools
import pytest

from icij_common.pydantic_utils import safe_copy
from icij_common.test_utils import async_true_after
from icij_worker import Task, TaskError, TaskState
from icij_worker.exceptions import UnregisteredTask
from icij_worker.objects import ProgressEvent, StacktraceItem
from icij_worker.utils.tests import MockManager, MockWorker


@pytest.fixture
def mock_manager(fs_storage_path, request) -> MockManager:
    app = getattr(request, "param", "test_async_app")
    app = request.getfixturevalue(app)
    task_manager = MockManager(app, fs_storage_path)
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
    await task_manager.save_task(task)
    event = ProgressEvent(task_id=task.id, progress=0.99, created_at=datetime.now())
    # When
    await worker.publish_event(event)
    # Then
    async with task_manager:
        consume_timeout = 20.0
        msg = f"Failed to consume error event in less than {consume_timeout}"

        async def _received_progress() -> bool:
            t = await task_manager.get_task(event.task_id)
            return t.progress > 0.0

        update = {
            "state": TaskState.RUNNING,
            "progress": 0.99,
            "max_retries": 1,
            "retries_left": 1,
        }
        expected_task = safe_copy(task, update=update)
        assert await async_true_after(_received_progress, after_s=consume_timeout), msg
        db_task = await task_manager.get_task(task_id=task.id)
        assert db_task == expected_task


@pytest.mark.parametrize(
    "retries_left,mock_worker",
    list(zip((0, 1), itertools.repeat(None))),
    indirect=["mock_worker"],
)
async def test_task_manager_should_consume_error_events(
    mock_manager: MockManager, mock_worker: MockWorker, retries_left
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
    )
    await task_manager.save_task(task)
    error = TaskError(
        name="error",
        message="with details",
        stacktrace=[StacktraceItem(name="SomeError", file="somefile", lineno=666)],
        cause="some cause",
    )
    # When
    await worker.publish_error_event(error, task, retries_left)
    # Then
    async with task_manager:
        consume_timeout = 20.0
        msg = f"Failed to consume error event in less than {consume_timeout}"

        async def _assert_has_state(state: TaskState) -> bool:
            saved = await task_manager.get_task(task_id=task.id)
            return saved.state is state

        if not retries_left:
            expected = partial(_assert_has_state, TaskState.ERROR)
            update = {
                "state": TaskState.ERROR,
                "retries_left": retries_left,
                "max_retries": 1,
            }
            expected_task = safe_copy(task, update=update)
        else:
            expected = partial(_assert_has_state, TaskState.QUEUED)
            update = {
                "state": TaskState.QUEUED,
                "retries_left": retries_left,
                "progress": 0.0,
                "max_retries": 1,
            }
            expected_task = safe_copy(task, update=update)
        assert await async_true_after(expected, after_s=consume_timeout), msg
        db_task = await task_manager.get_task(task_id=task.id)
        assert db_task == expected_task
        db_errors = await task_manager.get_task_errors(task.id)
        assert len(db_errors) == 1
        db_error = db_errors[0]
        assert db_error.task_id == task.id
        assert db_error.retries_left == retries_left
        assert db_error.error == error


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
    await task_manager.save_task(task)
    await task_manager.enqueue(task)
    await mock_worker.consume()
    worker.current = task
    await task_manager.save_task(task)

    # When
    await worker.publish_cancelled_event(requeue)

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
        assert isinstance(db_task.completed_at, expected_cancelled_at_type)
        db_task = db_task.model_dump()
        expected_task = expected_task.model_dump()
        db_task.pop("completed_at", None)
        expected_task.pop("completed_at", None)
        assert db_task == expected_task


async def test_save_task(mock_manager: MockManager):
    # Given
    task_manager = mock_manager
    task = Task(
        id="some-id",
        name="often_retriable",
        args=dict(),
        created_at=datetime.now(),
        state=TaskState.CREATED,
    )

    # When
    await task_manager.save_task(task)
    db_task = await task_manager.get_task(task_id=task.id)

    # Then
    expected_task = safe_copy(task, update={"max_retries": 666, "retries_left": 666})
    assert db_task == expected_task


async def test_save_task_to_group(
    mock_manager: MockManager, grouped_hello_world_task: Task
):
    # Given
    task_manager = mock_manager
    task = grouped_hello_world_task

    # When
    await task_manager.save_task(task)
    db_task = await task_manager.get_task(task_id=task.id)

    # Then
    expected_task = safe_copy(task, update={"max_retries": 3, "retries_left": 3})
    assert db_task == expected_task
    ns = await task_manager.get_task_group(task.id)
    assert ns == "hello"


async def test_save_unknown_task_should_raise_unregistered_task_error(
    mock_manager: MockManager,
):
    # Given
    task_manager = mock_manager

    task = Task(
        id="some-id",
        name="i_dont_exist",
        args=dict(),
        created_at=datetime.now(),
        state=TaskState.CREATED,
    )
    # When/Then
    with pytest.raises(UnregisteredTask):
        await task_manager.save_task(task)
