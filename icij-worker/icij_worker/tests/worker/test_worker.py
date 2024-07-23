## pylint: disable=redefined-outer-name,multiple-statements
from __future__ import annotations

import asyncio
import functools
from datetime import datetime
from pathlib import Path
from signal import Signals
from typing import Any, Dict, Optional
from unittest.mock import patch

import pytest

from icij_common.pydantic_utils import safe_copy
from icij_common.test_utils import async_true_after, fail_if_exception
from icij_worker import Task, TaskError, TaskResult, TaskState
from icij_worker.exceptions import TaskAlreadyCancelled
from icij_worker.objects import ErrorEvent, ProgressEvent
from icij_worker.utils.tests import MockManager, MockWorker
from icij_worker.worker.worker import add_missing_args


@pytest.fixture(
    scope="function",
    params=["test_failing_async_app_late_ack", "test_failing_async_app"],
)
def mock_failing_worker(mock_db: Path, request) -> MockWorker:
    app = request.getfixturevalue(request.param)
    worker = MockWorker(
        app,
        "test-worker",
        namespace=None,
        db_path=mock_db,
        task_queue_poll_interval_s=0.1,
    )
    return worker


_TASK_DB = dict()


async def test_work_once_asyncio_task(mock_worker: MockWorker):
    # Given
    worker = mock_worker
    task_manager = MockManager(worker.db_path, max_task_queue_size=10)
    created_at = datetime.now()
    task = Task(
        id="some-id",
        name="hello_world",
        created_at=created_at,
        state=TaskState.CREATED,
        arguments={"greeted": "world"},
    )

    # When
    await task_manager.enqueue(task, namespace=None)
    await worker.work_once()
    saved_task = await task_manager.get_task(task_id=task.id)
    saved_errors = await task_manager.get_task_errors(task_id=task.id)
    saved_result = await task_manager.get_task_result(task_id=task.id)

    # Then
    assert not saved_errors

    expected_task = Task(
        id="some-id",
        name="hello_world",
        progress=1.0,
        created_at=created_at,
        state=TaskState.DONE,
        arguments={"greeted": "world"},
    )
    completed_at = saved_task.completed_at
    assert isinstance(saved_task.completed_at, datetime)
    saved_task = saved_task.dict(by_alias=True)
    saved_task.pop("completedAt")
    expected_task = expected_task.dict(by_alias=True)
    expected_task.pop("completedAt")
    assert saved_task == expected_task
    expected_events = [
        ProgressEvent(task_id="some-id", progress=0.0),
        ProgressEvent(task_id="some-id", progress=0.1),
        ProgressEvent(task_id="some-id", progress=0.99),
        ProgressEvent(task_id="some-id", progress=1.0),
    ]
    assert worker.published_events == expected_events
    expected_result = TaskResult(
        task_id="some-id", result="Hello world !", completed_at=completed_at
    )
    assert saved_result == expected_result


async def test_work_once_run_sync_task(mock_worker: MockWorker):
    # Given
    worker = mock_worker
    task_manager = MockManager(worker.db_path, max_task_queue_size=10)
    created_at = datetime.now()
    task = Task(
        id="some-id",
        name="hello_world_sync",
        created_at=created_at,
        state=TaskState.CREATED,
        arguments={"greeted": "world"},
    )

    # When
    await task_manager.enqueue(task, namespace=None)
    await worker.work_once()
    saved_task = await task_manager.get_task(task_id=task.id)
    saved_result = await task_manager.get_task_result(task_id=task.id)
    saved_errors = await task_manager.get_task_errors(task_id=task.id)

    # Then
    assert not saved_errors

    expected_task = Task(
        id="some-id",
        name="hello_world_sync",
        progress=1.0,
        created_at=created_at,
        state=TaskState.DONE,
        arguments={"greeted": "world"},
    )
    completed_at = saved_task.completed_at
    assert isinstance(saved_task.completed_at, datetime)
    saved_task = saved_task.dict(by_alias=True)
    saved_task.pop("completedAt")
    expected_task = expected_task.dict(by_alias=True)
    expected_task.pop("completedAt")
    assert saved_task == expected_task
    expected_events = [
        ProgressEvent(task_id="some-id", progress=0.0),
        ProgressEvent(task_id="some-id", progress=1.0),
    ]
    assert worker.published_events == expected_events

    expected_result = TaskResult(
        task_id="some-id", result="Hello world !", completed_at=completed_at
    )
    assert saved_result == expected_result


async def test_task_wrapper_should_recover_from_recoverable_error(
    mock_failing_worker: MockWorker,
):
    # Given
    worker = mock_failing_worker
    task_manager = MockManager(worker.db_path, max_task_queue_size=10)
    created_at = datetime.now()
    task = Task(
        id="some-id",
        name="recovering_task",
        created_at=created_at,
        state=TaskState.CREATED,
    )

    # When/Then
    task = await task_manager.enqueue(task, namespace=None)
    assert task.state is TaskState.QUEUED
    await worker.work_once()
    retried_task = await task_manager.get_task(task_id=task.id)

    assert retried_task.state is TaskState.QUEUED
    assert retried_task.retries == 1

    await worker.work_once()
    saved_task = await task_manager.get_task(task_id=task.id)
    saved_result = await task_manager.get_task_result(task_id=task.id)
    saved_errors = await task_manager.get_task_errors(task_id=task.id)

    # Then
    expected_task = Task(
        id="some-id",
        name="recovering_task",
        progress=1.0,
        created_at=created_at,
        state=TaskState.DONE,
        retries=1,
    )
    completed_at = saved_task.completed_at
    assert isinstance(completed_at, datetime)
    saved_task = saved_task.dict(by_alias=True)
    saved_task.pop("completedAt")
    expected_task = expected_task.dict(by_alias=True)
    expected_task.pop("completedAt")
    assert saved_task == expected_task

    # No error should be saved
    assert not saved_errors
    # However we expect the worker to have logged them somewhere in the events
    expected_result = TaskResult(
        task_id="some-id",
        result="i told you i could recover",
        completed_at=completed_at,
    )
    assert saved_result == expected_result

    expected_events = [
        ProgressEvent(task_id="some-id", progress=0.0),
        ErrorEvent(
            task_id="some-id",
            state=TaskState.QUEUED,
            retries=1,
            error=TaskError(
                id="",
                task_id="some-id",
                name="Recoverable",
                message="i can recover from this",
                occurred_at=datetime.now(),
            ),
        ),
        ProgressEvent(task_id="some-id", progress=0.0),
        ProgressEvent(task_id="some-id", progress=0.0),
        ProgressEvent(task_id="some-id", progress=1.0),
    ]
    events = [e.dict(by_alias=True) for e in worker.published_events]
    event_errors = [e.pop("error", None) for e in events]
    event_error_names = [e["name"] if e is not None else e for e in event_errors]
    assert event_error_names == [None, "Recoverable", None, None, None]
    event_error_messages = [e["message"] if e is not None else e for e in event_errors]
    assert event_error_messages == [None, "i can recover from this", None, None, None]
    event_error_occurred_at = [
        isinstance(e["occurredAt"], datetime) if e else e for e in event_errors
    ]
    assert event_error_occurred_at == [None, True, None, None, None]
    expected_events = [e.dict(by_alias=True) for e in expected_events]
    for e in expected_events:
        e.pop("error", None)
    assert events == expected_events


async def test_task_wrapper_should_handle_fatal_error(mock_failing_worker: MockWorker):
    # Given
    worker = mock_failing_worker
    task_manager = MockManager(worker.db_path, max_task_queue_size=10)
    created_at = datetime.now()
    task = Task(
        id="some-id",
        name="fatal_error_task",
        created_at=created_at,
        state=TaskState.CREATED,
    )

    # When
    await task_manager.enqueue(task, namespace=None)
    await worker.work_once()
    saved_errors = await task_manager.get_task_errors(task_id="some-id")
    saved_task = await task_manager.get_task(task_id="some-id")

    # Then
    expected_task = Task(
        id="some-id",
        name="fatal_error_task",
        progress=0.1,
        created_at=created_at,
        state=TaskState.ERROR,
    )
    assert saved_task == expected_task

    assert len(saved_errors) == 1
    saved_error = saved_errors[0]
    assert saved_error.name == "ValueError"
    assert isinstance(saved_error.occurred_at, datetime)

    expected_events = [
        ProgressEvent(task_id="some-id", progress=0.0),
        ProgressEvent(task_id="some-id", progress=0.1),
        ErrorEvent(
            task_id="some-id",
            state=TaskState.ERROR,
            error=TaskError(
                id="",
                task_id="some-id",
                name="ValueError",
                message="this is fatal",
                occurred_at=datetime.now(),
            ),
        ),
    ]
    assert len(worker.published_events) == len(expected_events)
    assert worker.published_events[:-1] == expected_events[:-1]

    error_event = worker.published_events[-1]
    expected_error_event = expected_events[-1]
    assert isinstance(error_event.error, TaskError)
    assert error_event.error.name == "ValueError"
    assert error_event.error.message == "this is fatal"
    assert isinstance(error_event.error.occurred_at, datetime)
    error_event = error_event.dict(by_alias=True)
    error_event.pop("error")
    expected_error_event = expected_error_event.dict(by_alias=True)
    expected_error_event.pop("error")
    assert error_event == expected_error_event


async def test_task_wrapper_should_handle_unregistered_task(mock_worker: MockWorker):
    # Given
    worker = mock_worker
    task_manager = MockManager(worker.db_path, max_task_queue_size=10)
    created_at = datetime.now()
    task = Task(
        id="some-id",
        name="i_dont_exist",
        created_at=created_at,
        state=TaskState.CREATED,
    )

    # When
    await task_manager.enqueue(task, namespace=None)
    await worker.work_once()
    saved_task = await task_manager.get_task(task_id="some-id")
    saved_errors = await task_manager.get_task_errors(task_id="some-id")

    # Then
    expected_task = Task(
        id="some-id",
        name="i_dont_exist",
        progress=0.0,
        created_at=created_at,
        state=TaskState.ERROR,
    )
    assert saved_task == expected_task

    assert len(saved_errors) == 1
    saved_error = saved_errors[0]
    assert saved_error.name == "UnregisteredTask"
    assert isinstance(saved_error.occurred_at, datetime)

    expected_events = [
        ProgressEvent(task_id="some-id", progress=0.0),
        ErrorEvent(
            task_id="some-id",
            state=TaskState.ERROR,
            error=TaskError(
                id="error-id",
                task_id="some-id",
                name="UnregisteredTask",
                message="",
                occurred_at=datetime.now(),
            ),
        ),
    ]
    assert len(worker.published_events) == len(expected_events)
    assert worker.published_events[:-1] == expected_events[:-1]

    error_event = worker.published_events[-1]
    expected_error_event = expected_events[-1]
    assert isinstance(error_event.error, TaskError)
    assert error_event.error.name == "UnregisteredTask"
    assert error_event.error.message.startswith('UnregisteredTask task "i_dont_exist"')
    assert isinstance(error_event.error.occurred_at, datetime)
    error_event = error_event.dict(by_alias=True)
    error_event.pop("error")
    expected_error_event = expected_error_event.dict(by_alias=True)
    expected_error_event.pop("error")
    assert error_event == expected_error_event


async def test_work_once_should_not_run_already_cancelled_task(mock_worker: MockWorker):
    # Given
    worker = mock_worker
    task_manager = MockManager(worker.db_path, max_task_queue_size=10)
    created_at = datetime.now()
    task = Task(
        id="some-id",
        name="fatal_error_task",
        created_at=created_at,
        state=TaskState.CREATED,
    )

    # When
    def _cancel(w: MockWorker):
        updated = safe_copy(task, update={"state": TaskState.CANCELLED})
        w._current = updated  # pylint: disable=protected-access
        return updated

    await task_manager.enqueue(task, namespace=None)
    # We mock the fact the task is still received but cancelled right after
    with pytest.raises(TaskAlreadyCancelled):
        with patch.object(
            worker, "consume", side_effect=functools.partial(_cancel, worker)
        ):
            await worker.work_once()


@pytest.mark.parametrize("requeue", [True, False])
async def test_cancel_running_task(mock_worker: MockWorker, requeue: bool):
    # pylint: disable=protected-access
    # Given
    worker = mock_worker
    task_manager = MockManager(worker.db_path, max_task_queue_size=10)
    created_at = datetime.now()
    duration = 10
    task = Task(
        id="some-id",
        name="sleep_for",
        created_at=created_at,
        state=TaskState.CREATED,
        arguments={"duration": duration},
    )

    # When
    async with worker:
        t = asyncio.create_task(worker.work_once())
        worker._work_once_task = t

        await task_manager.enqueue(task, namespace=None)
        after_s = 2.0

        async def _assert_has_state(state: TaskState) -> bool:
            saved = await task_manager.get_task(task_id=task.id)
            return saved.state is state

        failure_msg = f"Failed to run task in less than {after_s}"
        assert await async_true_after(
            functools.partial(_assert_has_state, TaskState.RUNNING), after_s=after_s
        ), failure_msg
        await task_manager.cancel(task_id=task.id, requeue=requeue)

        done, _ = await asyncio.wait([t], timeout=after_s)
        if not done:
            failure_msg = f"Failed to finish work in less than {after_s}"
            pytest.fail(failure_msg)

        expected_state = TaskState.QUEUED if requeue else TaskState.CANCELLED
        failure_msg = f"Failed to cancel task in less than {after_s}"
        assert await async_true_after(
            functools.partial(_assert_has_state, expected_state), after_s=after_s
        ), failure_msg


@pytest.mark.parametrize("signal", [Signals.SIGINT, Signals.SIGTERM])
async def test_worker_should_terminate_task_and_cancellation_event_loops(
    mock_worker: MockWorker, signal: Signals
):
    # pylint: disable=protected-access
    # Given
    worker = mock_worker
    task_manager = MockManager(worker.db_path, max_task_queue_size=10)
    created_at = datetime.now()
    duration = 100
    task = Task(
        id="some-id",
        name="sleep_for",
        created_at=created_at,
        state=TaskState.CREATED,
        arguments={"duration": duration},
    )

    # When
    await task_manager.enqueue(task, namespace=None)
    asyncio_tasks = set()
    async with worker:
        work_forever_task = asyncio.create_task(worker.work_forever_async())
        work_forever_task.add_done_callback(asyncio_tasks.discard)
        worker._work_forever_task = work_forever_task
        asyncio_tasks.add(work_forever_task)

        async def _assert_running() -> bool:
            saved = await task_manager.get_task(task_id=task.id)
            return saved.state is TaskState.RUNNING

        after_s = 2.0
        failure_msg = f"Failed to run task in less than {after_s}"
        assert await async_true_after(_assert_running, after_s=after_s), failure_msg
        await worker.signal_handler(signal, graceful=True)

        # Then
        async def _cancelled_watch_loop() -> bool:
            return worker.watch_cancelled_task is None

        failure_msg = f"worker failed to exit cancel events loop in less than {after_s}"
        assert await async_true_after(
            _cancelled_watch_loop, after_s=after_s
        ), failure_msg

        async def _successful_exit() -> bool:
            return worker.successful_exit

        failure_msg = f"worker failed to exit working loop in less than {after_s}"
        assert await async_true_after(_successful_exit, after_s=after_s), failure_msg


@pytest.mark.parametrize(
    "provided_args,kwargs,maybe_output",
    [
        ({}, {}, None),
        ({"a": "a"}, {}, None),
        ({"a": "a"}, {"b": "b"}, "a-b-c"),
        ({"a": "a", "b": "b"}, {"c": "not-your-average-c"}, "a-b-not-your-average-c"),
    ],
)
def test_add_missing_args(
    provided_args: Dict[str, Any],
    kwargs: Dict[str, Any],
    maybe_output: Optional[str],
):
    # Given
    def fn(a: str, b: str, c: str = "c") -> str:
        return f"{a}-{b}-{c}"

    # When
    all_args = add_missing_args(fn, arguments=provided_args, **kwargs)
    # Then
    if maybe_output is not None:
        output = fn(**all_args)
        assert output == maybe_output
    else:
        with pytest.raises(
            TypeError,
        ):
            fn(**all_args)


@pytest.mark.pull("146")
async def test_worker_should_keep_working_on_fatal_error_in_task_codebase(
    mock_failing_worker: MockWorker,
):
    # Given
    worker = mock_failing_worker
    task_manager = MockManager(worker.db_path, max_task_queue_size=10)
    created_at = datetime.now()
    task = Task(
        id="some-id",
        name="fatal_error_task",
        created_at=created_at,
        state=TaskState.CREATED,
    )

    # When/Then
    await task_manager.enqueue(task, namespace=None)
    with fail_if_exception("fatal_error_task"):
        await worker.work_once()


async def test_worker_should_stop_working_on_fatal_error_in_worker_codebase(
    mock_failing_worker: MockWorker,
):
    # Given
    worker = mock_failing_worker
    task_manager = MockManager(worker.db_path, max_task_queue_size=10)
    created_at = datetime.now()
    task = Task(
        id="some-id",
        name="fatal_error_task",
        created_at=created_at,
        state=TaskState.CREATED,
    )

    # When/Then
    await task_manager.enqueue(task, namespace=None)
    with patch.object(worker, "_consume") as mocked_consume:

        class _FatalError(Exception): ...

        async def _fatal_error_during_consuming():
            raise _FatalError("i'm fatal")

        mocked_consume.side_effect = _fatal_error_during_consuming
        with pytest.raises(_FatalError):
            await worker.work_once()
