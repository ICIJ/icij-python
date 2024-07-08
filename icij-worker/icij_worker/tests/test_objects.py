from __future__ import annotations

from datetime import datetime
from typing import Optional

import pytest

from icij_worker.objects import (
    ErrorEvent,
    PRECEDENCE,
    ProgressEvent,
    READY_STATES,
    StacktraceItem,
    Task,
    TaskError,
    TaskEvent,
    TaskStatus,
    TaskUpdate,
)

_CREATED_AT = datetime.now()
_ERROR_OCCURRED_AT = datetime.now()
_ANOTHER_TIME = datetime.now()


def test_precedence_sanity_check():
    assert len(PRECEDENCE) == len(TaskStatus)


@pytest.mark.parametrize(
    "task,event,expected_update",
    [
        # Update the status
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.CREATED,
                created_at=_CREATED_AT,
            ),
            ProgressEvent(task_id="task-id", status=TaskStatus.RUNNING, progress=0.0),
            TaskUpdate(task_id="task-id", status=TaskStatus.RUNNING, progress=0.0),
        ),
        # Status is updated when not in a final state
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.CREATED,
                created_at=_CREATED_AT,
            ),
            ProgressEvent(task_id="task-id", status=TaskStatus.RUNNING, progress=0.0),
            TaskUpdate(task_id="task-id", status=TaskStatus.RUNNING, progress=0.0),
        ),
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.QUEUED,
                created_at=_CREATED_AT,
            ),
            ProgressEvent(task_id="task-id", progress=50.0),
            TaskUpdate(task_id="task-id", progress=50.0),
        ),
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.RUNNING,
                created_at=_CREATED_AT,
            ),
            ProgressEvent(task_id="task-id", status=TaskStatus.DONE, progress=100),
            TaskUpdate(task_id="task-id", status=TaskStatus.DONE, progress=100),
        ),
        # Update error + retries
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.CREATED,
                created_at=_CREATED_AT,
            ),
            ErrorEvent(
                task_id="task-id",
                retries=4,
                error=TaskError(
                    id="error-id",
                    task_id="task-id",
                    name="some-error",
                    message="some message",
                    stacktrace=[
                        StacktraceItem(
                            name="SomeError", file="some details", lineno=666
                        )
                    ],
                    occurred_at=_ERROR_OCCURRED_AT,
                ),
                status=TaskStatus.QUEUED,
            ),
            TaskUpdate(
                task_id="task-id",
                retries=4,
                error=TaskError(
                    id="error-id",
                    task_id="task-id",
                    name="some-error",
                    message="some message",
                    stacktrace=[
                        StacktraceItem(
                            name="SomeError", file="some details", lineno=666
                        )
                    ],
                    occurred_at=_ERROR_OCCURRED_AT,
                ),
                status=TaskStatus.QUEUED,
            ),
        ),
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.CREATED,
                created_at=_CREATED_AT,
            ),
            ErrorEvent(
                task_id="task-id",
                retries=None,
                error=TaskError(
                    id="error-id",
                    task_id="task-id",
                    name="some-error",
                    message="some message",
                    stacktrace=[
                        StacktraceItem(
                            name="SomeError", file="some details", lineno=666
                        )
                    ],
                    occurred_at=_ERROR_OCCURRED_AT,
                ),
                status=TaskStatus.ERROR,
            ),
            TaskUpdate(
                task_id="task-id",
                retries=None,
                error=TaskError(
                    id="error-id",
                    task_id="task-id",
                    name="some-error",
                    message="some message",
                    stacktrace=[
                        StacktraceItem(
                            name="SomeError", file="some details", lineno=666
                        )
                    ],
                    occurred_at=_ERROR_OCCURRED_AT,
                ),
                status=TaskStatus.ERROR,
            ),
        ),
        # Completed at is not updated
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.DONE,
                created_at=_CREATED_AT,
                completed_at=_CREATED_AT,
            ),
            ProgressEvent(
                task_id="task-id",
                status=TaskStatus.DONE,
                progress=100,
                completed_at=_ANOTHER_TIME,
            ),
            None,
        ),
        # The task is on a final state, nothing is updated
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.DONE,
                created_at=_CREATED_AT,
            ),
            ErrorEvent(
                task_id="task-id",
                status=TaskStatus.ERROR,
                retries=4,
                error=TaskError(
                    id="error-id",
                    task_id="task-id",
                    name="some-error",
                    message="some message",
                    stacktrace=[
                        StacktraceItem(
                            name="SomeError", file="some details", lineno=666
                        )
                    ],
                    occurred_at=_ERROR_OCCURRED_AT,
                ),
            ),
            None,
        ),
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.ERROR,
                created_at=_CREATED_AT,
            ),
            ProgressEvent(task_id="task-id", status=TaskStatus.DONE, progress=100),
            None,
        ),
        (
            Task(
                id="task-id",
                type="hello_world",
                status=TaskStatus.CANCELLED,
                created_at=_CREATED_AT,
            ),
            ProgressEvent(task_id="task-id", progress=50.0),
            None,
        ),
    ],
)
def test_resolve_event(
    task: Task, event: TaskEvent, expected_update: Optional[TaskEvent]
):
    # When
    updated = task.resolve_event(event)
    # Then
    assert updated == expected_update


_UNCHANGED = [(s, s, s) for s in TaskStatus]
_DONE_IS_DONE = [
    (TaskStatus.DONE, s, TaskStatus.DONE) for s in TaskStatus if s != TaskStatus.DONE
]
_SHOULD_CANCEL_UNREADY = [
    (s, TaskStatus.CANCELLED, TaskStatus.CANCELLED)
    for s in TaskStatus
    if s not in READY_STATES
]


@pytest.mark.parametrize(
    "stored,event_status,expected_resolved",
    _UNCHANGED
    + [
        (TaskStatus.CREATED, TaskStatus.QUEUED, TaskStatus.QUEUED),
        # Store as queue, receiving a late creation event, the task stays queue
        (TaskStatus.QUEUED, TaskStatus.CREATED, TaskStatus.QUEUED),
        (TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.RUNNING),
        (TaskStatus.QUEUED, TaskStatus.ERROR, TaskStatus.ERROR),
        (TaskStatus.QUEUED, TaskStatus.DONE, TaskStatus.DONE),
        # Late retry notice but the task is already failed
        (TaskStatus.ERROR, TaskStatus.QUEUED, TaskStatus.ERROR),
    ]
    + _DONE_IS_DONE
    + _SHOULD_CANCEL_UNREADY,
)
def test_resolve_status(
    stored: TaskStatus, event_status: TaskStatus, expected_resolved: TaskStatus
):
    # Given
    task = Task(
        id="some_id", status=stored, type="some-type", created_at=datetime.now()
    )
    update = TaskUpdate(task_id=task.id, status=event_status)
    # When
    resolved = TaskStatus.resolve_event_status(task, update)
    # Then
    assert resolved == expected_resolved


@pytest.mark.parametrize(
    "task_retries,event_retries,expected",
    [
        # Delayed queued event
        (None, None, TaskStatus.RUNNING),
        (1, None, TaskStatus.RUNNING),
        (2, 1, TaskStatus.RUNNING),
        # The event is signaling a retry
        (None, 1, TaskStatus.QUEUED),
        (1, 2, TaskStatus.QUEUED),
    ],
)
def test_resolve_running_queued_status(
    task_retries: Optional[int], event_retries: Optional[int], expected: TaskStatus
):
    # Given
    task = Task(
        id="some_id",
        status=TaskStatus.RUNNING,
        type="some-type",
        created_at=datetime.now(),
        retries=task_retries,
    )
    updated = TaskUpdate(
        task_id=task.id, status=TaskStatus.QUEUED, retries=event_retries
    )
    # When
    resolved = TaskStatus.resolve_event_status(task, updated)
    # Then
    assert resolved == expected
