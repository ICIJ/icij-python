from __future__ import annotations

from datetime import datetime
from typing import Optional

import pytest

from icij_worker.objects import (
    CancelEvent,
    CancelledEvent,
    ErrorEvent,
    PRECEDENCE,
    ProgressEvent,
    READY_STATES,
    StacktraceItem,
    Task,
    TaskError,
    TaskEvent,
    TaskState,
    TaskUpdate,
)

_CREATED_AT = datetime.now()
_ERROR_OCCURRED_AT = datetime.now()
_ANOTHER_TIME = datetime.now()


def test_precedence_sanity_check():
    assert len(PRECEDENCE) == len(TaskState)


@pytest.mark.parametrize(
    "task,event,expected_update",
    [
        # Update the state
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.CREATED,
                created_at=_CREATED_AT,
            ),
            ProgressEvent(task_id="task-id", progress=0.0),
            TaskUpdate(state=TaskState.RUNNING, progress=0.0),
        ),
        # State is updated when not in a final state
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.CREATED,
                created_at=_CREATED_AT,
            ),
            ProgressEvent(task_id="task-id", progress=0.0),
            TaskUpdate(state=TaskState.RUNNING, progress=0.0),
        ),
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.QUEUED,
                created_at=_CREATED_AT,
            ),
            ProgressEvent(task_id="task-id", progress=0.5),
            TaskUpdate(state=TaskState.RUNNING, progress=0.5),
        ),
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.RUNNING,
                created_at=_CREATED_AT,
            ),
            ProgressEvent(task_id="task-id", progress=1.0),
            TaskUpdate(state=TaskState.RUNNING, progress=1.0),
        ),
        # Update error + retries
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.CREATED,
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
                state=TaskState.QUEUED,
            ),
            TaskUpdate(retries=4, state=TaskState.QUEUED, progress=0.0),
        ),
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.CREATED,
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
                state=TaskState.ERROR,
            ),
            TaskUpdate(retries=None, state=TaskState.ERROR),
        ),
        # Completed at is not updated
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.DONE,
                created_at=_CREATED_AT,
                completed_at=_CREATED_AT,
            ),
            ProgressEvent(task_id="task-id", progress=1.0),
            None,
        ),
        # The task is on a final state, nothing is updated
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.DONE,
                created_at=_CREATED_AT,
            ),
            ErrorEvent(
                task_id="task-id",
                state=TaskState.ERROR,
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
                name="hello_world",
                state=TaskState.ERROR,
                created_at=_CREATED_AT,
            ),
            ProgressEvent(task_id="task-id", progress=1.0),
            None,
        ),
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.CANCELLED,
                created_at=_CREATED_AT,
            ),
            ProgressEvent(task_id="task-id", progress=0.5),
            None,
        ),
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.RUNNING,
                created_at=_CREATED_AT,
            ),
            CancelEvent(task_id="task-id", requeue=True, cancelled_at=_ANOTHER_TIME),
            TaskUpdate(
                cancelled_at=_ANOTHER_TIME, state=TaskState.QUEUED, progress=0.0
            ),
        ),
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.RUNNING,
                created_at=_CREATED_AT,
            ),
            CancelEvent(task_id="task-id", requeue=False, cancelled_at=_ANOTHER_TIME),
            TaskUpdate(cancelled_at=_ANOTHER_TIME, state=TaskState.CANCELLED),
        ),
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.RUNNING,
                created_at=_CREATED_AT,
            ),
            CancelledEvent(task_id="task-id", requeue=True, cancelled_at=_ANOTHER_TIME),
            TaskUpdate(
                cancelled_at=_ANOTHER_TIME, state=TaskState.QUEUED, progress=0.0
            ),
        ),
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.RUNNING,
                created_at=_CREATED_AT,
            ),
            CancelledEvent(
                task_id="task-id", requeue=False, cancelled_at=_ANOTHER_TIME
            ),
            TaskUpdate(cancelled_at=_ANOTHER_TIME, state=TaskState.CANCELLED),
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


_UNCHANGED = [(s, s, s) for s in TaskState]
_DONE_IS_DONE = [
    (TaskState.DONE, s, TaskState.DONE) for s in TaskState if s != TaskState.DONE
]
_SHOULD_CANCEL_UNREADY = [
    (s, TaskState.CANCELLED, TaskState.CANCELLED)
    for s in TaskState
    if s not in READY_STATES
]


@pytest.mark.parametrize(
    "stored,event_state,expected_resolved",
    _UNCHANGED
    + [
        (TaskState.CREATED, TaskState.QUEUED, TaskState.QUEUED),
        # Store as queue, receiving a late creation event, the task stays queue
        (TaskState.QUEUED, TaskState.CREATED, TaskState.QUEUED),
        (TaskState.QUEUED, TaskState.RUNNING, TaskState.RUNNING),
        (TaskState.QUEUED, TaskState.ERROR, TaskState.ERROR),
        (TaskState.QUEUED, TaskState.DONE, TaskState.DONE),
        # Late retry notice but the task is already failed
        (TaskState.ERROR, TaskState.QUEUED, TaskState.ERROR),
    ]
    + _DONE_IS_DONE
    + _SHOULD_CANCEL_UNREADY,
)
def test_resolve_update_state(
    stored: TaskState, event_state: TaskState, expected_resolved: TaskState
):
    # Given
    task = Task(id="some_id", state=stored, name="some-type", created_at=datetime.now())
    update = TaskUpdate(state=event_state)
    # When
    resolved = TaskState.resolve_update_state(task, update)
    # Then
    assert resolved == expected_resolved


@pytest.mark.parametrize(
    "task_retries,event_retries,expected",
    [
        # Delayed queued event
        (None, None, TaskState.RUNNING),
        (1, None, TaskState.RUNNING),
        (2, 1, TaskState.RUNNING),
        # The event is signaling a retry
        (None, 1, TaskState.QUEUED),
        (1, 2, TaskState.QUEUED),
    ],
)
def test_resolve_running_queued_state(
    task_retries: Optional[int], event_retries: Optional[int], expected: TaskState
):
    # Given
    task = Task(
        id="some_id",
        state=TaskState.RUNNING,
        name="some-type",
        created_at=datetime.now(),
        retries=task_retries,
    )
    updated = TaskUpdate(state=TaskState.QUEUED, retries=event_retries)
    # When
    resolved = TaskState.resolve_update_state(task, updated)
    # Then
    assert resolved == expected


def test_error_event_ser():
    # Given
    event = ErrorEvent(
        task_id="task-id",
        retries=4,
        error=TaskError(
            id="error-id",
            task_id="task-id",
            name="some-error",
            message="some message",
            stacktrace=[
                StacktraceItem(name="SomeError", file="some details", lineno=666)
            ],
            occurred_at=_ERROR_OCCURRED_AT,
        ),
        state=TaskState.QUEUED,
    )
    # When
    ser = event.dict(exclude_unset=True, by_alias=True)
    # Then
    expected = {
        "@type": "ErrorEvent",
        "error": {
            "@type": "TaskError",
            "id": "error-id",
            "message": "some message",
            "name": "some-error",
            "occurredAt": _ERROR_OCCURRED_AT,
            "stacktrace": [
                {"file": "some details", "lineno": 666, "name": "SomeError"}
            ],
            "taskId": "task-id",
        },
        "retries": 4,
        "state": TaskState.QUEUED,
        "taskId": "task-id",
    }
    assert ser == expected
