from __future__ import annotations

from datetime import datetime
from typing import Optional

import pytest

from icij_worker.objects import (
    CancelledEvent,
    ErrorEvent,
    PRECEDENCE,
    ProgressEvent,
    READY_STATES,
    ResultEvent,
    StacktraceItem,
    Task,
    TaskError,
    ManagerEvent,
    TaskState,
    TaskUpdate,
)

_CREATED_AT = datetime.now()
_ERROR_OCCURRED_AT = datetime.now()
_ANOTHER_TIME = datetime.now()


def test_precedence_sanity_check():
    assert len(PRECEDENCE) == len(TaskState)


@pytest.mark.parametrize(
    "task,event,max_retries,expected_task",
    [
        # ProgresEvent
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.CREATED,
                created_at=_CREATED_AT,
            ),
            ProgressEvent(task_id="task-id", progress=0.0),
            None,
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.RUNNING,
                created_at=_CREATED_AT,
                progress=0.0,
            ),
        ),
        # State is updated when not in a final state
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.QUEUED,
                created_at=_CREATED_AT,
            ),
            ProgressEvent(task_id="task-id", progress=0.5),
            None,
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.RUNNING,
                created_at=_CREATED_AT,
                progress=0.5,
            ),
        ),
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.RUNNING,
                created_at=_CREATED_AT,
            ),
            ProgressEvent(task_id="task-id", progress=1.0),
            None,
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.RUNNING,
                created_at=_CREATED_AT,
                progress=1.0,
            ),
        ),
        # Result event
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.RUNNING,
                progress=0.5,
                created_at=_CREATED_AT,
            ),
            ResultEvent(
                task_id="task-id", result="some-result", completed_at=_ANOTHER_TIME
            ),
            None,
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.DONE,
                progress=1.0,
                created_at=_CREATED_AT,
                completed_at=_ANOTHER_TIME,
            ),
        ),
        # Error event can retry
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.RUNNING,
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
            ),
            5,
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.QUEUED,
                created_at=_CREATED_AT,
                retries=4,
            ),
        ),
        # Error event can't retry
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.RUNNING,
                created_at=_CREATED_AT,
            ),
            ErrorEvent(
                task_id="task-id",
                retries=1,
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
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.ERROR,
                created_at=_CREATED_AT,
                retries=1,
            ),
        ),
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.RUNNING,
                created_at=_CREATED_AT,
            ),
            ErrorEvent(
                task_id="task-id",
                retries=5,
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
            5,
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.ERROR,
                created_at=_CREATED_AT,
                retries=5,
            ),
        ),
        # CancelledEvent, requeue
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.QUEUED,
                created_at=_CREATED_AT,
            ),
            CancelledEvent(task_id="task-id", requeue=True, cancelled_at=_ANOTHER_TIME),
            None,
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.QUEUED,
                created_at=_CREATED_AT,
                progress=0.0,
            ),
        ),
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.RUNNING,
                created_at=_CREATED_AT,
                progress=0.5,
            ),
            CancelledEvent(task_id="task-id", requeue=True, cancelled_at=_ANOTHER_TIME),
            None,
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.QUEUED,
                created_at=_CREATED_AT,
                progress=0.0,
            ),
        ),
        # CancelledEvent, error
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.QUEUED,
                created_at=_CREATED_AT,
            ),
            CancelledEvent(
                task_id="task-id", requeue=False, cancelled_at=_ANOTHER_TIME
            ),
            None,
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.CANCELLED,
                cancelled_at=_ANOTHER_TIME,
                created_at=_CREATED_AT,
            ),
        ),
        (
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.RUNNING,
                created_at=_CREATED_AT,
                progress=0.5,
            ),
            CancelledEvent(
                task_id="task-id", requeue=False, cancelled_at=_ANOTHER_TIME
            ),
            None,
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.CANCELLED,
                cancelled_at=_ANOTHER_TIME,
                created_at=_CREATED_AT,
                progress=0.5,
            ),
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
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.DONE,
                created_at=_CREATED_AT,
                completed_at=_CREATED_AT,
            ),
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
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.DONE,
                created_at=_CREATED_AT,
            ),
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
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.ERROR,
                created_at=_CREATED_AT,
            ),
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
            Task(
                id="task-id",
                name="hello_world",
                state=TaskState.CANCELLED,
                created_at=_CREATED_AT,
            ),
        ),
    ],
)
def test_as_resolve_event(
    task: Task,
    event: ManagerEvent,
    max_retries: Optional[int],
    expected_task: Optional[Task],
):
    # When
    updated = task.as_resolved(event, max_retries=max_retries)
    # Then
    assert updated == expected_task


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
        "taskId": "task-id",
    }
    assert ser == expected
