from __future__ import annotations

import json
import logging
import traceback
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum, unique
from functools import lru_cache
from typing import Sequence

from icij_common import neo4j_
from icij_common.pydantic_utils import (
    ISODatetime,
    icij_config,
    lowercamel_case_config,
    merge_configs,
    no_enum_values_config,
    safe_copy,
)
from icij_common.registrable import Registrable
from pydantic import (
    BaseModel,
    BeforeValidator,
    Field,
    field_validator,
    model_validator,
)
from typing_extensions import Annotated, Any, Self, final

from icij_worker.constants import (
    NEO4J_SHUTDOWN_EVENT_CREATED_AT,
    NEO4J_TASK_CANCEL_EVENT_CANCELLED_AT,
    NEO4J_TASK_CANCEL_EVENT_REQUEUE,
    NEO4J_TASK_COMPLETED_AT,
    NEO4J_TASK_ERROR_MESSAGE,
    NEO4J_TASK_ERROR_NAME,
    NEO4J_TASK_ERROR_OCCURRED_TYPE_OCCURRED_AT,
    NEO4J_TASK_ERROR_OCCURRED_TYPE_RETRIES_LEFT,
    NEO4J_TASK_ERROR_STACKTRACE,
    NEO4J_TASK_ID,
    NEO4J_TASK_NODE,
    NEO4J_TASK_RESULT_RESULT,
)

logger = logging.getLogger(__name__)

PROGRESS_HANDLER_ARG = "progress_handler"
_TASK_SCHEMA = None

TASK_ERROR_CAUSE = "cause"


@unique
class AsyncBackend(str, Enum):
    # pylint: disable=invalid-name@
    mock = "mock"
    neo4j = "neo4j"
    amqp = "amqp"


class FromTask(ABC):
    @classmethod
    @abstractmethod
    def from_task(cls, task: Task, **kwargs) -> FromTask: ...


@unique
class TaskState(str, Enum):
    CREATED = "CREATED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    ERROR = "ERROR"
    DONE = "DONE"
    CANCELLED = "CANCELLED"

    @classmethod
    def resolve_update_state(cls, stored: Task, update: TaskUpdate) -> TaskState:
        # A done task is always done
        if stored.state is TaskState.DONE:
            return stored.state
        # A task store as ready can't be updated unless there's a new ready state
        # (for instance ERROR -> DONE)
        if stored.state in READY_STATES and update.state not in READY_STATES:
            return stored.state
        if update.state is TaskState.QUEUED and stored.state is TaskState.RUNNING:
            # We have to store the most recent state
            if update.completed_at is not None:
                if (
                    stored.completed_at is None
                    or stored.completed_at < update.completed_at
                ):
                    return update.state
                return stored.state
            return stored.state
        # Otherwise the true state is the most advanced on in the state machine
        return max(stored.state, update.state)

    def __gt__(self, other: TaskState) -> bool:
        return state_precedence(self) < state_precedence(other)

    def __ge__(self, other: TaskState) -> bool:
        return state_precedence(self) <= state_precedence(other)

    def __lt__(self, other: TaskState) -> bool:
        return state_precedence(self) > state_precedence(other)

    def __le__(self, other: TaskState) -> bool:
        return state_precedence(self) >= state_precedence(other)


READY_STATES = frozenset({TaskState.DONE, TaskState.ERROR, TaskState.CANCELLED})
# Greatly inspired from Celery
PRECEDENCE = [
    TaskState.DONE,
    TaskState.ERROR,
    TaskState.CANCELLED,
    TaskState.RUNNING,
    TaskState.QUEUED,
    TaskState.CREATED,
]
PRECEDENCE_LOOKUP = dict(zip(PRECEDENCE, range(len(PRECEDENCE))))


def state_precedence(state: TaskState) -> int:
    return PRECEDENCE_LOOKUP[state]


def _deserialize_datetime_from_neo4j(value: Any) -> datetime:
    if not isinstance(value, datetime) and hasattr(value, "to_native"):
        value = value.to_native()
    return value


Neo4jDatetime = Annotated[
    ISODatetime, BeforeValidator(_deserialize_datetime_from_neo4j)
]


class Message(Registrable):
    model_config = merge_configs(lowercamel_case_config(), no_enum_values_config())


class TaskMessage(Message):
    task_id: str
    created_at: Neo4jDatetime
    retries_left: int | None = None
    max_retries: int | None = None


def _validate_task_args(v: dict[str, Any] | str | None):
    if v is None:
        v = dict()
    if isinstance(v, str):
        v = json.loads(v)
    return v


def _validate_task_progress(value: float):
    # pylint: disable=no-self-argument
    if value is not None and not 0 <= value <= 1.0:
        msg = f"progress is expected to be in [0.0, 1.0], found {value}"
        raise ValueError(msg)
    return value


@Message.register("Task")
class Task(Message):
    id: str
    name: str
    args: Annotated[dict[str, object] | None, BeforeValidator(_validate_task_args)] = (
        None
    )
    state: TaskState
    progress: Annotated[float | None, BeforeValidator(_validate_task_progress)] = None
    created_at: Neo4jDatetime
    completed_at: Neo4jDatetime | None = None
    retries_left: int | None = None
    max_retries: int | None = None

    _non_inherited_from_event = [
        "requeue",
        "created_at",
        "error",
        "occurred_at",
        "task_name",
        "result",
        "task_id",
        "retries_left",
    ]

    @model_validator(mode="before")
    @classmethod
    def retries_left_should_default_to_max_retries_when_missing(
        cls, values: dict[str, Any]
    ) -> dict[str, Any]:
        # pylint: disable=no-self-argument
        max_retries = values.get("max_retries")
        if values.get("retries_left") is None and max_retries is not None:
            values["retries_left"] = max_retries
        return values

    @classmethod
    def create(cls, *, task_id: str, task_name: str, args: dict[str, Any]) -> Task:
        created_at = datetime.now(timezone.utc)
        state = TaskState.CREATED
        return cls(
            id=task_id, name=task_name, args=args, created_at=created_at, state=state
        )

    def with_max_retries(self, max_retries: int | None) -> Task:
        as_dict = self.model_dump()
        as_dict["max_retries"] = max_retries
        return Task.model_validate(as_dict)

    @final
    @classmethod
    def from_neo4j(cls, record: "neo4j_.Record", *, key: str = "task") -> Task:
        node = record[key]
        labels = node.labels
        node = dict(node)
        if len(labels) != 2:
            raise ValueError(f"Expected task to have exactly 2 labels found {labels}")
        state = [label for label in labels if label != NEO4J_TASK_NODE]
        if len(state) != 1:
            raise ValueError(f"Invalid task labels {labels}")
        state = state[0]
        if "completedAt" in node:
            node["completedAt"] = node["completedAt"].to_native()
        if "args" in node:
            node["args"] = json.loads(node["args"])
        if "group" in node:
            node.pop("group")
        node["state"] = state
        return cls(**node)

    @classmethod
    def postgres_row_factory(cls, cursor: "BaseCursor[Any, Any]") -> "RowMaker[Task]":
        def as_row(values: Sequence[Any]) -> Task:
            as_dict = {
                k.name: v
                for k, v in zip(cursor.description, values)
                if k.name != "group_id"
            }
            return cls(**as_dict)

        return as_row

    @final
    def resolve_event(self, event: TaskEvent) -> TaskUpdate | None:
        if self.state in READY_STATES:
            return None
        updated = event.model_dump(exclude_unset=True, by_alias=False)
        for k in self._non_inherited_from_event:
            updated.pop(k, None)
        updated.pop(event.registry_key.default, None)
        base_update = TaskUpdate(**updated)
        # Update the state to make it consistent in case of race condition
        if isinstance(event, ProgressEvent):
            return self._progress_update(base_update)
        if isinstance(event, ErrorEvent):
            return self._error_update(base_update, event)
        if isinstance(event, CancelledEvent):
            return self._cancelled_update(base_update, event)
        if isinstance(event, ResultEvent):
            return self._result_update(base_update, event)
        raise TypeError(f"Unexpected event type {event.__class__}")

    def _result_update(self, base_update: TaskUpdate, event: ResultEvent) -> TaskUpdate:
        update = dict()
        update["progress"] = 1.0
        update["state"] = TaskState.DONE
        update["completed_at"] = event.created_at
        return safe_copy(base_update, update=update)

    def _cancelled_update(
        self, base_update: TaskUpdate, event: CancelledEvent
    ) -> TaskUpdate:
        update = dict()
        cancelled_state = TaskState.QUEUED if event.requeue else TaskState.CANCELLED
        update["state"] = cancelled_state
        if event.requeue:
            update["progress"] = 0.0
        else:
            update["completed_at"] = event.created_at
        updated = safe_copy(base_update, update=update)
        return updated

    def _error_update(self, base_update: TaskUpdate, event: ErrorEvent):
        update = dict()
        retries_left = min(self.retries_left, event.retries_left)
        can_retry = self.max_retries is not None and retries_left > 0
        update["state"] = TaskState.QUEUED if can_retry else TaskState.ERROR
        update["retries_left"] = retries_left
        updated = safe_copy(base_update, update=update)
        return updated

    def _progress_update(self, updated: TaskUpdate) -> TaskUpdate:
        state = TaskState.resolve_update_state(
            self, TaskUpdate(state=TaskState.RUNNING)
        )
        update = {"state": state}
        if state is TaskState.QUEUED:
            update["progress"] = 0.0
        return safe_copy(updated, update=update)

    def as_resolved(self, event: TaskEvent) -> Task:
        update = self.resolve_event(event)
        if update is None:
            return self
        return safe_copy(self, update=update.model_dump(exclude_unset=True))

    @final
    @classmethod
    def _schema(cls, by_alias: bool) -> dict[str, Any]:
        global _TASK_SCHEMA
        if _TASK_SCHEMA is None:
            _TASK_SCHEMA = dict()
            _TASK_SCHEMA[True] = cls.schema(by_alias=True)
            _TASK_SCHEMA[False] = cls.schema(by_alias=False)
        return _TASK_SCHEMA[by_alias]


class StacktraceItem(BaseModel):
    model_config = merge_configs(
        icij_config(), no_enum_values_config(), lowercamel_case_config()
    )

    name: str
    file: str
    lineno: int


@Message.register("TaskError")
class TaskError(Message):
    # Follow the "problem detail" spec: https://datatracker.ietf.org/doc/html/rfc9457,
    # the type is omitted for now since we gave no URI to resolve errors yet
    name: str
    message: str
    cause: str | None = None
    stacktrace: list[StacktraceItem] = Field(default_factory=list)

    @classmethod
    def from_exception(cls, exception: BaseException) -> TaskError:
        name = exception.__class__.__name__
        message = str(exception)
        stacktrace = traceback.StackSummary.extract(
            traceback.walk_tb(exception.__traceback__)
        )
        stacktrace = [
            StacktraceItem(name=f.name, file=f.filename, lineno=f.lineno)
            for f in stacktrace
        ]
        cause = exception.__cause__
        if cause is not None:
            cause = str(cause)
        error = cls(name=name, message=message, cause=cause, stacktrace=stacktrace)
        return error


@Message.register("TaskResult")
class TaskResult(Message):
    value: object


class TaskEvent(TaskMessage, FromTask, ABC):
    retries_left: int = 3


class WorkerEvent(Message, ABC): ...  # pylint: disable=multiple-statements


class ManagerEvent(TaskEvent, ABC): ...  # pylint: disable=multiple-statements


@Message.register("ProgressEvent")
class ProgressEvent(ManagerEvent):
    progress: float

    @field_validator("progress")
    @classmethod
    def _validate_progress(cls, value: float):
        # pylint: disable=no-self-argument
        if not 0 <= value <= 1.0:
            msg = f"progress is expected to be in [0.0, 1.0], found {value}"
            raise ValueError(msg)
        return value

    @classmethod
    def from_task(cls, task: Task, **kwargs) -> ProgressEvent:
        created_at = datetime.now(timezone.utc)
        event = cls(
            task_id=task.id, progress=task.progress, created_at=created_at, **kwargs
        )
        return event


@Message.register("CancelEvent")
class CancelEvent(WorkerEvent, TaskEvent):
    requeue: bool

    @classmethod
    def from_neo4j(
        cls,
        record: "neo4j_.Record",
        *,
        event_key: str = "event",
        task_key: str = "task",
    ) -> CancelEvent:
        task = record.get(task_key)
        event = record.get(event_key)
        task_id = task[NEO4J_TASK_ID]
        requeue = event[NEO4J_TASK_CANCEL_EVENT_REQUEUE]
        created_at = event[NEO4J_TASK_CANCEL_EVENT_CANCELLED_AT]
        return cls(task_id=task_id, requeue=requeue, created_at=created_at)

    @classmethod
    def from_task(cls, task: Task, *, requeue: bool, **kwargs) -> CancelEvent:
        # pylint: disable=arguments-differ
        return cls(
            task_id=task.id, requeue=requeue, created_at=datetime.now(timezone.utc)
        )


@Message.register("CancelledEvent")
class CancelledEvent(ManagerEvent):
    requeue: bool

    @classmethod
    def from_task(cls, task: Task, *, requeue: bool, **kwargs) -> CancelledEvent:
        # pylint: disable=arguments-differ
        created_at = datetime.now(timezone.utc)
        event = cls(task_id=task.id, created_at=created_at, requeue=requeue)
        return event


@Message.register("ResultEvent")
class ResultEvent(ManagerEvent):
    result: TaskResult

    @classmethod
    def from_task(cls, task: Task, result: object, **kwargs) -> ResultEvent:
        # pylint: disable=arguments-differ
        return cls(
            task_id=task.id,
            result=TaskResult(value=result),
            created_at=datetime.now(timezone.utc),
            **kwargs,
        )

    @classmethod
    def from_neo4j(
        cls,
        record: "neo4j_.Record",
        *,
        task_key: str = "task",
        result_key: str = "result",
    ) -> ResultEvent:
        result = record.get(result_key)
        if result is not None:
            result = TaskResult(value=json.loads(result[NEO4J_TASK_RESULT_RESULT]))
        task_id = record[task_key][NEO4J_TASK_ID]
        completed_at = record[task_key][NEO4J_TASK_COMPLETED_AT]
        as_dict = {"result": result, "task_id": task_id, "created_at": completed_at}
        return ResultEvent(**as_dict)

    @classmethod
    def postgres_row_factory(
        cls, cursor: "BaseCursor[Any, Any]"
    ) -> "RowMaker[ResultEvent]":
        def as_row(values: Sequence[Any]) -> ResultEvent:
            # pylint: disable=c-extension-no-member
            import ujson

            as_dict = {k.name: v for k, v in zip(cursor.description, values)}
            as_dict[NEO4J_TASK_RESULT_RESULT] = TaskResult(
                value=ujson.loads(as_dict[NEO4J_TASK_RESULT_RESULT])
            )
            return cls(**as_dict)

        return as_row


@Message.register("ErrorEvent")
class ErrorEvent(ManagerEvent):
    error: TaskError

    @classmethod
    def from_task(
        cls,
        task: Task,
        error: TaskError,
        retries_left: int,
        created_at: datetime,
        **kwargs,
    ) -> ErrorEvent:
        # pylint: disable=arguments-differ
        return cls(
            task_id=task.id,
            error=error,
            retries_left=retries_left,
            created_at=created_at,
        )

    @classmethod
    def from_neo4j(
        cls,
        record: "neo4j_.Record",
        *,
        task_key: str = "task",
        error_key: str = "error",
        rel_key: str = "rel",
    ) -> ErrorEvent:
        error = dict(record.value(error_key))
        if NEO4J_TASK_ERROR_STACKTRACE in error:
            stacktrace = [
                StacktraceItem(**json.loads(item))
                for item in error[NEO4J_TASK_ERROR_STACKTRACE]
            ]
            error[NEO4J_TASK_ERROR_STACKTRACE] = stacktrace
        error = TaskError(**error)
        rel = dict(record.value(rel_key))
        task_id = record[task_key][NEO4J_TASK_ID]
        retries_left = rel[NEO4J_TASK_ERROR_OCCURRED_TYPE_RETRIES_LEFT]
        created_at = rel[NEO4J_TASK_ERROR_OCCURRED_TYPE_OCCURRED_AT]
        return ErrorEvent(
            task_id=task_id,
            error=error,
            created_at=created_at,
            retries_left=retries_left,
        )

    @classmethod
    def postgres_row_factory(
        cls, cursor: "BaseCursor[Any, Any]"
    ) -> "RowMaker[ErrorEvent]":
        def as_row(values: Sequence[Any]) -> cls:
            # pylint: disable=c-extension-no-member
            import ujson

            as_dict = {k.name: v for k, v in zip(cursor.description, values)}
            stacktrace = ujson.loads(as_dict.pop(NEO4J_TASK_ERROR_STACKTRACE))
            message = as_dict.pop(NEO4J_TASK_ERROR_MESSAGE)
            name = as_dict.pop(NEO4J_TASK_ERROR_NAME)
            cause = as_dict.pop(TASK_ERROR_CAUSE)
            task_error = {
                "stacktrace": stacktrace,
                "name": name,
                "message": message,
                "cause": cause,
            }
            as_dict["error"] = task_error
            return cls(**as_dict)

        return as_row


@Message.register("ShutdownEvent")
class ShutdownEvent(WorkerEvent):
    created_at: Neo4jDatetime

    @classmethod
    def from_neo4j(
        cls,
        record: "neo4j_.Record",
        *,
        event_key: str = "event",
    ) -> Self:
        event = record.get(event_key)
        created_at = event[NEO4J_SHUTDOWN_EVENT_CREATED_AT]
        return ShutdownEvent(created_at=created_at)


class TaskUpdate(BaseModel, FromTask):
    model_config = merge_configs(
        icij_config(), lowercamel_case_config(), no_enum_values_config()
    )

    state: TaskState | None = None
    progress: float | None = None
    retries_left: int | None = None
    completed_at: datetime | None = None

    _from_task = ["state", "progress", "retries_left", "completed_at"]

    @classmethod
    def from_task(cls, task: Task, **kwargs) -> TaskUpdate:
        from_task = {attr: getattr(task, attr) for attr in cls._from_task.default}
        from_task = {k: v for k, v in from_task.items() if v is not None}
        return cls.model_validate(from_task)

    @classmethod
    @lru_cache(maxsize=1)
    def done(cls, completed_at: datetime | None = None) -> TaskUpdate:
        return cls(progress=1.0, completed_at=completed_at, state=TaskState.DONE)


def _id_title(title: str) -> str:
    id_title = []
    for i, letter in enumerate(title):
        if i and letter.isupper():
            id_title.append("-")
        id_title.append(letter.lower())
    return "".join(id_title)
