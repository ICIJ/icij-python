from __future__ import annotations

import json
import logging
import traceback
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum, unique
from functools import lru_cache
from typing import Callable, ClassVar, Sequence, Union, cast

from pydantic import Field, root_validator, validator
from pydantic.utils import ROOT_KEY
from typing_extensions import Any, Dict, List, Optional, Self, final

from icij_common import neo4j
from icij_common.pydantic_utils import (
    ICIJModel,
    ISODatetime,
    LowerCamelCaseModel,
    NoEnumModel,
    safe_copy,
)
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
from icij_worker.typing_ import AbstractSetIntStr, DictStrAny, MappingIntStrAny
from icij_worker.utils.registrable import RegistrableMixin

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
            if update.cancelled_at is not None:
                if (
                    stored.cancelled_at is None
                    or stored.cancelled_at < update.cancelled_at
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


class Neo4jDatetimeMixin(ISODatetime):
    @classmethod
    def _validate_neo4j_datetime(cls, value: Any) -> datetime:
        # Trick to avoid having to import neo4j here
        if not isinstance(value, datetime) and hasattr(value, "to_native"):
            value = value.to_native()
        return value


def _encode_registrable(v, **kwargs):
    return json.dumps(v.dict(exclude_unset=True), **kwargs)


class Registrable(ICIJModel, RegistrableMixin, ABC):
    registry_key: ClassVar[str] = Field(const=True, default="@type")

    class Config:
        json_encoders = {"Registrable": _encode_registrable}

    @root_validator(pre=True)
    def validate_type(cls, values):  # pylint: disable=no-self-argument
        values.pop("@type", None)
        return values

    @classmethod
    def parse_obj(cls, obj: Dict) -> Registrable:
        key = obj.pop(cls.registry_key.default)
        subcls = cls.resolve_class_name(key)
        return subcls(**obj)

    def dict(
        self,
        *,
        include: Optional[Union[AbstractSetIntStr, MappingIntStrAny]] = None,
        exclude: Optional[Union[AbstractSetIntStr, MappingIntStrAny]] = None,
        by_alias: bool = False,
        skip_defaults: Optional[bool] = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
    ) -> DictStrAny:
        as_dict = super().dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
        )
        if not exclude or self.__class__.registry_key.default not in exclude:
            as_dict[self.__class__.registry_key.default] = (
                self.__class__.registered_name
            )
        return as_dict

    def json(
        self,
        *,
        include: Optional[Union["AbstractSetIntStr", "MappingIntStrAny"]] = None,
        exclude: Optional[Union["AbstractSetIntStr", "MappingIntStrAny"]] = None,
        by_alias: bool = False,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        encoder: Optional[Callable[[Any], Any]] = None,
        models_as_dict: bool = True,
        **dumps_kwargs: Any,
    ) -> str:
        encoder = cast(Callable[[Any], Any], encoder or self.__json_encoder__)
        data = dict(
            self._iter(
                to_dict=models_as_dict,
                by_alias=by_alias,
                include=include,
                exclude=exclude,
                exclude_unset=exclude_unset,
                exclude_defaults=exclude_defaults,
                exclude_none=exclude_none,
            )
        )
        if self.__custom_root_type__:
            data = data[ROOT_KEY]
        data[self.__class__.registry_key.default] = self.__class__.registered_name
        return self.__config__.json_dumps(data, default=encoder, **dumps_kwargs)


class Message(Registrable): ...  # pylint: disable=multiple-statements


class TaskMessage(Message):
    task_id: str
    created_at: datetime
    retries_left: Optional[int] = None
    max_retries: Optional[int] = None


@Message.register("Task")
class Task(Message, NoEnumModel, LowerCamelCaseModel, Neo4jDatetimeMixin):
    id: str
    name: str
    args: Optional[Dict[str, object]] = None
    state: TaskState
    progress: Optional[float] = None
    created_at: datetime
    completed_at: Optional[datetime] = None
    cancelled_at: Optional[datetime] = None
    retries_left: Optional[int] = None
    max_retries: Optional[int] = None

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

    @validator("args", pre=True, always=True)
    def args_as_dict(cls, v: Optional[Dict[str, Any]]):
        # pylint: disable=no-self-argument
        if v is None:
            v = dict()
        return v

    @root_validator(pre=True)
    def retries_left_should_default_to_max_retries_when_missing(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        # pylint: disable=no-self-argument
        max_retries = values.get("max_retries")
        if values.get("retries_left") is None and max_retries is not None:
            values["retries_left"] = max_retries
        return values

    @classmethod
    def create(cls, *, task_id: str, task_name: str, args: Dict[str, Any]) -> Task:
        created_at = datetime.now(timezone.utc)
        state = TaskState.CREATED
        return cls(
            id=task_id,
            name=task_name,
            args=args,
            created_at=created_at,
            state=state,
        )

    def with_max_retries(self, max_retries: Optional[int]) -> Task:
        as_dict = self.dict()
        as_dict.pop("max_retries", None)
        return Task(max_retries=max_retries, **as_dict)

    @validator("args", pre=True)
    def _validate_args(cls, value: Any):  # pylint: disable=no-self-argument
        if isinstance(value, str):
            value = json.loads(value)
        return value

    @validator("created_at", pre=True)
    def _validate_created_at(cls, value: Any):  # pylint: disable=no-self-argument
        return cls._validate_neo4j_datetime(value)

    @validator("completed_at", pre=True)
    def _validate_completed_at(cls, value: Any):  # pylint: disable=no-self-argument
        return cls._validate_neo4j_datetime(value)

    @validator("cancelled_at", pre=True)
    def _validate_cancelled_at(cls, value: Any):  # pylint: disable=no-self-argument
        return cls._validate_neo4j_datetime(value)

    @validator("progress")
    def _validate_progress(cls, value: float):
        # pylint: disable=no-self-argument
        if value is not None and not 0 <= value <= 1.0:
            msg = f"progress is expected to be in [0.0, 1.0], found {value}"
            raise ValueError(msg)
        return value

    @final
    @classmethod
    def from_neo4j(cls, record: "neo4j.Record", *, key: str = "task") -> Task:
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
                if k.name != "group"
            }
            return cls(**as_dict)

        return as_row

    @final
    def resolve_event(self, event: TaskEvent) -> Optional[TaskUpdate]:
        if self.state in READY_STATES:
            return None
        updated = event.dict(exclude_unset=True, by_alias=False)
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
            update["cancelled_at"] = None
            update["progress"] = 0.0
        else:
            update["cancelled_at"] = event.created_at
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
        return safe_copy(self, update=update.dict(exclude_unset=True))

    @final
    @classmethod
    def _schema(cls, by_alias: bool) -> Dict[str, Any]:
        global _TASK_SCHEMA
        if _TASK_SCHEMA is None:
            _TASK_SCHEMA = dict()
            _TASK_SCHEMA[True] = cls.schema(by_alias=True)
            _TASK_SCHEMA[False] = cls.schema(by_alias=False)
        return _TASK_SCHEMA[by_alias]


class StacktraceItem(LowerCamelCaseModel):
    name: str
    file: str
    lineno: int


@Message.register("TaskError")
class TaskError(Message, LowerCamelCaseModel):
    # Follow the "problem detail" spec: https://datatracker.ietf.org/doc/html/rfc9457,
    # the type is omitted for now since we gave no URI to resolve errors yet
    name: str
    message: str
    cause: Optional[str] = None
    stacktrace: List[StacktraceItem] = Field(default_factory=list)

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


class TaskEvent(
    TaskMessage, NoEnumModel, LowerCamelCaseModel, Neo4jDatetimeMixin, FromTask, ABC
):
    retries_left: int = 3

    @validator("created_at", pre=True)
    def _validate_created_at(cls, value: Any):  # pylint: disable=no-self-argument
        return cls._validate_neo4j_datetime(value)


class WorkerEvent(Message, ABC): ...  # pylint: disable=multiple-statements


class ManagerEvent(TaskEvent, ABC): ...  # pylint: disable=multiple-statements


@Message.register("ProgressEvent")
class ProgressEvent(ManagerEvent):
    progress: float

    @validator("progress")
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
        cls, record: "neo4j.Record", *, event_key: str = "event", task_key: str = "task"
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
    result: object

    @classmethod
    def from_task(cls, task: Task, result: object, **kwargs) -> ResultEvent:
        # pylint: disable=arguments-differ
        return cls(
            task_id=task.id,
            result=result,
            created_at=datetime.now(timezone.utc),
            **kwargs,
        )

    @classmethod
    def from_neo4j(
        cls,
        record: "neo4j.Record",
        *,
        task_key: str = "task",
        result_key: str = "result",
    ) -> ResultEvent:
        result = record.get(result_key)
        if result is not None:
            result = json.loads(result[NEO4J_TASK_RESULT_RESULT])
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
            as_dict[NEO4J_TASK_RESULT_RESULT] = ujson.loads(
                as_dict[NEO4J_TASK_RESULT_RESULT]
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
        record: "neo4j.Record",
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
class ShutdownEvent(WorkerEvent, LowerCamelCaseModel, Neo4jDatetimeMixin):
    created_at: datetime

    @validator("created_at", pre=True)
    def _validate_created_at(cls, value: Any):  # pylint: disable=no-self-argument
        return cls._validate_neo4j_datetime(value)

    @classmethod
    def from_neo4j(
        cls,
        record: "neo4j.Record",
        *,
        event_key: str = "event",
    ) -> Self:
        event = record.get(event_key)
        created_at = event[NEO4J_SHUTDOWN_EVENT_CREATED_AT]
        return ShutdownEvent(created_at=created_at)


class TaskUpdate(NoEnumModel, LowerCamelCaseModel, FromTask):
    state: Optional[TaskState] = None
    progress: Optional[float] = None
    retries_left: Optional[int] = None
    completed_at: Optional[datetime] = None
    cancelled_at: Optional[datetime] = None

    _from_task = ["state", "progress", "retries_left", "completed_at", "cancelled_at"]

    @classmethod
    def from_task(cls, task: Task, **kwargs) -> TaskUpdate:
        from_task = {attr: getattr(task, attr) for attr in cls._from_task}
        from_task = {k: v for k, v in from_task.items() if v is not None}
        return cls(**from_task)

    @classmethod
    @lru_cache(maxsize=1)
    def done(cls, completed_at: Optional[datetime] = None) -> TaskUpdate:
        return cls(progress=1.0, completed_at=completed_at, state=TaskState.DONE)


def _id_title(title: str) -> str:
    id_title = []
    for i, letter in enumerate(title):
        if i and letter.isupper():
            id_title.append("-")
        id_title.append(letter.lower())
    return "".join(id_title)


Registrable.update_forward_refs()
