from __future__ import annotations

import json
import logging
import traceback
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum, unique

from pydantic import Field, validator
from typing_extensions import Any, Dict, List, Optional, final

from icij_common.neo4j.constants import (
    TASK_CANCEL_EVENT_CREATED_AT,
    TASK_CANCEL_EVENT_REQUEUE,
    TASK_ID,
    TASK_NODE,
)
from icij_common.pydantic_utils import (
    ISODatetime,
    LowerCamelCaseModel,
    NoEnumModel,
    safe_copy,
)

logger = logging.getLogger(__name__)

PROGRESS_HANDLER_ARG = "progress_handler"
_TASK_SCHEMA = None


class FromTask(ABC):

    @classmethod
    @abstractmethod
    def from_task(cls, task: Task, **kwargs) -> FromTask: ...


@unique
class TaskStatus(Enum):
    CREATED = "CREATED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    ERROR = "ERROR"
    DONE = "DONE"
    CANCELLED = "CANCELLED"

    @classmethod
    def resolve_event_status(cls, stored: Task, event: TaskEvent) -> TaskStatus:
        # A done task is always done
        if stored.status is TaskStatus.DONE:
            return stored.status
        # A task store as ready can't be updated unless there's a new ready state
        # (for instance ERROR -> DONE)
        if stored.status in READY_STATES and event.status not in READY_STATES:
            return stored.status
        if event.status is TaskStatus.QUEUED and stored.status is TaskStatus.RUNNING:
            # We have to store the most recent status
            if event.retries is None:
                return stored.status
            if stored.retries is None or event.retries > stored.retries:
                return event.status
            return stored.status
        # Otherwise the true status is the most advanced on in the state machine
        return max(stored.status, event.status)

    def __gt__(self, other: TaskStatus) -> bool:
        return status_precedence(self) < status_precedence(other)

    def __ge__(self, other: TaskStatus) -> bool:
        return status_precedence(self) <= status_precedence(other)

    def __lt__(self, other: TaskStatus) -> bool:
        return status_precedence(self) > status_precedence(other)

    def __le__(self, other: TaskStatus) -> bool:
        return status_precedence(self) >= status_precedence(other)


READY_STATES = frozenset({TaskStatus.DONE, TaskStatus.ERROR, TaskStatus.CANCELLED})
# Greatly inspired from Celery
PRECEDENCE = [
    TaskStatus.DONE,
    TaskStatus.ERROR,
    TaskStatus.CANCELLED,
    TaskStatus.RUNNING,
    TaskStatus.QUEUED,
    TaskStatus.CREATED,
]
PRECEDENCE_LOOKUP = dict(zip(PRECEDENCE, range(len(PRECEDENCE))))


def status_precedence(state: TaskStatus) -> int:
    return PRECEDENCE_LOOKUP[state]


class Neo4jDatetimeMixin(ISODatetime):
    @classmethod
    def _validate_neo4j_datetime(cls, value: Any) -> datetime:
        # Trick to avoid having to import neo4j here
        if not isinstance(value, datetime) and hasattr(value, "to_native"):
            value = value.to_native()
        return value


class Task(NoEnumModel, LowerCamelCaseModel, Neo4jDatetimeMixin):
    id: str
    type: str
    inputs: Optional[Dict[str, object]] = None
    status: TaskStatus
    progress: Optional[float] = None
    created_at: datetime
    completed_at: Optional[datetime] = None
    cancelled_at: Optional[datetime] = None
    retries: Optional[int] = None

    @validator("inputs", pre=True, always=True)
    def inputs_as_dict(cls, v: Optional[Dict[str, Any]]):
        # pylint: disable=no-self-argument
        if v is None:
            v = dict()
        return v

    @classmethod
    def create(
        cls, *, task_id: str, task_ype: str, task_inputs: Dict[str, Any]
    ) -> Task:
        created_at = datetime.now()
        status = TaskStatus.CREATED
        return cls(
            id=task_id,
            type=task_ype,
            inputs=task_inputs,
            created_at=created_at,
            status=status,
        )

    @validator("inputs", pre=True)
    def _validate_inputs(cls, value: Any):  # pylint: disable=no-self-argument
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
    def _validate_progress(cls, value: Optional[float]):
        # pylint: disable=no-self-argument
        if isinstance(value, float) and not 0 <= value <= 100:
            # We log here rather than raising since otherwise a single invalid log will
            # prevent anything any deserialization related
            logger.error("progress is expected to be in [0, 100], found %s", value)
        return value

    @final
    @classmethod
    def from_neo4j(cls, record: "neo4j.Record", *, key: str = "task") -> Task:
        node = record[key]
        labels = node.labels
        node = dict(node)
        if len(labels) != 2:
            raise ValueError(f"Expected task to have exactly 2 labels found {labels}")
        status = [label for label in labels if label != TASK_NODE]
        if len(status) != 1:
            raise ValueError(f"Invalid task labels {labels}")
        status = status[0]
        if "completedAt" in node:
            node["completedAt"] = node["completedAt"].to_native()
        if "inputs" in node:
            node["inputs"] = json.loads(node["inputs"])
        node["status"] = status
        return cls.parse_obj(node)

    @final
    @classmethod
    def mandatory_fields(cls, event: TaskEvent | Task, keep_id: bool) -> Dict[str, Any]:
        event = event.dict(by_alias=True, exclude_unset=True)
        mandatory = dict()
        for f, v in event.items():
            task_field = f.replace("task", "")
            task_field = f"{task_field[0].lower()}{task_field[1:]}"
            if task_field == "id" and not keep_id:
                continue
            if task_field not in cls._schema(by_alias=True)["required"]:
                continue
            mandatory[task_field] = v
        return mandatory

    @final
    def resolve_event(self, event: TaskEvent) -> Optional[TaskEvent]:
        if self.status in READY_STATES:
            return None
        resolved = event.dict(exclude_unset=True, by_alias=False)
        resolved.pop("task_id")
        resolved.pop("created_at", None)
        resolved.pop("task_type", None)
        resolved.pop("completed_at", None)
        # Update the status to make it consistent in case of race condition
        if event.status is not None:
            resolved["status"] = TaskStatus.resolve_event_status(self, event)
        # Copy the event a first time to unset non-updatable field
        if not resolved:
            return None
        base_resolved = TaskEvent(task_id=event.task_id)
        if "error" in resolved:
            resolved["error"] = TaskError.parse_obj(resolved["error"])
        resolved = safe_copy(base_resolved, update=resolved)
        return resolved

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


class TaskError(LowerCamelCaseModel, FromTask):
    # This helps to know if an error has already been processed or not
    id: str
    task_id: str
    # Follow the "problem detail" spec: https://datatracker.ietf.org/doc/html/rfc9457,
    # the type is omitted for now since we gave no URI to resolve errors yet
    name: str
    message: str
    cause: Optional[str] = None
    stacktrace: List[StacktraceItem] = Field(default_factory=list)
    occurred_at: datetime

    @classmethod
    def from_exception(cls, exception: BaseException, task: Task) -> TaskError:
        name = exception.__class__.__name__
        message = str(exception)
        error_id = f"{_id_title(name)}-{uuid.uuid4().hex}"
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
        error = cls.from_task(
            task,
            id=error_id,
            name=name,
            message=message,
            cause=cause,
            stacktrace=stacktrace,
            occurred_at=datetime.now(),
        )
        return error

    @classmethod
    def from_task(cls, task: Task, **kwargs) -> TaskError:
        return cls(task_id=task.id, **kwargs)

    @classmethod
    def from_neo4j(
        cls, record: "neo4j.Record", *, task_id: str, key: str = "error"
    ) -> TaskError:
        task = dict(record.value(key))
        task.update({"taskId": task_id})
        if "occurredAt" in task:
            task["occurredAt"] = task["occurredAt"].to_native()
        if "stacktrace" in task:
            stacktrace = [
                StacktraceItem(**json.loads(item)) for item in task["stacktrace"]
            ]
            task["stacktrace"] = stacktrace
        return cls.parse_obj(task)

    def trace(self) -> str:
        # TODO: fix this using Pydantic v2 computed_fields + cached property
        frames = [
            traceback.FrameSummary(filename=i.file, lineno=i.lineno, name=i.name)
            for i in self.stacktrace
        ]
        trace = "\n".join(traceback.StackSummary(frames).format())
        return trace


class TaskEvent(NoEnumModel, LowerCamelCaseModel, FromTask):
    task_id: str
    task_type: Optional[str] = None
    status: Optional[TaskStatus] = None
    progress: Optional[float] = None
    retries: Optional[int] = None
    error: Optional[TaskError] = None
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    _excluded_from_task = {"id", "type", "inputs", "cancelled_at"}

    @classmethod
    def from_task(cls, task: Task, **kwargs) -> TaskEvent:
        event = cls(task_id=task.id, task_type=task.type, **kwargs)
        return event

    @classmethod
    def from_error(
        cls, error: TaskError, task_id: str, retries: Optional[int] = None, **kwargs
    ) -> TaskEvent:
        status = TaskStatus.QUEUED if retries is not None else TaskStatus.ERROR
        event = cls(
            task_id=task_id, status=status, retries=retries, error=error, **kwargs
        )
        return event


class CancelledTaskEvent(NoEnumModel, LowerCamelCaseModel, Neo4jDatetimeMixin):
    task_id: str
    requeue: bool
    created_at: datetime

    @validator("created_at", pre=True)
    def _validate_created_at(cls, value: Any):  # pylint: disable=no-self-argument
        return cls._validate_neo4j_datetime(value)

    @classmethod
    def from_neo4j(
        cls, record: "neo4j.Record", *, event_key: str = "event", task_key: str = "task"
    ) -> CancelledTaskEvent:
        task = record.get(task_key)
        event = record.get(event_key)
        task_id = task[TASK_ID]
        requeue = event[TASK_CANCEL_EVENT_REQUEUE]
        created_at = event[TASK_CANCEL_EVENT_CREATED_AT]
        return cls(task_id=task_id, requeue=requeue, created_at=created_at)


class TaskResult(LowerCamelCaseModel, FromTask):
    # TODO: we could use generics here
    task_id: str
    result: object

    @classmethod
    def from_neo4j(
        cls,
        record: "neo4j.Record",
        *,
        task_key: str = "task",
        result_key: str = "result",
    ) -> TaskResult:
        result = record.get(result_key)
        if result is not None:
            result = json.loads(result["result"])
        task_id = record[task_key]["id"]
        as_dict = {"result": result, "task_id": task_id}
        return cls(**as_dict)

    @classmethod
    def from_task(cls, task: Task, result: object, **kwargs) -> TaskResult:
        # pylint: disable=arguments-differ
        return cls(task_id=task.id, result=result, **kwargs)


def _id_title(title: str) -> str:
    id_title = []
    for i, letter in enumerate(title):
        if i and letter.isupper():
            id_title.append("-")
        id_title.append(letter.lower())
    return "".join(id_title)
