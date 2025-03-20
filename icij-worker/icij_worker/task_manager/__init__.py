import asyncio
import logging
from abc import ABC, abstractmethod
from asyncio import Task as AsyncIOTask
from functools import cached_property
from typing import ClassVar, final

from pydantic import Field

from icij_common.pydantic_utils import merge_configs, no_enum_values_config, safe_copy
from icij_common.registrable import RegistrableConfig, RegistrableFromConfig
from icij_worker import AsyncApp, ResultEvent, Task, TaskState
from icij_worker.app import AsyncAppConfig
from icij_worker.exceptions import TaskAlreadyQueued, UnknownTask, UnregisteredTask
from icij_worker.objects import (
    AsyncBackend,
    CancelledEvent,
    ErrorEvent,
    ManagerEvent,
    ProgressEvent,
)
from icij_worker.routing_strategy import RoutingStrategy
from icij_worker.task_storage import TaskStorage

logger = logging.getLogger(__name__)


class TaskManagerConfig(RegistrableConfig):
    model_config = merge_configs(no_enum_values_config())

    registry_key: ClassVar[str] = Field(frozen=True, default="backend")
    backend: AsyncBackend = Field(frozen=True)

    app_path: str
    app_config: AsyncAppConfig = Field(default_factory=AsyncAppConfig)

    @property
    def app(self) -> AsyncApp:
        app = AsyncApp.load(self.app_path).with_config(self.app_config)
        return app


class TaskManager(TaskStorage, RegistrableFromConfig, ABC):
    def __init__(self, app: AsyncApp):
        self._app = app
        self._loop = asyncio.get_event_loop()
        self._consume_loop: AsyncIOTask | None = None

    @final
    async def __aenter__(self):
        await self._aenter__()
        self._consume_loop = self._loop.create_task(self.consume_events())

    async def _aenter__(self):
        pass

    @final
    async def __aexit__(self, exc_type, exc_value, tb):
        await self._aexit__(exc_type, exc_value, tb)
        if self._consume_loop is not None and not self._consume_loop.done():
            logger.info("cancelling worker event loop...")
            self._consume_loop.cancel()
            await asyncio.wait([self._consume_loop])
            logger.info("worker event loop cancelled")

    async def _aexit__(self, exc_type, exc_val, exc_tb):
        pass

    @cached_property
    def late_ack(self) -> bool:
        return self._app.config.late_ack

    @cached_property
    def max_task_queue_size(self) -> int:
        return self._app.config.max_task_queue_size

    @cached_property
    def _routing_strategy(self) -> RoutingStrategy:
        return self._app.routing_strategy

    @cached_property
    def app_name(self) -> str:
        return self._app.name

    @final
    async def enqueue(self, task: Task) -> Task:
        if task.state is not TaskState.CREATED:
            msg = f"invalid state {task.state}, expected {TaskState.CREATED}"
            raise ValueError(msg)
        task = await self.get_task(task.id)
        if task.state is TaskState.QUEUED:
            raise TaskAlreadyQueued(task.id)
        await self._enqueue(task)
        queued = safe_copy(task, update={"state": TaskState.QUEUED})
        await self.save_task(queued)
        return queued

    @final
    async def requeue(self, task: Task):
        logger.info("requeing Task(id=%s)", task.id)
        update = {"state": TaskState.QUEUED, "progress": 0.0}
        updated = safe_copy(task, update=update)
        await self._requeue(updated)
        logger.info("Task(id=%s) requeued", updated.id)

    @final
    async def save_task(self, task: Task) -> bool:
        max_retries = None
        try:
            group = await self.get_task_group(task_id=task.id)
        except UnknownTask as e:
            try:
                group = self._app.registry[task.name].group
                if group is not None:
                    group = group.name
                max_retries = self._app.registry[task.name].max_retries
            except KeyError:
                available_tasks = list(self._app.registry)
                raise UnregisteredTask(task.name, available_tasks) from e
        task = task.with_max_retries(max_retries)
        return await self.save_task_(task, group)

    async def _save_cancelled_event(self, event: CancelledEvent):
        task = await self.get_task(event.task_id)
        task = task.as_resolved(event)
        if event.requeue and not self.late_ack:
            await self.requeue(task)
        await self.save_task(task)

    @final
    async def consume_events(self):
        while True:
            msg = await self._consume()
            if isinstance(msg, ResultEvent):
                logger.debug("saving result for task: %s", msg.task_id)
                await self._save_result_event(msg)
            elif isinstance(msg, ErrorEvent):
                logger.debug("saving error: %s", msg)
                await self._save_error_event(msg)
            elif isinstance(msg, ProgressEvent):
                logger.debug("saving progress: %s", msg)
                await self._save_progress_event(msg)
            elif isinstance(msg, CancelledEvent):
                logger.debug("saving cancellation: %s", msg)
                await self._save_cancelled_event(msg)
            else:
                raise TypeError(f"unexpected message type {msg.__class__}")

    @final
    async def _save_result_event(self, result: ResultEvent):
        await self.save_result(result)
        task = await self.get_task(result.task_id)
        task = task.as_resolved(result)
        await self.save_task(task)

    @final
    async def _save_error_event(self, error: ErrorEvent):
        # Update the task retries count
        task = await self.get_task(error.task_id)
        task = task.as_resolved(error)
        await self.save_error(error)
        if task.state is TaskState.QUEUED:
            await self.requeue(task)
        await self.save_task(task)

    @abstractmethod
    async def cancel(self, task_id: str, *, requeue: bool): ...

    @abstractmethod
    async def shutdown_workers(self): ...

    @abstractmethod
    async def _consume(self) -> ManagerEvent: ...

    @abstractmethod
    async def _enqueue(self, task: Task) -> Task: ...

    @abstractmethod
    async def _requeue(self, task: Task): ...

    @abstractmethod
    async def get_health(self) -> dict[str, bool]: ...
