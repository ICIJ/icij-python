import asyncio
import functools
import logging
from abc import ABC, abstractmethod
from asyncio import Future
from functools import cached_property
from typing import List, Optional, final

from icij_common.pydantic_utils import safe_copy
from icij_worker import AsyncApp, ResultEvent, Task, TaskState
from icij_worker.exceptions import TaskAlreadyQueued
from icij_worker.namespacing import Namespacing
from icij_worker.objects import CancelledEvent, ErrorEvent, ManagerEvent, ProgressEvent
from icij_worker.task_storage import TaskStorage
from icij_worker.utils.asyncio_ import stop_other_tasks_when_exc

logger = logging.getLogger(__name__)


class TaskManager(TaskStorage, ABC):
    def __init__(self, app: AsyncApp):
        self._app = app
        self._loop = asyncio.get_event_loop()
        self._loops: List[Future] = []

    @final
    async def __aenter__(self):
        await self._aenter__()
        self._start_loops()

    async def _aenter__(self):
        pass

    @final
    async def __aexit__(self, exc_type, exc_value, tb):
        await self._aenter__()
        await self._stop_loops()

    async def _aexit__(self, exc_type, exc_val, exc_tb):
        pass

    @cached_property
    def late_ack(self) -> bool:
        return self._app.config.late_ack

    @cached_property
    def max_task_queue_size(self) -> int:
        return self._app.config.max_task_queue_size

    @cached_property
    def _namespacing(self) -> Namespacing:
        return self._app.namespacing

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
        namespace = await self.get_task_namespace(task.id)
        await self.save_task(queued, namespace)
        return queued

    @final
    async def requeue(self, task: Task):
        logger.info("requeing Task(id=%s)", task.id)
        update = {"state": TaskState.QUEUED, "progress": 0.0, "cancelled_at": None}
        updated = safe_copy(task, update=update)
        await self._requeue(updated)
        logger.info("Task(id=%s) requeued", updated.id)

    @final
    async def save_task(self, task: Task, namespace: Optional[str]) -> bool:
        if task.max_retries is None:
            max_retries = self._app.registry[task.name].max_retries
            task = safe_copy(task, update={"max_retries": max_retries})
        return await self.save_task_(task, namespace)

    async def _save_cancelled_event(self, event: CancelledEvent):
        task = await self.get_task(event.task_id)
        task = task.as_resolved(event)
        namespace = await self.get_task_namespace(event.task_id)
        if event.requeue and not self.late_ack:
            await self.requeue(task)
        await self.save_task(task, namespace=namespace)

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
        namespace = await self.get_task_namespace(task.id)
        await self.save_task(task, namespace)

    @final
    async def _save_error_event(self, error: ErrorEvent):
        # Update the task retries count
        task = await self.get_task(error.task_id)
        task = task.as_resolved(error)
        await self.save_error(error)
        if task.state is TaskState.QUEUED:
            await self.requeue(task)
        namespace = await self.get_task_namespace(task.id)
        await self.save_task(task, namespace)

    @abstractmethod
    async def cancel(self, task_id: str, *, requeue: bool): ...

    @abstractmethod
    async def _consume(self) -> ManagerEvent: ...

    @abstractmethod
    async def _enqueue(self, task: Task) -> Task: ...

    @abstractmethod
    async def _requeue(self, task: Task): ...

    def _start_loops(self):
        self._loops = [self.consume_events()]
        self._loops = [self._loop.create_task(t) for t in self._loops]
        callback = functools.partial(stop_other_tasks_when_exc, others=self._loops)
        for loop in self._loops:
            loop.add_done_callback(callback)

    async def _stop_loops(self):
        for loop in self._loops:
            loop.cancel()
        await asyncio.wait(self._loops, return_when=asyncio.ALL_COMPLETED)
        del self._loops
        self._loops = []
