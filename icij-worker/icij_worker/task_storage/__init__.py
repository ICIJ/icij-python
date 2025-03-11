from __future__ import annotations

from abc import ABC, abstractmethod

from icij_worker import ResultEvent, Task, TaskState
from icij_worker.objects import ErrorEvent, ProgressEvent
from icij_worker.routing_strategy import RoutingStrategy


class TaskStorageConfig(ABC):
    @abstractmethod
    def to_storage(self) -> TaskStorage:
        pass


class TaskStorage(ABC):
    _routing_strategy: RoutingStrategy

    @abstractmethod
    async def get_task(self, task_id: str) -> Task: ...

    @abstractmethod
    async def get_task_errors(self, task_id: str) -> list[ErrorEvent]: ...

    @abstractmethod
    async def get_task_result(self, task_id: str) -> ResultEvent: ...

    @abstractmethod
    async def get_task_group(self, task_id: str) -> str | None: ...

    @abstractmethod
    async def get_tasks(
        self,
        group: str | None,
        *,
        task_name: str | None = None,
        state: list[TaskState] | TaskState | None = None,
        **kwargs,
    ) -> list[Task]: ...

    async def _save_progress_event(self, event: ProgressEvent):
        # Might be better to be overridden to be performed in a transactional manner
        # when possible
        task = await self.get_task(event.task_id)
        task = task.as_resolved(event)
        group = await self.get_task_group(event.task_id)
        await self.save_task_(task, group=group)

    @abstractmethod
    async def save_task_(self, task: Task, group: str | None) -> bool: ...

    @abstractmethod
    async def save_result(self, result: ResultEvent): ...

    @abstractmethod
    async def save_error(self, error: ErrorEvent): ...

    @abstractmethod
    async def get_health(self) -> bool: ...
