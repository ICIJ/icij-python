from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional, Union

from icij_worker import AsyncApp, ResultEvent, Task, TaskState
from icij_worker.namespacing import Namespacing
from icij_worker.objects import ErrorEvent, ProgressEvent


class TaskStorageConfig(ABC):
    @abstractmethod
    def to_storage(self, namespacing: Optional[Namespacing]) -> TaskStorage:
        pass


class TaskStorage(ABC):
    _namespacing: Namespacing

    @abstractmethod
    async def get_task(self, task_id: str) -> Task: ...

    @abstractmethod
    async def get_task_errors(self, task_id: str) -> List[ErrorEvent]: ...

    @abstractmethod
    async def get_task_result(self, task_id: str) -> ResultEvent: ...

    @abstractmethod
    async def get_task_namespace(self, task_id: str) -> Optional[str]: ...

    @abstractmethod
    async def get_tasks(
        self,
        namespace: Optional[str],
        *,
        task_name: Optional[str] = None,
        state: Optional[Union[List[TaskState], TaskState]] = None,
        **kwargs,
    ) -> List[Task]: ...

    async def _save_progress_event(self, event: ProgressEvent):
        # Might be better to be overridden to be performed in a transactional manner
        # when possible
        task = await self.get_task(event.task_id)
        task = task.as_resolved(event)
        namespace = await self.get_task_namespace(event.task_id)
        await self.save_task_(task, namespace=namespace)

    @abstractmethod
    async def save_task_(self, task: Task, namespace: Optional[str]) -> bool: ...

    @abstractmethod
    async def save_result(self, result: ResultEvent): ...

    @abstractmethod
    async def save_error(self, error: ErrorEvent): ...
