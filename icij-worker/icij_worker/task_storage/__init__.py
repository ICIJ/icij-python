from abc import ABC, abstractmethod
from typing import List, Optional, Union

from icij_worker import Task, TaskError, TaskEvent, TaskResult, TaskState
from icij_worker.namespacing import Namespacing


class TaskStorage(ABC):
    _namespacing: Namespacing

    @abstractmethod
    async def get_task(self, task_id: str) -> Task: ...

    @abstractmethod
    async def get_task_errors(self, task_id: str) -> List[TaskError]: ...

    @abstractmethod
    async def get_task_result(self, task_id: str) -> TaskResult: ...

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

    @abstractmethod
    async def save_task(self, task: Task, namespace: Optional[str]) -> bool: ...

    async def save_event(self, event: TaskEvent):
        # Might be better to be overridden to be performed in a transactional manner
        # when possible
        task = await self.get_task(event.task_id)
        update = task.resolve_event(event)
        task_as_dict = task.dict()
        task_as_dict.update(update)
        updated = Task.parse_obj(task_as_dict)
        namespace = await self.get_task_namespace(event.task_id)
        await self.save_task(updated, namespace=namespace)

    @abstractmethod
    async def save_result(self, result: TaskResult): ...

    @abstractmethod
    async def save_error(self, error: TaskError): ...
