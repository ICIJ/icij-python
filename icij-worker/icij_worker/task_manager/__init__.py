from abc import ABC, abstractmethod
from typing import List, Optional, Union, final

from icij_worker import Task, TaskError, TaskResult, TaskStatus


class TaskManager(ABC):
    @final
    async def enqueue(self, task: Task, **kwargs) -> Task:
        if task.status is not TaskStatus.CREATED:
            msg = f"invalid status {task.status}, expected {TaskStatus.CREATED}"
            raise ValueError(msg)
        queued = await self._enqueue(task, **kwargs)
        if queued.status is not TaskStatus.QUEUED:
            msg = f"invalid status {queued.status}, expected {TaskStatus.QUEUED}"
            raise ValueError(msg)
        return queued

    @final
    async def cancel(self, *, task_id: str, requeue: bool):
        await self._cancel(task_id=task_id, requeue=requeue)

    @abstractmethod
    async def _enqueue(self, task: Task, **kwargs) -> Task:
        pass

    @abstractmethod
    async def _cancel(self, *, task_id: str, requeue: bool) -> Task:
        pass

    @abstractmethod
    async def get_task(self, *, task_id: str) -> Task:
        pass

    @abstractmethod
    async def get_task_errors(self, task_id: str) -> List[TaskError]:
        pass

    @abstractmethod
    async def get_task_result(self, task_id: str) -> TaskResult:
        pass

    @abstractmethod
    async def get_tasks(
        self,
        *,
        task_type: Optional[str] = None,
        status: Optional[Union[List[TaskStatus], TaskStatus]] = None,
        **kwargs,
    ) -> List[Task]:
        pass
