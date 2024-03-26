from abc import ABC, abstractmethod
from typing import List, Optional, Union, final

from icij_worker import Task, TaskError, TaskResult, TaskStatus


class TaskManager(ABC):
    @final
    async def enqueue(self, task: Task, project: str) -> Task:
        if task.status is not TaskStatus.CREATED:
            msg = f"invalid status {task.status}, expected {TaskStatus.CREATED}"
            raise ValueError(msg)
        task = await self._enqueue(task, project)
        if task.status is not TaskStatus.QUEUED:
            msg = f"invalid status {task.status}, expected {TaskStatus.QUEUED}"
            raise ValueError(msg)
        return task

    @final
    async def cancel(self, *, task_id: str, project: str, requeue: bool):
        await self._cancel(task_id=task_id, project=project, requeue=requeue)

    @abstractmethod
    async def _enqueue(self, task: Task, project: str) -> Task:
        pass

    @abstractmethod
    async def _cancel(self, *, task_id: str, project: str, requeue: bool) -> Task:
        pass

    @abstractmethod
    async def get_task(self, *, task_id: str, project: str) -> Task:
        pass

    @abstractmethod
    async def get_task_errors(self, task_id: str, project: str) -> List[TaskError]:
        pass

    @abstractmethod
    async def get_task_result(self, task_id: str, project: str) -> TaskResult:
        pass

    @abstractmethod
    async def get_tasks(
        self,
        project: str,
        task_type: Optional[str] = None,
        status: Optional[Union[List[TaskStatus], TaskStatus]] = None,
    ) -> List[Task]:
        pass
