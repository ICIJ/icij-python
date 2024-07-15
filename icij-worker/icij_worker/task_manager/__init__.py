from abc import ABC, abstractmethod
from typing import List, Optional, Union, final

from icij_worker import Task, TaskError, TaskResult, TaskState
from icij_worker.namespacing import Namespacing


class TaskManager(ABC):
    def __init__(self, namespacing: Optional[Namespacing] = None):
        if namespacing is None:
            namespacing = Namespacing()
        self._namespacing = namespacing

    @final
    async def enqueue(self, task: Task, namespace: Optional[str], **kwargs) -> Task:
        if task.state is not TaskState.CREATED:
            msg = f"invalid state {task.state}, expected {TaskState.CREATED}"
            raise ValueError(msg)
        queued = await self._enqueue(task, namespace=namespace, **kwargs)
        if queued.state is not TaskState.QUEUED:
            msg = f"invalid state {queued.state}, expected {TaskState.QUEUED}"
            raise ValueError(msg)
        return queued

    @final
    async def cancel(self, *, task_id: str, requeue: bool):
        await self._cancel(task_id=task_id, requeue=requeue)

    @abstractmethod
    async def _enqueue(self, task: Task, namespace: Optional[str], **kwargs) -> Task:
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
        namespace: Optional[str],
        *,
        task_type: Optional[str] = None,
        state: Optional[Union[List[TaskState], TaskState]] = None,
        **kwargs,
    ) -> List[Task]:
        pass
