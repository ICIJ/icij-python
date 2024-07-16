from abc import ABC, abstractmethod
from typing import Optional, final

from icij_worker import Task, TaskError, TaskResult, TaskState
from icij_worker.exceptions import TaskAlreadyExists, UnknownTask
from icij_worker.namespacing import Namespacing
from icij_worker.task_storage import TaskStorage


class TaskManager(TaskStorage, ABC):
    def __init__(self, app_name: str, namespacing: Optional[Namespacing] = None):
        self._app_name = app_name
        if namespacing is None:
            namespacing = Namespacing()
        self._namespacing = namespacing

    @final
    async def enqueue(self, task: Task, namespace: Optional[str], **kwargs) -> Task:
        if task.state is not TaskState.CREATED:
            msg = f"invalid state {task.state}, expected {TaskState.CREATED}"
            raise ValueError(msg)
        try:
            await self.get_task(task.id)
            raise TaskAlreadyExists(task.id)
        except UnknownTask:
            pass
        await self.save_task(task, namespace)
        queued = await self._enqueue(task, **kwargs)
        if queued.state is not TaskState.QUEUED:
            msg = f"invalid state {queued.state}, expected {TaskState.QUEUED}"
            raise ValueError(msg)
        await self.save_task(queued, namespace)
        return queued

    @final
    async def cancel(self, *, task_id: str, requeue: bool):
        await self._cancel(task_id=task_id, requeue=requeue)

    @abstractmethod
    async def _enqueue(self, task: Task, **kwargs) -> Task:
        pass

    @abstractmethod
    async def _cancel(self, *, task_id: str, requeue: bool):
        pass
