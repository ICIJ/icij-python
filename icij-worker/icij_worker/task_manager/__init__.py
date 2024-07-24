from abc import ABC, abstractmethod
from typing import Optional, final

from icij_worker import Task, TaskState
from icij_worker.exceptions import TaskAlreadyQueued, UnknownTask
from icij_worker.namespacing import Namespacing
from icij_worker.task_storage import TaskStorage


# TODO: make this one more than a simple ABC/interface and implement some logic in
# it so that the codebase is testable more easily (just like the Worker class). For
# instance there could be a share consume_results_and_errors, consume_event, in order
# to factorize the ways they are saved/recorded
class TaskManager(TaskStorage, ABC):
    def __init__(
        self,
        app_name: str,
        max_task_queue_size: Optional[int],
        namespacing: Optional[Namespacing] = None,
    ):
        self._app_name = app_name
        self._max_task_queue_size = max_task_queue_size
        if namespacing is None:
            namespacing = Namespacing()
        self._namespacing = namespacing

    @final
    async def enqueue(self, task: Task, namespace: Optional[str], **kwargs) -> Task:
        if task.state is not TaskState.CREATED:
            msg = f"invalid state {task.state}, expected {TaskState.CREATED}"
            raise ValueError(msg)
        try:
            task = await self.get_task(task.id)
            if task.state is TaskState.QUEUED:
                raise TaskAlreadyQueued(task.id)
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
    async def cancel(self, task_id: str, *, requeue: bool):
        await self._cancel(task_id=task_id, requeue=requeue)

    @abstractmethod
    async def _enqueue(self, task: Task, **kwargs) -> Task:
        pass

    @abstractmethod
    async def _cancel(self, *, task_id: str, requeue: bool):
        pass
