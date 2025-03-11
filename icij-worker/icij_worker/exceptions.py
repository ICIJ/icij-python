# pylint: disable=multiple-statements
from abc import ABC
from typing import Sequence


class ICIJWorkerError(ABC): ...


class UnknownApp(ICIJWorkerError, ValueError, ABC): ...


class MaxRetriesExceeded(ICIJWorkerError, RuntimeError): ...


class RecoverableError(ICIJWorkerError, Exception): ...


class UnknownTask(ICIJWorkerError, ValueError):
    def __init__(self, task_id: str, worker_id: str | None = None):
        msg = f'Unknown task "{task_id}"'
        if worker_id is not None:
            msg += f" for {worker_id}"
        super().__init__(msg)


class TaskQueueIsFull(ICIJWorkerError, RuntimeError):
    def __init__(self, max_queue_size: int | None):
        msg = "task queue is full"
        if max_queue_size is not None:
            msg += f" ({max_queue_size}/{max_queue_size})"
        super().__init__(msg)


class TaskAlreadyCancelled(ICIJWorkerError, RuntimeError):
    def __init__(self, task_id: str):
        super().__init__(f'Task(id="{task_id}") has been cancelled')


class TaskAlreadyQueued(ICIJWorkerError, ValueError):
    def __init__(self, task_id: str | None = None):
        msg = f'task "{task_id}" is already queued'
        super().__init__(msg)


class TaskAlreadyReserved(ICIJWorkerError, ValueError):
    def __init__(self, task_id: str | None = None):
        msg = "task "
        if task_id is not None:
            msg += f'"{task_id}" '
        msg += "is already reserved"
        super().__init__(msg)


class UnregisteredTask(ICIJWorkerError, ValueError):
    def __init__(self, task_name: str, available_tasks: Sequence[str], *args, **kwargs):
        msg = (
            f'UnregisteredTask task "{task_name}", available tasks: {available_tasks}. '
            f"Task must be registered using the @task decorator."
        )
        super().__init__(msg, *args, **kwargs)


class MissingTaskResult(ICIJWorkerError, LookupError):
    def __init__(self, task_id: str):
        msg = f'Result of task "{task_id}" couldn\'t be found, did it complete ?'
        super().__init__(msg)


class WorkerTimeoutError(ICIJWorkerError, RuntimeError): ...


class MessageDeserializationError(ICIJWorkerError, RuntimeError): ...
