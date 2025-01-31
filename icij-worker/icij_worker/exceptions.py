# pylint: disable=multiple-statements
from abc import ABC
from typing import Optional, Sequence


class ICIJError(ABC): ...


class UnknownApp(ICIJError, ValueError, ABC): ...


class MaxRetriesExceeded(ICIJError, RuntimeError): ...


class RecoverableError(ICIJError, Exception): ...


class UnknownTask(ICIJError, ValueError):
    def __init__(self, task_id: str, worker_id: Optional[str] = None):
        msg = f'Unknown task "{task_id}"'
        if worker_id is not None:
            msg += f" for {worker_id}"
        super().__init__(msg)


class TaskQueueIsFull(ICIJError, RuntimeError):
    def __init__(self, max_queue_size: Optional[int]):
        msg = "task queue is full"
        if max_queue_size is not None:
            msg += f" ({max_queue_size}/{max_queue_size})"
        super().__init__(msg)


class TaskAlreadyCancelled(ICIJError, RuntimeError):
    def __init__(self, task_id: str):
        super().__init__(f'Task(id="{task_id}") has been cancelled')


class TaskAlreadyQueued(ICIJError, ValueError):
    def __init__(self, task_id: Optional[str] = None):
        msg = f'task "{task_id}" is already queued'
        super().__init__(msg)


class TaskAlreadyReserved(ICIJError, ValueError):
    def __init__(self, task_id: Optional[str] = None):
        msg = "task "
        if task_id is not None:
            msg += f'"{task_id}" '
        msg += "is already reserved"
        super().__init__(msg)


class UnregisteredTask(ICIJError, ValueError):
    def __init__(self, task_name: str, available_tasks: Sequence[str], *args, **kwargs):
        msg = (
            f'UnregisteredTask task "{task_name}", available tasks: {available_tasks}. '
            f"Task must be registered using the @task decorator."
        )
        super().__init__(msg, *args, **kwargs)


class MissingTaskResult(ICIJError, LookupError):
    def __init__(self, task_id: str):
        msg = f'Result of task "{task_id}" couldn\'t be found, did it complete ?'
        super().__init__(msg)


class WorkerTimeoutError(ICIJError, RuntimeError): ...


class DAGError(Exception):
    def __init__(self, dag_task_id: str, error: "ErrorEvent"):
        self.dag_task_id = dag_task_id
        self.error_source = error.task_id
        self.error = error
        msg = (
            f'Error occurred in Task(id="{self.error_source}") which belongs to the'
            f' DAG of Task(id="{self.dag_task_id}")'
        )
        super().__init__(msg)


class MessageDeserializationError(ICIJError, RuntimeError): ...
