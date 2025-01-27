from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional, Union

from icij_worker.dag.dag import TaskDAG
from icij_worker import ResultEvent, Task, TaskState
from icij_worker.objects import ErrorEvent
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
    async def get_task_errors(self, task_id: str) -> List[ErrorEvent]: ...

    @abstractmethod
    async def get_task_result(self, task_id: str) -> ResultEvent: ...

    @abstractmethod
    async def get_task_group(self, task_id: str) -> Optional[str]: ...

    @abstractmethod
    async def get_tasks(
        self,
        group: Optional[str],
        *,
        task_name: Optional[str] = None,
        state: Optional[Union[List[TaskState], TaskState]] = None,
        **kwargs,
    ) -> List[Task]: ...

    @abstractmethod
    async def save_task_(self, task: Task, group: Optional[str]) -> bool: ...

    @abstractmethod
    async def save_result(self, result: ResultEvent): ...

    @abstractmethod
    async def save_error(self, error: ErrorEvent): ...


class DAGTaskStorage(TaskStorage):
    @abstractmethod
    async def _get_task_dag_(self, task_id: str) -> Optional[TaskDAG]:
        pass

    @abstractmethod
    async def _save_dag_dependency(
        self, task_id: str, *, parent_id: str, provided_arg: str
    ):
        pass
