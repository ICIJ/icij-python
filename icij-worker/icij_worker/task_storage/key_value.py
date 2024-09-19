from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Type

from icij_worker import RoutingStrategy, Task, TaskError, ResultEvent
from icij_worker.exceptions import UnknownTask
from icij_worker.objects import ErrorEvent, TaskUpdate
from icij_worker.task_storage import TaskStorage


class KeyValueStorage(TaskStorage, ABC):
    # Save each type in a different DB to speedup lookup, but that could be changed
    _tasks_db_name = "tasks"
    _results_db_name = "results"
    _errors_db_name = "errors"
    _routing_strategy: RoutingStrategy

    async def save_task_(self, task: Task, group: Optional[str]) -> bool:
        """When possible override this to be transactional"""
        key = self._key(task.id, obj_cls=ResultEvent)
        new_task = False
        try:
            ns = await self.get_task_group(task_id=task.id)
        except UnknownTask:
            new_task = True
            task = task.dict(exclude_unset=True)
            task["group"] = group
            await self._insert(self._tasks_db_name, task, key=key)
        else:
            if ns != group:
                msg = f"DB task group ({ns}) differs from" f" save task group: {group}"
                raise ValueError(msg)
            update = TaskUpdate.from_task(task).dict(exclude_none=True)
            await self._update(self._tasks_db_name, update, key=key)
        return new_task

    async def save_result(self, result: ResultEvent):
        res_key = self._key(result.task_id, obj_cls=ResultEvent)
        await self._insert(self._results_db_name, result.dict(), key=res_key)

    async def save_error(self, error: ErrorEvent):
        key = self._key(error.task_id, obj_cls=ErrorEvent)
        await self._add_to_array(self._errors_db_name, error.dict(), key=key)

    async def get_task(self, task_id: str) -> Task:
        key = self._key(task_id, obj_cls=Task)
        try:
            task = await self._read_key(self._tasks_db_name, key=key)
        except KeyError as e:
            raise UnknownTask(task_id) from e
        task.pop("group", None)
        return Task.parse_obj(task)

    async def get_task_group(self, task_id: str) -> Optional[str]:
        key = self._key(task_id, obj_cls=Task)
        try:
            task = await self._read_key(self._tasks_db_name, key=key)
        except KeyError as e:
            raise UnknownTask(task_id) from e
        group = task.get("group")
        return group

    async def get_task_errors(self, task_id: str) -> List[ErrorEvent]:
        key = self._key(task_id, obj_cls=TaskError)
        try:
            errors = await self._read_key(self._errors_db_name, key=key)
        except UnknownTask:
            return []
        errors = [TaskError.parse_obj(err) for err in errors]
        return errors

    async def get_task_result(self, task_id: str) -> ResultEvent:
        key = self._key(task_id, obj_cls=ResultEvent)
        try:
            result = await self._read_key(self._results_db_name, key=key)
        except KeyError as e:
            raise UnknownTask(task_id) from e
        return ResultEvent.parse_obj(result)

    @abstractmethod
    async def _read_key(self, db: str, *, key: str) -> Dict: ...

    @abstractmethod
    async def _insert(self, db: str, obj: Dict, *, key: str) -> str: ...

    @abstractmethod
    async def _update(self, db: str, update: Dict, *, key: str) -> str: ...

    @abstractmethod
    async def _add_to_array(self, db: str, obj: Dict, *, key: str) -> str: ...

    @abstractmethod
    def _key(self, task_id: str, obj_cls: Type) -> str: ...
