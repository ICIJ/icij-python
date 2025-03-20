from abc import ABC, abstractmethod

from icij_worker import ResultEvent, Task, TaskError
from icij_worker.exceptions import UnknownTask
from icij_worker.objects import ErrorEvent, TaskUpdate
from icij_worker.task_storage import TaskStorage

DBItem = list | dict


class KeyValueStorage(TaskStorage, ABC):
    # Save each type in a different DB to speedup lookup, but that could be changed
    _tasks_db_name = "tasks"
    _results_db_name = "results"
    _errors_db_name = "errors"

    async def save_task_(self, task: Task, group: str | None) -> bool:
        """When possible override this to be transactional"""
        key = self._key(task.id, obj_cls=ResultEvent)
        new_task = False
        try:
            db_group = await self.get_task_group(task_id=task.id)
        except UnknownTask:
            new_task = True
            task = task.model_dump(exclude_unset=True)
            task["group"] = group
            await self._insert(self._tasks_db_name, task, key=key)
        else:
            if db_group != group:
                msg = (
                    f"DB task group ({db_group}) differs from"
                    f" save task group: {group}"
                )
                raise ValueError(msg)
            update = TaskUpdate.from_task(task).model_dump(exclude_none=True)
            await self._update(self._tasks_db_name, update, key=key)
        return new_task

    async def save_result(self, result: ResultEvent):
        res_key = self._key(result.task_id, obj_cls=ResultEvent)
        await self._insert(self._results_db_name, result.model_dump(), key=res_key)

    async def save_error(self, error: ErrorEvent):
        key = self._key(error.task_id, obj_cls=ErrorEvent)
        await self._add_to_array(self._errors_db_name, error.model_dump(), key=key)

    async def get_task(self, task_id: str) -> Task:
        key = self._key(task_id, obj_cls=Task)
        try:
            task = await self._read_key(self._tasks_db_name, key=key)
        except KeyError as e:
            raise UnknownTask(task_id) from e
        task.pop("group", None)
        return Task.model_validate(task)

    async def get_task_group(self, task_id: str) -> str | None:
        key = self._key(task_id, obj_cls=Task)
        try:
            task = await self._read_key(self._tasks_db_name, key=key)
        except KeyError as e:
            raise UnknownTask(task_id) from e
        group = task.get("group")
        return group

    async def get_task_errors(self, task_id: str) -> list[ErrorEvent]:
        key = self._key(task_id, obj_cls=TaskError)
        try:
            errors = await self._read_key(self._errors_db_name, key=key)
        except UnknownTask:
            return []
        errors = [ErrorEvent.model_validate(err) for err in errors]
        return errors

    async def get_task_result(self, task_id: str) -> ResultEvent:
        key = self._key(task_id, obj_cls=ResultEvent)
        try:
            result = await self._read_key(self._results_db_name, key=key)
        except KeyError as e:
            raise UnknownTask(task_id) from e
        return ResultEvent.model_validate(result)

    @abstractmethod
    async def _read_key(self, db: str, *, key: str) -> dict: ...

    @abstractmethod
    async def _insert(self, db: str, obj: DBItem, *, key: str) -> str: ...

    @abstractmethod
    async def _update(self, db: str, update: DBItem, *, key: str) -> str: ...

    @abstractmethod
    async def _add_to_array(self, db: str, obj: DBItem, *, key: str) -> str: ...

    @abstractmethod
    def _key(self, task_id: str, obj_cls: type) -> str: ...
