from __future__ import annotations

import functools
import logging
from contextlib import AsyncExitStack
from copy import deepcopy
from pathlib import Path

import ujson
from pydantic import BaseModel
from sqlitedict import SqliteDict

from icij_common.pydantic_utils import icij_config
from icij_worker import Task, TaskState
from icij_worker.exceptions import UnknownTask
from icij_worker.task_storage import TaskStorageConfig
from icij_worker.task_storage.key_value import DBItem, KeyValueStorage

logger = logging.getLogger(__name__)


class FSKeyValueStorageConfig(BaseModel, TaskStorageConfig):
    model_config = icij_config()

    db_path: Path

    def to_storage(self) -> FSKeyValueStorage:
        return FSKeyValueStorage(self.db_path)


class FSKeyValueStorage(KeyValueStorage):
    # Save each type in a different DB to speedup lookup
    _tasks_db_name = "tasks"
    _results_db_name = "results"
    _errors_db_name = "errors"
    # pylint: disable=c-extension-no-member
    _encode = functools.partial(ujson.encode, default=str)
    _decode = functools.partial(ujson.decode)

    # pylint: enable=c-extension-no-member

    def __init__(self, db_path: Path):
        self._db_path = str(db_path)
        self._exit_stack = AsyncExitStack()
        self._dbs = dict()
        # TODO: add support for 1 DB / group
        self._dbs = self._make_group_dbs()

    async def __aenter__(self):
        for db in self._dbs.values():
            self._exit_stack.enter_context(db)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    async def _read_key(self, db: str, *, key: str):
        db = self._dbs[db]
        try:
            return db[key]
        except KeyError as e:
            raise UnknownTask(key) from e

    async def _insert(self, db: str, obj: DBItem, *, key: str):
        db = self._dbs[db]
        try:
            db[key] = obj
        except KeyError as e:
            raise UnknownTask(key) from e
        db.commit()

    async def _update(self, db: str, update: DBItem, *, key: str):
        db = self._dbs[db]
        try:
            task = db[key]
        except KeyError as e:
            raise UnknownTask(key) from e
        task.update(update)
        db[key] = task
        db.commit()

    async def _add_to_array(self, db: str, obj: DBItem, *, key: str):
        db = self._dbs[db]
        values = deepcopy(db.get(key, []))
        values.append(obj)
        db[key] = values
        db.commit()

    def _key(self, task_id: str, obj_cls: type) -> str:
        # Since each object type is saved in a different DB, we index by task ID
        return task_id

    async def get_tasks(
        self,
        group: str | None,
        *,
        task_name: str | None = None,
        state: list[TaskState] | TaskState | None = None,
        **kwargs,
    ) -> list[Task]:
        states = set()
        if state is not None:
            if isinstance(state, TaskState):
                states = {state}
            states = set(states)
        tasks = self._dbs[self._tasks_db_name].values()
        if group is not None:
            tasks = (t for t in tasks if t.get("group") == group)
        if task_name is not None:
            tasks = (t for t in tasks if t["name"] == task_name)
        if states:
            tasks = (t for t in tasks if t["state"] in states)
        tasks = list(tasks)
        for t in tasks:
            t.pop("group", None)
        task = [Task.model_validate(t) for t in tasks]
        return task

    def _make_db(self, filename: str, *, name: str) -> Sqlitedict:
        return SqliteDict(
            filename,
            tablename=name,
            encode=self._encode,
            decode=self._decode,
            journal_mode="DEFAULT",
        )

    def _make_group_dbs(self) -> dict[str, SqliteDict]:
        return {
            self._tasks_db_name: self._make_db(self._db_path, name=self._tasks_db_name),
            self._results_db_name: self._make_db(
                self._db_path, name=self._results_db_name
            ),
            self._errors_db_name: self._make_db(
                self._db_path, name=self._errors_db_name
            ),
        }

    async def get_health(self) -> bool:
        try:
            for db in self._dbs.values():
                len(db)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.exception("fs storage health failed: %s", e)
            return False
        return True
