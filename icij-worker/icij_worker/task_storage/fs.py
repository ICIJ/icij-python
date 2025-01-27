from __future__ import annotations

import functools
from collections import defaultdict
from contextlib import AsyncExitStack
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Optional, Type, Union

import ujson
from sqlitedict import SqliteDict

from icij_common.pydantic_utils import ICIJModel
from icij_worker import Task, TaskState
from icij_worker.dag.dag import TaskDAG
from icij_worker.exceptions import UnknownTask
from icij_worker.task_storage import TaskStorageConfig
from icij_worker.task_storage.key_value import DBItem, KeyValueStorage


class FSKeyValueStorageConfig(ICIJModel, TaskStorageConfig):
    db_path: Path

    def to_storage(self) -> FSKeyValueStorage:
        return FSKeyValueStorage(self.db_path)


class FSKeyValueStorage(KeyValueStorage):
    # Save each type in a different DB to speedup lookup
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

    def _key(self, task_id: str, obj_cls: Type) -> str:
        # Since each object type is saved in a different DB, we index by task ID
        return task_id

    async def get_tasks(
        self,
        group: Optional[str],
        *,
        task_name: Optional[str] = None,
        state: Optional[Union[List[TaskState], TaskState]] = None,
        **kwargs,
    ) -> List[Task]:
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
        task = [Task.parse_obj(t) for t in tasks]
        return task

    async def _save_dag_dependency(
        self, task_id: str, *, parent_id: str, provided_arg: str
    ):
        try:
            parents = await self._read_key(self._parents_db_name, key=task_id)
        except UnknownTask:
            parents = []
            await self._insert(self._parents_db_name, parents, key=task_id)
        parents.append([parent_id, provided_arg])
        await self._insert(self._parents_db_name, parents, key=task_id)

    async def _get_task_dag(self, task_id: str) -> Optional[TaskDAG]:
        unexplored = {task_id}
        graph = defaultdict(dict)
        seen = set()
        while unexplored:
            dag_relationships = []
            for t, parents in self._dbs[self._parents_db_name].items():
                if t in unexplored or any(p[0] in unexplored for p in parents):
                    dag_relationships += [(t, p) for p in parents]
            unexplored = set()
            if not dag_relationships:
                break
            for child_id, (parent_id, provided_arg) in dag_relationships:
                graph[child_id][parent_id] = provided_arg
                if child_id not in seen:
                    seen.add(child_id)
                    unexplored.add(child_id)
                if parent_id not in seen:
                    seen.add(parent_id)
                    unexplored.add(parent_id)
            unexplored = list(unexplored)
        if not graph:
            return None
        parents = set(p for graph_parents in graph.values() for p in graph_parents)
        all_tasks = set(graph.keys()).union(parents)
        sink = all_tasks - parents
        if len(sink) != 1:
            raise ValueError(f"Found several sinks in DAG {graph}: {sorted(sink)}")
        sink = next(iter(sink))
        dag = TaskDAG(dag_task_id=sink)
        for child, parents in graph.items():
            for parent, provided_arg in parents.items():
                dag.add_task_dep(child, parent_id=parent, provided_arg=provided_arg)
        dag.prepare()
        return dag

    def _make_db(self, filename: str, *, name: str) -> SqliteDict:
        return SqliteDict(
            filename,
            tablename=name,
            encode=self._encode,
            decode=self._decode,
            journal_mode="DEFAULT",
        )

    def _make_group_dbs(self) -> Dict[str, SqliteDict]:
        return {
            self._tasks_db_name: self._make_db(self._db_path, name=self._tasks_db_name),
            self._results_db_name: self._make_db(
                self._db_path, name=self._results_db_name
            ),
            self._errors_db_name: self._make_db(
                self._db_path, name=self._errors_db_name
            ),
            self._parents_db_name: self._make_db(
                self._db_path, name=self._parents_db_name
            ),
        }
