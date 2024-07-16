import json
from contextlib import asynccontextmanager
from copy import deepcopy
from typing import AsyncGenerator, Dict, List, Optional, Tuple, Union

import itertools
import neo4j
from neo4j.exceptions import ResultNotSingleError

from icij_common.neo4j.constants import (
    TASK_ARGUMENTS,
    TASK_CANCEL_EVENT_CANCELLED_AT,
    TASK_CANCEL_EVENT_CREATED_AT_DEPRECATED,
    TASK_CANCEL_EVENT_NODE,
    TASK_CREATED_AT,
    TASK_ERROR_DETAIL_DEPRECATED,
    TASK_ERROR_ID,
    TASK_ERROR_MESSAGE,
    TASK_ERROR_NAME,
    TASK_ERROR_NODE,
    TASK_ERROR_OCCURRED_AT,
    TASK_ERROR_OCCURRED_TYPE,
    TASK_ERROR_STACKTRACE,
    TASK_ERROR_TITLE_DEPRECATED,
    TASK_HAS_RESULT_TYPE,
    TASK_ID,
    TASK_INPUTS_DEPRECATED,
    TASK_LOCK_NODE,
    TASK_LOCK_TASK_ID,
    TASK_LOCK_WORKER_ID,
    TASK_NAMESPACE,
    TASK_NODE,
    TASK_RESULT_NODE,
    TASK_RESULT_RESULT,
    TASK_TYPE,
)
from icij_common.neo4j.db import db_specific_session
from icij_common.neo4j.migrate import retrieve_dbs
from icij_common.pydantic_utils import jsonable_encoder
from icij_worker import Task, TaskError, TaskResult, TaskState
from icij_worker.exceptions import MissingTaskResult, UnknownTask
from icij_worker.task_storage import TaskStorage


class Neo4jStorage(TaskStorage):

    def __init__(self, driver: neo4j.AsyncDriver):
        self._driver = driver
        self._task_meta: Dict[str, Tuple[str, str]] = dict()

    async def get_task(self, task_id: str) -> Task:
        async with self._task_session(task_id) as sess:
            return await sess.execute_read(_get_task_tx, task_id=task_id)

    async def get_task_errors(self, task_id: str) -> List[TaskError]:
        async with self._task_session(task_id) as sess:
            recs = await sess.execute_read(_get_task_errors_tx, task_id=task_id)
        errors = [TaskError.from_neo4j(rec, task_id=task_id) for rec in recs]
        return errors

    async def get_task_result(self, task_id: str) -> TaskResult:
        async with self._task_session(task_id) as sess:
            return await sess.execute_read(_get_task_result_tx, task_id=task_id)

    async def get_tasks(
        self,
        namespace: Optional[str],
        *,
        task_type: Optional[str] = None,
        state: Optional[Union[List[TaskState], TaskState]] = None,
        **kwargs,
    ) -> List[Task]:
        db = self._namespacing.neo4j_db(namespace)
        async with self._db_session(db) as sess:
            recs = await _get_tasks(
                sess, state=state, task_type=task_type, namespace=namespace
            )
        tasks = [Task.from_neo4j(r) for r in recs]
        return tasks

    async def get_task_namespace(self, task_id: str) -> Optional[str]:
        if task_id not in self._task_meta:
            await self._refresh_task_meta()
        try:
            return self._task_meta[task_id][1]
        except KeyError as e:
            raise UnknownTask(task_id) from e

    async def save_task(self, task: Task, namespace: Optional[str]):
        db = self._namespacing.neo4j_db(namespace)
        async with self._db_session(db) as sess:
            task_props = task.dict(by_alias=True, exclude_unset=True)
            await sess.execute_write(
                _save_task_tx,
                task_id=task.id,
                task_props=task_props,
                namespace=namespace,
            )
        self._task_meta[task.id] = (db, namespace)

    async def save_result(self, result: TaskResult):
        async with self._task_session(result.task_id) as sess:
            res_str = json.dumps(jsonable_encoder(result.result))
            await sess.execute_write(
                _save_result_tx, task_id=result.task_id, result=res_str
            )

    async def save_error(self, error: TaskError):
        async with self._task_session(error.task_id) as sess:
            error_props = error.dict(by_alias=True)
            error_props.pop("@type")
            error_props["stacktrace"] = [
                json.dumps(item) for item in error_props["stacktrace"]
            ]
            await sess.execute_write(
                _save_error_tx, task_id=error.task_id, error_props=error_props
            )

    @asynccontextmanager
    async def _db_session(self, db: str) -> AsyncGenerator[neo4j.AsyncSession, None]:
        async with db_specific_session(self._driver, db) as sess:
            yield sess

    @asynccontextmanager
    async def _task_session(
        self, task_id: str
    ) -> AsyncGenerator[neo4j.AsyncSession, None]:
        db = await self._get_task_db(task_id)
        async with self._db_session(db) as sess:
            yield sess

    async def _get_task_db(self, task_id: str) -> str:
        if task_id not in self._task_meta:
            await self._refresh_task_meta()
        try:
            return self._task_meta[task_id][0]
        except KeyError as e:
            raise UnknownTask(task_id) from e

    async def _refresh_task_meta(self):
        dbs = await retrieve_dbs(self._driver)
        for db in dbs:
            async with self._db_session(db.name) as sess:
                # Here we make the assumption that task IDs are unique across
                # projects and not per project
                task_meta = {
                    meta["taskId"]: (db.name, meta["taskNs"])
                    for meta in await sess.execute_read(_get_tasks_meta_tx)
                }
                self._task_meta.update(task_meta)


async def _get_tasks_meta_tx(tx: neo4j.AsyncTransaction) -> List[neo4j.Record]:
    query = f"""MATCH (task:{TASK_NODE})
RETURN task.{TASK_ID} as taskId, task.{TASK_NAMESPACE} as taskNs"""
    res = await tx.run(query)
    meta = [rec async for rec in res]
    return meta


async def _save_task_tx(
    tx: neo4j.AsyncTransaction,
    *,
    task_id: str,
    task_props: Dict,
    namespace: Optional[str],
):
    query = f"MATCH (task:{TASK_NODE} {{{TASK_ID}: $taskId }}) RETURN task"
    res = await tx.run(query, taskId=task_id)
    existing = None
    task_props = deepcopy(task_props)
    task_props.pop("@type", None)
    try:
        existing = await res.single(strict=True)
    except ResultNotSingleError:
        task_props[TASK_ARGUMENTS] = json.dumps(task_props.get(TASK_ARGUMENTS, dict()))
        task_props[TASK_NAMESPACE] = namespace
    else:
        not_updatable = {
            f.alias
            for f in Task.non_updatable_fields  # pylint: disable=not-an-iterable
        }
        task_props = {p: v for p, v in task_props.items() if p not in not_updatable}
    if existing is not None and existing["task"]["namespace"] != namespace:
        msg = (
            f"DB task namespace ({existing['task']['namespace']}) differs from"
            f" save task namespace: {namespace}"
        )
        raise ValueError(msg)
    query = f"MERGE (task:{TASK_NODE} {{{TASK_ID}: $taskId }}) SET task += $taskProps"
    await tx.run(query, taskId=task_id, taskProps=task_props)


async def _save_result_tx(tx: neo4j.AsyncTransaction, *, task_id: str, result: str):
    query = f"""MATCH (task:{TASK_NODE} {{{TASK_ID}: $taskId }})
MERGE (task)-[:{TASK_HAS_RESULT_TYPE}]->(result:{TASK_RESULT_NODE})
ON CREATE SET result.{TASK_RESULT_RESULT} = $result
RETURN task, result"""
    res = await tx.run(query, taskId=task_id, result=result)
    records = [rec async for rec in res]
    summary = await res.consume()
    if not records:
        raise UnknownTask(task_id)
    if not summary.counters.relationships_created:
        msg = f"Attempted to save result for task {task_id} but found existing result"
        raise ValueError(msg)


async def _save_error_tx(
    tx: neo4j.AsyncTransaction, task_id: str, *, error_props: Dict
):
    query = f"""MATCH (t:{TASK_NODE} {{{TASK_ID}: $taskId }})
CREATE (error:{TASK_ERROR_NODE} $errorProps)-[:{TASK_ERROR_OCCURRED_TYPE}]->(task)
RETURN task, error"""
    res = await tx.run(query, taskId=task_id, errorProps=error_props)
    try:
        await res.single(strict=True)
    except ResultNotSingleError as e:
        raise UnknownTask(task_id) from e


async def add_support_for_async_task_tx(tx: neo4j.AsyncTransaction):
    constraint_query = f"""CREATE CONSTRAINT constraint_task_unique_id
IF NOT EXISTS 
FOR (task:{TASK_NODE})
REQUIRE (task.{TASK_ID}) IS UNIQUE"""
    await tx.run(constraint_query)
    created_at_query = f"""CREATE INDEX index_task_created_at IF NOT EXISTS
FOR (task:{TASK_NODE})
ON (task.{TASK_CREATED_AT})"""
    await tx.run(created_at_query)
    type_query = f"""CREATE INDEX index_task_type IF NOT EXISTS
FOR (task:{TASK_NODE})
ON (task.{TASK_TYPE})"""
    await tx.run(type_query)
    error_timestamp_query = f"""CREATE INDEX index_task_error_timestamp IF NOT EXISTS
FOR (task:{TASK_ERROR_NODE})
ON (task.{TASK_ERROR_OCCURRED_AT})"""
    await tx.run(error_timestamp_query)
    error_id_query = f"""CREATE CONSTRAINT constraint_task_error_unique_id IF NOT EXISTS
FOR (task:{TASK_ERROR_NODE})
REQUIRE (task.{TASK_ERROR_ID}) IS UNIQUE"""
    await tx.run(error_id_query)
    task_lock_task_id_query = f"""CREATE CONSTRAINT constraint_task_lock_unique_task_id
IF NOT EXISTS
FOR (lock:{TASK_LOCK_NODE})
REQUIRE (lock.{TASK_LOCK_TASK_ID}) IS UNIQUE"""
    await tx.run(task_lock_task_id_query)
    task_lock_worker_id_query = f"""CREATE INDEX index_task_lock_worker_id IF NOT EXISTS
FOR (lock:{TASK_LOCK_NODE})
ON (lock.{TASK_LOCK_WORKER_ID})"""
    await tx.run(task_lock_worker_id_query)


async def _get_tasks(
    sess: neo4j.AsyncSession,
    state: Optional[Union[List[TaskState], TaskState]],
    task_type: Optional[str],
    namespace: Optional[str],
) -> List[neo4j.Record]:
    if isinstance(state, TaskState):
        state = [state]
    if state is not None:
        state = [s.value for s in state]
    return await sess.execute_read(
        _get_tasks_tx, state=state, task_type=task_type, namespace=namespace
    )


async def _get_task_tx(tx: neo4j.AsyncTransaction, *, task_id: str) -> Task:
    query = f"MATCH (task:{TASK_NODE} {{ {TASK_ID}: $taskId }}) RETURN task"
    res = await tx.run(query, taskId=task_id)
    tasks = [Task.from_neo4j(t) async for t in res]
    if not tasks:
        raise UnknownTask(task_id)
    return tasks[0]


async def _get_tasks_tx(
    tx: neo4j.AsyncTransaction,
    state: Optional[List[str]],
    *,
    task_type: Optional[str],
    namespace: Optional[str],
) -> List[neo4j.Record]:
    where = ""
    if task_type:
        where = f"WHERE task.{TASK_TYPE} = $type"
    if namespace is not None:
        if not where:
            where = "WHERE "
        else:
            where += " AND "
        where += f"task.{TASK_NAMESPACE} = $namespace"
    all_labels = [(TASK_NODE,)]
    if isinstance(state, str):
        state = (state,)
    if state is not None:
        all_labels.append(tuple(state))
    all_labels = list(itertools.product(*all_labels))
    if all_labels:
        query = "UNION\n".join(
            f"""MATCH (task:{':'.join(labels)}) {where}
            RETURN task
            ORDER BY task.{TASK_CREATED_AT} DESC"""
            for labels in all_labels
        )
    else:
        query = f"""MATCH (task:{TASK_NODE})
RETURN task
ORDER BY task.{TASK_CREATED_AT} DESC"""
    res = await tx.run(query, type=task_type, namespace=namespace)
    recs = [rec async for rec in res]
    return recs


async def _get_task_errors_tx(
    tx: neo4j.AsyncTransaction, *, task_id: str
) -> List[neo4j.Record]:
    query = f"""MATCH (task:{TASK_NODE} {{ {TASK_ID}: $taskId }})
MATCH (error:{TASK_ERROR_NODE})-[:{TASK_ERROR_OCCURRED_TYPE}]->(task)
RETURN error
ORDER BY error.{TASK_ERROR_OCCURRED_AT} DESC
"""
    res = await tx.run(query, taskId=task_id)
    errors = [err async for err in res]
    return errors


async def _get_task_result_tx(
    tx: neo4j.AsyncTransaction, *, task_id: str
) -> TaskResult:
    query = f"""MATCH (task:{TASK_NODE} {{ {TASK_ID}: $taskId }})
MATCH (task)-[:{TASK_HAS_RESULT_TYPE}]->(result:{TASK_RESULT_NODE})
RETURN task, result
"""
    res = await tx.run(query, taskId=task_id)
    results = [TaskResult.from_neo4j(t) async for t in res]
    if not results:
        raise MissingTaskResult(task_id)
    return results[0]


async def migrate_task_errors_v0_tx(tx: neo4j.AsyncSession):
    query = f"""MATCH (error:{TASK_ERROR_NODE})
// We leave the stacktrace and cause empty
SET error.{TASK_ERROR_NAME} = error.{TASK_ERROR_TITLE_DEPRECATED},
    error.{TASK_ERROR_MESSAGE} = error.{TASK_ERROR_DETAIL_DEPRECATED},
    error.{TASK_ERROR_STACKTRACE} = []
REMOVE error.{TASK_ERROR_TITLE_DEPRECATED}, error.{TASK_ERROR_DETAIL_DEPRECATED}
RETURN error
"""
    await tx.run(query)


async def migrate_cancelled_event_created_at_v0_tx(tx: neo4j.AsyncSession):
    query = f"""MATCH (event:{TASK_CANCEL_EVENT_NODE})
SET event.{TASK_CANCEL_EVENT_CANCELLED_AT} 
    = event.{TASK_CANCEL_EVENT_CREATED_AT_DEPRECATED}
REMOVE event.{TASK_CANCEL_EVENT_CREATED_AT_DEPRECATED}
RETURN event
"""
    await tx.run(query)


async def migrate_add_index_to_task_namespace_v0_tx(tx: neo4j.AsyncSession):
    create_index = f"""
CREATE INDEX index_task_namespace IF NOT EXISTS
FOR (task:{TASK_NAMESPACE})
ON (task.{TASK_NAMESPACE})
"""
    await tx.run(create_index)


# pylint: disable=line-too-long
async def migrate_task_inputs_to_arguments_v0_tx(tx: neo4j.AsyncSession):
    query = f"""MATCH (task:{TASK_NODE})
SET task.{TASK_ARGUMENTS} = task.{TASK_INPUTS_DEPRECATED}
REMOVE task.{TASK_INPUTS_DEPRECATED}
RETURN task
"""
    await tx.run(query)


# pylint: disable=line-too-long
MIGRATIONS = {
    "migrate_task_errors_v0": migrate_task_errors_v0_tx,
    "migrate_cancelled_event_created_at_v0": migrate_cancelled_event_created_at_v0_tx,
    "migrate_add_index_to_task_namespace_v0_tx": migrate_add_index_to_task_namespace_v0_tx,
    "migrate_task_inputs_to_arguments_v0_tx": migrate_task_inputs_to_arguments_v0_tx,
}
