import json
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime
from typing import AsyncGenerator, Dict, List, Optional, Tuple, Union

import itertools
import neo4j
from neo4j.exceptions import ResultNotSingleError

from icij_common.neo4j.constants import (
    TASK_ARGUMENTS,
    TASK_CANCEL_EVENT_CANCELLED_AT,
    TASK_CANCEL_EVENT_CREATED_AT_DEPRECATED,
    TASK_CANCEL_EVENT_NODE,
    TASK_COMPLETED_AT,
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
    TASK_NAME,
    TASK_NAMESPACE,
    TASK_NODE,
    TASK_PROGRESS,
    TASK_RESULT_NODE,
    TASK_RESULT_RESULT,
    TASK_TYPE_DEPRECATED,
)
from icij_common.neo4j.db import db_specific_session
from icij_common.neo4j.migrate import retrieve_dbs
from icij_common.pydantic_utils import jsonable_encoder
from icij_worker import Task, TaskError, TaskResult, TaskState
from icij_worker.exceptions import MissingTaskResult, UnknownTask
from icij_worker.objects import TaskUpdate
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
        task_name: Optional[str] = None,
        state: Optional[Union[List[TaskState], TaskState]] = None,
        **kwargs,
    ) -> List[Task]:
        db = self._namespacing.neo4j_db(namespace)
        async with self._db_session(db) as sess:
            recs = await _get_tasks(
                sess, state=state, task_name=task_name, namespace=namespace
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

    async def save_task(self, task: Task, namespace: Optional[str]) -> bool:
        db = self._namespacing.neo4j_db(namespace)
        async with self._db_session(db) as sess:
            task_props = task.dict(by_alias=True, exclude_unset=True)
            new_task = await sess.execute_write(
                _save_task_tx,
                task_id=task.id,
                task_props=task_props,
                namespace=namespace,
            )
        self._task_meta[task.id] = (db, namespace)
        return new_task

    async def save_result(self, result: TaskResult):
        async with self._task_session(result.task_id) as sess:
            res_str = json.dumps(jsonable_encoder(result.result))
            await sess.execute_write(
                _save_result_tx,
                task_id=result.task_id,
                result=res_str,
                completed_at=result.completed_at,
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
) -> bool:
    query = f"MATCH (task:{TASK_NODE} {{{TASK_ID}: $taskId }}) RETURN task"
    res = await tx.run(query, taskId=task_id)
    existing = None
    task_props = deepcopy(task_props)
    try:
        existing = await res.single(strict=True)
    except ResultNotSingleError:
        task_props[TASK_ARGUMENTS] = json.dumps(task_props.get(TASK_ARGUMENTS, dict()))
        task_props[TASK_NAMESPACE] = namespace
    else:
        task_obj = {"id": task_id}
        task_obj.update(task_props)
        task_props = TaskUpdate.from_task(Task.parse_obj(task_obj)).dict(
            by_alias=True, exclude_unset=True
        )
    task_props.pop("@type", None)
    if existing is not None and existing["task"]["namespace"] != namespace:
        msg = (
            f"DB task namespace ({existing['task']['namespace']}) differs from"
            f" save task namespace: {namespace}"
        )
        raise ValueError(msg)
    query = f"MERGE (task:{TASK_NODE} {{{TASK_ID}: $taskId }}) SET task += $taskProps"
    await tx.run(query, taskId=task_id, taskProps=task_props)
    return existing is None


async def _save_result_tx(
    tx: neo4j.AsyncTransaction, *, task_id: str, result: str, completed_at: datetime
):
    query = f"""MATCH (t:{TASK_NODE} {{{TASK_ID}: $taskId }})
SET t.{TASK_PROGRESS} = 1.0, t.{TASK_COMPLETED_AT} = $completedAt
WITH t
CALL apoc.create.setLabels(t, $labels) YIELD node AS task
MERGE (task)-[:{TASK_HAS_RESULT_TYPE}]->(result:{TASK_RESULT_NODE})
ON CREATE SET result.{TASK_RESULT_RESULT} = $result
RETURN task, result"""
    labels = [TASK_NODE, TaskState.DONE.value]
    res = await tx.run(
        query, taskId=task_id, result=result, labels=labels, completedAt=completed_at
    )
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
    query = f"""MATCH (task:{TASK_NODE} {{{TASK_ID}: $taskId }})
MERGE (error:{TASK_ERROR_NODE} {{ {TASK_ERROR_ID}: $errorId }} )-[
    :{TASK_ERROR_OCCURRED_TYPE}]->(task)
SET error += $errorProps
RETURN task, error"""
    error_id = error_props.pop(TASK_ERROR_ID)
    res = await tx.run(query, taskId=task_id, errorProps=error_props, errorId=error_id)
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
    type_query = f"""CREATE INDEX index_task_name IF NOT EXISTS
FOR (task:{TASK_NODE})
ON (task.{TASK_NAME})"""
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
    task_name: Optional[str],
    namespace: Optional[str],
) -> List[neo4j.Record]:
    if isinstance(state, TaskState):
        state = [state]
    if state is not None:
        state = [s.value for s in state]
    return await sess.execute_read(
        _get_tasks_tx, state=state, task_name=task_name, namespace=namespace
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
    task_name: Optional[str],
    namespace: Optional[str],
) -> List[neo4j.Record]:
    where = ""
    if task_name:
        where = f"WHERE task.{TASK_NAME} = $type"
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
    res = await tx.run(query, type=task_name, namespace=namespace)
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


async def migrate_task_errors_v0_tx(tx: neo4j.AsyncTransaction):
    query = f"""MATCH (error:{TASK_ERROR_NODE})
// We leave the stacktrace and cause empty
SET error.{TASK_ERROR_NAME} = error.{TASK_ERROR_TITLE_DEPRECATED},
    error.{TASK_ERROR_MESSAGE} = error.{TASK_ERROR_DETAIL_DEPRECATED},
    error.{TASK_ERROR_STACKTRACE} = []
REMOVE error.{TASK_ERROR_TITLE_DEPRECATED}, error.{TASK_ERROR_DETAIL_DEPRECATED}
RETURN error
"""
    await tx.run(query)


async def migrate_cancelled_event_created_at_v0_tx(tx: neo4j.AsyncTransaction):
    query = f"""MATCH (event:{TASK_CANCEL_EVENT_NODE})
SET event.{TASK_CANCEL_EVENT_CANCELLED_AT} 
    = event.{TASK_CANCEL_EVENT_CREATED_AT_DEPRECATED}
REMOVE event.{TASK_CANCEL_EVENT_CREATED_AT_DEPRECATED}
RETURN event
"""
    await tx.run(query)


async def migrate_add_index_to_task_namespace_v0_tx(tx: neo4j.AsyncTransaction):
    create_index = f"""
CREATE INDEX index_task_namespace IF NOT EXISTS
FOR (task:{TASK_NAMESPACE})
ON (task.{TASK_NAMESPACE})
"""
    await tx.run(create_index)


# pylint: disable=line-too-long
async def migrate_task_inputs_to_arguments_v0_tx(tx: neo4j.AsyncTransaction):
    query = f"""MATCH (task:{TASK_NODE})
SET task.{TASK_ARGUMENTS} = task.{TASK_INPUTS_DEPRECATED}
REMOVE task.{TASK_INPUTS_DEPRECATED}
RETURN task
"""
    await tx.run(query)


async def _rename_task_type_into_name_tx(tx: neo4j.AsyncTransaction):
    query = f"""MATCH (task:{TASK_NODE})
SET task.{TASK_NAME} = task.{TASK_TYPE_DEPRECATED}
REMOVE task.{TASK_TYPE_DEPRECATED}
RETURN task
"""
    await tx.run(query)


async def migrate_task_type_to_name_v0(sess: neo4j.AsyncSession):
    drop_index = "DROP INDEX index_task_tyoe IF EXISTS"
    await sess.run(drop_index)
    create_index = f"""CREATE INDEX index_task_name IF NOT EXISTS
FOR (task:{TASK_NODE})
ON (task.{TASK_NAME})
"""
    await sess.run(create_index)
    await sess.execute_write(_rename_task_type_into_name_tx)


async def migrate_task_progress_v0_tx(tx: neo4j.AsyncTransaction):
    query = f"""MATCH (task:{TASK_NODE})
SET task.{TASK_PROGRESS} = toFloat(task.{TASK_PROGRESS}) / 100.0
RETURN task
"""
    await tx.run(query)


# pylint: disable=line-too-long
MIGRATIONS = {
    "add_support_for_async_task_tx": add_support_for_async_task_tx,
    "migrate_task_errors_v0": migrate_task_errors_v0_tx,
    "migrate_cancelled_event_created_at_v0": migrate_cancelled_event_created_at_v0_tx,
    "migrate_add_index_to_task_namespace_v0_tx": migrate_add_index_to_task_namespace_v0_tx,
    "migrate_task_inputs_to_arguments_v0_tx": migrate_task_inputs_to_arguments_v0_tx,
    "migrate_task_type_to_name_v0": migrate_task_type_to_name_v0,
    "migrate_task_progress_v0_tx": migrate_task_progress_v0_tx,
}
