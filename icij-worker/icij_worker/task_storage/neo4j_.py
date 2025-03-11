import itertools
import json
import logging
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime
from typing import AsyncGenerator

import neo4j
from neo4j import AsyncTransaction
from neo4j.exceptions import ResultNotSingleError

from icij_common.neo4j_.db import db_specific_session, registry_db_session
from icij_common.neo4j_.migrate import retrieve_dbs
from icij_worker.constants import (
    NEO4J_SHUTDOWN_EVENT_CREATED_AT,
    NEO4J_SHUTDOWN_EVENT_NODE,
    NEO4J_TASK_ARGS,
    NEO4J_TASK_ARGUMENTS_DEPRECATED,
    NEO4J_TASK_CANCELLED_AT_DEPRECATED,
    NEO4J_TASK_CANCEL_EVENT_CANCELLED_AT,
    NEO4J_TASK_CANCEL_EVENT_CREATED_AT_DEPRECATED,
    NEO4J_TASK_CANCEL_EVENT_NODE,
    NEO4J_TASK_COMPLETED_AT,
    NEO4J_TASK_CREATED_AT,
    NEO4J_TASK_ERROR_DETAIL_DEPRECATED,
    NEO4J_TASK_ERROR_ID_DEPRECATED,
    NEO4J_TASK_ERROR_MESSAGE,
    NEO4J_TASK_ERROR_NAME,
    NEO4J_TASK_ERROR_NODE,
    NEO4J_TASK_ERROR_OCCURRED_AT_DEPRECATED,
    NEO4J_TASK_ERROR_OCCURRED_TYPE,
    NEO4J_TASK_ERROR_OCCURRED_TYPE_OCCURRED_AT,
    NEO4J_TASK_ERROR_OCCURRED_TYPE_RETRIES_LEFT,
    NEO4J_TASK_ERROR_STACKTRACE,
    NEO4J_TASK_ERROR_TITLE_DEPRECATED,
    NEO4J_TASK_GROUP,
    NEO4J_TASK_HAS_RESULT_TYPE,
    NEO4J_TASK_ID,
    NEO4J_TASK_INPUTS_DEPRECATED,
    NEO4J_TASK_LOCK_NODE,
    NEO4J_TASK_LOCK_TASK_ID,
    NEO4J_TASK_LOCK_WORKER_ID,
    NEO4J_TASK_MANAGER_EVENT_NODE,
    NEO4J_TASK_MANAGER_EVENT_NODE_CREATED_AT,
    NEO4J_TASK_MAX_RETRIES,
    NEO4J_TASK_NAME,
    NEO4J_TASK_NAMESPACE_DEPRECATED,
    NEO4J_TASK_NODE,
    NEO4J_TASK_PROGRESS,
    NEO4J_TASK_RESULT_NODE,
    NEO4J_TASK_RESULT_RESULT,
    NEO4J_TASK_RETRIES_DEPRECATED,
    NEO4J_TASK_RETRIES_LEFT,
    NEO4J_TASK_TYPE_DEPRECATED,
    NEO4J_WORKER_ID,
    NEO4J_WORKER_NODE,
)
from icij_worker.exceptions import MissingTaskResult, UnknownTask
from icij_worker.objects import ErrorEvent, ResultEvent, Task, TaskState, TaskUpdate
from icij_worker.task_storage import TaskStorage

logger = logging.getLogger(__name__)


class Neo4jStorage(TaskStorage):
    def __init__(self, driver: neo4j.AsyncDriver):
        self._driver = driver
        self._task_meta: dict[str, tuple[str, str]] = dict()

    async def get_task(self, task_id: str) -> Task:
        async with self._task_session(task_id) as sess:
            return await sess.execute_read(_get_task_tx, task_id=task_id)

    async def get_task_errors(self, task_id: str) -> list[ErrorEvent]:
        async with self._task_session(task_id) as sess:
            recs = await sess.execute_read(_get_task_errors_tx, task_id=task_id)
        errors = [ErrorEvent.from_neo4j(rec) for rec in recs]
        return errors

    async def get_task_result(self, task_id: str) -> ResultEvent:
        async with self._task_session(task_id) as sess:
            return await sess.execute_read(_get_task_result_tx, task_id=task_id)

    async def get_tasks(
        self,
        group: str | None,
        *,
        task_name: str | None = None,
        state: list[TaskState] | TaskState | None = None,
        **kwargs,
    ) -> list[Task]:
        db = self._routing_strategy.neo4j_db(group)
        async with self._db_session(db) as sess:
            recs = await _get_tasks(sess, state=state, task_name=task_name, group=group)
        tasks = [Task.from_neo4j(r) for r in recs]
        return tasks

    async def get_task_group(self, task_id: str) -> str | None:
        if task_id not in self._task_meta:
            await self._refresh_task_meta()
        try:
            return self._task_meta[task_id][1]
        except KeyError as e:
            raise UnknownTask(task_id) from e

    async def save_task_(self, task: Task, group: str | None) -> bool:
        db = self._routing_strategy.neo4j_db(group)
        async with self._db_session(db) as sess:
            task_props = task.model_dump(by_alias=True, exclude_unset=True)
            new_task = await sess.execute_write(
                _save_task_tx,
                task_id=task.id,
                task_props=task_props,
                group=group,
            )

        self._task_meta[task.id] = (db, group)
        return new_task

    async def save_result(self, result: ResultEvent):
        async with self._task_session(result.task_id) as sess:
            res_str = json.dumps(result.result.value)
            await sess.execute_write(
                _save_result_tx,
                task_id=result.task_id,
                result=res_str,
                completed_at=result.created_at,
            )

    async def save_error(self, error: ErrorEvent):
        async with self._task_session(error.task_id) as sess:
            error_props = error.error.model_dump(by_alias=True)
            error_props.pop("@type")
            error_props["stacktrace"] = [
                json.dumps(item) for item in error_props["stacktrace"]
            ]
            await sess.execute_write(
                _save_error_tx,
                task_id=error.task_id,
                error_props=error_props,
                retries_left=error.retries_left,
                created_at=error.created_at,
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

    async def get_health(self) -> bool:
        try:
            async with registry_db_session(self._driver) as sess:
                res = await sess.run("RETURN 1 AS health_check")
                await res.single()
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("neo4j health failed: %s", e)
            return False
        return True


async def _get_tasks_meta_tx(tx: neo4j.AsyncTransaction) -> list[neo4j.Record]:
    query = f"""MATCH (task:{NEO4J_TASK_NODE})
RETURN task.{NEO4J_TASK_ID} as taskId, task.{NEO4J_TASK_GROUP} as taskNs"""
    res = await tx.run(query)
    meta = [rec async for rec in res]
    return meta


async def _save_task_tx(
    tx: neo4j.AsyncTransaction,
    *,
    task_id: str,
    task_props: dict,
    group: str | None,
) -> bool:
    query = f"MATCH (task:{NEO4J_TASK_NODE} {{{NEO4J_TASK_ID}: $taskId }}) RETURN task"
    res = await tx.run(query, taskId=task_id)
    existing = None
    task_props = deepcopy(task_props)
    try:
        existing = await res.single(strict=True)
    except ResultNotSingleError:
        task_props[NEO4J_TASK_ARGS] = json.dumps(
            task_props.get(NEO4J_TASK_ARGS, dict())
        )
        task_props[NEO4J_TASK_GROUP] = group
    else:
        task_obj = {"id": task_id}
        task_obj.update(task_props)
        task_props = TaskUpdate.from_task(Task.model_validate(task_obj)).model_dump(
            by_alias=True, exclude_unset=True
        )
    task_props.pop("@type", None)
    if existing is not None and existing["task"]["group"] != group:
        msg = (
            f"DB task group ({existing['task']['group']}) differs from"
            f" save task group: {group}"
        )
        raise ValueError(msg)
    query = f"""MERGE (t:{NEO4J_TASK_NODE} {{{NEO4J_TASK_ID}: $taskId }})
SET t += $taskProps
WITH t
CALL apoc.create.setLabels(t, $labels) YIELD node AS task
RETURN task"""
    state = task_props.pop("state")
    labels = [NEO4J_TASK_NODE, state.value]
    await tx.run(query, taskId=task_id, taskProps=task_props, labels=labels)
    return existing is None


async def _save_result_tx(
    tx: neo4j.AsyncTransaction, *, task_id: str, result: str, completed_at: datetime
):
    query = f"""MATCH (t:{NEO4J_TASK_NODE} {{{NEO4J_TASK_ID}: $taskId }})
SET t.{NEO4J_TASK_PROGRESS} = 1.0, t.{NEO4J_TASK_COMPLETED_AT} = $completedAt
WITH t
CALL apoc.create.setLabels(t, $labels) YIELD node AS task
MERGE (task)-[:{NEO4J_TASK_HAS_RESULT_TYPE}]->(result:{NEO4J_TASK_RESULT_NODE})
ON CREATE SET result.{NEO4J_TASK_RESULT_RESULT} = $result
RETURN task, result"""
    labels = [NEO4J_TASK_NODE, TaskState.DONE.value]
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
    tx: neo4j.AsyncTransaction,
    task_id: str,
    *,
    error_props: dict,
    retries_left: int,
    created_at: datetime,
):
    query = f"""MATCH (task:{NEO4J_TASK_NODE} {{{NEO4J_TASK_ID}: $taskId }})
CREATE (error:{NEO4J_TASK_ERROR_NODE})-[rel:{NEO4J_TASK_ERROR_OCCURRED_TYPE}]->(task)
SET error += $errorProps,
    rel.{NEO4J_TASK_ERROR_OCCURRED_TYPE_OCCURRED_AT} = $occurredAt,
    rel.{NEO4J_TASK_ERROR_OCCURRED_TYPE_RETRIES_LEFT} = $retriesLeft  
RETURN task, error"""
    res = await tx.run(
        query,
        taskId=task_id,
        errorProps=error_props,
        retriesLeft=retries_left,
        occurredAt=created_at,
    )
    try:
        await res.single(strict=True)
    except ResultNotSingleError as e:
        raise UnknownTask(task_id) from e


async def add_support_for_async_task_tx(tx: neo4j.AsyncTransaction):
    constraint_query = f"""CREATE CONSTRAINT constraint_task_unique_id
IF NOT EXISTS 
FOR (task:{NEO4J_TASK_NODE})
REQUIRE (task.{NEO4J_TASK_ID}) IS UNIQUE"""
    await tx.run(constraint_query)
    created_at_query = f"""CREATE INDEX index_task_created_at IF NOT EXISTS
FOR (task:{NEO4J_TASK_NODE})
ON (task.{NEO4J_TASK_CREATED_AT})"""
    await tx.run(created_at_query)
    type_query = f"""CREATE INDEX index_task_name IF NOT EXISTS
FOR (task:{NEO4J_TASK_NODE})
ON (task.{NEO4J_TASK_NAME})"""
    await tx.run(type_query)
    error_timestamp_query = f"""CREATE INDEX index_task_error_timestamp IF NOT EXISTS
FOR (task:{NEO4J_TASK_ERROR_NODE})
ON (task.{NEO4J_TASK_ERROR_OCCURRED_AT_DEPRECATED})"""
    await tx.run(error_timestamp_query)
    error_id_query = f"""CREATE CONSTRAINT constraint_task_error_unique_id IF NOT EXISTS
FOR (task:{NEO4J_TASK_ERROR_NODE})
REQUIRE (task.{NEO4J_TASK_ERROR_ID_DEPRECATED}) IS UNIQUE"""
    await tx.run(error_id_query)
    task_lock_task_id_query = f"""CREATE CONSTRAINT constraint_task_lock_unique_task_id
IF NOT EXISTS
FOR (lock:{NEO4J_TASK_LOCK_NODE})
REQUIRE (lock.{NEO4J_TASK_LOCK_TASK_ID}) IS UNIQUE"""
    await tx.run(task_lock_task_id_query)
    task_lock_worker_id_query = f"""CREATE INDEX index_task_lock_worker_id IF NOT EXISTS
FOR (lock:{NEO4J_TASK_LOCK_NODE})
ON (lock.{NEO4J_TASK_LOCK_WORKER_ID})"""
    await tx.run(task_lock_worker_id_query)


async def _get_tasks(
    sess: neo4j.AsyncSession,
    state: list[TaskState] | TaskState | None,
    task_name: str | None,
    group: str | None,
) -> list[neo4j.Record]:
    if isinstance(state, TaskState):
        state = [state]
    if state is not None:
        state = [s.value for s in state]
    return await sess.execute_read(
        _get_tasks_tx, state=state, task_name=task_name, group=group
    )


async def _get_task_tx(tx: neo4j.AsyncTransaction, *, task_id: str) -> Task:
    query = f"MATCH (task:{NEO4J_TASK_NODE} {{ {NEO4J_TASK_ID}: $taskId }}) RETURN task"
    res = await tx.run(query, taskId=task_id)
    tasks = [Task.from_neo4j(t) async for t in res]
    if not tasks:
        raise UnknownTask(task_id)
    return tasks[0]


async def _get_tasks_tx(
    tx: neo4j.AsyncTransaction,
    state: list[str] | None,
    *,
    task_name: str | None,
    group: str | None,
) -> list[neo4j.Record]:
    where = ""
    if task_name:
        where = f"WHERE task.{NEO4J_TASK_NAME} = $type"
    if group is not None:
        if not where:
            where = "WHERE "
        else:
            where += " AND "
        where += f"task.{NEO4J_TASK_GROUP} = $group"
    all_labels = [(NEO4J_TASK_NODE,)]
    if isinstance(state, str):
        state = (state,)
    if state is not None:
        all_labels.append(tuple(state))
    all_labels = list(itertools.product(*all_labels))
    if all_labels:
        query = "UNION\n".join(
            f"""MATCH (task:{':'.join(labels)}) {where}
            RETURN task
            ORDER BY task.{NEO4J_TASK_CREATED_AT} DESC"""
            for labels in all_labels
        )
    else:
        query = f"""MATCH (task:{NEO4J_TASK_NODE})
RETURN task
ORDER BY task.{NEO4J_TASK_CREATED_AT} DESC"""
    res = await tx.run(query, type=task_name, group=group)
    recs = [rec async for rec in res]
    return recs


async def _get_task_errors_tx(
    tx: neo4j.AsyncTransaction, *, task_id: str
) -> list[neo4j.Record]:
    query = f"""MATCH (task:{NEO4J_TASK_NODE} {{ {NEO4J_TASK_ID}: $taskId }})
MATCH (error:{NEO4J_TASK_ERROR_NODE})-[rel:{NEO4J_TASK_ERROR_OCCURRED_TYPE}]->(task)
RETURN error, rel, task
ORDER BY rel.{NEO4J_TASK_ERROR_OCCURRED_TYPE_OCCURRED_AT} DESC
"""
    res = await tx.run(query, taskId=task_id)
    errors = [err async for err in res]
    return errors


async def _get_task_result_tx(
    tx: neo4j.AsyncTransaction, *, task_id: str
) -> ResultEvent:
    query = f"""MATCH (task:{NEO4J_TASK_NODE} {{ {NEO4J_TASK_ID}: $taskId }})
MATCH (task)-[:{NEO4J_TASK_HAS_RESULT_TYPE}]->(result:{NEO4J_TASK_RESULT_NODE})
RETURN task, result
"""
    res = await tx.run(query, taskId=task_id)
    results = [ResultEvent.from_neo4j(t) async for t in res]
    if not results:
        raise MissingTaskResult(task_id)
    return results[0]


async def migrate_task_errors_v0_tx(tx: neo4j.AsyncTransaction):
    query = f"""MATCH (error:{NEO4J_TASK_ERROR_NODE})
// We leave the stacktrace and cause empty
SET error.{NEO4J_TASK_ERROR_NAME} = error.{NEO4J_TASK_ERROR_TITLE_DEPRECATED},
    error.{NEO4J_TASK_ERROR_MESSAGE} = error.{NEO4J_TASK_ERROR_DETAIL_DEPRECATED},
    error.{NEO4J_TASK_ERROR_STACKTRACE} = []
REMOVE error.{NEO4J_TASK_ERROR_TITLE_DEPRECATED}, error.{NEO4J_TASK_ERROR_DETAIL_DEPRECATED}
RETURN error
"""
    await tx.run(query)


async def migrate_cancelled_event_created_at_v0_tx(tx: neo4j.AsyncTransaction):
    query = f"""MATCH (event:{NEO4J_TASK_CANCEL_EVENT_NODE})
SET event.{NEO4J_TASK_CANCEL_EVENT_CANCELLED_AT} 
    = event.{NEO4J_TASK_CANCEL_EVENT_CREATED_AT_DEPRECATED}
REMOVE event.{NEO4J_TASK_CANCEL_EVENT_CREATED_AT_DEPRECATED}
RETURN event
"""
    await tx.run(query)


async def migrate_add_index_to_task_namespace_v0_tx(tx: neo4j.AsyncTransaction):
    create_index = f"""
CREATE INDEX index_task_namespace IF NOT EXISTS
FOR (task:{NEO4J_TASK_NAMESPACE_DEPRECATED})
ON (task.{NEO4J_TASK_NAMESPACE_DEPRECATED})
"""
    await tx.run(create_index)


# pylint: disable=line-too-long
async def migrate_task_inputs_to_arguments_v0_tx(tx: neo4j.AsyncTransaction):
    query = f"""MATCH (task:{NEO4J_TASK_NODE})
SET task.{NEO4J_TASK_ARGS} = task.{NEO4J_TASK_INPUTS_DEPRECATED}
REMOVE task.{NEO4J_TASK_INPUTS_DEPRECATED}
RETURN task
"""
    await tx.run(query)


async def _rename_task_type_into_name_tx(tx: neo4j.AsyncTransaction):
    query = f"""MATCH (task:{NEO4J_TASK_NODE})
SET task.{NEO4J_TASK_NAME} = task.{NEO4J_TASK_TYPE_DEPRECATED}
REMOVE task.{NEO4J_TASK_TYPE_DEPRECATED}
RETURN task
"""
    await tx.run(query)


async def migrate_task_type_to_name_v0(sess: neo4j.AsyncSession):
    drop_index = "DROP INDEX index_task_type IF EXISTS"
    await sess.run(drop_index)
    create_index = f"""CREATE INDEX index_task_name IF NOT EXISTS
FOR (task:{NEO4J_TASK_NODE})
ON (task.{NEO4J_TASK_NAME})
"""
    await sess.run(create_index)
    await sess.execute_write(_rename_task_type_into_name_tx)


async def migrate_task_progress_v0_tx(tx: neo4j.AsyncTransaction):
    query = f"""MATCH (task:{NEO4J_TASK_NODE})
SET task.{NEO4J_TASK_PROGRESS} = toFloat(task.{NEO4J_TASK_PROGRESS}) / 100.0
RETURN task
"""
    await tx.run(query)


async def migrate_index_event_dates_v0_tx(tx: neo4j.AsyncTransaction):
    manager_event_query = f"""CREATE INDEX index_manager_events_created_at IF NOT EXISTS
FOR (event:{NEO4J_TASK_MANAGER_EVENT_NODE})
ON (event.{NEO4J_TASK_MANAGER_EVENT_NODE_CREATED_AT})"""
    await tx.run(manager_event_query)
    worker_event_query = f"""CREATE INDEX index_canceled_events_created_at IF NOT EXISTS
FOR (event:{NEO4J_TASK_CANCEL_EVENT_NODE})
ON (event.{NEO4J_TASK_CANCEL_EVENT_CANCELLED_AT})"""
    await tx.run(worker_event_query)


async def migrate_task_retries_and_error_v0_tx(
    tx: neo4j.AsyncTransaction,
):
    # Sadly, without the max retries save in DB, we can't compute the retries left, so
    # we just delete this attribute
    query = f"""MATCH (task:{NEO4J_TASK_NODE})
WHERE task.{NEO4J_TASK_RETRIES_LEFT} IS NULL
SET task.{NEO4J_TASK_MAX_RETRIES} = 3,
    task.{NEO4J_TASK_RETRIES_LEFT} = 3 - coalesce(task.{NEO4J_TASK_RETRIES_DEPRECATED}, 0)  
REMOVE task.{NEO4J_TASK_RETRIES_DEPRECATED}
RETURN task
"""
    await tx.run(query)
    query = f"""MATCH (error:{NEO4J_TASK_ERROR_NODE})-[rel:{NEO4J_TASK_ERROR_OCCURRED_TYPE}]-(task)
WHERE rel.{NEO4J_TASK_ERROR_OCCURRED_TYPE_OCCURRED_AT} IS NULL
SET rel.{NEO4J_TASK_ERROR_OCCURRED_TYPE_OCCURRED_AT} 
    = error.{NEO4J_TASK_ERROR_OCCURRED_AT_DEPRECATED},
    rel.{NEO4J_TASK_ERROR_OCCURRED_TYPE_RETRIES_LEFT} = 3
REMOVE error.{NEO4J_TASK_ERROR_OCCURRED_AT_DEPRECATED}, error.{NEO4J_TASK_ERROR_ID_DEPRECATED}
RETURN error
"""
    await tx.run(query)


async def migrate_task_arguments_into_args_v0_tx(
    tx: neo4j.AsyncTransaction,
):
    query = f"""MATCH (task:{NEO4J_TASK_NODE})
WHERE task.{NEO4J_TASK_ARGUMENTS_DEPRECATED} IS NOT NULL
SET task.{NEO4J_TASK_ARGS} = task.{NEO4J_TASK_ARGUMENTS_DEPRECATED}  
REMOVE task.{NEO4J_TASK_ARGUMENTS_DEPRECATED}
RETURN task
"""
    await tx.run(query)


async def migrate_task_namespace_into_group_v0(sess: neo4j.AsyncSession):
    drop_index = "DROP INDEX index_task_namespace IF EXISTS"
    await sess.run(drop_index)
    create_index = f"""CREATE INDEX index_task_group IF NOT EXISTS
FOR (task:{NEO4J_TASK_NODE})
ON (task.{NEO4J_TASK_GROUP})
"""
    await sess.run(create_index)
    await sess.execute_write(_rename_namespace_into_group_v0_tx)


async def _rename_namespace_into_group_v0_tx(tx: AsyncTransaction):
    query = f"""MATCH (task:{NEO4J_TASK_NODE})
WHERE task.{NEO4J_TASK_NAMESPACE_DEPRECATED} IS NOT NULL
SET task.{NEO4J_TASK_GROUP} = task.{NEO4J_TASK_NAMESPACE_DEPRECATED}  
REMOVE task.{NEO4J_TASK_NAMESPACE_DEPRECATED}
RETURN task
"""
    await tx.run(query)


async def migrate_add_task_shutdown_v0_tx(tx: neo4j.AsyncTransaction):
    create_worker_id_index = f"""
CREATE INDEX index_worker_id IF NOT EXISTS
FOR (worker:{NEO4J_WORKER_NODE})
ON (worker.{NEO4J_WORKER_ID})
"""
    await tx.run(create_worker_id_index)
    create_shutdown_index = f"""
CREATE INDEX index_shutdown_event_created_at IF NOT EXISTS
FOR (worker:{NEO4J_SHUTDOWN_EVENT_NODE})
ON (worker.{NEO4J_SHUTDOWN_EVENT_CREATED_AT})
"""
    await tx.run(create_shutdown_index)


async def migrate_rename_task_cancelled_at_into_created_at_v0_tx(
    tx: neo4j.AsyncTransaction,
):
    renamed_cancelled_at_into_created_at = f"""
MATCH (task:{NEO4J_TASK_NODE}:CANCELLED)
SET task.{NEO4J_TASK_COMPLETED_AT} = task.{NEO4J_TASK_CANCELLED_AT_DEPRECATED}
"""
    await tx.run(renamed_cancelled_at_into_created_at)

    delete_cancelled_at_prop = f"""
MATCH (task:{NEO4J_TASK_NODE})
WHERE task.{NEO4J_TASK_CANCELLED_AT_DEPRECATED} IS NOT NULL
REMOVE task.{NEO4J_TASK_CANCELLED_AT_DEPRECATED}
"""
    await tx.run(delete_cancelled_at_prop)


# pylint: disable=line-too-long
MIGRATIONS = [
    add_support_for_async_task_tx,
    migrate_task_errors_v0_tx,
    migrate_cancelled_event_created_at_v0_tx,
    migrate_add_index_to_task_namespace_v0_tx,
    migrate_task_inputs_to_arguments_v0_tx,
    migrate_task_type_to_name_v0,
    migrate_task_progress_v0_tx,
    migrate_index_event_dates_v0_tx,
    migrate_task_retries_and_error_v0_tx,
    migrate_task_arguments_into_args_v0_tx,
    migrate_task_namespace_into_group_v0,
    migrate_add_task_shutdown_v0_tx,
    migrate_rename_task_cancelled_at_into_created_at_v0_tx,
]
