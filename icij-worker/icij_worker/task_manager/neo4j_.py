import json
from datetime import datetime
from typing import List, Optional, Union

import itertools
import neo4j
from neo4j.exceptions import ConstraintError

from icij_common.neo4j.constants import (
    TASK_CANCELLED_BY_EVENT_REL,
    TASK_CANCEL_EVENT_CREATED_AT,
    TASK_CANCEL_EVENT_EFFECTIVE,
    TASK_CANCEL_EVENT_NODE,
    TASK_CANCEL_EVENT_REQUEUE,
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
    TASK_INPUTS,
    TASK_LOCK_NODE,
    TASK_LOCK_TASK_ID,
    TASK_LOCK_WORKER_ID,
    TASK_NODE,
    TASK_RESULT_NODE,
    TASK_TYPE,
)
from icij_worker import Task, TaskError, TaskResult, TaskStatus
from icij_worker.event_publisher.neo4j_ import Neo4jTaskNamespaceMixin
from icij_worker.exceptions import (
    MissingTaskResult,
    TaskAlreadyExists,
    TaskQueueIsFull,
    UnknownTask,
)
from icij_worker.task_manager import TaskManager


class Neo4JTaskManager(TaskManager, Neo4jTaskNamespaceMixin):
    def __init__(self, driver: neo4j.AsyncDriver, max_queue_size: int) -> None:
        super(TaskManager, self).__init__(driver)
        self._max_queue_size = max_queue_size

    @property
    def driver(self) -> neo4j.AsyncDriver:
        return self._driver

    async def get_task(self, *, task_id: str) -> Task:
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
        *,
        task_type: Optional[str] = None,
        status: Optional[Union[List[TaskStatus], TaskStatus]] = None,
        db: str,
        **kwargs,
    ) -> List[Task]:
        # pylint: disable=arguments-differ
        async with self._db_session(db) as sess:
            recs = await _get_tasks(sess, status=status, task_type=task_type)
        tasks = [Task.from_neo4j(r) for r in recs]
        return tasks

    async def _enqueue(self, task: Task, db: str, **kwargs) -> Task:
        # pylint: disable=arguments-differ
        self._task_dbs[task.id] = db
        async with self._db_session(db) as sess:
            inputs = json.dumps(task.inputs)
            return await sess.execute_write(
                _enqueue_task_tx,
                task_id=task.id,
                task_type=task.type,
                created_at=task.created_at,
                max_queue_size=self._max_queue_size,
                inputs=inputs,
            )

    async def _cancel(self, *, task_id: str, requeue: bool):
        async with self._task_session(task_id) as sess:
            await sess.execute_write(_cancel_task_tx, task_id=task_id, requeue=requeue)


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
    status: Optional[Union[List[TaskStatus], TaskStatus]],
    task_type: Optional[str],
) -> List[neo4j.Record]:
    if isinstance(status, TaskStatus):
        status = [status]
    if status is not None:
        status = [s.value for s in status]
    return await sess.execute_read(_get_tasks_tx, status=status, task_type=task_type)


async def _get_task_tx(tx: neo4j.AsyncTransaction, *, task_id: str) -> Task:
    query = f"MATCH (task:{TASK_NODE} {{ {TASK_ID}: $taskId }}) RETURN task"
    res = await tx.run(query, taskId=task_id)
    tasks = [Task.from_neo4j(t) async for t in res]
    if not tasks:
        raise UnknownTask(task_id)
    return tasks[0]


async def _get_tasks_tx(
    tx: neo4j.AsyncTransaction, status: Optional[List[str]], *, task_type: Optional[str]
) -> List[neo4j.Record]:
    where = ""
    if task_type:
        where = f"WHERE task.{TASK_TYPE} = $type"
    all_labels = [(TASK_NODE,)]
    if isinstance(status, str):
        status = (status,)
    if status is not None:
        all_labels.append(tuple(status))
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
    res = await tx.run(query, status=status, type=task_type)
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


async def _enqueue_task_tx(
    tx: neo4j.AsyncTransaction,
    *,
    task_id: str,
    task_type: str,
    created_at: datetime,
    inputs: str,
    max_queue_size: int,
) -> Task:
    count_query = f"""MATCH (task:{TASK_NODE}:`{TaskStatus.QUEUED.value}`)
RETURN count(task.id) AS nQueued
"""
    res = await tx.run(count_query)
    count = await res.single(strict=True)
    n_queued = count["nQueued"]
    if n_queued > max_queue_size:
        raise TaskQueueIsFull(max_queue_size)

    query = f"""CREATE (task:{TASK_NODE} {{ {TASK_ID}: $taskId }})
SET task:{TaskStatus.QUEUED.value},
    task.{TASK_TYPE} = $taskType,
    task.{TASK_INPUTS} = $inputs,
    task.{TASK_CREATED_AT} = $createdAt 
RETURN task
"""
    try:
        res = await tx.run(
            query,
            taskId=task_id,
            taskType=task_type,
            createdAt=created_at,
            inputs=inputs,
        )
        task = await res.single(strict=True)
    except ConstraintError as e:
        raise TaskAlreadyExists() from e
    return Task.from_neo4j(task)


async def _cancel_task_tx(tx: neo4j.AsyncTransaction, task_id: str, requeue: bool):
    query = f"""MATCH (task:{TASK_NODE} {{ {TASK_ID}: $taskId }})
CREATE (task)-[
    :{TASK_CANCELLED_BY_EVENT_REL}
]->(:{TASK_CANCEL_EVENT_NODE} {{ 
        {TASK_CANCEL_EVENT_CREATED_AT}: $createdAt, 
        {TASK_CANCEL_EVENT_EFFECTIVE}: false,
        {TASK_CANCEL_EVENT_REQUEUE}: $requeue
    }})
"""
    await tx.run(query, taskId=task_id, requeue=requeue, createdAt=datetime.now())


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
