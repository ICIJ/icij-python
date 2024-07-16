import json
from datetime import datetime
from typing import List, Optional, Union

import itertools
import neo4j
from neo4j.exceptions import ConstraintError

from icij_common.neo4j.constants import (
    TASK_ARGUMENTS,
    TASK_CANCELLED_BY_EVENT_REL,
    TASK_CANCEL_EVENT_CANCELLED_AT,
    TASK_CANCEL_EVENT_CREATED_AT_DEPRECATED,
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
    TASK_INPUTS_DEPRECATED,
    TASK_LOCK_NODE,
    TASK_LOCK_TASK_ID,
    TASK_LOCK_WORKER_ID,
    TASK_NAMESPACE,
    TASK_NODE,
    TASK_RESULT_NODE,
    TASK_TYPE,
)
from icij_worker import Namespacing, Task, TaskError, TaskResult, TaskState
from icij_worker.event_publisher.neo4j_ import Neo4jDBMixin
from icij_worker.exceptions import (
    MissingTaskResult,
    TaskAlreadyExists,
    TaskQueueIsFull,
    UnknownTask,
)
from icij_worker.task_manager import TaskManager


class Neo4JTaskManager(TaskManager, Neo4jDBMixin):
    def __init__(
        self,
        driver: neo4j.AsyncDriver,
        max_queue_size: int,
        namespacing: Optional[Namespacing] = None,
    ) -> None:
        super().__init__(namespacing)
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
        namespace: Optional[str],
        *,
        task_type: Optional[str] = None,
        state: Optional[Union[List[TaskState], TaskState]] = None,
        **kwargs,
    ) -> List[Task]:
        # pylint: disable=arguments-differ
        if namespace is None:
            msg = "namespace is mandatory to fetch the task from the appropriate DB"
            raise ValueError(msg)
        db = self._namespacing.neo4j_db(namespace)
        async with self._db_session(db) as sess:
            recs = await _get_tasks(sess, state=state, task_type=task_type)
        tasks = [Task.from_neo4j(r) for r in recs]
        return tasks

    async def _enqueue(self, task: Task, namespace: Optional[str], **kwargs) -> Task:
        # pylint: disable=arguments-differ
        db = self._namespacing.neo4j_db(namespace)
        ns_key = None
        if namespace is not None:
            ns_key = self._namespacing.namespace_to_db_key(namespace)
        self._task_dbs[task.id] = db
        async with self._db_session(db) as sess:
            inputs = json.dumps(task.arguments)
            return await sess.execute_write(
                _enqueue_task_tx,
                task_id=task.id,
                task_type=task.type,
                namespace_key=ns_key,
                created_at=task.created_at,
                max_queue_size=self._max_queue_size,
                arguments=inputs,
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
    state: Optional[Union[List[TaskState], TaskState]],
    task_type: Optional[str],
) -> List[neo4j.Record]:
    if isinstance(state, TaskState):
        state = [state]
    if state is not None:
        state = [s.value for s in state]
    return await sess.execute_read(_get_tasks_tx, state=state, task_type=task_type)


async def _get_task_tx(tx: neo4j.AsyncTransaction, *, task_id: str) -> Task:
    query = f"MATCH (task:{TASK_NODE} {{ {TASK_ID}: $taskId }}) RETURN task"
    res = await tx.run(query, taskId=task_id)
    tasks = [Task.from_neo4j(t) async for t in res]
    if not tasks:
        raise UnknownTask(task_id)
    return tasks[0]


async def _get_tasks_tx(
    tx: neo4j.AsyncTransaction, state: Optional[List[str]], *, task_type: Optional[str]
) -> List[neo4j.Record]:
    where = ""
    if task_type:
        where = f"WHERE task.{TASK_TYPE} = $type"
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
    res = await tx.run(query, state=state, type=task_type)
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
    namespace_key: str,
    created_at: datetime,
    arguments: str,
    max_queue_size: int,
) -> Task:
    count_query = f"""MATCH (task:{TASK_NODE}:`{TaskState.QUEUED.value}`)
RETURN count(task.id) AS nQueued
"""
    res = await tx.run(count_query)
    count = await res.single(strict=True)
    n_queued = count["nQueued"]
    if n_queued > max_queue_size:
        raise TaskQueueIsFull(max_queue_size)

    query = f"""CREATE (task:{TASK_NODE} {{ {TASK_ID}: $taskId }})
SET task:{TaskState.QUEUED.value},
    task.{TASK_TYPE} = $taskType,
    task.{TASK_NAMESPACE} = $namespaceKey,
    task.{TASK_ARGUMENTS} = $args,
    task.{TASK_CREATED_AT} = $createdAt
RETURN task
"""
    try:
        res = await tx.run(
            query,
            taskId=task_id,
            taskType=task_type,
            namespaceKey=namespace_key,
            createdAt=created_at,
            args=arguments,
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
        {TASK_CANCEL_EVENT_CANCELLED_AT}: $cancelledAt, 
        {TASK_CANCEL_EVENT_EFFECTIVE}: false,
        {TASK_CANCEL_EVENT_REQUEUE}: $requeue
    }})
"""
    await tx.run(query, taskId=task_id, requeue=requeue, cancelledAt=datetime.now())


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
