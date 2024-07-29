import json
from datetime import datetime
from typing import Optional, cast

import neo4j
from neo4j.exceptions import ResultNotSingleError

from icij_common.neo4j.constants import (
    TASK_CANCELLED_BY_EVENT_REL,
    TASK_CANCEL_EVENT_CANCELLED_AT,
    TASK_CANCEL_EVENT_EFFECTIVE,
    TASK_CANCEL_EVENT_NODE,
    TASK_CANCEL_EVENT_REQUEUE,
    TASK_ID,
    TASK_LOCK_NODE,
    TASK_LOCK_TASK_ID,
    TASK_LOCK_WORKER_ID,
    TASK_MANAGER_EVENT_EVENT,
    TASK_MANAGER_EVENT_NODE,
    TASK_MANAGER_EVENT_NODE_CREATED_AT,
    TASK_NODE,
    TASK_PROGRESS,
    TASK_RETRIES,
)
from icij_worker import AsyncApp, Task, TaskState
from icij_worker.exceptions import TaskQueueIsFull, UnknownTask
from icij_worker.objects import ManagerEvent, Message
from icij_worker.task_manager import TaskManager
from icij_worker.task_storage.neo4j_ import Neo4jStorage
from icij_worker.utils.neo4j_ import Neo4jConsumerMixin


class Neo4JTaskManager(TaskManager, Neo4jConsumerMixin, Neo4jStorage):

    def __init__(
        self,
        app: AsyncApp,
        driver: neo4j.AsyncDriver,
        event_refresh_interval_s: float = 0.1,
    ) -> None:
        super().__init__(app)
        super(Neo4jConsumerMixin, self).__init__(driver)
        self._event_refresh_interval_s = event_refresh_interval_s

    @property
    def driver(self) -> neo4j.AsyncDriver:
        return self._driver

    async def _enqueue(self, task: Task):
        # pylint: disable=arguments-differ
        db = await self._get_task_db(task_id=task.id)
        async with self._db_session(db) as sess:
            await sess.execute_write(
                _enqueue_task_tx,
                task_id=task.id,
                max_queue_size=self.max_task_queue_size,
            )

    async def _requeue(self, task: Task):
        # pylint: disable=arguments-differ
        db = await self._get_task_db(task_id=task.id)
        async with self._db_session(db) as sess:
            return await sess.execute_write(
                _requeue_task_tx,
                task_id=task.id,
                retries=task.retries,
                max_queue_size=self.max_task_queue_size,
            )

    async def _consume(self) -> ManagerEvent:
        event_as_json = await self._consume_(
            _consume_manager_events_tx,
            refresh_interval_s=self._event_refresh_interval_s,
            db_filter=None,
        )
        return cast(ManagerEvent, Message.parse_obj(json.loads(event_as_json)))

    async def cancel(self, task_id: str, *, requeue: bool):
        async with self._task_session(task_id) as sess:
            await sess.execute_write(_cancel_task_tx, task_id=task_id, requeue=requeue)


async def _enqueue_task_tx(
    tx: neo4j.AsyncTransaction, *, task_id: str, max_queue_size: int
) -> Task:
    count_query = f"""MATCH (task:{TASK_NODE}:`{TaskState.QUEUED.value}`)
RETURN count(task.id) AS nQueued
"""
    res = await tx.run(count_query)
    count = await res.single(strict=True)
    n_queued = count["nQueued"]
    if max_queue_size is not None and n_queued >= max_queue_size:
        raise TaskQueueIsFull(max_queue_size)

    query = f"""MATCH (t:{TASK_NODE} {{ {TASK_ID}: $taskId }})
WITH t
CALL apoc.create.setLabels(t, $labels) YIELD node AS task
RETURN task
"""
    labels = [TASK_NODE, TaskState.QUEUED.value]
    res = await tx.run(query, taskId=task_id, labels=labels)
    recs = [rec async for rec in res]
    if not recs:
        raise UnknownTask(task_id)
    if len(recs) > 1:
        raise ValueError(f"Multiple tasks found for task {task_id}")
    return Task.from_neo4j(recs[0])


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


async def _consume_manager_events_tx(tx: neo4j.AsyncTransaction) -> Optional[str]:
    get_event_query = f"""MATCH (event:{TASK_MANAGER_EVENT_NODE})
RETURN event.{TASK_MANAGER_EVENT_EVENT} as eventAsJson
ORDER BY event.{TASK_MANAGER_EVENT_NODE_CREATED_AT} ASC
LIMIT 1
"""
    res = await tx.run(get_event_query)
    try:
        event = await res.single(strict=True)
    except ResultNotSingleError:
        return None
    return event["eventAsJson"]


async def _requeue_task_tx(
    tx: neo4j.AsyncTransaction, *, task_id: str, retries: int, max_queue_size: int
):
    count_query = f"""MATCH (task:{TASK_NODE}:`{TaskState.QUEUED.value}`)
RETURN count(task.id) AS nQueued
"""
    res = await tx.run(count_query)
    count = await res.single(strict=True)
    n_queued = count["nQueued"]
    if max_queue_size is not None and n_queued >= max_queue_size:
        raise TaskQueueIsFull(max_queue_size)
    query = f"""MATCH (t:{TASK_NODE} {{ {TASK_ID}: $taskId}})
SET t.{TASK_RETRIES} = $retries, t.{TASK_PROGRESS} = 0.0
WITH t
CALL apoc.create.setLabels(t, $labels) YIELD node AS task
RETURN task
"""
    labels = [TASK_NODE, TaskState.QUEUED.value]
    res = await tx.run(query, taskId=task_id, retries=retries, labels=labels)
    try:
        await res.single(strict=True)
    except ResultNotSingleError as e:
        raise UnknownTask(task_id) from e


async def _dlq_task_tx(tx: neo4j.AsyncTransaction, *, task_id: str, worker_id: str):
    query = f"""MATCH (lock:{TASK_LOCK_NODE} {{ {TASK_LOCK_TASK_ID}: $taskId }})
WHERE lock.{TASK_LOCK_WORKER_ID} = $workerId
WITH lock
MATCH (t:{TASK_NODE} {{ {TASK_ID}: lock.{TASK_LOCK_TASK_ID} }})
CALL apoc.create.setLabels(t, $labels) YIELD node AS task
DELETE lock
RETURN task, lock
"""
    res = await tx.run(query, taskId=task_id, workerId=worker_id)
    try:
        await res.single(strict=True)
    except ResultNotSingleError as e:
        raise UnknownTask(task_id, worker_id) from e
