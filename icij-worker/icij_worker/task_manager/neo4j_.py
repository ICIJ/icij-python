from datetime import datetime
from typing import Optional

import neo4j

from icij_common.neo4j.constants import (
    TASK_CANCELLED_BY_EVENT_REL,
    TASK_CANCEL_EVENT_CANCELLED_AT,
    TASK_CANCEL_EVENT_EFFECTIVE,
    TASK_CANCEL_EVENT_NODE,
    TASK_CANCEL_EVENT_REQUEUE,
    TASK_ID,
    TASK_NODE,
)
from icij_worker import Namespacing, Task, TaskState
from icij_worker.exceptions import TaskQueueIsFull, UnknownTask
from icij_worker.task_manager import TaskManager
from icij_worker.task_storage.neo4j_ import Neo4jStorage


class Neo4JTaskManager(TaskManager, Neo4jStorage):
    def __init__(
        self,
        app_name: str,
        driver: neo4j.AsyncDriver,
        max_task_queue_size: int,
        namespacing: Optional[Namespacing] = None,
    ) -> None:
        super().__init__(app_name, max_task_queue_size, namespacing)
        super(TaskManager, self).__init__(driver)

    @property
    def driver(self) -> neo4j.AsyncDriver:
        return self._driver

    async def _enqueue(self, task: Task, **kwargs) -> Task:
        # pylint: disable=arguments-differ
        db = await self._get_task_db(task_id=task.id)
        async with self._db_session(db) as sess:
            return await sess.execute_write(
                _enqueue_task_tx,
                task_id=task.id,
                max_queue_size=self._max_task_queue_size,
            )

    async def _cancel(self, *, task_id: str, requeue: bool):
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
    if n_queued > max_queue_size:
        raise TaskQueueIsFull(max_queue_size)

    query = f"""MATCH (task:{TASK_NODE} {{ {TASK_ID}: $taskId }})
SET task:{TaskState.QUEUED.value}
RETURN task
"""
    res = await tx.run(query, taskId=task_id)
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
