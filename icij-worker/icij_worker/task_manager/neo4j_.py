import json
from datetime import datetime, timezone
from typing import cast

import neo4j
from neo4j import AsyncDriver, AsyncGraphDatabase
from neo4j.exceptions import ResultNotSingleError
from pydantic import Field

from icij_common.neo4j_.migrate import retrieve_dbs
from icij_common.registrable import FromConfig
from icij_worker import AsyncApp, Task, TaskState
from icij_worker.constants import (
    NEO4J_SHUTDOWN_EVENT_CREATED_AT,
    NEO4J_SHUTDOWN_EVENT_NODE,
    NEO4J_TASK_CANCELLED_BY_EVENT_REL,
    NEO4J_TASK_CANCEL_EVENT_CANCELLED_AT,
    NEO4J_TASK_CANCEL_EVENT_EFFECTIVE,
    NEO4J_TASK_CANCEL_EVENT_NODE,
    NEO4J_TASK_CANCEL_EVENT_REQUEUE,
    NEO4J_TASK_ID,
    NEO4J_TASK_LOCK_NODE,
    NEO4J_TASK_LOCK_TASK_ID,
    NEO4J_TASK_LOCK_WORKER_ID,
    NEO4J_TASK_MANAGER_EVENT_EVENT,
    NEO4J_TASK_MANAGER_EVENT_NODE,
    NEO4J_TASK_MANAGER_EVENT_NODE_CREATED_AT,
    NEO4J_TASK_NODE,
    NEO4J_TASK_PROGRESS,
    NEO4J_TASK_RETRIES_LEFT,
    TASK_ID,
)
from icij_worker.exceptions import (
    MessageDeserializationError,
    TaskQueueIsFull,
    UnknownTask,
)
from icij_worker.objects import AsyncBackend, ManagerEvent, Message
from icij_worker.task_manager import TaskManager, TaskManagerConfig
from icij_worker.utils.neo4j_ import Neo4jConsumerMixin


@TaskManagerConfig.register()
class Neo4JTaskManagerConfig(TaskManagerConfig):
    backend: AsyncBackend = Field(frozen=True, default=AsyncBackend.neo4j)
    event_refresh_interval_s: float = 0.1
    neo4j_host: str = "localhost"
    neo4j_port: int = 7687
    neo4j_user: str = "neo4j"
    neo4j_password: str = "theneo4jpassword"
    neo4j_scheme: str = "neo4j"

    def to_driver(self) -> AsyncDriver:
        uri = f"{self.neo4j_scheme}://{self.neo4j_host}:{self.neo4j_port}"
        auth = neo4j.basic_auth(self.neo4j_user, self.neo4j_password)
        driver = AsyncGraphDatabase.driver(uri, auth=auth)
        return driver


@TaskManager.register(AsyncBackend.neo4j)
class Neo4JTaskManager(TaskManager, Neo4jConsumerMixin):
    def __init__(
        self,
        app: AsyncApp,
        driver: neo4j.AsyncDriver,
        event_refresh_interval_s: float = 0.1,
    ) -> None:
        super().__init__(app)
        super(TaskManager, self).__init__(driver)
        self._event_refresh_interval_s = event_refresh_interval_s

    @classmethod
    def _from_config(cls, config: Neo4JTaskManagerConfig, **extras) -> FromConfig:
        driver = config.to_driver()
        tm = cls(
            config.app, driver, event_refresh_interval_s=config.event_refresh_interval_s
        )
        return tm

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
                retries_left=task.retries_left,
                max_queue_size=self.max_task_queue_size,
            )

    async def _consume(self) -> ManagerEvent:
        event_as_json = await self._consume_(
            _consume_manager_events_tx,
            refresh_interval_s=self._event_refresh_interval_s,
            db_filter=None,
        )
        try:
            event = Message.model_validate(json.loads(event_as_json))
        except Exception as e:
            msg = f"invalid event {event_as_json}"
            raise MessageDeserializationError(msg) from e
        return cast(ManagerEvent, event)

    async def cancel(self, task_id: str, *, requeue: bool):
        async with self._task_session(task_id) as sess:
            await sess.execute_write(_cancel_task_tx, task_id=task_id, requeue=requeue)

    async def shutdown_workers(self):
        dbs = await retrieve_dbs(self._driver)
        for db in dbs:
            async with self._db_session(db.name) as sess:
                await sess.execute_write(_shutdown_workers_tx)

    async def get_health(self) -> bool:
        return await super(Neo4jConsumerMixin, self).get_health()


async def _enqueue_task_tx(
    tx: neo4j.AsyncTransaction, *, task_id: str, max_queue_size: int
) -> Task:
    count_query = f"""MATCH (task:{NEO4J_TASK_NODE}:`{TaskState.QUEUED.value}`)
RETURN count(task.id) AS nQueued
"""
    res = await tx.run(count_query)
    count = await res.single(strict=True)
    n_queued = count["nQueued"]
    if max_queue_size is not None and n_queued >= max_queue_size:
        raise TaskQueueIsFull(max_queue_size)

    query = f"""MATCH (t:{NEO4J_TASK_NODE} {{ {NEO4J_TASK_ID}: $taskId }})
WITH t
CALL apoc.create.setLabels(t, $labels) YIELD node AS task
RETURN task
"""
    labels = [NEO4J_TASK_NODE, TaskState.QUEUED.value]
    res = await tx.run(query, taskId=task_id, labels=labels)
    recs = [rec async for rec in res]
    if not recs:
        raise UnknownTask(task_id)
    if len(recs) > 1:
        raise ValueError(f"Multiple tasks found for task {task_id}")
    return Task.from_neo4j(recs[0])


async def _cancel_task_tx(tx: neo4j.AsyncTransaction, task_id: str, requeue: bool):
    query = f"""MATCH (task:{NEO4J_TASK_NODE} {{ {NEO4J_TASK_ID}: $taskId }})
CREATE (task)-[
    :{NEO4J_TASK_CANCELLED_BY_EVENT_REL}
]->(:{NEO4J_TASK_CANCEL_EVENT_NODE} {{ 
        {NEO4J_TASK_CANCEL_EVENT_CANCELLED_AT}: $cancelledAt, 
        {NEO4J_TASK_CANCEL_EVENT_EFFECTIVE}: false,
        {NEO4J_TASK_CANCEL_EVENT_REQUEUE}: $requeue
    }})
"""
    await tx.run(
        query, taskId=task_id, requeue=requeue, cancelledAt=datetime.now(timezone.utc)
    )


async def _shutdown_workers_tx(tx: neo4j.AsyncTransaction):
    query = f"""CREATE (event:{NEO4J_SHUTDOWN_EVENT_NODE} {{
        {NEO4J_SHUTDOWN_EVENT_CREATED_AT}: $createdAt 
    }})
"""
    await tx.run(query, createdAt=datetime.now(timezone.utc))


async def _consume_manager_events_tx(tx: neo4j.AsyncTransaction) -> str | None:
    get_event_query = f"""MATCH (event:{NEO4J_TASK_MANAGER_EVENT_NODE})
RETURN event.{NEO4J_TASK_MANAGER_EVENT_EVENT} as eventAsJson
ORDER BY event.{NEO4J_TASK_MANAGER_EVENT_NODE_CREATED_AT} ASC
LIMIT 1
"""
    res = await tx.run(get_event_query)
    try:
        event = await res.single(strict=True)
    except ResultNotSingleError:
        return None
    return event["eventAsJson"]


async def _requeue_task_tx(
    tx: neo4j.AsyncTransaction, *, task_id: str, retries_left: int, max_queue_size: int
):
    count_query = f"""MATCH (task:{NEO4J_TASK_NODE}:`{TaskState.QUEUED.value}`)
RETURN count(task.id) AS nQueued
"""
    res = await tx.run(count_query)
    count = await res.single(strict=True)
    n_queued = count["nQueued"]
    if max_queue_size is not None and n_queued >= max_queue_size:
        raise TaskQueueIsFull(max_queue_size)
    query = f"""MATCH (t:{NEO4J_TASK_NODE} {{ {TASK_ID}: $taskId}})
SET t.{NEO4J_TASK_RETRIES_LEFT} = $retriesLeft, t.{NEO4J_TASK_PROGRESS} = 0.0
WITH t
CALL apoc.create.setLabels(t, $labels) YIELD node AS task
RETURN task
"""
    labels = [NEO4J_TASK_NODE, TaskState.QUEUED.value]
    res = await tx.run(query, taskId=task_id, retriesLeft=retries_left, labels=labels)
    try:
        await res.single(strict=True)
    except ResultNotSingleError as e:
        raise UnknownTask(task_id) from e


async def _dlq_task_tx(tx: neo4j.AsyncTransaction, *, task_id: str, worker_id: str):
    query = f"""MATCH (lock:{NEO4J_TASK_LOCK_NODE} {{
     {NEO4J_TASK_LOCK_TASK_ID}: $taskId 
    }})
WHERE lock.{NEO4J_TASK_LOCK_WORKER_ID} = $workerId
WITH lock
MATCH (t:{NEO4J_TASK_NODE} {{ {TASK_ID}: lock.{NEO4J_TASK_LOCK_TASK_ID} }})
CALL apoc.create.setLabels(t, $labels) YIELD node AS task
DELETE lock
RETURN task, lock
"""
    res = await tx.run(query, taskId=task_id, workerId=worker_id)
    try:
        await res.single(strict=True)
    except ResultNotSingleError as e:
        raise UnknownTask(task_id, worker_id) from e
