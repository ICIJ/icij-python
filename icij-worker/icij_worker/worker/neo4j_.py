from __future__ import annotations

import functools
from typing import (
    Callable,
    ClassVar,
    Optional,
)

import neo4j
from neo4j.exceptions import ConstraintError, ResultNotSingleError
from pydantic import Field

from icij_worker.constants import (
    NEO4J_TASK_CANCELLED_BY_EVENT_REL,
    NEO4J_TASK_CANCEL_EVENT_CANCELLED_AT,
    NEO4J_TASK_CANCEL_EVENT_EFFECTIVE,
    NEO4J_TASK_CANCEL_EVENT_NODE,
    NEO4J_TASK_GROUP,
    NEO4J_TASK_LOCK_NODE,
    NEO4J_TASK_LOCK_TASK_ID,
    NEO4J_TASK_LOCK_WORKER_ID,
    NEO4J_TASK_NODE,
)
from icij_worker import AsyncApp, AsyncBackend, Task, TaskState, Worker, WorkerConfig
from icij_worker.event_publisher.neo4j_ import Neo4jEventPublisher
from icij_worker.exceptions import TaskAlreadyReserved, UnknownTask
from icij_worker.objects import CancelEvent, WorkerEvent
from icij_worker.utils.neo4j_ import Neo4jConsumerMixin


@WorkerConfig.register()
class Neo4jWorkerConfig(WorkerConfig):
    type: ClassVar[str] = Field(const=True, default=AsyncBackend.neo4j.value)

    cancelled_tasks_refresh_interval_s: float = 0.1
    poll_interval_s: float = 0.1
    neo4j_connection_timeout: float = 5.0
    neo4j_host: str = "127.0.0.1"
    neo4j_password: Optional[str] = None
    neo4j_port: int = 7687
    neo4j_uri_scheme: str = "bolt"
    neo4j_user: Optional[str] = None

    @property
    def neo4j_uri(self) -> str:
        return f"{self.neo4j_uri_scheme}://{self.neo4j_host}:{self.neo4j_port}"

    def to_neo4j_driver(self) -> neo4j.AsyncDriver:
        auth = None
        if self.neo4j_password:
            # TODO: add support for expiring and auto renew auth:
            #  https://neo4j.com/docs/api/python-driver/current/api.html
            #  #neo4j.auth_management.AuthManagers.expiration_based
            auth = neo4j.basic_auth(self.neo4j_user, self.neo4j_password)
        driver = neo4j.AsyncGraphDatabase.driver(
            self.neo4j_uri,
            connection_timeout=self.neo4j_connection_timeout,
            connection_acquisition_timeout=self.neo4j_connection_timeout,
            max_transaction_retry_time=self.neo4j_connection_timeout,
            auth=auth,
        )
        return driver


def _no_filter(group: str) -> bool:
    # pylint: disable=unused-argument
    return True


@Worker.register(AsyncBackend.neo4j)
class Neo4jWorker(Worker, Neo4jEventPublisher, Neo4jConsumerMixin):

    def __init__(
        self,
        app: AsyncApp,
        worker_id: Optional[str] = None,
        *,
        group: Optional[str],
        driver: neo4j.AsyncDriver,
        poll_interval_s: float,
        **kwargs,
    ):
        super().__init__(app, worker_id, group=group, **kwargs)
        super(Worker, self).__init__(driver)
        if self._group is not None:
            db_filter = self._routing_strategy.db_filter_factory(self._group)
        else:
            db_filter = _no_filter
        self._db_filter: Callable[[str], bool] = db_filter
        self._poll_interval_s = poll_interval_s

    @classmethod
    def _from_config(cls, config: Neo4jWorkerConfig, **extras) -> Neo4jWorker:
        tasks_refresh_interval_s = config.cancelled_tasks_refresh_interval_s
        worker = cls(
            driver=config.to_neo4j_driver(),
            poll_interval_s=config.poll_interval_s,
            cancelled_tasks_refresh_interval_s=tasks_refresh_interval_s,
            **extras,
        )
        return worker

    async def _consume(self) -> Task:
        task = await self._consume_(
            functools.partial(_consume_task_tx, group=self._group, worker_id=self.id),
            refresh_interval_s=self._poll_interval_s,
            db_filter=self._db_filter,
        )
        return Task.from_neo4j({"task": task})

    async def _consume_worker_events(self) -> WorkerEvent:
        return await self._consume_(
            functools.partial(_consume_worker_events_tx, group=self._group),
            refresh_interval_s=self._poll_interval_s,
            db_filter=self._db_filter,
        )

    async def _acknowledge(self, task: Task):
        async with self._task_session(task.id) as sess:
            await sess.execute_write(
                _acknowledge_task_tx, task_id=task.id, worker_id=self.id
            )

    async def _negatively_acknowledge(self, nacked: Task):
        async with self._task_session(nacked.id) as sess:
            await sess.execute_write(
                _unlock_task_tx, task_id=nacked.id, worker_id=self.id
            )

    async def _aexit__(self, exc_type, exc_val, exc_tb):
        await self._driver.__aexit__(exc_type, exc_val, exc_tb)


async def _consume_task_tx(
    tx: neo4j.AsyncTransaction, *, worker_id: str, group: Optional[str]
) -> Optional[neo4j.Record]:
    where_ns = ""
    if group is not None:
        where_ns = f"WHERE t.{NEO4J_TASK_GROUP} = $group"
    query = f"""MATCH (t:{NEO4J_TASK_NODE}:`{TaskState.QUEUED.value}`)
{where_ns}
WITH t
LIMIT 1
CALL apoc.create.setLabels(t, $labels) YIELD node AS task
WITH task
CREATE (lock:{NEO4J_TASK_LOCK_NODE} {{
    {NEO4J_TASK_LOCK_TASK_ID}: task.id,
    {NEO4J_TASK_LOCK_WORKER_ID}: $workerId 
}})
RETURN task"""
    labels = [NEO4J_TASK_NODE, TaskState.RUNNING.value]
    res = await tx.run(query, workerId=worker_id, labels=labels, group=group)
    try:
        task = await res.single(strict=True)
    except ResultNotSingleError:
        return None
    except ConstraintError as e:
        raise TaskAlreadyReserved() from e
    return task["task"]


async def _consume_worker_events_tx(
    tx: neo4j.AsyncTransaction, group: Optional[str], **_
) -> Optional[WorkerEvent]:
    where_ns = ""
    if group is not None:
        where_ns = f"AND task.{NEO4J_TASK_GROUP} = $group"
    get_event_query = f"""MATCH (task:{NEO4J_TASK_NODE})-[
    :{NEO4J_TASK_CANCELLED_BY_EVENT_REL}
]->(event:{NEO4J_TASK_CANCEL_EVENT_NODE})
WHERE NOT event.{NEO4J_TASK_CANCEL_EVENT_EFFECTIVE}{where_ns}
SET event.{NEO4J_TASK_CANCEL_EVENT_EFFECTIVE} = true
RETURN task, event
ORDER BY event.{NEO4J_TASK_CANCEL_EVENT_CANCELLED_AT} ASC
LIMIT 1
"""
    res = await tx.run(get_event_query, group=group)
    try:
        event = await res.single(strict=True)
    except ResultNotSingleError:
        return None
    return CancelEvent.from_neo4j(event)


async def _acknowledge_task_tx(
    tx: neo4j.AsyncTransaction, *, task_id: str, worker_id: str
):
    query = f"""MATCH (lock:{NEO4J_TASK_LOCK_NODE} {{
    {NEO4J_TASK_LOCK_TASK_ID}: $taskId 
}})
WHERE lock.{NEO4J_TASK_LOCK_WORKER_ID} = $workerId
DELETE lock
RETURN lock"""
    res = await tx.run(query, taskId=task_id, workerId=worker_id)
    try:
        await res.single(strict=True)
    except ResultNotSingleError as e:
        raise UnknownTask(task_id, worker_id) from e


async def _unlock_task_tx(tx: neo4j.AsyncTransaction, *, task_id: str, worker_id: str):
    query = f"""MATCH (lock:{NEO4J_TASK_LOCK_NODE} {{
     {NEO4J_TASK_LOCK_TASK_ID}: $taskId 
 }})
WHERE lock.{NEO4J_TASK_LOCK_WORKER_ID} = $workerId
DELETE lock
RETURN lock
"""
    res = await tx.run(query, taskId=task_id, workerId=worker_id)
    try:
        await res.single(strict=True)
    except ResultNotSingleError as e:
        raise UnknownTask(task_id, worker_id) from e
