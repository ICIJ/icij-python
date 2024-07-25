from __future__ import annotations

import asyncio
import functools
from typing import (
    Awaitable,
    Callable,
    ClassVar,
    Optional,
    TypeVar,
)

import neo4j
from neo4j.exceptions import ConstraintError, ResultNotSingleError
from pydantic import Field

from icij_common.neo4j.constants import (
    TASK_CANCELLED_BY_EVENT_REL,
    TASK_CANCEL_EVENT_CANCELLED_AT,
    TASK_CANCEL_EVENT_EFFECTIVE,
    TASK_CANCEL_EVENT_NODE,
    TASK_ID,
    TASK_LOCK_NODE,
    TASK_LOCK_TASK_ID,
    TASK_LOCK_WORKER_ID,
    TASK_NAMESPACE,
    TASK_NODE,
    TASK_PROGRESS,
    TASK_RETRIES,
)
from icij_common.neo4j.migrate import retrieve_dbs
from icij_common.pydantic_utils import ICIJModel
from icij_worker import (
    AsyncApp,
    Task,
    TaskError,
    TaskResult,
    TaskState,
    Worker,
    WorkerConfig,
    WorkerType,
)
from icij_worker.event_publisher.neo4j_ import Neo4jEventPublisher
from icij_worker.exceptions import TaskAlreadyReserved, UnknownTask
from icij_worker.objects import CancelEvent, WorkerEvent

_TASK_MANDATORY_FIELDS_BY_ALIAS = {
    f for f in Task.schema(by_alias=True)["required"] if f != "id"
}

T = TypeVar("T", bound=ICIJModel)
ConsumeT = Callable[[neo4j.AsyncTransaction, ...], Awaitable[Optional[T]]]


@WorkerConfig.register()
class Neo4jWorkerConfig(WorkerConfig):
    type: ClassVar[str] = Field(const=True, default=WorkerType.neo4j.value)

    cancelled_tasks_refresh_interval_s: float = 0.1
    new_tasks_refresh_interval_s: float = 0.1
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


def _no_filter(namespace: str) -> bool:
    # pylint: disable=unused-argument
    return True


@Worker.register(WorkerType.neo4j)
class Neo4jWorker(Worker, Neo4jEventPublisher):

    def __init__(
        self,
        app: AsyncApp,
        worker_id: Optional[str] = None,
        *,
        namespace: Optional[str],
        driver: neo4j.AsyncDriver,
        new_tasks_refresh_interval_s: float,
        cancelled_tasks_refresh_interval_s: float,
        **kwargs,
    ):
        super().__init__(app, worker_id, namespace=namespace, **kwargs)
        super(Worker, self).__init__(driver)
        if self._namespace is not None:
            db_filter = self._namespacing.db_filter_factory(self._namespace)
        else:
            db_filter = _no_filter
        self._db_filter: Callable[[str], bool] = db_filter
        self._cancelled_tasks_refresh_interval_s = cancelled_tasks_refresh_interval_s
        self._new_tasks_refresh_interval_s = new_tasks_refresh_interval_s

    @classmethod
    def _from_config(cls, config: Neo4jWorkerConfig, **extras) -> Neo4jWorker:
        tasks_refresh_interval_s = config.cancelled_tasks_refresh_interval_s
        worker = cls(
            driver=config.to_neo4j_driver(),
            new_tasks_refresh_interval_s=config.new_tasks_refresh_interval_s,
            cancelled_tasks_refresh_interval_s=tasks_refresh_interval_s,
            **extras,
        )
        worker.set_config(config)
        return worker

    async def _consume(self) -> Task:
        task = await self._consume_(
            functools.partial(_consume_task_tx, namespace=self._namespace),
            refresh_interval_s=self._new_tasks_refresh_interval_s,
        )
        return Task.from_neo4j({"task": task})

    async def _consume_worker_events(self) -> WorkerEvent:
        return await self._consume_(
            functools.partial(_consume_cancelled_task_tx, namespace=self._namespace),
            refresh_interval_s=self._cancelled_tasks_refresh_interval_s,
        )

    async def _consume_(self, consume_tx: ConsumeT, refresh_interval_s: float) -> T:
        dbs = []
        refresh_dbs_i = 0
        while "i'm waiting until I find something interesting":
            # Refresh project list once in an while
            refresh_dbs = (refresh_dbs_i % 10) == 0
            if refresh_dbs:
                dbs = await retrieve_dbs(self._driver)
                dbs = [db for db in dbs if self._db_filter(db.name)]
            for db in dbs:
                async with self._db_session(db.name) as sess:
                    received = await sess.execute_write(consume_tx, worker_id=self.id)
                    if "namespace" in received:
                        self._task_meta[received.id] = (db.name, received["namespace"])
                    if received is not None:
                        return received
            await asyncio.sleep(refresh_interval_s)
            refresh_dbs_i += 1

    async def _save_result(self, result: TaskResult):
        await super(Worker, self).save_result(result)

    async def _save_error(self, error: TaskError):
        await super(Worker, self).save_error(error)

    async def _acknowledge(self, task: Task):
        async with self._task_session(task.id) as sess:
            await sess.execute_write(
                _acknowledge_task_tx, task_id=task.id, worker_id=self.id
            )

    async def _negatively_acknowledge(self, nacked: Task):
        if nacked.state is TaskState.QUEUED:
            nack_fn = functools.partial(_requeue_task_tx, retries=nacked.retries)
        elif nacked.state is TaskState.ERROR:
            nack_fn = _dlq_task_tx
        else:
            msg = (
                f"expected {TaskState.QUEUED} or {TaskState.ERROR},"
                f" found {nacked.state}"
            )
            raise ValueError(msg)
        async with self._task_session(nacked.id) as sess:
            await sess.execute_write(nack_fn, task_id=nacked.id, worker_id=self.id)

    async def _requeue(self, task: Task, acknowledge: bool):
        # pylint: disable=unused-argument
        async with self._task_session(task.id) as sess:
            await sess.execute_write(
                _requeue_task_tx,
                task_id=task.id,
                worker_id=self.id,
                retries=task.retries,
            )

    async def _aexit__(self, exc_type, exc_val, exc_tb):
        await self._driver.__aexit__(exc_type, exc_val, exc_tb)


async def _consume_task_tx(
    tx: neo4j.AsyncTransaction,
    *,
    worker_id: str,
    namespace: Optional[str],
) -> Optional[neo4j.Record]:
    where_ns = ""
    if namespace is not None:
        where_ns = f"WHERE t.{TASK_NAMESPACE} = $namespace"
    query = f"""MATCH (t:{TASK_NODE}:`{TaskState.QUEUED.value}`)
{where_ns}
WITH t
LIMIT 1
CALL apoc.create.setLabels(t, $labels) YIELD node AS task
WITH task
CREATE (lock:{TASK_LOCK_NODE} {{
    {TASK_LOCK_TASK_ID}: task.id,
    {TASK_LOCK_WORKER_ID}: $workerId 
}})
RETURN task"""
    labels = [TASK_NODE, TaskState.RUNNING.value]
    res = await tx.run(query, workerId=worker_id, labels=labels, namespace=namespace)
    try:
        task = await res.single(strict=True)
    except ResultNotSingleError:
        return None
    except ConstraintError as e:
        raise TaskAlreadyReserved() from e
    return task["task"]


async def _consume_cancelled_task_tx(
    tx: neo4j.AsyncTransaction, namespace: Optional[str], **_
) -> Optional[WorkerEvent]:
    where_ns = ""
    if namespace is not None:
        where_ns = f"AND task.{TASK_NAMESPACE} = $namespace"
    get_event_query = f"""MATCH (task:{TASK_NODE})-[
    :{TASK_CANCELLED_BY_EVENT_REL}
]->(event:{TASK_CANCEL_EVENT_NODE})
WHERE NOT event.{TASK_CANCEL_EVENT_EFFECTIVE}{where_ns}
RETURN task, event
ORDER BY event.{TASK_CANCEL_EVENT_CANCELLED_AT} ASC
LIMIT 1
"""
    res = await tx.run(get_event_query, namespace=namespace)
    try:
        event = await res.single(strict=True)
    except ResultNotSingleError:
        return None
    return CancelEvent.from_neo4j(event)


async def _acknowledge_task_tx(
    tx: neo4j.AsyncTransaction, *, task_id: str, worker_id: str
):
    query = f"""MATCH (lock:{TASK_LOCK_NODE} {{ {TASK_LOCK_TASK_ID}: $taskId }})
WHERE lock.{TASK_LOCK_WORKER_ID} = $workerId
DELETE lock
RETURN lock"""
    res = await tx.run(query, taskId=task_id, workerId=worker_id)
    try:
        await res.single(strict=True)
    except ResultNotSingleError as e:
        raise UnknownTask(task_id, worker_id) from e


async def _requeue_task_tx(
    tx: neo4j.AsyncTransaction, *, task_id: str, worker_id: str, retries: int
):
    query = f"""MATCH (lock:{TASK_LOCK_NODE} {{ {TASK_LOCK_TASK_ID}: $taskId }})
WHERE lock.{TASK_LOCK_WORKER_ID} = $workerId
WITH lock
MATCH (t:{TASK_NODE} {{ {TASK_ID}: lock.{TASK_LOCK_TASK_ID} }})
SET t.{TASK_RETRIES} = $retries, t.{TASK_PROGRESS} = 0.0
WITH t, lock
CALL apoc.create.setLabels(t, $labels) YIELD node AS task
DELETE lock
RETURN task, lock
"""
    labels = [TASK_NODE, TaskState.QUEUED.value]
    res = await tx.run(
        query, taskId=task_id, workerId=worker_id, retries=retries, labels=labels
    )
    try:
        await res.single(strict=True)
    except ResultNotSingleError as e:
        raise UnknownTask(task_id, worker_id) from e


async def _dlq_task_tx(tx: neo4j.AsyncTransaction, *, task_id: str, worker_id: str):
    query = f"""MATCH (lock:{TASK_LOCK_NODE} {{ {TASK_LOCK_TASK_ID}: $taskId }})
WHERE lock.{TASK_LOCK_WORKER_ID} = $workerId
WITH lock
MATCH (t:{TASK_NODE} {{ {TASK_ID}: lock.{TASK_LOCK_TASK_ID} }})
CALL apoc.create.setLabels(t, $labels) YIELD node AS task
DELETE lock
RETURN task, lock
"""
    labels = [TASK_NODE, TaskState.ERROR.value]
    res = await tx.run(query, taskId=task_id, workerId=worker_id, labels=labels)
    try:
        await res.single(strict=True)
    except ResultNotSingleError as e:
        raise UnknownTask(task_id, worker_id) from e
