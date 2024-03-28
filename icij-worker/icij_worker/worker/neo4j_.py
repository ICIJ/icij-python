from __future__ import annotations

import asyncio
import functools
import json
from datetime import datetime
from typing import (
    Awaitable,
    Callable,
    ClassVar,
    Dict,
    Optional,
    TypeVar,
)

import neo4j
from neo4j.exceptions import ConstraintError, ResultNotSingleError
from pydantic import Field

from icij_common.neo4j.constants import (
    TASK_CANCELLED_AT,
    TASK_CANCELLED_BY_EVENT_REL,
    TASK_CANCEL_EVENT_CREATED_AT,
    TASK_CANCEL_EVENT_EFFECTIVE,
    TASK_CANCEL_EVENT_NODE,
    TASK_ERROR_NODE,
    TASK_ERROR_OCCURRED_TYPE,
    TASK_HAS_RESULT_TYPE,
    TASK_ID,
    TASK_LOCK_NODE,
    TASK_LOCK_TASK_ID,
    TASK_LOCK_WORKER_ID,
    TASK_NODE,
    TASK_PROGRESS,
    TASK_RESULT_NODE,
    TASK_RESULT_RESULT,
    TASK_RETRIES,
)
from icij_common.neo4j.migrate import retrieve_projects
from icij_common.pydantic_utils import ICIJModel, jsonable_encoder
from icij_worker import (
    AsyncApp,
    Task,
    TaskError,
    TaskResult,
    TaskStatus,
    Worker,
    WorkerConfig,
    WorkerType,
)
from icij_worker.event_publisher.neo4j_ import Neo4jEventPublisher
from icij_worker.exceptions import TaskAlreadyReserved, UnknownTask
from icij_worker.task import CancelledTaskEvent

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


@Worker.register(WorkerType.neo4j)
class Neo4jWorker(Worker, Neo4jEventPublisher):
    def __init__(
        self,
        app: AsyncApp,
        worker_id: str,
        driver: neo4j.AsyncDriver,
        new_tasks_refresh_interval_s: float,
        cancelled_tasks_refresh_interval_s: float,
        **kwargs,
    ):
        super().__init__(app, worker_id, **kwargs)
        self._cancelled_tasks_refresh_interval_s = cancelled_tasks_refresh_interval_s
        self._new_tasks_refresh_interval_s = new_tasks_refresh_interval_s
        self._driver = driver

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
        return await self._consume_(
            _consume_task_tx,
            refresh_interval_s=self._new_tasks_refresh_interval_s,
        )

    async def _consume_cancelled(self) -> CancelledTaskEvent:
        return await self._consume_(
            _consume_cancelled_task_tx,
            refresh_interval_s=self._cancelled_tasks_refresh_interval_s,
        )

    async def _consume_(self, consume_tx: ConsumeT, refresh_interval_s: float) -> T:
        projects = []
        refresh_projects_i = 0
        while "i'm waiting until I find something interesting":
            # Refresh project list once in an while
            refresh_projects = refresh_projects_i % 10
            if not refresh_projects:
                projects = await retrieve_projects(self._driver)
            for p in projects:
                async with self._project_session(p.name) as sess:
                    tx = functools.partial(consume_tx, project_id=p.name)
                    received = await sess.execute_write(tx, worker_id=self.id)
                    if received is not None:
                        return received
            await asyncio.sleep(refresh_interval_s)
            refresh_projects_i += 1

    async def _negatively_acknowledge(self, nacked: Task, *, cancelled: bool):
        if nacked.status is TaskStatus.QUEUED:
            nack_fn = functools.partial(
                _nack_and_requeue_task_tx, retries=nacked.retries, cancelled=cancelled
            )
        elif nacked.status is TaskStatus.CANCELLED:
            nack_fn = functools.partial(
                _nack_and_cancel_task_tx, cancelled_at=nacked.cancelled_at
            )
        else:
            nack_fn = _nack_task_tx
        project_id = self._get_task_project_id(nacked.id)
        async with self._project_session(project_id) as sess:
            await sess.execute_write(nack_fn, task_id=nacked.id, worker_id=self.id)

    async def _save_result(self, result: TaskResult):
        project_id = result.project_id
        if project_id is None:
            raise ValueError(
                "neo4j expects project to be provided in order to fetch tasks from the"
                " project's DB"
            )
        async with self._project_session(project_id) as sess:
            res_str = json.dumps(jsonable_encoder(result.result))
            await sess.execute_write(
                _save_result_tx, task_id=result.task_id, result=res_str
            )

    async def _save_error(self, error: TaskError):
        project_id = error.project_id
        if project_id is None:
            raise ValueError(
                "neo4j expects project to be provided in order to fetch tasks from the"
                " project's DB"
            )
        async with self._project_session(project_id) as sess:
            await sess.execute_write(
                _save_error_tx,
                task_id=error.task_id,
                error_props=error.dict(by_alias=True),
            )

    async def _acknowledge(self, task: Task, completed_at: datetime):
        project_id = self._get_task_project_id(task.id)
        async with self._project_session(project_id) as sess:
            await sess.execute_write(
                _acknowledge_task_tx,
                task_id=task.id,
                worker_id=self.id,
                completed_at=completed_at,
            )

    async def _aexit__(self, exc_type, exc_val, exc_tb):
        await self._driver.__aexit__(exc_type, exc_val, exc_tb)


async def _consume_task_tx(
    tx: neo4j.AsyncTransaction, *, worker_id: str, project_id: str
) -> Optional[Task]:
    query = f"""MATCH (t:{TASK_NODE}:`{TaskStatus.QUEUED.value}`)
WITH t
LIMIT 1
CALL apoc.create.setLabels(t, $labels) YIELD node AS task
WITH task
CREATE (lock:{TASK_LOCK_NODE} {{
    {TASK_LOCK_TASK_ID}: task.id,
    {TASK_LOCK_WORKER_ID}: $workerId 
}})
RETURN task, lock"""
    labels = [TASK_NODE, TaskStatus.RUNNING.value]
    res = await tx.run(query, workerId=worker_id, labels=labels)
    try:
        task = await res.single(strict=True)
    except ResultNotSingleError:
        return None
    except ConstraintError as e:
        raise TaskAlreadyReserved() from e
    return Task.from_neo4j(task, project_id=project_id)


async def _consume_cancelled_task_tx(
    tx: neo4j.AsyncTransaction, project_id: str, **_
) -> Optional[CancelledTaskEvent]:
    get_event_query = f"""MATCH (task:{TASK_NODE})-[
    :{TASK_CANCELLED_BY_EVENT_REL}
]->(event:{TASK_CANCEL_EVENT_NODE})
WHERE NOT event.{TASK_CANCEL_EVENT_EFFECTIVE}
RETURN task, event
ORDER BY event.{TASK_CANCEL_EVENT_CREATED_AT} ASC
LIMIT 1
"""
    res = await tx.run(get_event_query)
    try:
        event = await res.single(strict=True)
    except ResultNotSingleError:
        return None
    return CancelledTaskEvent.from_neo4j(event, project_id=project_id)


async def _acknowledge_task_tx(
    tx: neo4j.AsyncTransaction, *, task_id: str, worker_id: str, completed_at: datetime
):
    query = f"""MATCH (lock:{TASK_LOCK_NODE} {{ {TASK_LOCK_TASK_ID}: $taskId }})
WHERE lock.{TASK_LOCK_WORKER_ID} = $workerId
WITH lock    
MATCH (t:{TASK_NODE} {{ {TASK_ID}: lock.{TASK_LOCK_TASK_ID} }})
SET t.progress = 100.0, t.completedAt = $completedAt
WITH t , lock
CALL apoc.create.setLabels(t, $labels) YIELD node AS task
DELETE lock
RETURN task"""
    labels = [TASK_NODE, TaskStatus.DONE.value]
    res = await tx.run(
        query,
        taskId=task_id,
        workerId=worker_id,
        labels=labels,
        completedAt=completed_at,
    )
    try:
        await res.single(strict=True)
    except ResultNotSingleError as e:
        raise UnknownTask(task_id, worker_id) from e


async def _nack_task_tx(tx: neo4j.AsyncTransaction, *, task_id: str, worker_id: str):
    query = f"""MATCH (lock:{TASK_LOCK_NODE} {{ {TASK_LOCK_TASK_ID}: $taskId }})
WHERE lock.{TASK_LOCK_WORKER_ID} = $workerId
WITH lock
MATCH (t:{TASK_NODE} {{ {TASK_ID}: lock.{TASK_LOCK_TASK_ID} }})
CALL apoc.create.setLabels(t, $labels) YIELD node AS task
DELETE lock
RETURN task, lock
"""
    labels = [TASK_NODE, TaskStatus.ERROR.value]
    res = await tx.run(query, taskId=task_id, workerId=worker_id, labels=labels)
    try:
        await res.single(strict=True)
    except ResultNotSingleError as e:
        raise UnknownTask(task_id, worker_id) from e


async def _nack_and_requeue_task_tx(
    tx: neo4j.AsyncTransaction,
    *,
    task_id: str,
    worker_id: str,
    retries: int,
    cancelled: bool,
):
    clean_cancelled_query = f"""
WITH task, lock
OPTIONAL MATCH (task)-[
    :{TASK_CANCELLED_BY_EVENT_REL}
]->(event:{TASK_CANCEL_EVENT_NODE})
SET event.{TASK_CANCEL_EVENT_EFFECTIVE} = true
"""
    clean_cancelled = clean_cancelled_query if cancelled else ""
    query = f"""MATCH (lock:{TASK_LOCK_NODE} {{ {TASK_LOCK_TASK_ID}: $taskId }})
WHERE lock.{TASK_LOCK_WORKER_ID} = $workerId
WITH lock
MATCH (t:{TASK_NODE} {{ {TASK_ID}: lock.{TASK_LOCK_TASK_ID} }})
SET t.{TASK_PROGRESS} = 0.0, 
    t.{TASK_RETRIES} = $retries
DELETE lock
WITH t, lock
CALL apoc.create.setLabels(t, $labels) YIELD node AS task{clean_cancelled}
RETURN task, lock"""
    labels = [TASK_NODE, TaskStatus.QUEUED.value]
    res = await tx.run(
        query,
        taskId=task_id,
        workerId=worker_id,
        labels=labels,
        retries=retries,
    )
    try:
        await res.single(strict=True)
    except ResultNotSingleError as e:
        raise UnknownTask(task_id, worker_id) from e


async def _nack_and_cancel_task_tx(
    tx: neo4j.AsyncTransaction, task_id: str, worker_id: str, cancelled_at: datetime
):
    # Mark all cancel events as effective
    query = f"""MATCH (t:{TASK_NODE} {{ {TASK_ID}: $taskId }})
OPTIONAL MATCH (t)-[
    :{TASK_CANCELLED_BY_EVENT_REL}
]->(event:{TASK_CANCEL_EVENT_NODE})
SET event.{TASK_CANCEL_EVENT_EFFECTIVE} = true
WITH DISTINCT t 
CALL apoc.create.setLabels(t, $labels) YIELD node AS task
WITH task
SET task.{TASK_CANCELLED_AT} = $cancelledAt
WITH task
MATCH (lock:{TASK_LOCK_NODE} {{ {TASK_LOCK_TASK_ID}: task.{TASK_ID} }})
DELETE lock
RETURN task
"""
    labels = [TASK_NODE, TaskStatus.CANCELLED.value]
    res = await tx.run(query, taskId=task_id, labels=labels, cancelledAt=cancelled_at)
    try:
        await res.single(strict=True)
    except ResultNotSingleError as e:
        raise UnknownTask(task_id, worker_id) from e


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
