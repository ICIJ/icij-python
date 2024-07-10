import json
from contextlib import asynccontextmanager
from copy import deepcopy
from typing import AsyncGenerator, Dict, List, Optional

import neo4j
from neo4j.exceptions import ResultNotSingleError

from icij_common.neo4j.constants import (
    TASK_ERROR_NODE,
    TASK_ERROR_OCCURRED_TYPE,
    TASK_ID,
    TASK_NODE,
)
from icij_common.neo4j.db import db_specific_session
from icij_common.neo4j.migrate import retrieve_dbs
from icij_worker.event_publisher.event_publisher import EventPublisher
from icij_worker.exceptions import UnknownTask
from icij_worker.objects import Message, Task, TaskEvent, TaskStatus


class Neo4jDBMixin:
    def __init__(self, driver: neo4j.AsyncDriver):
        self._driver = driver
        self._task_dbs: Dict[str, str] = dict()

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
        if task_id not in self._task_dbs:
            await self._refresh_task_dbs()
        try:
            return self._task_dbs[task_id]
        except KeyError as e:
            raise UnknownTask(task_id) from e

    async def _refresh_task_dbs(self):
        dbs = await retrieve_dbs(self._driver)
        for db in dbs:
            async with self._db_session(db.name) as sess:
                # Here we make the assumption that task IDs are unique across
                # projects and not per project
                task_dbs = {
                    t_id: db.name
                    for t_id in await sess.execute_read(_get_tasks_meta_tx)
                }
                self._task_dbs.update(task_dbs)


async def _get_tasks_meta_tx(tx: neo4j.AsyncTransaction) -> List[str]:
    query = f"""MATCH (task:{TASK_NODE})
RETURN task.{TASK_ID} as taskId"""
    res = await tx.run(query)
    ids = [rec["taskId"] async for rec in res]
    return ids


class Neo4jEventPublisher(Neo4jDBMixin, EventPublisher):

    async def _publish_event(self, event: TaskEvent):
        async with self._task_session(event.task_id) as sess:
            await _publish_event(sess, event)

    @property
    def driver(self) -> neo4j.AsyncDriver:
        return self._driver


async def _publish_event(sess: neo4j.AsyncSession, event: TaskEvent):
    event = {k: v for k, v in event.dict(by_alias=True).items() if v is not None}
    if "status" in event:
        event["status"] = event["status"].value
    error = event.pop("error", None)
    if error is not None:
        error["stacktrace"] = [json.dumps(item) for item in error["stacktrace"]]
    await sess.execute_write(_publish_event_tx, event, error)


async def _publish_event_tx(
    tx: neo4j.AsyncTransaction, event: Dict, error: Optional[Dict]
):
    task_id = event["taskId"]
    create_task = f"""MERGE (task:{TASK_NODE} {{{TASK_ID}: $taskId }})
ON CREATE SET task += $createProps"""
    status = event.get("status")
    if status:
        create_task += f", task:`{status}`"
    create_task += "\nRETURN task"
    if error is not None:
        event["error"] = deepcopy(error)
        event["error"]["stacktrace"] = [
            json.loads(item) for item in event["error"]["stacktrace"]
        ]
    as_event = Message.parse_obj(event)
    create_props = Task.mandatory_fields(as_event, keep_id=False)
    create_props.pop("status", None)
    res = await tx.run(create_task, taskId=task_id, createProps=create_props)
    tasks = [Task.from_neo4j(rec) async for rec in res]
    task = tasks[0]
    resolved = task.resolve_event(as_event)
    resolved = (
        resolved.dict(exclude_unset=True, by_alias=True)
        if resolved is not None
        else resolved
    )
    if resolved:
        resolved.pop("taskId")
        # Status can't be updated by event, only by ack, nack, enqueue and so on
        resolved.pop("status", None)
        resolved.pop("error", None)
        resolved.pop("occurredAt", None)
        update_task = f"""MATCH (task:{TASK_NODE} {{{TASK_ID}: $taskId }})
SET task += $updateProps
RETURN count(*) as numTasks"""
        labels = [TASK_NODE]
        res = await tx.run(
            update_task, taskId=task_id, updateProps=resolved, labels=labels
        )
        try:
            await res.single(strict=True)
        except ResultNotSingleError as e:
            raise UnknownTask(task_id) from e
    if error is not None:
        create_error = f"""MATCH (task:{TASK_NODE} {{{TASK_ID}: $taskId }})
WITH task
MERGE (error:{TASK_ERROR_NODE} {{id: $errorId}})
ON CREATE SET error = $errorProps
MERGE (error)-[:{TASK_ERROR_OCCURRED_TYPE}]->(task)
RETURN task, error
"""
        error_id = error.pop("id")
        labels = [TASK_NODE, TaskStatus[event["status"]].value]
        res = await tx.run(
            create_error,
            taskId=task_id,
            errorId=error_id,
            errorProps=error,
            labels=labels,
        )
        try:
            await res.single(strict=True)
        except ResultNotSingleError as e:
            raise UnknownTask(task_id) from e
