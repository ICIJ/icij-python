from contextlib import asynccontextmanager
from typing import AsyncGenerator, Dict, List, Optional

import neo4j
from neo4j.exceptions import ResultNotSingleError

from icij_common.neo4j.constants import (
    TASK_ERROR_NODE,
    TASK_ERROR_OCCURRED_TYPE,
    TASK_ID,
    TASK_NODE,
)
from icij_common.neo4j.migrate import retrieve_projects
from icij_common.neo4j.projects import project_db_session
from icij_worker.event_publisher.event_publisher import EventPublisher
from icij_worker.task import Task, TaskEvent, TaskStatus
from icij_worker.exceptions import UnknownTask


class Neo4jTaskProjectMixin:
    _driver: neo4j.AsyncDriver
    _task_projects: Dict[str, str] = dict()

    @asynccontextmanager
    async def _project_session(
        self, project_id: str
    ) -> AsyncGenerator[neo4j.AsyncSession, None]:
        async with project_db_session(self._driver, project_id) as sess:
            yield sess

    async def _get_task_project_id(self, task_id: str) -> str:
        if task_id not in self._task_projects:
            await self._refresh_task_projects()
        try:
            return self._task_projects[task_id]
        except KeyError as e:
            raise UnknownTask(task_id) from e

    async def _refresh_task_projects(self):
        projects = await retrieve_projects(self._driver)
        for p in projects:
            async with self._project_session(p) as sess:
                # Here we make the assumption that task IDs are unique across
                # projects and not per project
                task_projects = {
                    t: p.name for t in await sess.execute_read(_get_task_ids_tx)
                }
                self._task_projects.update(task_projects)


async def _get_task_ids_tx(tx: neo4j.AsyncTransaction) -> List[str]:
    query = f"""MATCH (task:{TASK_NODE})
RETURN task.{TASK_ID} as taskId"""
    res = await tx.run(query)
    ids = [rec["taskId"] async for rec in res]
    return ids


class Neo4jEventPublisher(Neo4jTaskProjectMixin, EventPublisher):
    def __init__(self, driver: neo4j.AsyncDriver):
        self._driver = driver

    async def _publish_event(self, event: TaskEvent):
        project_id = event.project_id
        if project_id is None:
            msg = (
                "neo4j expects project_id to be provided in order to fetch tasks from"
                " the project_id's DB"
            )
            raise ValueError(msg)
        async with self._project_session(project_id) as sess:
            await _publish_event(sess, event)

    @property
    def driver(self) -> neo4j.AsyncDriver:
        return self._driver


async def _publish_event(sess: neo4j.AsyncSession, event: TaskEvent):
    event = {k: v for k, v in event.dict(by_alias=True).items() if v is not None}
    if "status" in event:
        event["status"] = event["status"].value
    error = event.pop("error", None)
    await sess.execute_write(_publish_event_tx, event, error)


async def _publish_event_tx(
    tx: neo4j.AsyncTransaction, event: Dict, error: Optional[Dict]
):
    task_id = event["taskId"]
    project_id = event["project"]
    create_task = f"""MERGE (task:{TASK_NODE} {{{TASK_ID}: $taskId }})
ON CREATE SET task += $createProps"""
    status = event.get("status")
    if status:
        create_task += f", task:`{status}`"
    create_task += "\nRETURN task"
    event_as_event = TaskEvent(**event)
    create_props = Task.mandatory_fields(event_as_event, keep_id=False)
    create_props.pop("status", None)
    res = await tx.run(create_task, taskId=task_id, createProps=create_props)
    tasks = [Task.from_neo4j(rec, project_id=project_id) async for rec in res]
    task = tasks[0]
    resolved = task.resolve_event(event_as_event)
    resolved = (
        resolved.dict(exclude_unset=True, by_alias=True)
        if resolved is not None
        else resolved
    )
    if resolved:
        resolved.pop("taskId")
        # Status can't be updated by event, only by ack, nack, enqueue and so on
        resolved.pop("status", None)
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
