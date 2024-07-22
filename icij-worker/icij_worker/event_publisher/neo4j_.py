import json
from copy import deepcopy
from typing import Dict, Optional

import neo4j
from neo4j.exceptions import ResultNotSingleError

from icij_common.neo4j.constants import (
    TASK_ERROR_ID,
    TASK_ERROR_NODE,
    TASK_ERROR_OCCURRED_TYPE,
    TASK_ID,
    TASK_NODE,
)
from icij_worker.event_publisher.event_publisher import EventPublisher
from icij_worker.exceptions import UnknownTask
from icij_worker.objects import Message, Task, TaskEvent, TaskState
from icij_worker.task_storage.neo4j_ import Neo4jStorage


class Neo4jEventPublisher(Neo4jStorage, EventPublisher):

    async def _publish_event(self, event: TaskEvent):
        async with self._task_session(event.task_id) as sess:
            await _publish_event(sess, event)

    @property
    def driver(self) -> neo4j.AsyncDriver:
        return self._driver


async def _publish_event(sess: neo4j.AsyncSession, event: TaskEvent):
    event = {k: v for k, v in event.dict(by_alias=True).items() if v is not None}
    if "state" in event:
        event["state"] = event["state"].value
    error = event.pop("error", None)
    if error is not None:
        error["stacktrace"] = [json.dumps(item) for item in error["stacktrace"]]
        error.pop("@type")
    await sess.execute_write(_publish_event_tx, event, error)


async def _publish_event_tx(
    tx: neo4j.AsyncTransaction, event: Dict, error: Optional[Dict]
):
    task_id = event["taskId"]
    create_task = f"""MERGE (task:{TASK_NODE} {{{TASK_ID}: $taskId }})
ON CREATE SET task += $createProps"""
    state = event.get("state")
    if state:
        create_task += f", task:`{state}`"
    create_task += "\nRETURN task"
    if error is not None:
        event["error"] = deepcopy(error)
        event["error"]["stacktrace"] = [
            json.loads(item) for item in event["error"]["stacktrace"]
        ]
    as_event = Message.parse_obj(event)
    create_props = Task.mandatory_fields(as_event, keep_id=False)
    create_props.pop("state", None)
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
        create_error = f"""MATCH (t:{TASK_NODE} {{{TASK_ID}: $taskId }})
WITH t
CALL apoc.create.setLabels(t, $labels) YIELD node AS task
WITH task
MERGE (error:{TASK_ERROR_NODE} {{{TASK_ERROR_ID}: $errorId}})
ON CREATE SET error += $errorProps
MERGE (error)-[:{TASK_ERROR_OCCURRED_TYPE}]->(task)
RETURN task, error
"""
        error_id = error.pop("id")
        labels = [TASK_NODE, TaskState[event["state"]].value]
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
