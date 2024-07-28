# pylint: disable=redefined-outer-name
import json
from datetime import datetime
from typing import List

import neo4j
import pytest

from icij_worker import (
    ManagerEvent,
    Neo4JTaskManager,
    Neo4jEventPublisher,
    Task,
    TaskState,
)
from icij_worker.objects import (
    ErrorEvent,
    Message,
    ProgressEvent,
    StacktraceItem,
    TaskError,
)


@pytest.fixture(scope="function")
def publisher(neo4j_async_app_driver: neo4j.AsyncDriver) -> Neo4jEventPublisher:
    worker = Neo4jEventPublisher(neo4j_async_app_driver)
    return worker


@pytest.mark.parametrize(
    "event",
    [
        ProgressEvent(task_id="task-0", progress=0.66),
        ErrorEvent(
            task_id="task-0",
            retries=2,
            error=TaskError(
                id="error-id",
                task_id="task-0",
                name="some-error",
                message="some message",
                stacktrace=[
                    StacktraceItem(name="SomeError", file="some details", lineno=666)
                ],
                occurred_at=datetime.now(),
            ),
        ),
        ErrorEvent(
            task_id="task-0",
            retries=1,
            error=TaskError(
                id="error-id",
                task_id="task-0",
                name="some-error",
                message="some message",
                stacktrace=[
                    StacktraceItem(name="SomeError", file="some details", lineno=666)
                ],
                occurred_at=datetime.now(),
            ),
        ),
    ],
)
async def test_worker_publish_event(
    populate_tasks: List[Task], publisher: Neo4jEventPublisher, event: ManagerEvent
):
    # Given
    driver = publisher.driver
    task = populate_tasks[0]
    assert task.state == TaskState.QUEUED
    assert task.progress is None
    assert task.retries is None
    assert task.completed_at is None

    # When
    await publisher.publish_event(event)

    # Then
    query = "MATCH (event:_ManagerEvent) RETURN event"
    db_events, _, _ = await driver.execute_query(query)
    assert len(db_events) == 1
    json_event = db_events[0]["event"]["event"]
    db_event = Message.parse_obj(json.loads(json_event))
    assert db_event == event


async def test_worker_publish_done_task_event_should_not_update_task(
    publisher: Neo4jEventPublisher,
    neo4j_task_manager: Neo4JTaskManager,
):
    # Given
    task_manager = neo4j_task_manager
    query = """CREATE (task:_Task:DONE {
        id: 'task-0', 
        name: 'hello_world',
        createdAt: $now,
        completedAt: $now,
        arguments: '{"greeted": "0"}'
     }) 
    RETURN task"""
    async with publisher.driver.session() as sess:
        res = await sess.run(query, now=datetime.now())
        completed = await res.single()
    completed = Task.from_neo4j(completed)
    event = ProgressEvent(task_id=completed.id, progress=0.99)

    # When
    await publisher.publish_event(event)
    saved_task = await task_manager.get_task(task_id=completed.id)

    # Then
    assert saved_task == completed


@pytest.mark.xfail(
    reason="worker and event publish should always know from which DB is task is"
    " coming from"
)
async def test_worker_publish_event_for_unknown_task(
    publisher: Neo4jEventPublisher, neo4j_task_manager: Neo4JTaskManager
):
    # This is useful when task is not reserved yet
    # Given
    task_manager = neo4j_task_manager
    task_id = "some-id"
    task_name = "hello_world"
    created_at = datetime.now()
    task = Task(
        id=task_id,
        name=task_name,
        created_at=created_at,
        state=TaskState.QUEUED,
    )
    event = ProgressEvent(task_id=task_id, progress=0.0)

    # When
    await publisher.publish_event(event)
    saved_task = await task_manager.get_task(task_id=task_id)

    # Then
    assert saved_task == task


async def test_worker_publish_event_should_use_state_resolution(
    publisher: Neo4jEventPublisher, neo4j_task_manager: Neo4JTaskManager
):
    # Given
    task_manager = neo4j_task_manager
    task_id = "task-0"
    query = """CREATE (task:_Task:DONE {
id: $taskId, 
name: 'hello_world',
createdAt: $now,
completedAt: $now,
arguments: '{"greeted": "0"}'
}) 
RETURN task"""
    async with publisher.driver.session() as sess:
        await sess.run(query, now=datetime.now(), taskId=task_id)
    task = await task_manager.get_task(task_id=task_id)
    assert task.state is TaskState.DONE

    event = ProgressEvent(task_id=task.id, progress=0.0)

    # When
    await publisher.publish_event(event)
    saved_task = await task_manager.get_task(task_id=task.id)

    # Then
    assert saved_task == task
