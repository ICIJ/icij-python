# pylint: disable=redefined-outer-name
from datetime import datetime
from typing import List

import neo4j
import pytest

from icij_common.pydantic_utils import safe_copy
from icij_worker import (
    Neo4JTaskManager,
    Neo4jEventPublisher,
    Task,
    TaskEvent,
    TaskState,
)
from icij_worker.objects import (
    ErrorEvent,
    ProgressEvent,
    StacktraceItem,
    TaskError,
    TaskUpdate,
)


@pytest.fixture(scope="function")
def publisher(neo4j_async_app_driver: neo4j.AsyncDriver) -> Neo4jEventPublisher:
    worker = Neo4jEventPublisher(neo4j_async_app_driver)
    return worker


@pytest.mark.parametrize(
    "event,task_update",
    [
        (
            ProgressEvent(task_id="task-0", progress=0.66, state=TaskState.RUNNING),
            TaskUpdate(task_id="task-0", progress=0.66),
        ),
        (
            ErrorEvent(
                task_id="task-0",
                retries=2,
                error=TaskError(
                    id="error-id",
                    task_id="task-0",
                    name="some-error",
                    message="some message",
                    stacktrace=[
                        StacktraceItem(
                            name="SomeError", file="some details", lineno=666
                        )
                    ],
                    occurred_at=datetime.now(),
                ),
                state=TaskState.QUEUED,
            ),
            TaskUpdate(
                task_id="task-0",
                retries=2,
                error=TaskError(
                    id="error-id",
                    task_id="task-0",
                    name="some-error",
                    message="some message",
                    stacktrace=[
                        StacktraceItem(
                            name="SomeError", file="some details", lineno=666
                        )
                    ],
                    occurred_at=datetime.now(),
                ),
                state=TaskState.QUEUED,
            ),
        ),
    ],
)
async def test_worker_publish_event(
    populate_tasks: List[Task],
    publisher: Neo4jEventPublisher,
    event: TaskEvent,
    task_update: TaskUpdate,
):
    # Given
    task_manager = Neo4JTaskManager(publisher.driver, max_queue_size=10)
    task = populate_tasks[0]
    assert task.state == TaskState.QUEUED
    assert task.progress is None
    assert task.retries is None
    assert task.completed_at is None

    # When
    await publisher.publish_event(event)
    saved_task = await task_manager.get_task(task_id=task.id)

    # Then
    update = task_update.dict(exclude_unset=True)
    update.pop("task_id", None)
    update.pop("error", None)
    expected = safe_copy(task, update=update)
    assert saved_task == expected


async def test_worker_publish_done_task_event_should_not_update_task(
    publisher: Neo4jEventPublisher,
):
    # Given
    query = """CREATE (task:_Task:DONE {
        id: 'task-0', 
        type: 'hello_world',
        createdAt: $now,
        completedAt: $now,
        arguments: '{"greeted": "0"}'
     }) 
    RETURN task"""
    async with publisher.driver.session() as sess:
        res = await sess.run(query, now=datetime.now())
        completed = await res.single()
    completed = Task.from_neo4j(completed)
    task_manager = Neo4JTaskManager(publisher.driver, max_queue_size=10)
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
async def test_worker_publish_event_for_unknown_task(publisher: Neo4jEventPublisher):
    # This is useful when task is not reserved yet
    # Given
    task_manager = Neo4JTaskManager(publisher.driver, max_queue_size=10)
    task_id = "some-id"
    task_type = "hello_world"
    created_at = datetime.now()
    task = Task(
        id=task_id,
        type=task_type,
        created_at=created_at,
        state=TaskState.QUEUED,
    )
    event = ProgressEvent(task_id=task_id, state=TaskState.QUEUED, progress=0.0)

    # When
    await publisher.publish_event(event)
    saved_task = await task_manager.get_task(task_id=task_id)

    # Then
    assert saved_task == task


async def test_worker_publish_event_should_use_state_resolution(
    publisher: Neo4jEventPublisher,
):
    # Given
    task_id = "task-0"
    query = """CREATE (task:_Task:DONE {
id: $taskId, 
type: 'hello_world',
createdAt: $now,
completedAt: $now,
arguments: '{"greeted": "0"}'
}) 
RETURN task"""
    async with publisher.driver.session() as sess:
        await sess.run(query, now=datetime.now(), taskId=task_id)
    task_manager = Neo4JTaskManager(publisher.driver, max_queue_size=10)
    task = await task_manager.get_task(task_id=task_id)
    assert task.state is TaskState.DONE

    event = ProgressEvent(task_id=task.id, state=TaskState.RUNNING, progress=0.0)

    # When
    await publisher.publish_event(event)
    saved_task = await task_manager.get_task(task_id=task.id)

    # Then
    assert saved_task == task
