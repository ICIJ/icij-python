# pylint: disable=redefined-outer-name
from datetime import datetime
from typing import List

import neo4j
import pytest

from icij_common.neo4j.constants import TASK_CANCEL_EVENT_CREATED_AT_DEPRECATED
from icij_worker import Neo4JTaskManager, Task, TaskError, TaskState
from icij_worker.task_storage.neo4j_ import (
    migrate_add_index_to_task_namespace_v0_tx,
    migrate_cancelled_event_created_at_v0_tx,
    migrate_task_errors_v0_tx,
    migrate_task_inputs_to_arguments_v0_tx,
)

_NOW = datetime.now()


@pytest.fixture(scope="function")
async def populate_tasks_legacy(neo4j_test_driver: neo4j.AsyncDriver, request):
    driver = neo4j_test_driver
    namespace = getattr(request, "param", None)
    query_0 = """CREATE (task:_Task:QUEUED {
    namespace: $namespace,
    id: 'task-0', 
    type: 'hello_world',
    createdAt: $now,
    inputs: '{"greeted": "0"}'
 }) 
RETURN task"""
    await driver.execute_query(query_0, now=_NOW, namespace=namespace)
    query_1 = """CREATE (task:_Task:RUNNING {
    id: 'task-1', 
    namespace: $namespace,
    type: 'hello_world',
    progress: 66.6,
    createdAt: $now,
    retries: 1,
    inputs: '{"greeted": "1"}'
 }) 
RETURN task"""
    await driver.execute_query(query_1, now=_NOW, namespace=namespace)


@pytest.fixture(scope="function")
async def _populate_errors_legacy(
    populate_tasks: List[Task], neo4j_async_app_driver: neo4j.AsyncDriver
) -> neo4j.AsyncDriver:
    task_with_error = populate_tasks[1]
    query_0 = """MATCH (task:_Task { id: $taskId })
CREATE  (error:_TaskError {
    id: 'error-0',
    title: 'error',
    detail: 'with details',
    occurredAt: $now 
})-[:_OCCURRED_DURING]->(task)
RETURN error"""
    await neo4j_async_app_driver.execute_query(
        query_0, taskId=task_with_error.id, now=datetime.now()
    )
    query_1 = """MATCH (task:_Task { id: $taskId })
CREATE  (error:_TaskError {
    id: 'error-1',
    title: 'error',
    detail: 'same error again',
    occurredAt: $now 
})-[:_OCCURRED_DURING]->(task)
RETURN error"""
    await neo4j_async_app_driver.execute_query(
        query_1,
        taskId=task_with_error.id,
        now=datetime.now(),
    )
    return neo4j_async_app_driver


async def test_migrate_task_errors_v0_tx(
    _populate_errors_legacy: neo4j.AsyncDriver,  # pylint: disable=invalid-name
    neo4j_task_manager: Neo4JTaskManager,
):
    # Given
    task_id = "task-1"
    driver = _populate_errors_legacy
    task_manager = neo4j_task_manager
    # When
    async with driver.session() as session:
        await session.execute_write(migrate_task_errors_v0_tx)

    # Then / When
    retrieved_errors = await task_manager.get_task_errors(task_id=task_id)
    expected_errors = [
        TaskError(
            id="error-1",
            task_id="task-1",
            name="error",
            message="same error again",
            cause=None,
            stacktrace=[],
            occurred_at=datetime.now(),
        ),
        TaskError(
            id="error-0",
            task_id="task-1",
            name="error",
            message="with details",
            cause=None,
            stacktrace=[],
            occurred_at=datetime.now(),
        ),
    ]
    expected_errors = [e.dict() for e in expected_errors]
    for e in expected_errors:
        e.pop("occurred_at")
    retrieved_errors = [e.dict() for e in retrieved_errors]
    for e in retrieved_errors:
        e.pop("occurred_at")
    assert retrieved_errors == expected_errors


async def test_migrate_cancelled_event_created_at_v0_tx(
    neo4j_test_driver: neo4j.AsyncDriver,
):
    # Given
    created_at = datetime.now()
    driver = neo4j_test_driver
    query = f""" CREATE (task:_Task {{ taskID: $taskId }})-[
    :_CANCELLED_BY]->(:_CancelEvent {{ 
        {TASK_CANCEL_EVENT_CREATED_AT_DEPRECATED}: $createdAt, 
        effective: false,
        requeue: false
    }})
"""
    await driver.execute_query(query, createdAt=created_at, taskId="some-id")

    # When
    async with driver.session() as sess:
        await sess.execute_write(migrate_cancelled_event_created_at_v0_tx)
        event_query = "MATCH (evt:_CancelEvent) RETURN evt"
        res = await sess.run(event_query)
        rec = await res.single(strict=True)

    # Then
    rec = rec["evt"]
    assert "createdAt" not in rec
    assert rec["cancelledAt"].to_native() == created_at


async def test_migrate_add_index_to_task_namespace_v0_tx(
    neo4j_test_driver: neo4j.AsyncDriver,
):
    async with neo4j_test_driver.session() as sess:
        indexes_res = await sess.run("SHOW INDEXES")
        existing_indexes = set()
        async for rec in indexes_res:
            existing_indexes.add(rec["name"])
        assert "index_task_namespace" not in existing_indexes

        # When
        await sess.execute_write(migrate_add_index_to_task_namespace_v0_tx)

        # Then
        indexes_res = await sess.run("SHOW INDEXES")
        existing_indexes = set()
        async for rec in indexes_res:
            existing_indexes.add(rec["name"])
        assert "index_task_namespace" in existing_indexes


async def test_migrate_task_inputs_to_arguments_v0_tx(
    neo4j_test_driver: neo4j.AsyncDriver, populate_tasks_legacy
):
    # pylint: disable=unused-argument
    # Given
    task_manager = Neo4JTaskManager("test-app", neo4j_test_driver, max_queue_size=10)
    driver = task_manager.driver
    # When
    async with driver.session() as session:
        await session.execute_write(migrate_task_inputs_to_arguments_v0_tx)

    # Then / When
    expected = [
        Task(
            id="task-0",
            type="hello_world",
            arguments={"greeted": "0"},
            state=TaskState.QUEUED,
            created_at=datetime.now(),
        ),
        Task(
            id="task-1",
            type="hello_world",
            arguments={"greeted": "1"},
            state=TaskState.RUNNING,
            progress=66.6,
            created_at=datetime.now(),
            retries=1,
        ),
    ]
    expected = [t.dict(by_alias=True, exclude_unset=True) for t in expected]
    for t in expected:
        t.pop("createdAt")
    expected = sorted(expected, key=lambda x: x["id"])
    retrieved_tasks = await task_manager.get_tasks(namespace=None)
    retrieved_tasks = [
        t.dict(by_alias=True, exclude_unset=True) for t in retrieved_tasks
    ]
    for t in retrieved_tasks:
        t.pop("createdAt")
    retrieved_tasks = sorted(retrieved_tasks, key=lambda x: x["id"])
    assert retrieved_tasks == expected
