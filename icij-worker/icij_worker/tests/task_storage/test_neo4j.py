# pylint: disable=redefined-outer-name
from datetime import datetime
from typing import List

import neo4j
import pytest
from neo4j import AsyncDriver

from icij_worker.constants import (
    NEO4J_TASK_CANCEL_EVENT_CREATED_AT_DEPRECATED,
    NEO4J_TASK_TYPE_DEPRECATED,
)
from icij_worker import AsyncApp, Neo4JTaskManager, Task, TaskError, TaskState
from icij_worker.app import AsyncAppConfig
from icij_worker.objects import ErrorEvent, StacktraceItem
from icij_worker.task_storage.neo4j_ import (
    migrate_add_index_to_task_namespace_v0_tx,
    migrate_add_task_shutdown_v0_tx,
    migrate_cancelled_event_created_at_v0_tx,
    migrate_index_event_dates_v0_tx,
    migrate_rename_task_cancelled_at_into_created_at_v0_tx,
    migrate_task_arguments_into_args_v0_tx,
    migrate_task_errors_v0_tx,
    migrate_task_inputs_to_arguments_v0_tx,
    migrate_task_namespace_into_group_v0,
    migrate_task_progress_v0_tx,
    migrate_task_retries_and_error_v0_tx,
    migrate_task_type_to_name_v0,
)

_NOW = datetime.now()


@pytest.fixture
def neo4j_task_manager(neo4j_test_driver: neo4j.AsyncDriver) -> Neo4JTaskManager:
    app = AsyncApp("test-app", config=AsyncAppConfig(max_task_queue_size=True))
    task_manager = Neo4JTaskManager(app, neo4j_test_driver)
    return task_manager


@pytest.fixture(scope="function")
async def populate_tasks_legacy_v0(neo4j_test_driver: neo4j.AsyncDriver, request):
    driver = neo4j_test_driver
    namespace = getattr(request, "param", None)
    query_0 = """CREATE (task:_Task:QUEUED {
    namespace: $namespace,
    id: 'task-0', 
    name: 'hello_world',
    createdAt: $now,
    inputs: '{"greeted": "0"}'
 }) 
RETURN task"""
    await driver.execute_query(query_0, now=_NOW, namespace=namespace)
    query_1 = """CREATE (task:_Task:RUNNING {
    id: 'task-1', 
    namespace: $namespace,
    name: 'hello_world',
    progress: 0.66,
    createdAt: $now,
    retriesLeft: 1,
    inputs: '{"greeted": "1"}'
 }) 
RETURN task"""
    await driver.execute_query(query_1, now=_NOW, namespace=namespace)


@pytest.fixture(scope="function")
async def populate_tasks_legacy_v1(neo4j_test_driver: neo4j.AsyncDriver, request):
    namespace = getattr(request, "param", None)
    query_0 = """CREATE (task:_Task:QUEUED {
    namespace: $namespace,
    id: 'task-0', 
    type: 'hello_world',
    createdAt: $now,
    args: '{"greeted": "0"}'
 }) 
RETURN task"""
    await neo4j_test_driver.execute_query(query_0, now=_NOW, namespace=namespace)
    query_1 = """CREATE (task:_Task:RUNNING {
    id: 'task-1', 
    namespace: $namespace,
    type: 'hello_world',
    progress: 0.66,
    createdAt: $now,
    retriesLeft: 1,
    args: '{"greeted": "1"}'
 }) 
RETURN task"""
    await neo4j_test_driver.execute_query(query_1, now=_NOW, namespace=namespace)


@pytest.fixture(scope="function")
async def populate_tasks_legacy_v2(neo4j_test_driver: neo4j.AsyncDriver, request):
    namespace = getattr(request, "param", None)
    query_0 = """CREATE (task:_Task:QUEUED {
    namespace: $namespace,
    id: 'task-0', 
    name: 'hello_world',
    createdAt: $now,
    args: '{"greeted": "0"}'
 }) 
RETURN task"""
    await neo4j_test_driver.execute_query(query_0, now=_NOW, namespace=namespace)
    query_1 = """CREATE (task:_Task:RUNNING {
    id: 'task-1', 
    namespace: $namespace,
    name: 'hello_world',
    progress: 66.6,
    createdAt: $now,
    retriesLeft: 1,
    args: '{"greeted": "1"}'
 }) 
RETURN task"""
    await neo4j_test_driver.execute_query(query_1, now=_NOW, namespace=namespace)


@pytest.fixture(scope="function")
async def populate_tasks_legacy_v3(neo4j_async_app_driver: neo4j.AsyncDriver, request):
    namespace = getattr(request, "param", None)
    query_0 = """CREATE (task:_Task:QUEUED {
    namespace: $namespace,
    id: 'task-0', 
    name: 'hello_world',
    createdAt: $now,
    args: '{"greeted": "0"}'
 }) 
RETURN task"""
    await neo4j_async_app_driver.execute_query(query_0, now=_NOW, namespace=namespace)
    query_1 = """CREATE (task:_Task:RUNNING {
    id: 'task-1', 
    namespace: $namespace,
    name: 'hello_world',
    progress: 0.66,
    createdAt: $now,
    retries: 1,
    args: '{"greeted": "1"}'
 }) 
RETURN task"""
    await neo4j_async_app_driver.execute_query(query_1, now=_NOW, namespace=namespace)


@pytest.fixture(scope="function")
async def populate_tasks_legacy_v4(neo4j_test_driver: neo4j.AsyncDriver):
    task_name = "hello_world"
    query_0 = """CREATE (task:_Task:QUEUED {
    namespace: $namespace,
    id: 'task-0', 
    name: $taskName,
    createdAt: $now,
    arguments: '{"greeted": "0"}'
 }) 
RETURN task"""
    await neo4j_test_driver.execute_query(
        query_0, now=_NOW, taskName=task_name, namespace=None
    )
    query_1 = """CREATE (task:_Task:RUNNING {
id: 'task-1', 
namespace: $namespace,
name: $taskName,
progress: 0.66,
createdAt: $now,
retriesLeft: 1,
arguments: '{"greeted": "1"}'
}) 
RETURN task"""
    await neo4j_test_driver.execute_query(
        query_1, now=_NOW, taskName=task_name, namespace=None
    )


@pytest.fixture(scope="function")
async def populate_tasks_legacy_v5(neo4j_test_driver: neo4j.AsyncDriver):
    query_0 = """CREATE (task:_Task:QUEUED {
    namespace: 'hello_world_namespace',
    id: 'task-0', 
    name: 'hello_world',
    createdAt: $now,
    args: '{"greeted": "0"}'
 }) 
RETURN task"""
    await neo4j_test_driver.execute_query(query_0, now=_NOW)
    query_1 = """CREATE (task:_Task:RUNNING {
    id: 'task-1', 
    name: 'hello_world',
    progress: 0.66,
    createdAt: $now,
    retriesLeft: 1,
    args: '{"greeted": "1"}'
 }) 
RETURN task"""
    await neo4j_test_driver.execute_query(query_1, now=_NOW)


@pytest.fixture(scope="function")
async def populate_tasks_legacy_v6(neo4j_test_driver: neo4j.AsyncDriver):
    query_0 = """CREATE (task:_Task:CANCELLED {
    id: 'task-0', 
    name: 'hello_world',
    createdAt: $now,
    cancelledAt: $now,
    args: '{"greeted": "0"}'
 }) 
RETURN task"""
    await neo4j_test_driver.execute_query(query_0, now=_NOW)


@pytest.fixture(scope="function")
async def populate_errors_legacy_v0(
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


@pytest.fixture(scope="function")
async def populate_errors_legacy_v1(
    neo4j_async_app_driver: neo4j.AsyncDriver,
) -> neo4j.AsyncDriver:
    query_0 = """MATCH (task:_Task { id: $taskId })
CREATE  (error:_TaskError {
    id: 'error-0',
    name: 'error',
    stacktrace: ['{"name": "SomeError", "file": "somefile", "lineno": 666}'],
    message: "with details",
    cause: "some cause",
    occurredAt: $now 
})-[:_OCCURRED_DURING]->(task)
RETURN error"""
    await neo4j_async_app_driver.execute_query(
        query_0, taskId="task-1", now=datetime.now()
    )
    query_1 = """MATCH (task:_Task { id: $taskId })
CREATE  (error:_TaskError {
    id: 'error-1',
    name: 'error',
    stacktrace: ['{"name": "SomeError", "file": "somefile", "lineno": 666}'],
    message: 'same error again',
    cause: "some cause",
    occurredAt: $now 
})-[:_OCCURRED_DURING]->(task)
RETURN error"""
    await neo4j_async_app_driver.execute_query(
        query_1, taskId="task-1", now=datetime.now()
    )
    return neo4j_async_app_driver


async def test_migrate_task_errors_v0_tx(populate_errors_legacy_v0: AsyncDriver):
    # Given
    driver = populate_errors_legacy_v0
    # When
    async with driver.session() as session:
        await session.execute_write(migrate_task_errors_v0_tx)

    # Then / When
    query = "MATCH (error:_TaskError) RETURN error ORDER BY error.occurredAt DESC"
    recs, _, _ = await driver.execute_query(query)
    errors = [rec["error"] for rec in recs]
    ids = [err["id"] for err in errors]
    assert ids == ["error-1", "error-0"]
    assert all("name" in err for err in errors)
    assert all("message" in err for err in errors)
    assert all("stacktrace" in err for err in errors)
    assert not any("title" in err for err in errors)
    assert not any("detail" in err for err in errors)


async def test_migrate_cancelled_event_created_at_v0_tx(
    neo4j_test_driver: neo4j.AsyncDriver,
):
    # Given
    created_at = datetime.now()
    driver = neo4j_test_driver
    query = f""" CREATE (task:_Task {{ taskID: $taskId }})-[
    :_CANCELLED_BY]->(:_CancelEvent {{ 
        {NEO4J_TASK_CANCEL_EVENT_CREATED_AT_DEPRECATED}: $createdAt, 
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
    neo4j_task_manager: Neo4JTaskManager, populate_tasks_legacy_v0
):
    # pylint: disable=unused-argument
    # Given
    task_manager = neo4j_task_manager
    driver = task_manager.driver
    # When
    async with driver.session() as session:
        await session.execute_write(migrate_task_inputs_to_arguments_v0_tx)

    # Then / When
    expected = [
        Task(
            id="task-0",
            name="hello_world",
            args={"greeted": "0"},
            state=TaskState.QUEUED,
            created_at=datetime.now(),
        ),
        Task(
            id="task-1",
            name="hello_world",
            args={"greeted": "1"},
            state=TaskState.RUNNING,
            progress=0.66,
            created_at=datetime.now(),
            retries_left=1,
        ),
    ]
    expected = [t.dict(by_alias=True, exclude_unset=True) for t in expected]
    for t in expected:
        t.pop("createdAt")
    expected = sorted(expected, key=lambda x: x["id"])
    retrieved_tasks = await task_manager.get_tasks(group=None)
    retrieved_tasks = [
        t.dict(by_alias=True, exclude_unset=True) for t in retrieved_tasks
    ]
    for t in retrieved_tasks:
        t.pop("createdAt")
    retrieved_tasks = sorted(retrieved_tasks, key=lambda x: x["id"])
    assert retrieved_tasks == expected


async def test_migrate_task_type_to_name_v0_tx(
    neo4j_task_manager: Neo4JTaskManager,
    populate_tasks_legacy_v1,  # pylint: disable=unused-argument
):
    # Given
    task_manager = neo4j_task_manager
    driver = neo4j_task_manager.driver
    create_legacy_index = f"""CREATE INDEX task_type_index
FOR (task:_Task) 
ON (task.{NEO4J_TASK_TYPE_DEPRECATED})"""
    await driver.execute_query(create_legacy_index)
    # When
    async with driver.session() as sess:
        await migrate_task_type_to_name_v0(sess)
        # Then
        indexes_res = await sess.run("SHOW INDEXES")
        existing_indexes = set()
        async for rec in indexes_res:
            existing_indexes.add(rec["name"])
        assert "index_task_name" in existing_indexes
        assert "index_task_type" not in existing_indexes
        expected = [
            Task(
                id="task-0",
                name="hello_world",
                args={"greeted": "0"},
                state=TaskState.QUEUED,
                created_at=datetime.now(),
            ),
            Task(
                id="task-1",
                name="hello_world",
                args={"greeted": "1"},
                state=TaskState.RUNNING,
                progress=0.66,
                created_at=datetime.now(),
                retries_left=1,
            ),
        ]
        expected = [t.dict(by_alias=True, exclude_unset=True) for t in expected]
        for t in expected:
            t.pop("createdAt")
        expected = sorted(expected, key=lambda x: x["id"])
        retrieved_tasks = await task_manager.get_tasks(group=None)
        retrieved_tasks = [
            t.dict(by_alias=True, exclude_unset=True) for t in retrieved_tasks
        ]
        for t in retrieved_tasks:
            t.pop("createdAt")
        retrieved_tasks = sorted(retrieved_tasks, key=lambda x: x["id"])
        assert retrieved_tasks == expected


async def test_migrate_task_progress_v0_tx(
    neo4j_task_manager: Neo4JTaskManager, populate_tasks_legacy_v2
):
    # pylint: disable=unused-argument
    # Given
    task_manager = neo4j_task_manager
    driver = task_manager.driver
    # When
    async with driver.session() as session:
        await session.execute_write(migrate_task_progress_v0_tx)

    # Then / When
    expected = [
        Task(
            id="task-0",
            name="hello_world",
            args={"greeted": "0"},
            state=TaskState.QUEUED,
            created_at=datetime.now(),
        ),
        Task(
            id="task-1",
            name="hello_world",
            args={"greeted": "1"},
            state=TaskState.RUNNING,
            progress=66.6 / 100,
            created_at=datetime.now(),
            retries_left=1,
        ),
    ]
    expected = [t.dict(by_alias=True, exclude_unset=True) for t in expected]
    for t in expected:
        t.pop("createdAt")
    expected = sorted(expected, key=lambda x: x["id"])
    retrieved_tasks = await task_manager.get_tasks(group=None)
    retrieved_tasks = [
        t.dict(by_alias=True, exclude_unset=True) for t in retrieved_tasks
    ]
    for t in retrieved_tasks:
        t.pop("createdAt")
    retrieved_tasks = sorted(retrieved_tasks, key=lambda x: x["id"])
    assert retrieved_tasks == pytest.approx(expected, abs=0.01)


async def test_migrate_index_event_dates_v0_tx(neo4j_test_driver: neo4j.AsyncDriver):
    async with neo4j_test_driver.session() as sess:
        indexes_res = await sess.run("SHOW INDEXES")
        existing_indexes = set()
        async for rec in indexes_res:
            existing_indexes.add(rec["name"])
        assert "index_manager_events_created_at" not in existing_indexes
        assert "index_canceled_events_created_at" not in existing_indexes

        # When
        await sess.execute_write(migrate_index_event_dates_v0_tx)

        # Then
        indexes_res = await sess.run("SHOW INDEXES")
        existing_indexes = set()
        async for rec in indexes_res:
            existing_indexes.add(rec["name"])
        assert "index_manager_events_created_at" in existing_indexes
        assert "index_canceled_events_created_at" in existing_indexes


async def test_migrate_task_retries_and_error_v0_tx(
    neo4j_task_manager: Neo4JTaskManager,
    populate_tasks_legacy_v3,  # pylint: disable=unused-argument
    populate_errors_legacy_v1,  # pylint: disable=unused-argument
):

    task_manager = neo4j_task_manager
    driver = task_manager.driver
    # When
    async with driver.session() as session:
        await session.execute_write(migrate_task_retries_and_error_v0_tx)

    # Then / When
    expected_tasks = [
        Task(
            id="task-0",
            name="hello_world",
            args={"greeted": "0"},
            state=TaskState.QUEUED,
            created_at=datetime.now(),
            retries_left=3,
            max_retries=3,
        ),
        Task(
            id="task-1",
            name="hello_world",
            args={"greeted": "1"},
            state=TaskState.RUNNING,
            progress=0.66,
            created_at=datetime.now(),
            retries_left=2,
            max_retries=3,
        ),
    ]
    expected_tasks = [t.dict(by_alias=True) for t in expected_tasks]
    for t in expected_tasks:
        t.pop("createdAt")
    expected_tasks = sorted(expected_tasks, key=lambda x: x["id"])
    retrieved_tasks = await task_manager.get_tasks(group=None)
    retrieved_tasks = [t.dict(by_alias=True) for t in retrieved_tasks]
    for t in retrieved_tasks:
        t.pop("createdAt")
    retrieved_tasks = sorted(retrieved_tasks, key=lambda x: x["id"])
    assert retrieved_tasks == expected_tasks

    retrieved_errors = await task_manager.get_task_errors(task_id="task-1")
    expected_errors = [
        ErrorEvent(
            task_id="task-1",
            error=TaskError(
                name="error",
                message="same error again",
                cause="some cause",
                stacktrace=[
                    StacktraceItem(name="SomeError", file="somefile", lineno=666)
                ],
            ),
            created_at=datetime.now(),
            retries_left=3,
        ),
        ErrorEvent(
            task_id="task-1",
            error=TaskError(
                name="error",
                message="with details",
                cause="some cause",
                stacktrace=[
                    StacktraceItem(name="SomeError", file="somefile", lineno=666)
                ],
            ),
            created_at=datetime.now(),
            retries_left=3,
        ),
    ]
    expected_errors = [e.dict(exclude={"created_at"}) for e in expected_errors]
    retrieved_errors = [e.dict(exclude={"created_at"}) for e in retrieved_errors]
    assert retrieved_errors == expected_errors


async def test_migrate_task_arguments_into_args_v0_tx(
    neo4j_task_manager: Neo4JTaskManager,
    populate_tasks_legacy_v4,  # pylint: disable=unused-argument
):
    # Given
    task_manager = neo4j_task_manager
    driver = neo4j_task_manager.driver
    # When
    async with driver.session() as sess:
        await sess.execute_write(migrate_task_arguments_into_args_v0_tx)
        # Then
        expected = [
            Task(
                id="task-0",
                name="hello_world",
                args={"greeted": "0"},
                state=TaskState.QUEUED,
                created_at=datetime.now(),
            ),
            Task(
                id="task-1",
                name="hello_world",
                args={"greeted": "1"},
                state=TaskState.RUNNING,
                progress=0.66,
                created_at=datetime.now(),
                retries_left=1,
            ),
        ]
        expected = [t.dict(by_alias=True, exclude_unset=True) for t in expected]
        for t in expected:
            t.pop("createdAt")
        expected = sorted(expected, key=lambda x: x["id"])
        retrieved_tasks = await task_manager.get_tasks(group=None)
        retrieved_tasks = [
            t.dict(by_alias=True, exclude_unset=True) for t in retrieved_tasks
        ]
        for t in retrieved_tasks:
            t.pop("createdAt")
        retrieved_tasks = sorted(retrieved_tasks, key=lambda x: x["id"])
        assert retrieved_tasks == expected


async def test_migrate_task_namespace_into_group_v0(
    neo4j_task_manager: Neo4JTaskManager,
    populate_tasks_legacy_v5,  # pylint: disable=unused-argument
):
    # Given
    task_manager = neo4j_task_manager
    driver = neo4j_task_manager.driver
    create_legacy_index = f"""CREATE INDEX task_type_index
FOR (task:_Task) 
ON (task.{NEO4J_TASK_TYPE_DEPRECATED})"""
    await driver.execute_query(create_legacy_index)
    # When
    async with driver.session() as sess:
        await migrate_task_namespace_into_group_v0(sess)
        # Then
        indexes_res = await sess.run("SHOW INDEXES")
        existing_indexes = set()
        async for rec in indexes_res:
            existing_indexes.add(rec["name"])
        assert "index_task_group" in existing_indexes
        assert "index_task_namespace" not in existing_indexes
        expected = [
            Task(
                id="task-0",
                name="hello_world",
                args={"greeted": "0"},
                state=TaskState.QUEUED,
                created_at=datetime.now(),
            ),
            Task(
                id="task-1",
                name="hello_world",
                args={"greeted": "1"},
                state=TaskState.RUNNING,
                progress=0.66,
                created_at=datetime.now(),
                retries_left=1,
            ),
        ]
        expected = [t.dict(by_alias=True, exclude_unset=True) for t in expected]
        for t in expected:
            t.pop("createdAt")
        expected = sorted(expected, key=lambda x: x["id"])
        retrieved_tasks = await task_manager.get_tasks(group="hello_world_namespace")
        retrieved_tasks = [
            t.dict(by_alias=True, exclude_unset=True) for t in retrieved_tasks
        ]
        for t in retrieved_tasks:
            t.pop("createdAt")
        retrieved_tasks = sorted(retrieved_tasks, key=lambda x: x["id"])
        assert retrieved_tasks == expected[:1]

        retrieved_tasks = await task_manager.get_tasks(group=None)
        retrieved_tasks = [
            t.dict(by_alias=True, exclude_unset=True) for t in retrieved_tasks
        ]
        for t in retrieved_tasks:
            t.pop("createdAt")
        retrieved_tasks = sorted(retrieved_tasks, key=lambda x: x["id"])
        assert retrieved_tasks == expected


async def test_migrate_add_task_shutdown_v0_tx(
    neo4j_test_driver: neo4j.AsyncDriver,
    populate_tasks_legacy_v5,  # pylint: disable=unused-argument
):
    async with neo4j_test_driver.session() as sess:
        indexes_res = await sess.run("SHOW INDEXES")
        existing_indexes = set()
        async for rec in indexes_res:
            existing_indexes.add(rec["name"])
        assert "index_worker_id" not in existing_indexes
        assert "index_shutdown_event_created_at" not in existing_indexes

        # When
        await sess.execute_write(migrate_add_task_shutdown_v0_tx)

        # Then
        indexes_res = await sess.run("SHOW INDEXES")
        existing_indexes = set()
        async for rec in indexes_res:
            existing_indexes.add(rec["name"])
        assert "index_worker_id" in existing_indexes
        assert "index_shutdown_event_created_at" in existing_indexes


async def test_migrate_rename_task_cancelled_at_into_created_at_v0_tx(
    neo4j_test_driver: neo4j.AsyncDriver,
    populate_tasks_legacy_v6,  # pylint: disable=unused-argument
):
    # When
    async with neo4j_test_driver.session() as sess:
        await sess.execute_write(migrate_rename_task_cancelled_at_into_created_at_v0_tx)
        task_query = "MATCH (task:_Task) RETURN task"
        tasks = await sess.run(task_query)
        tasks = [t async for t in tasks]
        assert len(tasks) == 1
        task = tasks[0]["task"]
        assert task.get("cancelledAt") is None
        assert task.get("completedAt") is not None
