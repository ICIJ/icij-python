# pylint: disable=redefined-outer-scope
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import neo4j
import pytest
import pytest_asyncio
from neo4j.time import DateTime

from icij_common.pydantic_utils import safe_copy
from icij_worker import (
    Neo4JTaskManager,
    Task,
    TaskError,
    TaskResult,
    TaskState,
)
from icij_worker.exceptions import MissingTaskResult, TaskAlreadyQueued, TaskQueueIsFull
from icij_worker.objects import CancelEvent, StacktraceItem


@pytest_asyncio.fixture(scope="function")
async def _populate_errors(
    populate_tasks: List[Task], neo4j_async_app_driver: neo4j.AsyncDriver
) -> List[Tuple[Task, List[TaskError]]]:
    task_with_error = populate_tasks[1]
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
    recs_0, _, _ = await neo4j_async_app_driver.execute_query(
        query_0, taskId=task_with_error.id, now=datetime.now()
    )
    e_0 = TaskError.from_neo4j(recs_0[0], task_id=task_with_error.id)
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
    recs_1, _, _ = await neo4j_async_app_driver.execute_query(
        query_1,
        taskId=task_with_error.id,
        now=datetime.now(),
    )
    e_1 = TaskError.from_neo4j(recs_1[0], task_id=task_with_error.id)
    return list(zip(populate_tasks, [[], [e_0, e_1]]))


_NOW = datetime.now()
_AFTER = datetime.now()


@pytest_asyncio.fixture(scope="function")
async def _populate_results(
    populate_tasks: List[Task], neo4j_async_app_driver: neo4j.AsyncDriver
) -> List[Tuple[Task, List[TaskResult]]]:
    query_1 = """CREATE (task:_Task:DONE {
    id: 'task-2', 
    name: 'hello_world',
    createdAt: $now,
    completedAt: $after,
    arguments: '{"greeted": "2"}'
})
CREATE (result:_TaskResult { result: '"Hello 2"' })
CREATE (task)-[:_HAS_RESULT]->(result)
RETURN task, result"""
    recs_0, _, _ = await neo4j_async_app_driver.execute_query(
        query_1, now=_NOW, after=_AFTER
    )
    t_2 = Task.from_neo4j(recs_0[0])
    r_2 = TaskResult.from_neo4j(recs_0[0])
    tasks = populate_tasks + [t_2]
    return list(zip(tasks, [None, None, r_2]))


@pytest.mark.parametrize(
    "populate_tasks,namespace,task,,expected_task,is_new",
    [
        # Insertion
        (
            None,
            None,
            Task(
                id="task-3",
                name="hello_world",
                arguments={"greeted": "3"},
                state=TaskState.CREATED,
                created_at=_NOW,
                retries=1,
            ),
            {
                "id": "task-3",
                "arguments": '{"greeted": "3"}',
                "retries": 1,
                "state": "CREATED",
                "name": "hello_world",
                "createdAt": datetime.now(),
            },
            True,
        ),
        # Insertion with ns
        (
            "some-namespace",
            "some-namespace",
            Task(
                id="task-3",
                name="hello_world",
                arguments={"greeted": "3"},
                state=TaskState.CREATED,
                created_at=_NOW,
                retries=1,
            ),
            {
                "id": "task-3",
                "arguments": '{"greeted": "3"}',
                "retries": 1,
                "state": "CREATED",
                "name": "hello_world",
                "createdAt": datetime.now(),
                "namespace": "some-namespace",
            },
            True,
        ),
        # Update
        (
            None,
            None,
            Task(
                id="task-1",
                name="hello_world",
                arguments={"greeted": "1"},
                state=TaskState.RUNNING,
                progress=0.8,
                created_at=_NOW,
                retries=0,
            ),
            {
                "id": "task-1",
                "arguments": '{"greeted": "1"}',
                "progress": 0.8,
                "retries": 0,
                "state": "RUNNING",
                "createdAt": datetime.now(),
                "name": "hello_world",
            },
            False,
        ),
        # Update with ns
        (
            "some-namespace",
            "some-namespace",
            Task(
                id="task-1",
                name="hello_world",
                arguments={"greeted": "1"},
                state=TaskState.RUNNING,
                progress=0.8,
                created_at=_NOW,
                retries=0,
            ),
            {
                "id": "task-1",
                "arguments": '{"greeted": "1"}',
                "progress": 0.80,
                "retries": 0,
                "state": "RUNNING",
                "name": "hello_world",
                "namespace": "some-namespace",
                "createdAt": datetime.now(),
            },
            False,
        ),
        # Should not update non updatable fields
        (
            None,
            None,
            Task(
                id="task-1",
                name="updated_task",
                arguments={"updated": "input"},
                state=TaskState.RUNNING,
                created_at=_NOW,
            ),
            {
                "id": "task-1",
                "arguments": '{"greeted": "1"}',
                "progress": 0.66,
                "retries": 1,
                "state": "RUNNING",
                "name": "hello_world",
                "createdAt": datetime.now(),
            },
            False,
        ),
    ],
    indirect=["populate_tasks"],
)
async def test_save_task(
    populate_tasks,
    neo4j_task_manager: Neo4JTaskManager,
    task: Task,
    namespace: Optional[str],
    expected_task: Dict,
    is_new: bool,
):
    ## pylint: disable=unused-argument
    # Given
    driver = neo4j_task_manager.driver
    # When
    saved = await neo4j_task_manager.save_task(task, namespace)
    assert saved == is_new
    # Then
    query = "MATCH (task:_Task { id: $taskId }) RETURN task"
    recs, _, _ = await driver.execute_query(query, taskId=task.id)
    assert len(recs) == 1
    db_task = dict(recs[0]["task"])
    created_at = db_task.pop("createdAt", None)
    assert isinstance(created_at, DateTime)
    expected_task.pop("createdAt")
    assert db_task == expected_task


async def test_save_task_with_different_ns_should_fail(
    populate_tasks, neo4j_task_manager: Neo4JTaskManager
):
    # pylint: disable=unused-argument
    # Given
    other_ns = "other-namespace"
    task = Task(
        id="task-1",
        name="hello_world",
        arguments={"greeted": "1"},
        state=TaskState.RUNNING,
        progress=0.8,
        created_at=_NOW,
        retries=0,
    )
    # When/Then
    msg = re.escape(
        "DB task namespace (None) differs from save task namespace: other-namespace"
    )
    with pytest.raises(ValueError, match=msg):
        await neo4j_task_manager.save_task(task, other_ns)


@pytest.mark.parametrize(
    "populate_tasks,expected_namespace",
    [(None, None), ("some_namespace", "some_namespace")],
    indirect=["populate_tasks"],
)
async def test_get_task_namespace(
    populate_tasks,
    neo4j_task_manager: Neo4JTaskManager,
    expected_namespace: str,
):
    # pylint: disable=unused-argument
    # When
    namespace = await neo4j_task_manager.get_task_namespace("task-0")
    # Then
    assert namespace == expected_namespace


async def test_task_manager_get_task(
    populate_tasks: List[Task], neo4j_task_manager: Neo4JTaskManager
):
    # Given
    second_task = populate_tasks[1]

    # When
    task = await neo4j_task_manager.get_task(task_id=second_task.id)
    task = task.dict(by_alias=True)

    # Then
    expected_task = Task(
        id="task-1",
        name="hello_world",
        arguments={"greeted": "1"},
        state=TaskState.RUNNING,
        progress=0.66,
        created_at=datetime.now(),
        retries=1,
    )
    expected_task = expected_task.dict(by_alias=True)
    expected_task.pop("createdAt")

    assert task.pop("createdAt")  # We just check that it's not None
    assert task == expected_task


async def test_task_manager_get_completed_task(
    _populate_results: List[Tuple[Task, List[TaskResult]]],
    neo4j_task_manager: Neo4JTaskManager,
):
    # pylint: disable=invalid-name
    # Given
    last_task = _populate_results[-1][0]

    # When
    task = await neo4j_task_manager.get_task(task_id=last_task.id)

    # Then
    assert isinstance(task.completed_at, datetime)


@pytest.mark.parametrize(
    "populate_tasks,namespace,states,task_name,expected_ix",
    [
        (None, None, None, None, [0, 1]),
        (None, None, [], None, [0, 1]),
        (None, None, None, "hello_word", []),
        (None, None, None, "i_dont_exists", []),
        ("some-namespace", "some-namespace", TaskState.QUEUED, None, [0]),
        (None, None, TaskState.QUEUED, None, [0]),
        (None, None, [TaskState.QUEUED], None, [0]),
        (None, None, TaskState.RUNNING, None, [1]),
        (None, None, TaskState.CANCELLED, None, []),
    ],
    indirect=["populate_tasks"],
)
async def test_task_manager_get_tasks(
    neo4j_task_manager: Neo4JTaskManager,
    populate_tasks: List[Task],
    namespace: Optional[str],
    states: Optional[List[TaskState]],
    task_name,
    expected_ix: List[int],
):
    # When
    tasks = await neo4j_task_manager.get_tasks(
        state=states, task_name=task_name, namespace=namespace
    )
    tasks = sorted(tasks, key=lambda t: t.id)

    # Then
    expected_tasks = [populate_tasks[i] for i in expected_ix]
    assert tasks == expected_tasks


@pytest.mark.parametrize(
    "task_id,expected_errors",
    [
        ("task-0", []),
        (
            "task-1",
            [
                TaskError(
                    id="error-0",
                    task_id="task-1",
                    name="error",
                    message="with details",
                    occurred_at=datetime.now(),
                    stacktrace=[
                        StacktraceItem(name="SomeError", file="somefile", lineno=666)
                    ],
                    cause="some cause",
                ),
                TaskError(
                    id="error-1",
                    task_id="task-1",
                    name="error",
                    message="same error again",
                    stacktrace=[
                        StacktraceItem(name="SomeError", file="somefile", lineno=666)
                    ],
                    cause="some cause",
                    occurred_at=datetime.now(),
                ),
            ],
        ),
    ],
)
async def test_get_task_errors(
    neo4j_task_manager: Neo4JTaskManager,
    _populate_errors: List[Tuple[Task, List[TaskError]]],
    task_id: str,
    expected_errors: List[TaskError],
):
    # pylint: disable=invalid-name
    # When
    retrieved_errors = await neo4j_task_manager.get_task_errors(task_id=task_id)

    # Then
    retrieved_errors = [e.dict(by_alias=True) for e in retrieved_errors]
    assert all(e["occurredAt"] for e in retrieved_errors)
    for e in retrieved_errors:
        e.pop("occurredAt")
    expected_errors = [e.dict(by_alias=True) for e in expected_errors[::-1]]
    for e in expected_errors:
        e.pop("occurredAt")
    assert retrieved_errors == expected_errors


@pytest.mark.parametrize(
    "task_id,expected_result",
    [
        ("task-0", None),
        ("task-1", None),
        (
            "task-2",
            TaskResult(task_id="task-2", result="Hello 2", completed_at=_AFTER),
        ),
    ],
)
async def test_task_manager_get_task_result(
    neo4j_task_manager: Neo4JTaskManager,
    _populate_results: List[Tuple[str, Optional[TaskResult]]],
    task_id: str,
    expected_result: Optional[TaskResult],
):
    # pylint: disable=invalid-name
    # When/ Then
    if expected_result is None:
        expected_msg = (
            f'Result of task "{task_id}" couldn\'t be found, did it complete ?'
        )
        with pytest.raises(MissingTaskResult, match=expected_msg):
            await neo4j_task_manager.get_task_result(task_id=task_id)
    else:
        result = await neo4j_task_manager.get_task_result(task_id=task_id)
        assert result == expected_result


async def test_task_manager_enqueue(
    neo4j_task_manager: Neo4JTaskManager, hello_world_task: Task
):
    # Given
    task = hello_world_task

    # When
    queued = await neo4j_task_manager.enqueue(task, namespace=None)

    # Then
    update = {"state": TaskState.QUEUED}
    expected = safe_copy(task, update=update)
    assert queued == expected


async def test_task_manager_enqueue_with_namespace(
    neo4j_task_manager: Neo4JTaskManager, hello_world_task: Task
):
    # Given
    task = hello_world_task
    driver = neo4j_task_manager.driver
    namespace = "some.namespace"

    # When
    await neo4j_task_manager.enqueue(task, namespace=namespace)
    query = "MATCH (task:_Task) RETURN task"
    recs, _, _ = await driver.execute_query(query)
    assert len(recs) == 1
    task = recs[0]
    assert task["task"]["namespace"] == namespace


async def test_task_manager_enqueue_should_raise_for_existing_task(
    neo4j_task_manager: Neo4JTaskManager, hello_world_task: Task
):
    # Given
    task = hello_world_task
    await neo4j_task_manager.enqueue(task, namespace=None)

    # When/Then
    with pytest.raises(TaskAlreadyQueued):
        await neo4j_task_manager.enqueue(task, namespace=None)


@pytest.mark.parametrize("requeue", [True, False])
async def test_task_manager_cancel(
    neo4j_task_manager: Neo4JTaskManager,
    requeue: bool,
    hello_world_task: Task,
):
    # Given
    driver = neo4j_task_manager.driver
    task = hello_world_task

    # When
    task = await neo4j_task_manager.enqueue(task, namespace=None)
    await neo4j_task_manager.cancel(task_id=task.id, requeue=requeue)
    query = """MATCH (task:_Task { id: $taskId })-[
    :_CANCELLED_BY]->(event:_CancelEvent)
RETURN task, event"""
    recs, _, _ = await driver.execute_query(query, taskId=task.id)
    assert len(recs) == 1
    event = CancelEvent.from_neo4j(recs[0])
    # Then
    assert event.task_id == task.id
    assert event.cancelled_at is not None
    assert event.requeue == requeue


async def test_task_manager_enqueue_should_raise_when_queue_full(
    neo4j_async_app_driver: neo4j.AsyncDriver, hello_world_task: Task
):
    task_manager = Neo4JTaskManager(
        "test-app", neo4j_async_app_driver, max_task_queue_size=-1
    )
    task = hello_world_task

    # When
    with pytest.raises(TaskQueueIsFull):
        await task_manager.enqueue(task, namespace=None)
