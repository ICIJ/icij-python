# pylint: disable=redefined-outer-scope
from copy import deepcopy
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import neo4j
import pytest
import pytest_asyncio
from neo4j.time import DateTime

from icij_common.pydantic_utils import safe_copy
from icij_common.test_utils import async_true_after
from icij_worker import (
    AsyncApp,
    Neo4JTaskManager,
    Neo4jWorker,
    ResultEvent,
    Task,
    TaskError,
    TaskState,
)
from icij_worker.app import AsyncAppConfig
from icij_worker.exceptions import MissingTaskResult, TaskAlreadyQueued, TaskQueueIsFull
from icij_worker.objects import (
    CancelEvent,
    CancelledEvent,
    ErrorEvent,
    ManagerEvent,
    ProgressEvent,
    StacktraceItem,
)
from icij_worker.tests.conftest import TestableNeo4JTaskManager


@pytest_asyncio.fixture(scope="function")
async def _populate_errors(
    populate_tasks: List[Task], neo4j_async_app_driver: neo4j.AsyncDriver
) -> List[Tuple[Task, List[ErrorEvent]]]:
    task_with_error = populate_tasks[1]
    query_0 = """MATCH (task:_Task { id: $taskId })
CREATE  (error:_TaskError {
    name: 'error',
    stacktrace: ['{"name": "SomeError", "file": "somefile", "lineno": 666}'],
    message: "with details",
    cause: "some cause"
})-[rel:_OCCURRED_DURING]->(task)
SET rel.occurredAt = $now, rel.retriesLeft = $retriesLeft
RETURN error, rel, task"""
    recs_0, _, _ = await neo4j_async_app_driver.execute_query(
        query_0,
        taskId=task_with_error.id,
        now=datetime.now(),
        retriesLeft=3,
    )
    e_0 = ErrorEvent.from_neo4j(recs_0[0])
    query_1 = """MATCH (task:_Task { id: $taskId })
CREATE  (error:_TaskError {
    name: 'error',
    stacktrace: ['{"name": "SomeError", "file": "somefile", "lineno": 666}'],
    message: 'same error again',
    cause: "some cause"
})-[rel:_OCCURRED_DURING]->(task)
SET rel.occurredAt = $now, rel.retriesLeft = $retriesLeft 
RETURN error, rel, task"""
    recs_1, _, _ = await neo4j_async_app_driver.execute_query(
        query_1,
        taskId=task_with_error.id,
        now=datetime.now(),
        retriesLeft=2,
    )
    e_1 = ErrorEvent.from_neo4j(recs_1[0])
    return list(zip(populate_tasks, [[], [e_0, e_1]]))


_NOW = datetime.now()
_AFTER = datetime.now()


@pytest_asyncio.fixture(scope="function")
async def _populate_results(
    populate_tasks: List[Task], neo4j_async_app_driver: neo4j.AsyncDriver
) -> List[Tuple[Task, List[ResultEvent]]]:
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
    r_2 = ResultEvent.from_neo4j(recs_0[0])
    tasks = populate_tasks + [t_2]
    return list(zip(tasks, [None, None, r_2]))


@pytest.mark.parametrize(
    "populate_tasks,task,expected_task,is_new",
    [
        # Insertion
        (
            None,
            Task(
                id="task-3",
                name="hello_world",
                arguments={"greeted": "3"},
                state=TaskState.CREATED,
                created_at=_NOW,
                retries_left=1,
            ),
            {
                "id": "task-3",
                "arguments": '{"greeted": "3"}',
                "retriesLeft": 1,
                "state": "CREATED",
                "name": "hello_world",
                "createdAt": datetime.now(),
            },
            True,
        ),
        # Insertion with ns
        (
            "hello",
            Task(
                id="task-3",
                name="namespaced_hello_world",
                arguments={"greeted": "3"},
                state=TaskState.CREATED,
                created_at=_NOW,
                retries_left=1,
            ),
            {
                "id": "task-3",
                "arguments": '{"greeted": "3"}',
                "retriesLeft": 1,
                "maxRetries": 3,
                "state": "CREATED",
                "name": "namespaced_hello_world",
                "createdAt": datetime.now(),
                "namespace": "hello",
            },
            True,
        ),
        # Update
        (
            None,
            Task(
                id="task-1",
                name="hello_world",
                arguments={"greeted": "1"},
                state=TaskState.RUNNING,
                progress=0.8,
                created_at=_NOW,
                retries_left=0,
            ),
            {
                "id": "task-1",
                "arguments": '{"greeted": "1"}',
                "progress": 0.8,
                "retriesLeft": 0,
                "state": "RUNNING",
                "createdAt": datetime.now(),
                "name": "hello_world",
            },
            False,
        ),
        # Update with ns
        (
            "hello",
            Task(
                id="task-1",
                name="namespaced_hello_world",
                arguments={"greeted": "1"},
                state=TaskState.RUNNING,
                progress=0.8,
                created_at=_NOW,
                retries_left=0,
            ),
            {
                "id": "task-1",
                "arguments": '{"greeted": "1"}',
                "progress": 0.80,
                "retriesLeft": 0,
                "state": "RUNNING",
                "name": "namespaced_hello_world",
                "namespace": "hello",
                "createdAt": datetime.now(),
            },
            False,
        ),
        # Should not update non updatable fields
        (
            None,
            Task(
                id="task-1",
                name="updated_task",
                arguments={"updated": "input"},
                state=TaskState.RUNNING,
                created_at=_NOW,
                retries_left=1,
            ),
            {
                "id": "task-1",
                "arguments": '{"greeted": "1"}',
                "progress": 0.66,
                "retriesLeft": 1,
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
    neo4j_task_manager: TestableNeo4JTaskManager,
    task: Task,
    expected_task: Dict,
    is_new: bool,
):
    ## pylint: disable=unused-argument
    # Given
    driver = neo4j_task_manager.driver
    # When
    saved = await neo4j_task_manager.save_task(task)
    assert saved == is_new
    # Then
    query = "MATCH (task:_Task { id: $taskId }) RETURN task"
    recs, _, _ = await driver.execute_query(query, taskId=task.id)
    assert len(recs) == 1
    db_task = dict(recs[0]["task"])
    created_at = db_task.pop("createdAt", None)
    assert isinstance(created_at, DateTime)
    expected_task.pop("createdAt")
    expected_state = expected_task.pop("state")
    assert recs[0]["task"].labels == set(("_Task", expected_state))
    assert db_task == expected_task


@pytest.mark.parametrize(
    "populate_tasks,expected_namespace",
    [(None, None), ("some_namespace", "some_namespace")],
    indirect=["populate_tasks"],
)
async def test_get_task_namespace(
    populate_tasks,
    neo4j_task_manager: TestableNeo4JTaskManager,
    expected_namespace: str,
):
    # pylint: disable=unused-argument
    # When
    namespace = await neo4j_task_manager.get_task_namespace("task-0")
    # Then
    assert namespace == expected_namespace


async def test_task_manager_get_task(
    populate_tasks: List[Task], neo4j_task_manager: TestableNeo4JTaskManager
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
        retries_left=1,
    )
    expected_task = expected_task.dict(by_alias=True)
    expected_task.pop("createdAt")

    assert task.pop("createdAt")  # We just check that it's not None
    assert task == expected_task


async def test_task_manager_get_completed_task(
    _populate_results: List[Tuple[Task, List[ResultEvent]]],
    neo4j_task_manager: TestableNeo4JTaskManager,
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
    neo4j_task_manager: TestableNeo4JTaskManager,
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
                ErrorEvent(
                    task_id="task-1",
                    error=TaskError(
                        name="error",
                        message="same error again",
                        stacktrace=[
                            StacktraceItem(
                                name="SomeError", file="somefile", lineno=666
                            )
                        ],
                        cause="some cause",
                    ),
                    created_at=datetime.now(),
                    retries_left=2,
                ),
                ErrorEvent(
                    task_id="task-1",
                    error=TaskError(
                        name="error",
                        message="with details",
                        stacktrace=[
                            StacktraceItem(
                                name="SomeError", file="somefile", lineno=666
                            )
                        ],
                        cause="some cause",
                    ),
                    created_at=datetime.now(),
                    retries_left=3,
                ),
            ],
        ),
    ],
)
async def test_get_task_errors(
    neo4j_task_manager: TestableNeo4JTaskManager,
    _populate_errors: List[Tuple[Task, List[TaskError]]],
    task_id: str,
    expected_errors: List[ErrorEvent],
):
    # pylint: disable=invalid-name
    # When
    retrieved_errors = await neo4j_task_manager.get_task_errors(task_id=task_id)

    # Then
    retries_left = [e.retries_left for e in retrieved_errors]
    expected_retries_left = [e.retries_left for e in expected_errors]
    assert retries_left == expected_retries_left
    retrieved_errors = [e.error for e in retrieved_errors]
    expected_errors = [e.error for e in expected_errors]
    assert retrieved_errors == expected_errors


@pytest.mark.parametrize(
    "task_id,expected_result",
    [
        ("task-0", None),
        ("task-1", None),
        (
            "task-2",
            ResultEvent(task_id="task-2", result="Hello 2", created_at=_AFTER),
        ),
    ],
)
async def test_task_manager_get_task_result(
    neo4j_task_manager: TestableNeo4JTaskManager,
    _populate_results: List[Tuple[str, Optional[ResultEvent]]],
    task_id: str,
    expected_result: Optional[ResultEvent],
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
    neo4j_task_manager: TestableNeo4JTaskManager, hello_world_task: Task
):
    # Given
    task = hello_world_task
    await neo4j_task_manager.save_task(task)

    # When
    queued = await neo4j_task_manager.enqueue(task)

    # Then
    update = {"state": TaskState.QUEUED}
    expected = safe_copy(task, update=update)
    assert queued == expected


async def test_task_manager_enqueue_with_namespace(
    neo4j_task_manager: TestableNeo4JTaskManager, namespaced_hello_world_task: Task
):
    # Given
    task = namespaced_hello_world_task
    driver = neo4j_task_manager.driver
    await neo4j_task_manager.save_task(task)

    # When
    await neo4j_task_manager.enqueue(task)
    query = "MATCH (task:_Task) RETURN task"
    recs, _, _ = await driver.execute_query(query)
    assert len(recs) == 1
    task = recs[0]
    assert task["task"]["namespace"] == "hello"


async def test_task_manager_enqueue_should_raise_for_existing_task(
    neo4j_task_manager: TestableNeo4JTaskManager, hello_world_task: Task
):
    # Given
    task = hello_world_task
    await neo4j_task_manager.save_task(task)
    await neo4j_task_manager.enqueue(task)

    # When/Then
    with pytest.raises(TaskAlreadyQueued):
        await neo4j_task_manager.enqueue(task)


@pytest.mark.parametrize(
    "neo4j_task_manager",
    ["test_async_app", "test_async_app_late"],
    indirect=["neo4j_task_manager"],
)
async def test_task_manager_requeue(neo4j_task_manager: TestableNeo4JTaskManager):
    # pylint: disable=unused-argument
    # Given
    task_manager = neo4j_task_manager
    created_at = datetime.now()
    task = Task(
        id="some-id",
        name="hello_world",
        created_at=created_at,
        state=TaskState.RUNNING,
        progress=0.66,
    )
    await neo4j_task_manager.save_task(task)

    # When
    await task_manager.save_task(task)
    await task_manager.requeue(task)

    # Then
    saved = await task_manager.get_task(task_id=task.id)

    # Then
    expected = safe_copy(task, update={"state": TaskState.QUEUED, "progress": 0.0})
    assert saved == expected


_TASK = Task.create(
    task_id="some-task-id", task_name="sleep_for", arguments={"duration": 100}
)
_EVENTS = (
    ProgressEvent.from_task(
        safe_copy(_TASK, update={"progress": 0.66, "state": TaskState.RUNNING})
    ),
    ErrorEvent.from_task(
        _TASK,
        TaskError.from_exception(ValueError("there's an error here")),
        retries_left=3,
        created_at=datetime.now(timezone.utc),
    ),
    ResultEvent.from_task(
        safe_copy(_TASK, update={"completed_at": datetime.now()}), "some-result"
    ),
    CancelledEvent.from_task(
        safe_copy(_TASK, update={"cancelled_at": datetime.now()}), requeue=True
    ),
)
_LATER_EVENT = ProgressEvent.from_task(safe_copy(_TASK, update={"progress": 0.99}))


@pytest.mark.parametrize("event", _EVENTS)
async def test_task_manager_consume(
    neo4j_task_manager: TestableNeo4JTaskManager, event: ManagerEvent
):
    # Given
    task_manager = neo4j_task_manager
    driver = task_manager.driver
    worker = Neo4jWorker(
        task_manager.app,  # pylint: disable=protected-access
        "test-worker",
        namespace=None,
        driver=driver,
        poll_interval_s=0.1,
    )
    await task_manager.save_task(_TASK)
    await worker.publish_event(event)
    await worker.publish_event(_LATER_EVENT)

    # When
    async with task_manager:
        # Then
        consume_timeout = 10.0
        msg = f"Failed to consume error in less than {consume_timeout}"

        async def _not_empty() -> bool:
            return bool(task_manager.consumed)

        assert await async_true_after(_not_empty, after_s=consume_timeout), msg
        consumed = task_manager.consumed[0]
        assert consumed == event


@pytest.mark.parametrize("requeue", [True, False])
async def test_task_manager_cancel(
    neo4j_task_manager: TestableNeo4JTaskManager,
    requeue: bool,
    hello_world_task: Task,
):
    # Given
    driver = neo4j_task_manager.driver
    task = hello_world_task
    await neo4j_task_manager.save_task(task)

    # When
    task = await neo4j_task_manager.enqueue(task)
    await neo4j_task_manager.cancel(task_id=task.id, requeue=requeue)
    query = """MATCH (task:_Task { id: $taskId })-[
    :_CANCELLED_BY]->(event:_CancelEvent)
RETURN task, event"""
    recs, _, _ = await driver.execute_query(query, taskId=task.id)
    assert len(recs) == 1
    event = CancelEvent.from_neo4j(recs[0])
    # Then
    assert event.task_id == task.id
    assert event.requeue == requeue


async def test_task_manager_enqueue_should_raise_when_queue_full(
    neo4j_async_app_driver: neo4j.AsyncDriver,
    hello_world_task: Task,
    test_async_app: AsyncApp,
):
    app = AsyncApp("test-app", config=AsyncAppConfig(max_task_queue_size=1))
    app._registry = deepcopy(  # pylint: disable=protected-access
        test_async_app._registry  # pylint: disable=protected-access
    )
    task_manager = Neo4JTaskManager(app, neo4j_async_app_driver)
    task = hello_world_task
    await task_manager.save_task(task)
    await task_manager.save_task(_TASK)

    # When
    await task_manager.enqueue(task)
    with pytest.raises(TaskQueueIsFull):
        await task_manager.enqueue(_TASK)


async def test_task_manager_save_error(
    hello_world_task: Task, neo4j_task_manager: TestableNeo4JTaskManager
):
    # Given
    task = hello_world_task
    await neo4j_task_manager.save_task(task)
    error = TaskError(
        name="error",
        message="with details",
        stacktrace=[StacktraceItem(name="SomeError", file="somefile", lineno=666)],
        cause="some cause",
    )
    error_event = ErrorEvent(
        task_id="some-id", error=error, retries_left=4, created_at=datetime.now()
    )
    # When
    await neo4j_task_manager.save_error(error_event)
    db_errors = await neo4j_task_manager.get_task_errors(task.id)
    # Then
    assert db_errors == [error_event]


async def test_task_manager_save_result(
    hello_world_task: Task, neo4j_task_manager: TestableNeo4JTaskManager
):
    # Given
    task = hello_world_task
    await neo4j_task_manager.save_task(task)
    result = ResultEvent(
        task_id=task.id, result="some result", created_at=datetime.now()
    )
    # When
    await neo4j_task_manager.save_result(result)
    db_result = await neo4j_task_manager.get_task_result(task.id)
    # Then
    assert result == db_result


async def test_task_manager_should_raise_when_saving_existing_result(
    populate_tasks: List[Task], neo4j_task_manager: TestableNeo4JTaskManager
):
    # Given
    task_manager = neo4j_task_manager
    task = populate_tasks[0]
    assert task.state == TaskState.QUEUED
    result = "hello everyone"
    task = safe_copy(task, update={"completed_at": datetime.now()})
    task_result = ResultEvent.from_task(task=task, result=result)
    # When
    await task_manager.save_result(result=task_result)
    # Then
    expected = "Attempted to save result for task task-0 but found existing result"
    with pytest.raises(ValueError, match=expected):
        await task_manager.save_result(result=task_result)
