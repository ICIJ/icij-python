# pylint: disable=redefined-outer-name
import asyncio
from datetime import datetime
from typing import Callable, List, Optional

import neo4j
import pytest
from neo4j.exceptions import ClientError

from icij_common.neo4j.db import Database, NEO4J_COMMUNITY_DB, db_specific_session
from icij_common.pydantic_utils import safe_copy
from icij_common.test_utils import fail_if_exception
from icij_worker import (
    Namespacing,
    Neo4JTaskManager,
    Neo4jWorker,
    TaskError,
    TaskResult,
    TaskState,
)
from icij_worker.objects import (
    CancelTaskEvent,
    CancelledEvent,
    ErrorEvent,
    ProgressEvent,
    StacktraceItem,
    Task,
)
from icij_worker.tests.worker.conftest import make_app
from icij_worker.worker import neo4j_


@pytest.fixture(
    scope="function",
    params=[{"app": "test_async_app_late"}, {"app": "test_async_app"}],
)
def worker(neo4j_async_app_driver: neo4j.AsyncDriver, request) -> Neo4jWorker:
    params = getattr(request, "param", dict()) or dict()
    app = params.get("app", "test_async_app")
    app = request.getfixturevalue(app)
    namespace = params.get("namespace")
    worker = Neo4jWorker(
        app,
        "test-worker",
        namespace=namespace,
        driver=neo4j_async_app_driver,
        cancelled_tasks_refresh_interval_s=0.1,
        new_tasks_refresh_interval_s=0.1,
    )
    return worker


async def _count_locks(driver: neo4j.AsyncDriver, db: str) -> int:
    # Now let's check that no lock if left in the DB
    count_locks_query = "MATCH (lock:_TaskLock) RETURN count(*) as nLocks"
    async with db_specific_session(driver, db=db) as sess:
        recs = await sess.run(count_locks_query)
        counts = await recs.single(strict=True)
    return counts["nLocks"]


@pytest.mark.parametrize(
    "populate_tasks,worker",
    [("some-namespace", {"namespace": "some-namespace"}), (None, None)],
    indirect=["populate_tasks", "worker"],
)
async def test_worker_consume_task(populate_tasks: List[Task], worker: Neo4jWorker):
    # pylint: disable=unused-argument
    # When
    task = asyncio.create_task(worker.consume())
    # Then
    timeout = 2
    with fail_if_exception(f"failed to consume task in less than {timeout}s"):
        await asyncio.wait([task], timeout=timeout)

    # Now let's check that no lock if left in the DB
    count_locks_query = "MATCH (lock:_TaskLock) RETURN count(*) as nLocks"
    async with db_specific_session(worker.driver, NEO4J_COMMUNITY_DB) as sess:
        recs = await sess.run(count_locks_query)
        counts = await recs.single(strict=True)
    assert counts["nLocks"] == 1


async def test_should_consume_with_namespace(
    populate_tasks, neo4j_async_app_driver: neo4j.AsyncDriver, monkeypatch
):
    # pylint: disable=unused-argument
    # Given
    mocked_other_db = "other-db"

    async def _mocked_retrieved_db(driver: neo4j.AsyncDriver) -> List[Database]:
        # pylint: disable=unused-argument
        return [Database(name=mocked_other_db)]

    monkeypatch.setattr(neo4j_, "retrieve_dbs", _mocked_retrieved_db)
    other_namespace = "some-namespace"

    class MockedNamespacing(Namespacing):
        @staticmethod
        def db_filter_factory(worker_namespace: str) -> Callable[[str], bool]:
            return lambda x: x == mocked_other_db

        @staticmethod
        def neo4j_db(namespace: str) -> str:
            if namespace == other_namespace:
                return mocked_other_db
            return super().neo4j_db(namespace)

    namespacing = MockedNamespacing()
    app = make_app(namespacing)
    refresh_interval = 0.1
    worker = Neo4jWorker(
        app,
        "test-worker",
        namespace=other_namespace,
        driver=neo4j_async_app_driver,
        cancelled_tasks_refresh_interval_s=refresh_interval,
        new_tasks_refresh_interval_s=refresh_interval,
    )
    # When
    async with worker:
        # Then
        with pytest.raises(ClientError) as ex:
            await worker.consume()

    assert ex.value.code == "Neo.ClientError.Database.DatabaseNotFound"
    expected = (
        "Unable to get a routing table for database 'other-db' because"
        " this database does not exist"
    )
    assert ex.value.message == expected


@pytest.mark.parametrize("worker", [None], indirect=["worker"])
async def test_worker_consume_cancel_event(
    populate_cancel_events: List[CancelTaskEvent], worker: Neo4jWorker
):
    # pylint: disable=unused-argument,protected-access
    # When
    task = asyncio.create_task(worker._consume_cancelled())
    # Then
    timeout = 2
    await asyncio.wait([task], timeout=timeout)
    if not task.done():
        pytest.fail(f"failed to consume task in less than {timeout}s")
    event = task.result()
    assert event == populate_cancel_events[0]


@pytest.mark.parametrize(
    "worker,nacked_state",
    [
        ({"app": "test_async_app_late"}, TaskState.QUEUED),
        ({"app": "test_async_app_late"}, TaskState.ERROR),
    ],
    indirect=["worker"],
)
async def test_worker_negatively_acknowledge(
    populate_tasks: List[Task],
    worker: Neo4jWorker,
    neo4j_task_manager: Neo4JTaskManager,
    nacked_state: TaskState,
):
    # pylint: disable=unused-argument,protected-access
    # Given
    task_manager = neo4j_task_manager
    # When
    task = await worker.consume()
    task = safe_copy(task, update={"state": nacked_state})
    n_locks = await _count_locks(worker.driver, db=NEO4J_COMMUNITY_DB)
    assert n_locks == 1
    await worker._negatively_acknowledge(task)
    nacked = await task_manager.get_task(task_id=task.id)

    # Then
    assert nacked == task
    n_locks = await _count_locks(worker.driver, db=NEO4J_COMMUNITY_DB)
    assert n_locks == 0


@pytest.mark.parametrize("is_error", [True, False])
async def test_worker_requeue(
    worker: Neo4jWorker, neo4j_task_manager: Neo4JTaskManager, is_error: bool
):
    # pylint: disable=unused-argument
    # Given
    task_manager = neo4j_task_manager
    created_at = datetime.now()
    task = Task(
        id="some-id", name="hello_world", created_at=created_at, state=TaskState.CREATED
    )
    n_locks = await _count_locks(worker.driver, db=NEO4J_COMMUNITY_DB)
    assert n_locks == 0

    # When
    await task_manager.enqueue(task, namespace=None)
    task = await worker.consume()
    n_locks = await _count_locks(worker.driver, db=NEO4J_COMMUNITY_DB)
    assert n_locks == 1
    # Let's publish some event to increment the progress and check that it's reset
    # correctly to 0
    task = safe_copy(task, update={"progress": 50.0})
    event = ProgressEvent.from_task(task=task)
    await worker.publish_event(event)
    with_progress = safe_copy(task, update={"progress": event.progress})
    await worker.requeue(task, is_error)
    if is_error:
        task_error = TaskError(
            id="error-id",
            task_id=task.id,
            name="someErrorTitle",
            message="with_details",
            stacktrace=[
                StacktraceItem(name="someErrorTitle", file="with_details", lineno=666)
            ],
            occurred_at=datetime.now(),
        )
        retries = task.retries or 0 + 1
        await worker.publish_error_event(task_error, task, retries=retries)
        update = {"state": TaskState.QUEUED, "retries": retries, "progress": 0.0}
    else:
        cancelled_evt = CancelledEvent.from_task(task=task, requeue=True)
        await worker.publish_event(cancelled_evt)
        update = {
            "state": TaskState.QUEUED,
            "cancelled_at": cancelled_evt.cancelled_at,
            "progress": 0.0,
        }
    nacked = await task_manager.get_task(task_id=task.id)

    # Then
    expected_nacked = safe_copy(with_progress, update=update)
    assert nacked == expected_nacked
    n_locks = await _count_locks(worker.driver, db=NEO4J_COMMUNITY_DB)
    assert n_locks == 0


@pytest.mark.parametrize("worker", [None], indirect=["worker"])
async def test_worker_save_result(
    populate_tasks: List[Task],
    worker: Neo4jWorker,
    neo4j_task_manager: Neo4JTaskManager,
):
    # Given
    task_manager = neo4j_task_manager
    task = populate_tasks[0]
    assert task.state == TaskState.QUEUED
    result = "hello everyone"
    task = safe_copy(task, update={"completed_at": datetime.now()})
    task_result = TaskResult.from_task(task=task, result=result)

    # When
    await worker.save_result(result=task_result)
    saved_task = await task_manager.get_task(task_id=task.id)
    saved_result = await task_manager.get_task_result(task_id=task.id)

    # Then
    expected = safe_copy(task, update={"state": TaskState.DONE, "progress": 100.0})
    assert saved_task == expected
    assert saved_result == task_result


@pytest.mark.parametrize(
    "worker,retries", [(None, None), (None, 4)], indirect=["worker"]
)
async def test_worker_save_error(
    populate_tasks: List[Task],
    worker: Neo4jWorker,
    neo4j_task_manager: Neo4JTaskManager,
    retries: Optional[int],
):
    task_manager = neo4j_task_manager
    task = populate_tasks[1]
    assert task.state == TaskState.RUNNING
    exc = ValueError("there's an error here")
    error = TaskError.from_exception(exc, task=task)

    # When
    await worker.publish_event(ErrorEvent.from_error(error, task.id, retries=retries))
    await worker.save_error(error)
    saved_task = await task_manager.get_task(task_id=task.id)
    saved_errors = await task_manager.get_task_errors(task_id=task.id)

    # Then
    if retries is not None:
        expected = safe_copy(
            task,
            update={"state": TaskState.QUEUED, "progress": 0.0, "retries": retries},
        )
    else:
        expected = safe_copy(task, update={"state": TaskState.ERROR})
    assert saved_task == expected
    assert saved_errors == [error]


@pytest.mark.parametrize("worker", [None], indirect=["worker"])
async def test_worker_should_raise_when_saving_existing_result(
    populate_tasks: List[Task], worker: Neo4jWorker
):
    # Given
    task = populate_tasks[0]
    assert task.state == TaskState.QUEUED
    result = "hello everyone"
    task = safe_copy(task, update={"completed_at": datetime.now()})
    task_result = TaskResult.from_task(task=task, result=result)

    # When
    await worker.save_result(result=task_result)
    # Then
    expected = "Attempted to save result for task task-0 but found existing result"
    with pytest.raises(ValueError, match=expected):
        await worker.save_result(result=task_result)


async def test_worker_ack_cm(
    populate_tasks: List[Task],
    worker: Neo4jWorker,
    neo4j_task_manager: Neo4JTaskManager,
):
    # Given
    created = populate_tasks[0]
    task_manager = neo4j_task_manager

    # When
    async with worker.ack_cm:
        task = await task_manager.get_task(task_id=created.id)
        assert task.state is TaskState.RUNNING
        task = safe_copy(task, update={"completed_at": datetime.now()})
        await worker.save_result(TaskResult.from_task(task, "some_res"))

    # Then
    task = await task_manager.get_task(task_id=created.id)
    update = {"progress": 100.0, "state": TaskState.DONE}
    expected_task = safe_copy(task, update=update).dict(by_alias=True)
    expected_task.pop("completedAt")
    assert task.completed_at is not None
    task = task.dict(by_alias=True)
    task.pop("completedAt")
    assert task == expected_task
    # Now let's check that no lock if left in the DB
    count_locks_query = "MATCH (lock:_TaskLock) RETURN count(*) as nLocks"
    recs, _, _ = await worker.driver.execute_query(count_locks_query)
    assert recs[0]["nLocks"] == 0
