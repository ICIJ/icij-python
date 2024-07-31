# pylint: disable=redefined-outer-name
import asyncio
import json
from datetime import datetime
from typing import Callable, List

import itertools
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
    ResultEvent,
    TaskError,
    TaskState,
)
from icij_worker.objects import (
    CancelEvent,
    CancelledEvent,
    ErrorEvent,
    ManagerEvent,
    Message,
    ProgressEvent,
    Task,
)
from icij_worker.tests.conftest import count_locks
from icij_worker.tests.worker.conftest import make_app
from icij_worker.utils import neo4j_


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
        poll_interval_s=0.1,
    )
    return worker


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
        poll_interval_s=refresh_interval,
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
    populate_cancel_events: List[CancelEvent], worker: Neo4jWorker
):
    # pylint: disable=unused-argument,protected-access
    # When
    task = asyncio.create_task(worker._consume_worker_events())
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
    n_locks = await count_locks(worker.driver, db=NEO4J_COMMUNITY_DB)
    assert n_locks == 1
    await worker._negatively_acknowledge(task)
    nacked = await task_manager.get_task(task_id=task.id)

    # Then
    expected = safe_copy(populate_tasks[0], update={"state": TaskState.RUNNING})
    assert nacked == expected
    n_locks = await count_locks(worker.driver, db=NEO4J_COMMUNITY_DB)
    assert n_locks == 0


_TASK = Task.create(
    task_id="some-id", task_name="sleep_for", arguments={"duration": 100}
)
_EVENTS = (
    ProgressEvent.from_task(
        safe_copy(_TASK, update={"progress": 0.66, "state": TaskState.RUNNING})
    ),
    ErrorEvent.from_task(
        task=_TASK,
        error=TaskError.from_exception(ValueError("there's an error here")),
        retries_left=3,
        created_at=datetime.now(),
    ),
    ResultEvent.from_task(
        safe_copy(_TASK, update={"completed_at": datetime.now()}), "some-result"
    ),
    CancelledEvent.from_task(
        safe_copy(_TASK, update={"cancelled_at": datetime.now()}), requeue=True
    ),
)


@pytest.mark.parametrize(
    "event,worker", list(zip(_EVENTS, itertools.repeat(None))), indirect=["worker"]
)
async def test_worker_publish_event(
    worker: Neo4jWorker, neo4j_task_manager: Neo4JTaskManager, event: ManagerEvent
):
    # When
    driver = worker.driver
    await neo4j_task_manager.save_task(_TASK)
    await worker.publish_event(event)

    # Then
    query = "MATCH (event:_ManagerEvent) RETURN event"
    saved_events, _, _ = await driver.execute_query(query)

    assert len(saved_events) == 1
    saved = saved_events[0]
    saved = Message.parse_obj(json.loads(saved["event"]["event"]))
    assert saved == event


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
        await worker.save_result(ResultEvent.from_task(task, "some_res"))

    # Then
    task = await task_manager.get_task(task_id=created.id)
    update = {"progress": 1.0, "state": TaskState.DONE}
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
