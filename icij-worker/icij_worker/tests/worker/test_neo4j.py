# pylint: disable=redefined-outer-name
import asyncio
from datetime import datetime
from typing import List

import neo4j
import pytest

from icij_common.neo4j.projects import project_db_session
from icij_common.pydantic_utils import safe_copy
from icij_common.test_utils import TEST_PROJECT, fail_if_exception
from icij_worker import (
    AsyncApp,
    Neo4JTaskManager,
    Neo4jWorker,
    Task,
    TaskError,
    TaskEvent,
    TaskResult,
    TaskStatus,
)
from icij_worker.task import CancelledTaskEvent


@pytest.fixture(scope="function")
def worker(
    test_app: AsyncApp, neo4j_async_app_driver: neo4j.AsyncDriver
) -> Neo4jWorker:
    worker = Neo4jWorker(
        test_app,
        "test-worker",
        neo4j_async_app_driver,
        cancelled_tasks_refresh_interval_s=0.1,
        new_tasks_refresh_interval_s=0.1,
    )
    return worker


async def _count_locks(driver: neo4j.AsyncDriver, project: str) -> int:
    # Now let's check that no lock if left in the DB
    count_locks_query = "MATCH (lock:_TaskLock) RETURN count(*) as nLocks"
    async with project_db_session(driver, project=project) as sess:
        recs = await sess.run(count_locks_query)
        counts = await recs.single(strict=True)
    return counts["nLocks"]


async def test_worker_consume_task(populate_tasks: List[Task], worker: Neo4jWorker):
    # pylint: disable=unused-argument
    # Given
    project = TEST_PROJECT
    # When
    task = asyncio.create_task(worker.consume())
    # Then
    timeout = 2
    with fail_if_exception(f"failed to consume task in less than {timeout}s"):
        await asyncio.wait([task], timeout=timeout)

    # Now let's check that no lock if left in the DB
    count_locks_query = "MATCH (lock:_TaskLock) RETURN count(*) as nLocks"
    async with project_db_session(worker.driver, project) as sess:
        recs = await sess.run(count_locks_query)
        counts = await recs.single(strict=True)
    assert counts["nLocks"] == 1


async def test_worker_consume_cancel_event(
    populate_cancel_events: List[CancelledTaskEvent], worker: Neo4jWorker
):
    # pylint: disable=unused-argument,protected-access
    # Given
    # When
    task = asyncio.create_task(worker._consume_cancelled())
    # Then
    timeout = 2
    await asyncio.wait([task], timeout=timeout)
    if not task.done():
        pytest.fail(f"failed to consume task in less than {timeout}s")
    event = task.result()
    assert event == populate_cancel_events[0]
    assert event.project_id == TEST_PROJECT


async def test_worker_negatively_acknowledge(
    populate_tasks: List[Task], worker: Neo4jWorker
):
    # pylint: disable=unused-argument
    # Given
    task_manager = Neo4JTaskManager(worker.driver, max_queue_size=10)
    # When
    task = await worker.consume()
    n_locks = await _count_locks(worker.driver, project=task.project_id)
    assert n_locks == 1
    await worker.negatively_acknowledge(task, requeue=False)
    nacked = await task_manager.get_task(task_id=task.id)

    # Then
    update = {"status": TaskStatus.ERROR}
    expected_nacked = safe_copy(task, update=update)
    assert nacked == expected_nacked
    n_locks = await _count_locks(worker.driver, project=task.project_id)
    assert n_locks == 0


async def test_worker_negatively_acknowledge_and_requeue(
    populate_tasks: List[Task], worker: Neo4jWorker
):
    # pylint: disable=unused-argument
    # Given
    task_manager = Neo4JTaskManager(worker.driver, max_queue_size=10)
    project = TEST_PROJECT
    created_at = datetime.now()
    task = Task(
        project_id=project,
        id="some-id",
        type="hello_world",
        created_at=created_at,
        status=TaskStatus.CREATED,
    )
    n_locks = await _count_locks(worker.driver, project=project)
    assert n_locks == 0

    # When
    await task_manager.enqueue(task)
    task = await worker.consume()
    n_locks = await _count_locks(worker.driver, project=project)
    assert n_locks == 1
    # Let's publish some event to increment the progress and check that it's reset
    # correctly to 0
    event = TaskEvent(task_id=task.id, progress=50.0)
    await worker.publish_event(event, task)
    with_progress = safe_copy(task, update={"progress": event.progress})
    await worker.negatively_acknowledge(task, requeue=True)
    nacked = await task_manager.get_task(task_id=task.id)

    # Then
    update = {"status": TaskStatus.QUEUED, "progress": 0.0, "retries": 1.0}
    expected_nacked = safe_copy(with_progress, update=update)
    assert nacked == expected_nacked
    n_locks = await _count_locks(worker.driver, project=project)
    assert n_locks == 0


@pytest.mark.parametrize("requeue", [True, False])
async def test_worker_negatively_acknowledge_and_cancel(
    worker: Neo4jWorker, requeue: bool
):
    # pylint: disable=unused-argument
    # Given
    task_manager = Neo4JTaskManager(worker.driver, max_queue_size=10)
    project = TEST_PROJECT
    created_at = datetime.now()
    task = Task(
        id="some-id",
        type="hello_world",
        project_id=TEST_PROJECT,
        created_at=created_at,
        status=TaskStatus.CREATED,
    )
    n_locks = await _count_locks(worker.driver, project=project)
    assert n_locks == 0

    # When
    await task_manager.enqueue(task)
    task = await worker.consume()
    n_locks = await _count_locks(worker.driver, project=project)
    assert n_locks == 1
    # Let's publish some event to increment the progress and check that it's reset
    # correctly to 0
    event = TaskEvent(task_id=task.id, progress=50.0)
    await worker.publish_event(event, task)
    with_progress = safe_copy(task, update={"progress": event.progress})
    await worker.negatively_acknowledge(task, cancel=True, requeue=requeue)
    nacked = await task_manager.get_task(task_id=task.id)

    # Then
    nacked = nacked.dict(exclude_unset=True)
    if requeue:
        update = {"status": TaskStatus.QUEUED, "progress": 0.0}
    else:
        assert nacked["cancelled_at"] is not None
        nacked.pop("cancelled_at")
        update = {"status": TaskStatus.CANCELLED}
    expected_nacked = safe_copy(with_progress, update=update)
    expected_nacked = expected_nacked.dict(exclude_unset=True)
    assert nacked == expected_nacked
    n_locks = await _count_locks(worker.driver, project=project)
    assert n_locks == 0


async def test_worker_save_result(populate_tasks: List[Task], worker: Neo4jWorker):
    # Given
    task_manager = Neo4JTaskManager(worker.driver, max_queue_size=10)
    project = TEST_PROJECT
    task = populate_tasks[0]
    assert task.status == TaskStatus.QUEUED
    result = "hello everyone"
    task_result = TaskResult(task_id=task.id, project_id=project, result=result)

    # When
    await worker.save_result(result=task_result)
    saved_task = await task_manager.get_task(task_id=task.id)
    saved_result = await task_manager.get_task_result(task_id=task.id)

    # Then
    assert saved_task == task
    assert saved_result == task_result


async def test_worker_should_raise_when_saving_existing_result(
    populate_tasks: List[Task], worker: Neo4jWorker
):
    # Given
    project = TEST_PROJECT
    task = populate_tasks[0]
    assert task.status == TaskStatus.QUEUED
    result = "hello everyone"
    task_result = TaskResult(task_id=task.id, project_id=project, result=result)

    # When
    await worker.save_result(result=task_result)
    # Then
    expected = "Attempted to save result for task task-0 but found existing result"
    with pytest.raises(ValueError, match=expected):
        await worker.save_result(result=task_result)


async def test_worker_acknowledgment_cm(
    populate_tasks: List[Task], worker: Neo4jWorker
):
    # Given
    created = populate_tasks[0]
    task_manager = Neo4JTaskManager(worker.driver, max_queue_size=10)

    # When
    async with worker.acknowledgment_cm():
        await worker.consume()
        task = await task_manager.get_task(task_id=created.id)
        assert task.status is TaskStatus.RUNNING

    # Then
    task = await task_manager.get_task(task_id=created.id)
    update = {"progress": 100.0, "status": TaskStatus.DONE}
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


async def test_worker_save_error(populate_tasks: List[Task], worker: Neo4jWorker):
    # pylint: disable=unused-argument
    # Given
    task_manager = Neo4JTaskManager(worker.driver, max_queue_size=10)
    project = TEST_PROJECT
    error = TaskError(
        id="error-id",
        task_id=populate_tasks[0].id,
        project_id=project,
        title="someErrorTitle",
        detail="with_details",
        occurred_at=datetime.now(),
    )

    # When
    task = await worker.consume()
    await worker.save_error(error=error)
    await worker.publish_error_event(error, task)
    saved_task = await task_manager.get_task(task_id=task.id)
    saved_errors = await task_manager.get_task_errors(task_id=task.id)

    # Then
    # We don't expect the task status to be updated by saving the error, the negative
    # acknowledgment will do it
    assert saved_task == task
    assert saved_errors == [error]
