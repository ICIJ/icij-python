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
    Neo4jWorker,
    Task,
    TaskError,
    TaskEvent,
    TaskResult,
    TaskStatus,
)
from icij_worker.task_manager.neo4j import Neo4JTaskManager


@pytest.fixture(scope="function")
def worker(
    test_app: AsyncApp, neo4j_async_app_driver: neo4j.AsyncDriver
) -> Neo4jWorker:
    worker = Neo4jWorker(test_app, "test-worker", neo4j_async_app_driver)
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


async def test_worker_negatively_acknowledge(
    populate_tasks: List[Task], worker: Neo4jWorker
):
    # pylint: disable=unused-argument
    # When
    task, project = await worker.consume()
    n_locks = await _count_locks(worker.driver, project=project)
    assert n_locks == 1
    nacked = await worker.negatively_acknowledge(task, project, requeue=False)

    # Then
    update = {"status": TaskStatus.ERROR}
    expected_nacked = safe_copy(task, update=update)
    assert nacked == expected_nacked
    n_locks = await _count_locks(worker.driver, project=project)
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
        id="some-id",
        type="hello_world",
        created_at=created_at,
        status=TaskStatus.CREATED,
    )
    n_locks = await _count_locks(worker.driver, project=project)
    assert n_locks == 0

    # When
    await task_manager.enqueue(task, project)
    task, project = await worker.consume()
    n_locks = await _count_locks(worker.driver, project=project)
    assert n_locks == 1
    # Let's publish some event to increment the progress and check that it's reset
    # correctly to 0
    event = TaskEvent(task_id=task.id, progress=50.0)
    await worker.publish_event(event, project)
    with_progress = safe_copy(task, update={"progress": event.progress})
    nacked = await worker.negatively_acknowledge(task, project, requeue=True)

    # Then
    update = {"status": TaskStatus.QUEUED, "progress": 0.0, "retries": 1.0}
    expected_nacked = safe_copy(with_progress, update=update)
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
    task_result = TaskResult(task_id=task.id, result=result)

    # When
    await worker.save_result(result=task_result, project=project)
    saved_task = await task_manager.get_task(task_id=task.id, project=project)
    saved_result = await task_manager.get_task_result(task_id=task.id, project=project)

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
    task_result = TaskResult(task_id=task.id, result=result)

    # When
    await worker.save_result(result=task_result, project=project)
    # Then
    expected = "Attempted to save result for task task-0 but found existing result"
    with pytest.raises(ValueError, match=expected):
        await worker.save_result(result=task_result, project=project)


async def test_worker_acknowledgment_cm(
    populate_tasks: List[Task], worker: Neo4jWorker
):
    # Given
    created = populate_tasks[0]
    task_manager = Neo4JTaskManager(worker.driver, max_queue_size=10)
    project = TEST_PROJECT

    # When
    async with worker.acknowledgment_cm(created, project):
        await worker.consume()
        task = await task_manager.get_task(task_id=created.id, project=TEST_PROJECT)
        assert task.status is TaskStatus.RUNNING

    # Then
    task = await task_manager.get_task(task_id=created.id, project=TEST_PROJECT)
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
        title="someErrorTitle",
        detail="with_details",
        occurred_at=datetime.now(),
    )

    # When
    task, _ = await worker.consume()
    await worker.save_error(error=error, task=task, project=project)
    saved_task = await task_manager.get_task(task_id=task.id, project=project)
    saved_errors = await task_manager.get_task_errors(task_id=task.id, project=project)

    # Then
    # We don't expect the task status to be updated by saving the error, the negative
    # acknowledgment will do it
    assert saved_task == task
    assert saved_errors == [error]
