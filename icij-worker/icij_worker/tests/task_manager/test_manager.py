# pylint: disable=redefined-outer-name
import itertools
from datetime import datetime, timezone
from functools import partial

import pytest

from icij_common.pydantic_utils import safe_copy
from icij_common.test_utils import async_true_after
from icij_worker import ResultEvent, Task, TaskError, TaskState
from icij_worker.exceptions import UnregisteredTask
from icij_worker.objects import ProgressEvent, StacktraceItem
from icij_worker.utils.tests import MockManager, MockWorker


@pytest.fixture
def mock_manager(fs_storage_path, request) -> MockManager:
    app = getattr(request, "param", "test_async_app")
    app = request.getfixturevalue(app)
    task_manager = MockManager(app, fs_storage_path)
    return task_manager


@pytest.fixture
def mock_dag_manager(fs_storage_path, test_dag_app) -> MockManager:
    task_manager = MockManager(test_dag_app, fs_storage_path)
    return task_manager


async def test_consume_progress_event(
    mock_manager: MockManager, mock_worker: MockWorker
):
    # Given
    task_manager = mock_manager
    worker = mock_worker
    task_name = "sleep_for"
    task = Task(
        id="some-id",
        name=task_name,
        created_at=datetime.now(),
        state=TaskState.RUNNING,
        progress=0.0,
    )
    await task_manager.save_task(task)
    event = ProgressEvent(task_id=task.id, progress=0.99, created_at=datetime.now())
    # When
    await worker.publish_event(event)
    # Then
    async with task_manager:
        consume_timeout = 20.0
        msg = f"Failed to consume error event in less than {consume_timeout}"

        async def _received_progress() -> bool:
            t = await task_manager.get_task(event.task_id)
            return t.progress > 0.0

        update = {
            "state": TaskState.RUNNING,
            "progress": 0.99,
            "max_retries": 1,
            "retries_left": 1,
        }
        expected_task = safe_copy(task, update=update)
        assert await async_true_after(_received_progress, after_s=consume_timeout), msg
        db_task = await task_manager.get_task(task_id=task.id)
        assert db_task == expected_task


@pytest.mark.parametrize(
    "retries_left,mock_worker",
    list(zip((0, 1), itertools.repeat(None))),
    indirect=["mock_worker"],
)
async def test_task_manager_should_consume_error_events(
    mock_manager: MockManager, mock_worker: MockWorker, retries_left
):
    # Given
    task_manager = mock_manager
    worker = mock_worker
    task_name = "sleep_for"
    task = Task(
        id="some-id",
        name=task_name,
        created_at=datetime.now(),
        state=TaskState.RUNNING,
    )
    await task_manager.save_task(task)
    error = TaskError(
        name="error",
        message="with details",
        stacktrace=[StacktraceItem(name="SomeError", file="somefile", lineno=666)],
        cause="some cause",
    )
    # When
    await worker.publish_error_event(error, task, retries_left)
    # Then
    async with task_manager:
        consume_timeout = 20.0
        msg = f"Failed to consume error event in less than {consume_timeout}"

        async def _assert_has_state(state: TaskState) -> bool:
            saved = await task_manager.get_task(task_id=task.id)
            return saved.state is state

        if not retries_left:
            expected = partial(_assert_has_state, TaskState.ERROR)
            update = {
                "state": TaskState.ERROR,
                "retries_left": retries_left,
                "max_retries": 1,
            }
            expected_task = safe_copy(task, update=update)
        else:
            expected = partial(_assert_has_state, TaskState.QUEUED)
            update = {
                "state": TaskState.QUEUED,
                "retries_left": retries_left,
                "progress": 0.0,
                "max_retries": 1,
            }
            expected_task = safe_copy(task, update=update)
        assert await async_true_after(expected, after_s=consume_timeout), msg
        db_task = await task_manager.get_task(task_id=task.id)
        assert db_task == expected_task
        db_errors = await task_manager.get_task_errors(task.id)
        assert len(db_errors) == 1
        db_error = db_errors[0]
        assert db_error.task_id == task.id
        assert db_error.retries_left == retries_left
        assert db_error.error == error


@pytest.mark.parametrize(
    "requeue,mock_manager",
    list(itertools.product((True, False), ("test_async_app", "test_async_app_late"))),
    indirect=["mock_manager"],
)
async def test_consume_cancelled_event(
    mock_manager: MockManager,
    mock_worker: MockWorker,
    requeue: bool,
    hello_world_task: Task,
):
    # Given
    task_manager = mock_manager
    worker = mock_worker
    task = hello_world_task
    await task_manager.save_task(task)
    await task_manager.enqueue(task)
    await mock_worker.consume()
    worker.current = task
    await task_manager.save_task(task)

    # When
    await worker.publish_cancelled_event(requeue)

    # Then
    async with task_manager:
        consume_timeout = 2.0
        msg = f"Failed to consume error event in less than {consume_timeout}"

        async def _assert_has_state(state: TaskState) -> bool:
            saved = await task_manager.get_task(task_id=task.id)
            return saved.state is state

        if requeue:
            expected = partial(_assert_has_state, TaskState.QUEUED)
            expected_task = safe_copy(
                task,
                update={
                    "state": TaskState.QUEUED,
                    "progress": 0.0,
                    "retries_left": 3,
                    "max_retries": 3,
                },
            )
            expected_cancelled_at_type = type(None)
        else:
            expected = partial(_assert_has_state, TaskState.CANCELLED)
            expected_task = safe_copy(
                task,
                update={
                    "state": TaskState.CANCELLED,
                    "progress": 0.0,
                    "retries_left": 3,
                    "max_retries": 3,
                },
            )
            expected_cancelled_at_type = datetime
        assert await async_true_after(expected, after_s=consume_timeout), msg
        db_task = await task_manager.get_task(task_id=task.id)
        assert isinstance(db_task.completed_at, expected_cancelled_at_type)
        db_task = db_task.dict()
        expected_task = expected_task.dict()
        db_task.pop("completed_at", None)
        expected_task.pop("completed_at", None)
        assert db_task == expected_task


async def test_save_task(mock_manager: MockManager):
    # Given
    task_manager = mock_manager
    task = Task(
        id="some-id",
        name="often_retriable",
        args=dict(),
        created_at=datetime.now(),
        state=TaskState.CREATED,
    )

    # When
    await task_manager.save_task(task)
    db_task = await task_manager.get_task(task_id=task.id)

    # Then
    expected_task = safe_copy(task, update={"max_retries": 666, "retries_left": 666})
    assert db_task == expected_task


async def test_save_task_to_group(
    mock_manager: MockManager, grouped_hello_world_task: Task
):
    # Given
    task_manager = mock_manager
    task = grouped_hello_world_task

    # When
    await task_manager.save_task(task)
    db_task = await task_manager.get_task(task_id=task.id)

    # Then
    expected_task = safe_copy(task, update={"max_retries": 3, "retries_left": 3})
    assert db_task == expected_task
    group = await task_manager.get_task_group(task.id)
    assert group == "hello"


async def test_save_unknown_task_should_raise_unregistered_task_error(
    mock_manager: MockManager,
):
    # Given
    task_manager = mock_manager

    task = Task(
        id="some-id",
        name="i_dont_exist",
        args=dict(),
        created_at=datetime.now(),
        state=TaskState.CREATED,
    )
    # When/Then
    with pytest.raises(UnregisteredTask):
        await task_manager.save_task(task)


async def test_enqueue_dag(mock_dag_manager: MockManager):
    # Given
    task_manager = mock_dag_manager
    dag_task_id = "dag-task-id"
    args = {"a_input": "some-input"}
    dag_task = Task.create(task_id=dag_task_id, task_name="d", args=args)
    # When
    await task_manager.save_task(dag_task)
    queued = await task_manager.enqueue(dag_task)

    # Then
    dag = await task_manager.get_task_dag(dag_task_id)
    assert len(dag) == 4
    expected_queued = safe_copy(dag_task, update={"state": TaskState.QUEUED})
    assert queued == expected_queued
    for t_id in dag:
        if t_id == queued.id:
            continue
        task = await task_manager.get_task(t_id)
        if task.name == "a":
            assert task.state is TaskState.QUEUED
            assert task.args == {"a_input": "some-input"}
        else:
            assert task.state is TaskState.CREATED


@pytest.mark.parametrize(
    "mock_worker", [{"app": "test_async_app_late"}], indirect=["mock_worker"]
)
async def test_consume_progress_event_dag(
    mock_dag_manager: MockManager, mock_worker: MockWorker
):
    # Given
    task_manager = mock_dag_manager
    worker = mock_worker
    dag_task = Task(
        id="dag-task-id",
        name="d",
        created_at=datetime.now(),
        state=TaskState.RUNNING,
        progress=0.0,
        args={"a_input": "some-input"},
    )
    await task_manager.save_task(dag_task)
    dag = await task_manager.get_task_dag(dag_task.id)
    parents = sorted((t_id for t_id in dag if t_id != dag_task.id))
    events = [
        ProgressEvent(task_id=t_id, created_at=datetime.now(timezone.utc), progress=0.5)
        for t_id in parents
    ]
    events = sorted(events, key=lambda e: e.created_at)
    consume_timeout = 10.0
    current_progress = 0.0
    async with task_manager:
        for i, event in enumerate(events):
            # When
            await worker.publish_event(event)

            # Then
            msg = f"Failed to consume progress event in less than {consume_timeout}"

            async def _updated_progress(current_p: float) -> bool:
                t = await task_manager.get_task(dag_task.id)
                return t.progress > current_p

            assert await async_true_after(
                partial(_updated_progress, current_progress), after_s=consume_timeout
            ), msg
            parent_task = await task_manager.get_task(task_id=event.task_id)
            assert parent_task.state is TaskState.RUNNING
            assert parent_task.progress == event.progress
            db_dag_task = await task_manager.get_task(task_id=dag_task.id)
            expected_total = (event.progress * (i + 1)) / len(events)
            assert db_dag_task.progress == expected_total
            current_progress = db_dag_task.progress


@pytest.mark.parametrize(
    "mock_worker", [{"app": "test_async_app_late"}], indirect=["mock_worker"]
)
async def test_consume_error_events_dag(
    mock_dag_manager: MockManager, mock_worker: MockWorker
):
    assert False


@pytest.mark.parametrize(
    "mock_worker", [{"app": "test_async_app_late"}], indirect=["mock_worker"]
)
async def test_consume_result_event_dag(
    mock_dag_manager: MockManager, mock_worker: MockWorker
):
    # Given
    task_manager = mock_dag_manager
    worker = mock_worker
    dag_task = Task(
        id="dag-task-id",
        name="d",
        created_at=datetime.now(),
        state=TaskState.CREATED,
        progress=0.0,
        args={"a_input": "some-input"},
    )
    await task_manager.save_task(dag_task)
    await task_manager.enqueue(dag_task)

    async with task_manager:
        dag = await task_manager.get_task_dag(dag_task.id)
        a_id = dag.start_nodes[0]
        task_a = await task_manager.get_task(a_id)
        b_id = [k for k in dag.arg_providers if "id-b" in k][0]
        c_id = [k for k in dag.arg_providers if "id-c" in k][0]

        # When
        await worker.publish_event(
            ResultEvent.from_task(task_a, result="a received some_text")
        )

        # Then
        consume_timeout = 20.0
        msg = f"failed to consume result event in less than {consume_timeout}"

        async def _b_and_c_queued() -> bool:
            b = await task_manager.get_task(b_id)
            if b.state is not TaskState.QUEUED:
                return False
            c = await task_manager.get_task(c_id)
            if c.state is not TaskState.QUEUED:
                return False
            return True

        assert await async_true_after(_b_and_c_queued, after_s=consume_timeout), msg

        task_b = await task_manager.get_task(b_id)
        assert task_b.args == {"b_input": "a received some_text"}
        task_c = await task_manager.get_task(c_id)
        assert task_c.args == {"task_c": "a received some_text"}


def test_consume_result_event_final_dag():
    assert False


def test_consume_cancel_dag():
    assert False


def test_consume_cancelled_event_dag():
    assert False


def test_save_task_dag():
    assert False


def test_dag_progress():
    assert False
