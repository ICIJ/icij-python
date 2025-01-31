# pylint: disable=redefined-outer-name
import itertools
from datetime import datetime, timezone
from functools import partial

import pytest

from icij_common.pydantic_utils import safe_copy
from icij_common.test_utils import async_true_after
from icij_worker import ResultEvent, Task, TaskError, TaskState
from icij_worker.exceptions import UnregisteredTask
from icij_worker.objects import (
    CancelEvent,
    CancelledEvent,
    ErrorEvent,
    ProgressEvent,
    StacktraceItem,
)
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
            async def _updated_progress(current_p: float) -> bool:
                t = await task_manager.get_task(dag_task.id)
                return t.progress > current_p

            assert await async_true_after(
                partial(_updated_progress, current_progress), after_s=consume_timeout
            )
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
async def test_consume_fatal_error_events_dag(
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

        async def _b_and_c_queued() -> bool:
            b = await task_manager.get_task(b_id)
            if b.state is not TaskState.QUEUED:
                return False
            c = await task_manager.get_task(c_id)
            if c.state is not TaskState.QUEUED:
                return False
            return True

        assert await async_true_after(_b_and_c_queued, after_s=consume_timeout)

        # When
        await worker.publish_event(
            ErrorEvent(
                task_id=b_id,
                error=TaskError(name="some error", message="some message"),
                retries_left=0,
                created_at=datetime.now(timezone.utc),
            )
        )

        async def _d_error() -> bool:
            d = await task_manager.get_task(dag_task.id)
            return d.state is TaskState.ERROR

        assert await async_true_after(_d_error, after_s=consume_timeout)

        task_b = await task_manager.get_task(b_id)
        assert task_b.state is TaskState.ERROR

        async def _c_cancelled() -> bool:
            c = await task_manager.get_task(c_id)
            return c.state is TaskState.CANCELLED

        assert await async_true_after(_c_cancelled, after_s=consume_timeout)


@pytest.mark.parametrize(
    "mock_worker", [{"app": "test_async_app_late"}], indirect=["mock_worker"]
)
async def test_consume_recoverable_error_events_dag(
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

        async def _b_and_c_queued() -> bool:
            b = await task_manager.get_task(b_id)
            if b.state is not TaskState.QUEUED:
                return False
            c = await task_manager.get_task(c_id)
            if c.state is not TaskState.QUEUED:
                return False
            return True

        assert await async_true_after(_b_and_c_queued, after_s=consume_timeout)

        # When
        await worker.publish_event(
            ErrorEvent(
                task_id=b_id,
                error=TaskError(name="some error", message="some message"),
                retries_left=1,
                created_at=datetime.now(timezone.utc),
            )
        )

        async def _b_retries_decremented() -> bool:
            b = await task_manager.get_task(b_id)
            return b.retries_left == 1

        assert await async_true_after(_b_retries_decremented, after_s=consume_timeout)

        task_b = await task_manager.get_task(b_id)
        assert task_b.state is TaskState.QUEUED
        assert task_b.retries_left == 1

        task_d = await task_manager.get_task(dag_task.id)
        assert task_d.state is TaskState.RUNNING


@pytest.mark.parametrize(
    "mock_worker", [{"app": "test_async_app_late"}], indirect=["mock_worker"]
)
async def test_consume_result_events(
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

        async def _b_and_c_queued() -> bool:
            b = await task_manager.get_task(b_id)
            if b.state is not TaskState.QUEUED:
                return False
            c = await task_manager.get_task(c_id)
            if c.state is not TaskState.QUEUED:
                return False
            return True

        assert await async_true_after(_b_and_c_queued, after_s=consume_timeout)

        task_b = await task_manager.get_task(b_id)
        assert task_b.args == {"b_input": "a received some_text"}
        task_c = await task_manager.get_task(c_id)
        assert task_c.args == {"c_input": "a received some_text"}

        # When
        await worker.publish_event(
            ResultEvent.from_task(task_b, result="b received some_text")
        )
        await worker.publish_event(
            ResultEvent.from_task(task_c, result="c received some_text")
        )

        async def _b_c_done() -> bool:
            b = await task_manager.get_task(b_id)
            if b.state is not TaskState.DONE:
                return False
            c = await task_manager.get_task(c_id)
            if c.state is not TaskState.DONE:
                return False
            return True

        assert await async_true_after(_b_c_done, after_s=consume_timeout)

        task_d = await task_manager.get_task(dag_task.id)
        assert task_d.args == {
            "left_input": "b received some_text",
            "right_input": "c received some_text",
        }

        # When
        await worker.publish_event(
            ResultEvent.from_task(
                dag_task, result="b received some_text d c received some_text"
            )
        )

        async def _d_done() -> bool:
            d = await task_manager.get_task(dag_task.id)
            return d.state is TaskState.DONE

        assert await async_true_after(_d_done, after_s=consume_timeout)

        task_d = await task_manager.get_task(dag_task.id)
        assert task_d.state is TaskState.DONE
        assert isinstance(task_d.completed_at, datetime)
        res = await task_manager.get_task_result(dag_task.id)
        assert res.result == "b received some_text d c received some_text"


async def test_consume_cancel_dag_sink(mock_dag_manager: MockManager):
    # Given
    task_manager = mock_dag_manager
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
        b_id = [k for k in dag.arg_providers if "id-b" in k][0]
        c_id = [k for k in dag.arg_providers if "id-c" in k][0]

        # When
        await task_manager.cancel(dag_task.id, requeue=False)

        # Then
        consume_timeout = 20.0

        async def _d_cancelled() -> bool:
            d = await task_manager.get_task(dag_task.id)
            return d.state is TaskState.CANCELLED

        assert await async_true_after(_d_cancelled, after_s=consume_timeout)
        task_a = await task_manager.get_task(a_id)
        assert task_a.state is TaskState.CANCELLED
        task_b = await task_manager.get_task(b_id)
        assert task_b.state is TaskState.CANCELLED
        task_c = await task_manager.get_task(c_id)
        assert task_c.state is TaskState.CANCELLED


async def test_consume_cancel_dag(
    mock_dag_manager: MockManager,
):
    # Given
    task_manager = mock_dag_manager
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
        b_id = [k for k in dag.arg_providers if "id-b" in k][0]
        c_id = [k for k in dag.arg_providers if "id-c" in k][0]

        # When
        await task_manager.cancel(a_id, requeue=False)

        # Then
        consume_timeout = 20.0
        msg = f"failed to cancel event in less than {consume_timeout}"

        async def _d_cancelled() -> bool:
            d = await task_manager.get_task(dag_task.id)
            return d.state is TaskState.CANCELLED

        assert await async_true_after(_d_cancelled, after_s=consume_timeout), msg
        task_a = await task_manager.get_task(a_id)
        assert task_a.state is TaskState.CANCELLED
        task_b = await task_manager.get_task(b_id)
        assert task_b.state is TaskState.CANCELLED
        task_c = await task_manager.get_task(c_id)
        assert task_c.state is TaskState.CANCELLED


@pytest.mark.timeout(40)
@pytest.mark.parametrize(
    "mock_worker", [{"app": "test_async_app_late"}], indirect=["mock_worker"]
)
async def test_consume_cancel_and_requeue_sink_dag(
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
        c_id = [k for k in dag.arg_providers if "id-c" in k][0]

        consume_timeout = 20.0
        await worker.publish_event(
            ResultEvent.from_task(task_a, result="a received some_text")
        )

        async def _c_queued() -> bool:
            c = await task_manager.get_task(c_id)
            return c.state is TaskState.QUEUED

        assert await async_true_after(_c_queued, after_s=consume_timeout)

        await worker.publish_event(
            ProgressEvent(
                task_id=c_id, progress=0.5, created_at=datetime.now(timezone.utc)
            )
        )

        async def _c_running() -> bool:
            c = await task_manager.get_task(c_id)
            return c.state is TaskState.RUNNING

        assert await async_true_after(_c_running, after_s=consume_timeout)
        progress_when_cancelled = (await task_manager.get_task(dag_task.id)).progress

        # When
        await task_manager.cancel(dag_task.id, requeue=True)
        # Then
        # timeout needed for this one
        cancel = await worker.consume_worker_events()
        assert isinstance(cancel, CancelEvent)
        cancelled = CancelledEvent(
            task_id=cancel.task_id,
            created_at=datetime.now(timezone.utc),
            requeue=cancel.requeue,
        )
        await worker.publish_event(cancelled)

        async def _c_requeued() -> bool:
            c = await task_manager.get_task(c_id)
            return c.state is TaskState.QUEUED

        assert await async_true_after(_c_requeued, after_s=consume_timeout)

        task_d = await task_manager.get_task(dag_task.id)
        assert task_d.state is TaskState.RUNNING
        assert task_d.progress < progress_when_cancelled


@pytest.mark.timeout(40)
@pytest.mark.parametrize(
    "mock_worker", [{"app": "test_async_app_late"}], indirect=["mock_worker"]
)
async def test_consume_cancel_and_requeue_dag(
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
        c_id = [k for k in dag.arg_providers if "id-c" in k][0]

        consume_timeout = 20.0
        await worker.publish_event(
            ResultEvent.from_task(task_a, result="a received some_text")
        )

        async def _c_queued() -> bool:
            c = await task_manager.get_task(c_id)
            return c.state is TaskState.QUEUED

        assert await async_true_after(_c_queued, after_s=consume_timeout)

        await worker.publish_event(
            ProgressEvent(
                task_id=c_id, progress=0.5, created_at=datetime.now(timezone.utc)
            )
        )

        async def _c_running() -> bool:
            c = await task_manager.get_task(c_id)
            return c.state is TaskState.RUNNING

        assert await async_true_after(_c_running, after_s=consume_timeout)
        progress_when_cancelled = (await task_manager.get_task(dag_task.id)).progress

        # When
        await task_manager.cancel(c_id, requeue=True)
        # Then
        # timeout needed for this one
        cancel = await worker.consume_worker_events()
        assert isinstance(cancel, CancelEvent)
        cancelled = CancelledEvent(
            task_id=cancel.task_id,
            created_at=datetime.now(timezone.utc),
            requeue=cancel.requeue,
        )
        await worker.publish_event(cancelled)

        async def _c_requeued() -> bool:
            c = await task_manager.get_task(c_id)
            return c.state is TaskState.QUEUED

        assert await async_true_after(_c_requeued, after_s=consume_timeout)

        task_d = await task_manager.get_task(dag_task.id)
        assert task_d.state is TaskState.RUNNING
        assert task_d.progress < progress_when_cancelled
