# pylint: disable=redefined-outer-name
import functools
import re
from datetime import datetime
from typing import List, Optional, Union

import pytest
import ujson
from sqlitedict import SqliteDict

from icij_common.pydantic_utils import safe_copy
from icij_worker import Task, TaskError, TaskResult
from icij_worker.objects import StacktraceItem, TaskState
from icij_worker.tests.conftest import TestableFSKeyValueStorage


@functools.lru_cache
def task_0() -> Task:
    t = Task(
        id="task-0",
        type="task-type-0",
        created_at=datetime.now(),
        state=TaskState.CREATED,
        arguments={"greeted": "world"},
    )
    return t


@functools.lru_cache
def task_1() -> Task:
    t = Task(
        id="task-1",
        type="task-type-1",
        created_at=datetime.now(),
        state=TaskState.QUEUED,
        arguments={},
    )
    return t


@pytest.fixture()
def populate_tasks(fs_storage: TestableFSKeyValueStorage) -> List[Task]:
    db = _make_db(fs_storage.db_path, table_name="tasks")
    with db:
        t_0 = task_0()
        t_0_as_dict = t_0.dict()
        t_0_as_dict["namespace"] = "some-namespace"
        db[t_0.id] = t_0_as_dict
        t_1 = task_1()
        db[t_1.id] = t_1.dict()
        db.commit()
    return [t_0, t_1]


def _make_db(db_path: str, *, table_name: str) -> SqliteDict:
    # pylint: disable=c-extension-no-member
    return SqliteDict(
        filename=db_path,
        tablename=table_name,
        encode=functools.partial(ujson.encode, default=str),
        decode=ujson.decode,
    )


async def test_save_task(fs_storage: TestableFSKeyValueStorage, hello_world_task: Task):
    # Given
    task = hello_world_task
    db = _make_db(fs_storage.db_path, table_name="tasks")
    # When
    await fs_storage.save_task(task)
    # Then
    with db:
        db_task = db.get(task.id)
    assert db_task is not None
    db_task.pop("namespace", None)
    db_task = Task.parse_obj(db_task)
    assert db_task == task


async def test_save_task_with_different_ns_should_fail(
    fs_storage: TestableFSKeyValueStorage, hello_world_task: Task
):
    # Given
    task = hello_world_task
    await fs_storage.save_task(task)
    # When
    msg = re.escape(
        "DB task namespace (None) differs from save task namespace: some-namespace"
    )
    with pytest.raises(ValueError, match=msg):
        await fs_storage.save_task(task, "some-namespace")


async def test_save_task_should_not_update_non_updatable_field(
    fs_storage: TestableFSKeyValueStorage, hello_world_task: Task
):
    # Given
    task = hello_world_task
    await fs_storage.save_task(task)
    updates = {
        "type": "another-type",
        "created_at": datetime.now(),
        "arguments": {"other": "input"},
    }
    updated = safe_copy(task, update=updates)
    # When
    await fs_storage.save_task(updated)
    stored = await fs_storage.get_task(task.id)
    # Then
    assert stored == task


async def test_save_result(fs_storage: TestableFSKeyValueStorage):
    # Give
    result = TaskResult(task_id="some_task_id", result="some_result")
    db = _make_db(fs_storage.db_path, table_name="results")
    # When
    await fs_storage.save_result(result)
    # Then
    with db:
        db_result = db.get(result.task_id)
    assert db_result is not None
    db_result = TaskResult.parse_obj(db_result)
    assert db_result == result


async def test_save_error(fs_storage: TestableFSKeyValueStorage):
    # Give
    error = TaskError(
        id="some-id",
        task_id="some-task-id",
        name="error",
        message="with details",
        occurred_at=datetime.now(),
        stacktrace=[StacktraceItem(name="SomeError", file="somefile", lineno=666)],
        cause="some cause",
    )

    db = _make_db(fs_storage.db_path, table_name="errors")
    # When
    await fs_storage.save_error(error)
    # Then
    with db:
        db_errors = db.get(error.task_id)
    assert db_errors is not None
    db_errors = [TaskError.parse_obj(err) for err in db_errors]
    assert db_errors == [error]


@pytest.mark.parametrize(
    "task_id,expected_task",
    [("task-0", task_0()), ("task-1", task_1())],
)
async def test_get_task(
    fs_storage: TestableFSKeyValueStorage,
    populate_tasks: List[Task],  # pylint: disable=unused-argument
    task_id: str,
    expected_task: Task,
):
    # When
    task = await fs_storage.get_task(task_id)
    # Then
    assert task == expected_task


@pytest.mark.parametrize(
    "namespace,task_type,state,expected_task",
    [
        (None, None, None, [task_0(), task_1()]),
        ("some-namespace", None, None, [task_0()]),
        (None, "task-type-1", None, [task_1()]),
        (None, None, TaskState.CREATED, [task_0()]),
    ],
)
async def test_get_tasks(
    fs_storage: TestableFSKeyValueStorage,
    populate_tasks: List[Task],  # pylint: disable=unused-argument
    namespace: Optional[str],
    task_type: Optional[str],
    state: Optional[Union[List[TaskState], TaskState]],
    expected_task: List[Task],
):
    # Given
    store = fs_storage
    # When
    tasks = await store.get_tasks(namespace=namespace, task_type=task_type, state=state)
    # Then
    assert tasks == expected_task


async def test_get_result(fs_storage: TestableFSKeyValueStorage):
    # Given
    store = fs_storage
    db = _make_db(fs_storage.db_path, table_name="results")
    res = TaskResult(task_id="some-id", result="Hello world !")
    with db:
        db["task-0"] = res.dict()
        db.commit()
    # When
    result = await store.get_task_result(task_id="task-0")
    # Then
    assert result == res


async def test_get_error(fs_storage: TestableFSKeyValueStorage):
    # Given
    store = fs_storage
    db = _make_db(fs_storage.db_path, table_name="errors")
    err = TaskError(
        id="error-id",
        task_id="task-0",
        name="some-error",
        message="some message",
        stacktrace=[StacktraceItem(name="SomeError", file="some details", lineno=666)],
        occurred_at=datetime.now(),
    )
    with db:
        db["task-0"] = [err.dict()]
        db.commit()
    # When
    db_errors = await store.get_task_errors(task_id="task-0")
    # Then
    assert db_errors == [err]
