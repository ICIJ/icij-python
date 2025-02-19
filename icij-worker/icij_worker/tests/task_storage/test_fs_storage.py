# pylint: disable=redefined-outer-name
import functools
import re
from datetime import datetime
from typing import List, Optional, Union

import pytest
import ujson
from sqlitedict import SqliteDict

from icij_common.pydantic_utils import safe_copy
from icij_worker import ResultEvent, Task, TaskError
from icij_worker.objects import ErrorEvent, StacktraceItem, TaskResult, TaskState
from icij_worker.tests.conftest import TestableFSKeyValueStorage


@functools.lru_cache
def task_0() -> Task:
    t = Task(
        id="task-0",
        name="task-type-0",
        created_at=datetime.now(),
        state=TaskState.CREATED,
        args={"greeted": "world"},
    )
    return t


@functools.lru_cache
def task_1() -> Task:
    t = Task(
        id="task-1",
        name="task-type-1",
        created_at=datetime.now(),
        state=TaskState.QUEUED,
        args={},
    )
    return t


@pytest.fixture()
def populate_tasks(fs_storage: TestableFSKeyValueStorage) -> List[Task]:
    db = _make_db(fs_storage.db_path, table_name="tasks")
    with db:
        t_0 = task_0()
        t_0_as_dict = t_0.dict()
        t_0_as_dict["group"] = "some-group"
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
    is_new = await fs_storage.save_task_(task, None)
    # Then
    with db:
        db_task = db.get(task.id)
    assert is_new
    assert db_task is not None
    db_task.pop("group", None)
    db_task = Task.parse_obj(db_task)
    assert db_task == task


async def test_save_task_with_different_ns_should_fail(
    fs_storage: TestableFSKeyValueStorage, hello_world_task: Task
):
    # Given
    task = hello_world_task
    await fs_storage.save_task_(task, None)
    # When
    msg = re.escape("DB task group (None) differs from save task group: some-group")
    with pytest.raises(ValueError, match=msg):
        await fs_storage.save_task_(task, "some-group")


async def test_save_task_should_not_update_non_updatable_field(
    fs_storage: TestableFSKeyValueStorage, hello_world_task: Task
):
    # Given
    task = hello_world_task
    await fs_storage.save_task_(task, None)
    updates = {
        "name": "another-type",
        "created_at": datetime.now(),
        "args": {"other": "input"},
    }
    updated = safe_copy(task, update=updates)
    # When
    is_new = await fs_storage.save_task_(updated, None)
    assert not is_new
    stored = await fs_storage.get_task(task.id)
    # Then
    assert stored == task


async def test_save_result(fs_storage: TestableFSKeyValueStorage):
    # Given
    task = task_1()
    await fs_storage.save_task_(task, None)
    result = ResultEvent(
        task_id="task-1",
        result=TaskResult(value="some_result"),
        created_at=datetime.now(),
    )
    result_db = _make_db(fs_storage.db_path, table_name="results")
    # When
    await fs_storage.save_result(result)
    # Then
    with result_db:
        db_result = result_db.get(result.task_id)
    assert db_result is not None
    db_result = ResultEvent.parse_obj(db_result)
    assert db_result == result


async def test_save_error(fs_storage: TestableFSKeyValueStorage):
    # Give
    error = TaskError(
        name="error",
        message="with details",
        stacktrace=[StacktraceItem(name="SomeError", file="somefile", lineno=666)],
        cause="some cause",
    )
    created_at = datetime.now()
    error_event = ErrorEvent(
        task_id="some-id", error=error, retries_left=3, created_at=created_at
    )

    db = _make_db(fs_storage.db_path, table_name="errors")
    # When
    await fs_storage.save_error(error_event)
    # Then
    with db:
        db_errors = db.get(error_event.task_id)
    assert db_errors is not None
    db_errors = [ErrorEvent.parse_obj(err) for err in db_errors]
    assert db_errors == [error_event]


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
    "group,task_name,state,expected_tasks",
    [
        (None, None, None, [task_0(), task_1()]),
        ("some-group", None, None, [task_0()]),
        (None, "task-type-1", None, [task_1()]),
        (None, None, TaskState.CREATED, [task_0()]),
    ],
)
async def test_get_tasks(
    fs_storage: TestableFSKeyValueStorage,
    populate_tasks: List[Task],  # pylint: disable=unused-argument
    group: Optional[str],
    task_name: Optional[str],
    state: Optional[Union[List[TaskState], TaskState]],
    expected_tasks: List[Task],
):
    # Given
    storage = fs_storage
    # When
    tasks = await storage.get_tasks(group=group, task_name=task_name, state=state)
    # Then
    assert tasks == expected_tasks


async def test_get_result(fs_storage: TestableFSKeyValueStorage):
    # Given
    storage = fs_storage
    db = _make_db(fs_storage.db_path, table_name="results")
    res = ResultEvent(
        task_id="some-id",
        result=TaskResult(value="Hello world !"),
        created_at=datetime.now(),
    )
    with db:
        db["task-0"] = res.dict()
        db.commit()
    # When
    result = await storage.get_task_result(task_id="task-0")
    # Then
    assert result == res


async def test_get_error(fs_storage: TestableFSKeyValueStorage):
    # Given
    store = fs_storage
    db = _make_db(fs_storage.db_path, table_name="errors")
    err = ErrorEvent(
        task_id="task-0",
        error=TaskError(
            name="some-error",
            message="some message",
            stacktrace=[
                StacktraceItem(name="SomeError", file="some details", lineno=666)
            ],
        ),
        retries_left=3,
        created_at=datetime.now(),
    )
    with db:
        db["task-0"] = [err.dict()]
        db.commit()
    # When
    db_errors = await store.get_task_errors(task_id="task-0")
    # Then
    assert db_errors == [err]


async def test_get_health(fs_storage: TestableFSKeyValueStorage):
    # When
    async with fs_storage:
        health = await fs_storage.get_health()
    # Then
    assert health


async def test_get_health_should_fail(
    monkeypatch,
    fs_storage: TestableFSKeyValueStorage,
):
    # Given
    def _failing_len(self):
        raise OSError("failing...")

    monkeypatch.setattr("icij_worker.task_storage.fs.SqliteDict.__len__", _failing_len)
    # When
    async with fs_storage:
        health = await fs_storage.get_health()
    # Then
    assert not health
