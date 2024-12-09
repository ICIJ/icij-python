import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock

from aiohttp.typedefs import StrOrURL

from icij_worker import Task, TaskState
from icij_worker.ds_task_client import DatashareTaskClient


async def test_ds_task_client_create_task(monkeypatch):
    # Given
    datashare_url = "http://some-url"
    task_name = "hello"
    task_id = f"{task_name}-{uuid.uuid4()}"
    args = {"greeted": "world"}
    group = "PYTHON"

    @asynccontextmanager
    async def _put_and_assert(_, url: StrOrURL, *, data: Any = None, **kwargs: Any):
        assert url == f"/api/task/{task_id}?group={group}"
        expected_task = {
            "@type": "Task",
            "id": task_id,
            "state": "CREATED",
            "name": "hello",
            "args": {"greeted": "world"},
        }
        expected_data = expected_task
        assert data is None
        json_data = kwargs.pop("json")
        assert not kwargs
        assert json_data == expected_data
        mocked_res = AsyncMock()
        mocked_res.json.return_value = {"taskId": task_id}
        yield mocked_res

    monkeypatch.setattr("icij_worker.utils.http.AiohttpClient._put", _put_and_assert)

    task_client = DatashareTaskClient(datashare_url)
    async with task_client:
        # When
        t_id = await task_client.create_task(task_name, args, id_=task_id, group=group)
    assert t_id == task_id


async def test_ds_task_client_get_task(monkeypatch):
    # Given
    datashare_url = "http://some-url"
    task_name = "hello"
    task_id = f"{task_name}-{uuid.uuid4()}"

    @asynccontextmanager
    async def _get_and_assert(
        _, url: StrOrURL, *, allow_redirects: bool = True, **kwargs: Any
    ):
        assert url == f"/api/task/{task_id}"
        task = {
            "@type": "Task",
            "id": task_id,
            "state": "CREATED",
            "createdAt": datetime.now(),
            "name": "hello",
            "args": {"greeted": "world"},
        }
        assert allow_redirects
        assert not kwargs
        mocked_res = AsyncMock()
        mocked_res.json.return_value = task
        yield mocked_res

    monkeypatch.setattr("icij_worker.utils.http.AiohttpClient._get", _get_and_assert)

    task_client = DatashareTaskClient(datashare_url)
    async with task_client:
        # When
        task = await task_client.get_task(task_id)
    assert isinstance(task, Task)


async def test_ds_task_client_get_task_state(monkeypatch):
    # Given
    datashare_url = "http://some-url"
    task_name = "hello"
    task_id = f"{task_name}-{uuid.uuid4()}"

    @asynccontextmanager
    async def _get_and_assert(
        _, url: StrOrURL, *, allow_redirects: bool = True, **kwargs: Any
    ):
        assert url == f"/api/task/{task_id}"
        task = {
            "@type": "Task",
            "id": task_id,
            "state": "DONE",
            "createdAt": datetime.now(),
            "completedAt": datetime.now(),
            "name": "hello",
            "args": {"greeted": "world"},
            "result": "hellow world",
        }
        assert allow_redirects
        assert not kwargs
        mocked_res = AsyncMock()
        mocked_res.json.return_value = task
        yield mocked_res

    monkeypatch.setattr("icij_worker.utils.http.AiohttpClient._get", _get_and_assert)

    task_client = DatashareTaskClient(datashare_url)
    async with task_client:
        # When
        res = await task_client.get_task_state(task_id)
    assert res == TaskState.DONE


async def test_ds_task_client_get_task_result(monkeypatch):
    # Given
    datashare_url = "http://some-url"
    task_name = "hello"
    task_id = f"{task_name}-{uuid.uuid4()}"

    @asynccontextmanager
    async def _get_and_assert(
        _, url: StrOrURL, *, allow_redirects: bool = True, **kwargs: Any
    ):
        assert url == f"/api/task/{task_id}"
        task = {
            "@type": "Task",
            "id": task_id,
            "state": "DONE",
            "createdAt": datetime.now(),
            "completedAt": datetime.now(),
            "name": "hello",
            "args": {"greeted": "world"},
            "result": "hellow world",
        }
        assert allow_redirects
        assert not kwargs
        mocked_res = AsyncMock()
        mocked_res.json.return_value = task
        yield mocked_res

    monkeypatch.setattr("icij_worker.utils.http.AiohttpClient._get", _get_and_assert)

    task_client = DatashareTaskClient(datashare_url)
    async with task_client:
        # When
        res = await task_client.get_task_result(task_id)
    assert res == "hellow world"
