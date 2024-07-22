# pylint: disable=redefined-outer-name
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pytest

from icij_worker import AsyncApp, Namespacing

# noinspection PyUnresolvedReferences
from icij_worker.utils.tests import (  # pylint: disable=unused-import
    MockWorker,
    mock_db,
    mock_db_session,
)


def make_app(namespacing: Optional[Namespacing] = None):
    app = AsyncApp(name="test-app", dependencies=[], namespacing=namespacing)

    @app.task
    async def hello_word(greeted: str):
        return f"hello {greeted}"

    return app


@pytest.fixture(scope="module")
def test_app() -> AsyncApp:
    return make_app()


@pytest.fixture(
    scope="function",
    params=[{"app": "test_async_app"}, {"app": "test_async_app_late"}],
)
def mock_worker(mock_db: Path, request) -> MockWorker:
    param = getattr(request, "param", dict())
    app = request.getfixturevalue(param.get("app"))
    worker = MockWorker(
        app,
        "test-worker",
        namespace=param.get("namespace"),
        db_path=mock_db,
        task_queue_poll_interval_s=0.1,
        teardown_dependencies=False,
    )
    return worker
