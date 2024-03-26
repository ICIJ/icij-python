# pylint: disable=redefined-outer-name
from __future__ import annotations

from pathlib import Path

import pytest

from icij_worker import AsyncApp

# noinspection PyUnresolvedReferences
from icij_worker.utils.tests import (  # pylint: disable=unused-import
    MockWorker,
    mock_db,
    mock_db_session,
)


@pytest.fixture(scope="module")
def test_app() -> AsyncApp:
    app = AsyncApp(name="test-app", dependencies=[])

    @app.task
    async def hello_word(greeted: str):
        return f"hello {greeted}"

    return app


@pytest.fixture(scope="function")
def mock_worker(test_async_app: AsyncApp, mock_db: Path) -> MockWorker:
    worker = MockWorker(
        test_async_app,
        "test-worker",
        mock_db,
        task_queue_poll_interval_s=0.1,
        teardown_dependencies=False,
    )
    return worker
