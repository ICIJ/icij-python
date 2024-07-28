# pylint: disable=redefined-outer-name
from __future__ import annotations

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
