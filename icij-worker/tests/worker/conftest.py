# pylint: disable=redefined-outer-name
from __future__ import annotations


import pytest

from icij_worker import AsyncApp, RoutingStrategy

# noinspection PyUnresolvedReferences
from icij_worker.utils.tests import MockWorker  # pylint: disable=unused-import


def make_app(routing_strategy: RoutingStrategy | None = None):
    app = AsyncApp(name="test-app", dependencies=[], routing_strategy=routing_strategy)

    @app.task
    async def hello_word(greeted: str):
        return f"hello {greeted}"

    return app


@pytest.fixture(scope="module")
def test_app() -> AsyncApp:
    return make_app()
