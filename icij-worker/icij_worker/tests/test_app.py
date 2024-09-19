# pylint: disable=redefined-outer-name
from typing import List

import pytest

from icij_worker import AsyncApp, RoutingStrategy


class DummyNamespacing(RoutingStrategy):

    def app_tasks_filter(self, *, task_group: str, app_group: str) -> bool:
        return task_group.endswith(app_group)


@pytest.fixture()
def grouped_app() -> AsyncApp:
    app = AsyncApp("grouped-app")

    @app.task(group="grouped-a")
    def i_m_a():
        return "I'm a"

    @app.task(group="grouped-b")
    def i_m_b():
        return "I'm b"

    return app


@pytest.mark.parametrize(
    "group,expected_keys",
    [("", ["i_m_a", "i_m_b"]), ("a", ["i_m_a"]), ("b", ["i_m_b"])],
)
def test_filter_tasks(grouped_app: AsyncApp, group: str, expected_keys: List[str]):
    # Given
    app = grouped_app
    namespacing = DummyNamespacing()

    # When
    app = app.with_routing_strategy(namespacing).filter_tasks(group)

    # Then
    assert app.registered_keys == expected_keys
