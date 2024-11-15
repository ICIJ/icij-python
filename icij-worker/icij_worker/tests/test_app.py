# pylint: disable=redefined-outer-name
from typing import List, Optional

import pytest

from icij_worker import AsyncApp, RoutingStrategy
from icij_worker.app import TaskGroup


class DummyRouting(RoutingStrategy):

    def app_tasks_filter(
        self, *, task_group: Optional[TaskGroup], app_group_name: str
    ) -> bool:
        if task_group is None:
            return False
        return task_group.name.endswith(app_group_name)


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
    routing = DummyRouting()

    # When
    app = app.with_routing_strategy(routing).filter_tasks(group)

    # Then
    assert app.registered_keys == expected_keys
