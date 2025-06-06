# pylint: disable=redefined-outer-name
import pytest

from icij_common.test_utils import fail_if_exception
from icij_worker import AsyncApp, RoutingStrategy
from icij_worker.app import TaskGroup


class DummyRouting(RoutingStrategy):

    def app_tasks_filter(
        self, *, task_group: TaskGroup | None, app_group_name: str
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


def test_load_from_plugin():
    # Given
    # This is the name in the plugged app registry, see tests/test-plugin/pyproject.toml
    path = "plugged_app"
    # When
    app = AsyncApp.load(path)
    assert isinstance(app, AsyncApp)
    assert app.name == "plugged"


@pytest.mark.parametrize(
    "group,expected_keys",
    [("", ["i_m_a", "i_m_b"]), ("a", ["i_m_a"]), ("b", ["i_m_b"])],
)
def test_filter_tasks(grouped_app: AsyncApp, group: str, expected_keys: list[str]):
    # Given
    app = grouped_app
    routing = DummyRouting()

    # When
    app = app.with_routing_strategy(routing).filter_tasks(group)

    # Then
    assert app.registered_keys == expected_keys


def test_validate_group_name_should_not_raise():
    # Given
    app = AsyncApp("some-app")
    group = TaskGroup(name="some-group")
    with fail_if_exception("failed to register 2 tasks with the same group"):
        # When
        @app.task(group=group)
        def empty_a():
            return None

        # Then
        @app.task(group=group)
        def empty_b():
            return None
