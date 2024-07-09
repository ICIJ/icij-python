# pylint: disable=redefined-outer-name
import pytest

from icij_worker import AsyncApp, Namespacing


class DummyNamespacing(Namespacing):

    def should_run_task(self, task_key: str) -> bool:
        return "running" in task_key


@pytest.fixture(scope="session")
def namespaced_app() -> AsyncApp:
    app = AsyncApp("namespaced-app")

    @app.task(key="i_m_running")
    def running_hello_world():
        return "I'm running"

    @app.task(key="i_m_filtered")
    def filtered_hello_world():
        return "I'm not running"

    return app


def test_filter_tasks(namespaced_app: AsyncApp):
    # Given
    app = namespaced_app
    namespacing = DummyNamespacing()

    # When
    app = namespaced_app.with_namespacing(namespacing).filter_tasks()

    # Then
    expected_keys = ["i_m_running"]
    assert app.registered_keys == expected_keys
