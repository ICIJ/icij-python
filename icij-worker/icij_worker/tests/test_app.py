# pylint: disable=redefined-outer-name
from typing import List

import pytest

from icij_worker import AsyncApp, Namespacing


class DummyNamespacing(Namespacing):

    def app_tasks_filter(self, *, task_namespace: str, app_namespace: str) -> bool:
        return task_namespace.endswith(app_namespace)


@pytest.fixture()
def namespaced_app() -> AsyncApp:
    app = AsyncApp("namespaced-app")

    @app.task(namespace="namespaced-a")
    def i_m_a():
        return "I'm a"

    @app.task(namespace="namespaced-b")
    def i_m_b():
        return "I'm b"

    return app


@pytest.mark.parametrize(
    "namespace,expected_keys",
    [("", ["i_m_a", "i_m_b"]), ("a", ["i_m_a"]), ("b", ["i_m_b"])],
)
def test_filter_tasks(
    namespaced_app: AsyncApp, namespace: str, expected_keys: List[str]
):
    # Given
    app = namespaced_app
    namespacing = DummyNamespacing()

    # When
    app = app.with_namespacing(namespacing).filter_tasks(namespace)

    # Then
    assert app.registered_keys == expected_keys
