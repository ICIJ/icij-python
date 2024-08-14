import re
from typing import Annotated, List, Optional

import pytest

from icij_worker import AsyncApp, Task
from icij_worker.app import Depends
from icij_worker.dag.dag import TaskDAG
from icij_worker.typing_ import RateProgress


def test_dag():
    # Given
    dag_task_id = "some-id"
    graph = {"f": {"g", "d"}, "d": {"b", "c"}, "c": {"a"}, "b": {"a"}}
    dag = TaskDAG(dag_task_id, graph=graph)
    # When/Then
    dag.prepare()
    expected_start_nodes = ["a", "g"]
    start_nodes = sorted(dag.start_nodes)
    assert start_nodes == expected_start_nodes


def test_dag_should_raise_when_several_final_states():
    # Given
    dag_task_id = "some-id"
    graph = {"d": {"c", "b"}, "b": {"a"}, "e": "c"}
    dag = TaskDAG(dag_task_id, graph=graph)
    # When
    expected = "final state"
    with pytest.raises(ValueError, match=expected):
        dag.prepare()


def test_dag_from_app(test_async_app: AsyncApp):
    # Given
    app = test_async_app
    task_id = "detect-id"
    arguments = {"model": "some-model", "documents": []}
    dag_task = Task.create(task_id=task_id, task_name="detect", arguments=arguments)
    # When
    dag = TaskDAG.from_app(dag_task, app)
    # Then
    assert len(dag.created_tasks) == 1
    child = next(iter(dag.created_tasks))
    assert child.id.startswith(f"dag-{dag_task.id}-preprocess-")
    assert child.created_at == dag_task.created_at
    assert child.max_retries == 5
    expected_providers = {"detect-id": {"preprocessed": child.id}}
    assert dag.arg_providers == expected_providers


def test_dag_from_app_should_cache_tasks():
    # Given
    app = AsyncApp("test-app")

    @app.task
    def a(a_input: str) -> str:
        return a_input + " a"

    @app.task
    def b(b_input: Annotated[str, Depends(on=a)]) -> str:
        return b_input + " b"

    @app.task
    def c(c_input: Annotated[str, Depends(on=a)]) -> str:
        return c_input + " c"

    @app.task
    def d(
        left_input: Annotated[str, Depends(on=b)],
        right_input: Annotated[str, Depends(on=c)],
    ) -> str:
        return f"{left_input} d {right_input}"

    dag_task = Task.create(
        task_id="d-task-id", task_name="d", arguments={"a_input": "dag_input"}
    )
    # When
    dag = TaskDAG.from_app(dag_task, app)
    # Then
    assert len(dag.created_tasks) == 3


def test_dag_validate_args_should_raise_for_missing_arg():
    # Given
    app = AsyncApp("test-app")

    @app.task(max_retries=5, progress_weight=3.0)
    async def preprocess(
        documents: List[str],
        progress: Optional[RateProgress] = None,
    ) -> List[str]:
        # pylint: disable=unused-argument
        return []

    @app.task
    def detect(
        preprocessed: Annotated[List[str], Depends(on=preprocess)],
        model: str,
    ) -> List[str]:
        # pylint: disable=unused-argument
        return []

    dag_task = Task.create(
        task_id="detect-task-id", task_name="detect", arguments=dict()
    )

    # When/Then
    match = """Missing arguments:
- preprocess: documents"""
    with pytest.raises(ValueError, match=match):
        _ = TaskDAG.from_app(dag_task, app)


def test_dag_validate_args_should_raise_for_extra_arg():
    # Given
    app = AsyncApp("test-app")

    @app.task(max_retries=5, progress_weight=3.0)
    async def preprocess(
        documents: List[str],
        progress: Optional[RateProgress] = None,
    ) -> List[str]:
        # pylint: disable=unused-argument
        return []

    @app.task
    def detect(
        preprocessed: Annotated[List[str], Depends(on=preprocess)],
        model: str,
    ) -> List[str]:
        # pylint: disable=unused-argument
        return []

    dag_task = Task.create(
        task_id="detect-task-id",
        task_name="detect",
        arguments={"extra": "some-extra", "documents": []},
    )

    # When/Then
    match = "Arguments ['extra'] were provided as input but are not used by DAG tasks"
    with pytest.raises(ValueError, match=re.escape(match)):
        _ = TaskDAG.from_app(dag_task, app)
