from __future__ import annotations

import inspect
from functools import cached_property
from graphlib import TopologicalSorter
from itertools import chain, repeat
from typing import Dict, List, Optional, Set

from icij_worker.app import AsyncApp
from icij_worker.objects import Task


class TaskDAG(TopologicalSorter):
    def __init__(self, dag_task_id: str, graph: Optional[Dict] = None):
        super().__init__(graph)
        self._dag_task_id = dag_task_id
        self._start_nodes = None
        self._arg_providers: Dict[str, Dict[str, str]] = dict()
        self._dag_tasks: Optional[Set[Task]] = None

    @property
    def arg_providers(self) -> Dict[str, Dict[str, str]]:
        return self._arg_providers

    @classmethod
    def from_app(cls, dag_task: Task, app: AsyncApp) -> TaskDAG:
        dag = cls(dag_task.id)
        dag._dag_tasks = set()
        find_deps = [dag_task]
        parent_tasks = dict()
        while dependencies := list(
            chain(
                *(
                    zip(app.registry[t.name].parents.items(), repeat(t))
                    for t in find_deps
                )
            )
        ):
            find_deps = []
            for (provided_arg, parent), child in dependencies:
                parent_task = parent_tasks.get(parent)
                if parent_task is None:
                    parent_task = Task.from_parent(
                        dag_task, name=parent.__name__
                    ).with_max_retries(app)
                    parent_tasks[parent] = parent_task
                dag.add_task_dep(
                    child.id, parent=parent_task, provided_arg=provided_arg
                )
                find_deps.append(parent_task)
        _validate_args(dag, dag_task, app)
        return dag

    def add_task_dep(self, child_id: str, *, parent: Task, provided_arg: str):
        super().add(child_id, parent.id)
        if child_id not in self._arg_providers:
            self._arg_providers[child_id] = dict()
        self._arg_providers[child_id][provided_arg] = parent.id
        self._dag_tasks.add(parent)

    @property
    def created_tasks(self) -> Set[Task]:
        if self._dag_tasks is None:
            raise ValueError(
                f"only DAG created with {self.from_app.__name__} have created tasks"
            )
        return self._dag_tasks

    @cached_property
    def dag_task_id(self) -> str:
        return self._dag_task_id

    @cached_property
    def start_nodes(self) -> List:
        if self._start_nodes is None:
            raise ValueError("call to prepare() is needed to find start nodes")
        return self._start_nodes

    def successors(self, task_id) -> Set[str]:
        return set(self._node2info[task_id].successors)

    def prepare(self) -> None:
        super().prepare()
        self._check_single_final_state()
        self._start_nodes = [
            i.node for i in self._node2info.values() if not i.npredecessors
        ]

    def _check_single_final_state(self) -> None:
        end_states = [i.node for i in self._node2info.values() if not i.successors]
        if len(end_states) > 1:
            msg = f"Found several final states {sorted(end_states)}"
            raise ValueError(msg)

    def __iter__(self):
        yield from self._node2info

    def get_argument_provider(self, task_id: str, *, argument_name: str) -> str:
        return self._arg_providers[task_id][argument_name]


def _validate_args(dag: TaskDAG, dag_task: Task, app: AsyncApp):
    missing = []
    provided_as_input = set(dag_task.arguments)
    extra_inputs = set(provided_as_input)
    extra_inputs -= set(_get_task_params(app, dag_task))
    for t in dag.created_tasks:
        params = _get_task_params(app, t)
        expected_provided_by_others = params - provided_as_input
        provided_by_others = set(dag.arg_providers.get(t.id, []))
        missing_for_t = expected_provided_by_others - provided_by_others
        if missing_for_t:
            missing.append((t.name, sorted(missing_for_t)))
        provided_as_input_consumed_by_t = params - provided_by_others
        extra_inputs -= provided_as_input_consumed_by_t
    if missing or extra_inputs:
        msg = ""
        if missing:
            msg += "Missing arguments:\n- "
            msg += "\n- ".join(
                f'{t_id}: {"".join(sorted(missing_args))}'
                for t_id, missing_args in missing
            )
            msg += "\n"
        if extra_inputs:
            msg += (
                f"Arguments {sorted(extra_inputs)} were provided as input but are"
                f" not used by DAG tasks"
            )
        raise ValueError(msg)


def _get_task_params(app, t):
    task_fn = app.registry[t.name].task
    params = inspect.signature(task_fn).parameters.values()
    params = set(p.name for p in params if p.name != "progress")
    return params
