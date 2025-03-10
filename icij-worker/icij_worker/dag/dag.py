from __future__ import annotations

from collections import Counter, defaultdict, deque, namedtuple

import inspect
from copy import deepcopy
from dataclasses import dataclass, field
from functools import cached_property
from graphlib import CycleError, TopologicalSorter
from itertools import chain, repeat
from typing import Any

from icij_worker.app import AsyncApp
from icij_worker.exceptions import UnknownTask
from icij_worker.objects import Task


class NodeInfo:  # Copy of _NodeInfo by with __eq__ implemented for tests
    __slots__ = "node", "npredecessors", "successors"

    def __init__(self, node):
        self.node = node
        self.npredecessors = 0
        self.successors = []

    def __eq__(self, other) -> bool:
        if not isinstance(other, NodeInfo):
            return False
        if self.node != other.node:
            return False
        if self.npredecessors != other.npredecessors:
            return False
        if self.successors != other.successors:
            return False
        return True


class TaskDAG(TopologicalSorter):
    def __init__(self, dag_task_id: str, graph: dict | None = None):
        super().__init__(graph)
        self._dag_task_id = dag_task_id
        self._start_nodes = None
        self._arg_providers: dict[str, dict[str, str]] = dict()
        self._dag_tasks: set[Task] | None = None

    def _get_nodeinfo(self, node):
        if (result := self._node2info.get(node)) is None:
            self._node2info[node] = result = NodeInfo(node)
        return result

    @property
    def arg_providers(self) -> dict[str, dict[str, str]]:
        return self._arg_providers

    @classmethod
    def from_app(cls, app: AsyncApp, dag_task: Task) -> TaskDAG:
        dag = cls(dag_task.id)
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
                    child.id, parent_id=parent_task.id, provided_arg=provided_arg
                )
                dag.add_created(parent_task)
                find_deps.append(parent_task)
        _validate_args(dag, dag_task, app)
        return dag

    def add_task_dep(self, child_id: str, *, parent_id: str, provided_arg: str):
        super().add(child_id, parent_id)
        if child_id not in self._arg_providers:
            self._arg_providers[child_id] = dict()
        self._arg_providers[child_id][provided_arg] = parent_id

    def add_created(self, task: Task):
        if self._dag_tasks is None:
            self._dag_tasks = set()
        self._dag_tasks.add(task)

    @property
    def created_tasks(self) -> set[Task]:
        if self._dag_tasks is None:
            raise ValueError(
                f"only DAG created with {self.from_app.__name__} have created tasks"
            )
        return self._dag_tasks

    @cached_property
    def dag_task_id(self) -> str:
        return self._dag_task_id

    @cached_property
    def start_nodes(self) -> list:
        if self._start_nodes is None:
            raise ValueError("call to prepare() is needed to find start nodes")
        return self._start_nodes

    def successors(self, task_id) -> set[str]:
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

    def __eq__(self, other) -> bool:
        if not isinstance(other, TaskDAG):
            return False
        if self.arg_providers != other.arg_providers:
            return False
        # Gain a bit of time before computing the heavy stuff
        if set(self._node2info.keys()) != set(other._node2info.keys()):
            return False
        left_node_2_info = deepcopy(self._node2info)
        right_node_2_info = deepcopy(other._node2info)
        for k, v in left_node_2_info.items():
            left_node_2_info[k].successors = sorted(v.successors)
        for k, v in right_node_2_info.items():
            right_node_2_info[k].successors = sorted(v.successors)
        if left_node_2_info != right_node_2_info:
            return False
        return True

    def __len__(self) -> int:
        return len(self._node2info)

    def get_argument_provider(self, task_id: str, *, argument_name: str) -> str:
        return self._arg_providers[task_id][argument_name]


def _validate_args(dag: TaskDAG, dag_task: Task, app: AsyncApp):
    missing = []
    provided_as_input = set(dag_task.args)
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


@dataclass(frozen=True)
class TaskNode:
    id: str
    args: dict[str, Any]
    parents: list[str] = field(default_factory=list)
    children: list[str] = field(default_factory=list)

    def is_root(self) -> bool:
        return not self.parents

    def is_leaf(self) -> bool:
        return not self.parents


ProvidedArg = namedtuple("ProvidedArg", ["name", "value"])


@dataclass(frozen=True)
class DependencyEdge:
    start: str
    end: str
    provided_arg: ProvidedArg
    args: dict[str, Any] | None = None


@dataclass(frozen=True)
class DAG:
    # TODO: we must be able to set some weights for progress
    single_leaf: bool = True
    _tasks: dict[str, TaskNode] = field(default_factory=dict)
    _dependencies: list[DependencyEdge] = field(default_factory=list)

    def get_task(self, task_id: str) -> TaskNode:
        node = self._tasks[task_id]
        return node

    def add_task(self, task: TaskNode) -> None:
        if task in self:
            raise ValueError(f"task {task.id} is already registered in the graph")
        self._tasks[task.id] = task

    def add_task_dependency(self, dependency: DependencyEdge) -> None:
        self._validate_dep(dependency.start, end=dependency.end)
        self._dependencies.append(dependency)
        self._tasks[dependency.end].parents.append(dependency.start)
        self._tasks[dependency.start].children.append(dependency.end)

    def updated_task_args(self, task_id: str, arg: ProvidedArg) -> None:
        if task_id not in self:
            raise ValueError(f"task {task_id} is not in the graph")
        updated = self._tasks[task_id]
        args = updated.args.copy()
        args[arg.name] = arg.value
        new = TaskNode(
            id=updated.id, args=args, parents=updated.parents, children=updated.children
        )
        self._tasks[new.id] = new

    def leafs(self) -> list[TaskNode]:
        leafs = [r for r in self._tasks.values() if r.is_leaf()]
        return leafs

    def next_dependencies(self, task_id: str) -> list[DependencyEdge]:
        res = []
        for edge in self._dependencies:
            if edge.start == task_id:
                res.append(edge)
        return res

    def previous_dependencies(self, task_id: str) -> list[DependencyEdge]:
        res = []
        for edge in self._dependencies:
            if edge.end == task_id:
                res.append(edge)
        return res

    def validate(
        self, inputs: dict[str, Any], expected_args: dict[str, set[str]]
    ) -> None:
        # TODO: requiring the expected_args is not robust as we don't know how the
        #  user will provide them...
        cycle = self._find_cycles()
        if cycle:
            raise CycleError("tasks are in a cycle", cycle)
        if self.single_leaf and len(self.leafs()) > 1:
            raise ValueError("expected a single final task")
        self._validate_inputs(inputs, expected_args=expected_args)

    def _validate_inputs(
        self, inputs: dict[str, Any], *, expected_args: dict[str, set[str]]
    ) -> None:
        missing = []
        duplicates = []
        provided_as_input = set(inputs)
        extra_inputs = set(provided_as_input)
        deps_by_child = defaultdict(list)
        for dep in self._dependencies:
            deps_by_child[dep.end].append(dep)
        for t in self._tasks.values():
            expected = expected_args[t.id]
            expected_provided_by_parents = expected - provided_as_input
            provided_by_parents = [dep.provided_arg for dep in deps_by_child[t.id]]
            dups = [k for k, v in Counter(provided_by_parents) if v > 1]
            duplicates.append((t.id, dups))
            provided_by_parents = set(provided_by_parents)
            missing_for_t = expected_provided_by_parents - set(provided_by_parents)
            if missing_for_t:
                missing.append((t.id, sorted(missing_for_t)))
            provided_as_input_consumed_by_t = expected - provided_by_parents
            extra_inputs -= provided_as_input_consumed_by_t
        if missing or extra_inputs or duplicates:
            msg = _format_invalid_dag_error(
                missing, extra_inputs=extra_inputs, duplicates=duplicates
            )
            raise ValueError(msg)

    def _validate_dep(self, start: str, *, end: str) -> None:
        if start not in self:
            raise UnknownTask(start)
        if end not in self:
            raise UnknownTask(end)
        for edge in self._dependencies:
            if edge.start == start and edge.end == end:
                raise ValueError(f"{start} and {end} are already connected")

    def __contains__(self, task: TaskNode | str) -> bool:
        if isinstance(task, str):
            return task in self._tasks
        return task.id in self._tasks

    def _dfs(self, visited: set[str], task_id: str) -> deque[str]:
        if task_id in visited:
            path = deque()
            path.append(task_id)
            return path
        for edge in self.next_dependencies(task_id):
            path = self._dfs(visited | {task_id}, edge.end)
            if path:
                path.appendleft(task_id)
                return path
        return deque()

    def _find_cycles(self) -> list[list[str]]:
        cycles = (list(self._dfs(set(), t)) for t in self._tasks)
        cycles = [c for c in cycles if c]
        return cycles


def _format_invalid_dag_error(
    missing: list[tuple[str, list[str]]],
    *,
    duplicates: list[tuple[str, list[str]]],
    extra_inputs: list[tuple[str, list[str]]],
):
    msg = ""
    if missing:
        msg += "Missing arguments:\n- "
        msg += "\n- ".join(
            f'{t_id}: {"".join(sorted(missing_args))}' for t_id, missing_args in missing
        )
        msg += "\n"
    if duplicates:
        msg += "Argument provided by several tasks:\n- "
        msg += "\n- ".join(
            f'{t_id}: {"".join(sorted(dup_args))}' for t_id, dup_args in duplicates
        )
        msg += "\n"
    if extra_inputs:
        msg += (
            f"Arguments {sorted(extra_inputs)} were provided as input but are"
            f" not used by DAG tasks"
        )
    return msg
