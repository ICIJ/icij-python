import functools
import logging
from contextlib import asynccontextmanager
from copy import deepcopy
from inspect import iscoroutinefunction, signature
from typing import Callable, Dict, List, Optional, Tuple, Type, Union, final

from pydantic import validator
from typing_extensions import Self

from icij_common.pydantic_utils import ICIJModel, ICIJSettings
from icij_worker.routing_strategy import RoutingStrategy
from icij_worker.typing_ import Dependency
from icij_worker.utils import run_deps
from icij_worker.utils.imports import import_variable

logger = logging.getLogger(__name__)

PROGRESS_HANDLER_ARG = "progress"


class TaskGroup(ICIJModel):
    name: str
    timeout_s: Optional[int] = None
    max_task_queue_size: Optional[int] = None


class AsyncAppConfig(ICIJSettings):
    late_ack: bool = True
    recover_from_worker_timeout: bool = False
    max_task_queue_size: Optional[int] = None

    class Config:
        env_prefix = "ICIJ_APP_"


class RegisteredTask(ICIJModel):
    task: Callable
    recover_from: Tuple[Type[Exception], ...] = tuple()
    max_retries: Optional[int]
    group: Optional[TaskGroup]

    @validator("group", pre=True)
    def validate_group_instance(cls, v):  # pylint: disable=no-self-argument
        if isinstance(v, str):
            v = TaskGroup(name=v)
        return v


class AsyncApp:
    def __init__(
        self,
        name: str,
        config: AsyncAppConfig = None,
        dependencies: Optional[List[Dependency]] = None,
        routing_strategy: Optional[RoutingStrategy] = None,
    ):
        self._name = name
        if config is None:
            config = AsyncAppConfig()  # This will load from the env
        self._config = config
        self._registry = dict()
        self._groups = dict()
        if dependencies is None:
            dependencies = []
        self._dependencies = dependencies
        if routing_strategy is None:
            routing_strategy = RoutingStrategy()
        self._routing_strategy = routing_strategy

    @property
    def config(self) -> AsyncAppConfig:
        return self._config

    def with_config(self, value: AsyncAppConfig) -> Self:
        if not isinstance(value, AsyncAppConfig):
            raise TypeError(f"Expected {AsyncAppConfig.__name__}, got {value}")
        self._config = value
        return self

    @property
    def registry(self) -> Dict[str, RegisteredTask]:
        return self._registry

    @property
    def registered_keys(self) -> List[str]:
        return sorted(self._registry)

    @functools.cached_property
    def name(self) -> str:
        return self._name

    @functools.cached_property
    def routing_strategy(self) -> RoutingStrategy:
        return self._routing_strategy

    def with_routing_strategy(self, ns: RoutingStrategy) -> Self:
        self._routing_strategy = ns
        return self

    def task(
        self,
        name: Optional[str] = None,
        recover_from: Tuple[Type[Exception]] = tuple(),
        max_retries: Optional[int] = None,
        *,
        group: Optional[Union[str, TaskGroup]] = None,
    ) -> Callable:
        if callable(name) and not recover_from and max_retries is None:
            f = name
            return functools.partial(self._register_task, name=f.__name__, group=group)(
                f
            )
        if max_retries is None:
            max_retries = 3
        return functools.partial(
            self._register_task,
            name=name,
            recover_from=recover_from,
            max_retries=max_retries,
            group=group,
        )

    def task_group(self, name: str) -> Optional[TaskGroup]:
        return self._groups.get(name)

    @property
    def task_groups(self) -> List[TaskGroup]:
        return list(self._groups.values())

    @final
    @asynccontextmanager
    async def lifetime_dependencies(self, **kwargs):
        ctx = f"{self.name} async app"
        async with run_deps(self._dependencies, ctx=ctx, **kwargs):
            yield

    def _register_task(
        self,
        f: Callable,
        *,
        name: Optional[str] = None,
        recover_from: Tuple[Type[Exception]] = tuple(),
        max_retries: Optional[int] = None,
        group: Optional[Union[str, TaskGroup]] = None,
    ) -> Callable:
        if not iscoroutinefunction(f) and supports_progress(f):
            msg = (
                f"{f} is not a coroutine, progress is not supported as progress"
                f" reporting is inherently async, turn your function task into a"
                f" coroutine if necessary and use `await progress(my_progress)`"
            )
            raise ValueError(msg)
        if name is None:
            name = f.__name__
        registered = self._registry.get(name)
        if registered is not None:
            raise ValueError(f'Task "{name}" is already registered: {registered}')
        registered = RegisteredTask(
            task=f, max_retries=max_retries, recover_from=recover_from, group=group
        )
        self._validate_group(registered)
        self._registry[name] = registered
        if registered.group is not None:
            self._groups[registered.group.name] = registered.group

        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    def _validate_group(self, task: RegisteredTask):
        if task.group is None:
            return
        existing = self._groups.get(task.group.name)
        if existing is not None and existing.name != task.group:
            msg = (
                f"invalid task group {task.group}, it has the same name as registered "
                f"group {existing}, use {existing} directly or specify a different name"
            )
            raise ValueError(msg)

    @classmethod
    def load(cls, app_path: str, config: Optional[AsyncAppConfig] = None) -> Self:
        app = deepcopy(import_variable(app_path))
        if config is not None:
            app.with_config(config)
        return app

    def filter_tasks(self, group: Optional[str]) -> Self:
        if group is None:
            return self
        kept = {
            t_name
            for t_name, t in self._registry.items()
            if self._routing_strategy.app_tasks_filter(
                task_group=t.group.name, app_group=group
            )
        }
        discarded = set(self._registry) - kept
        logger.info(
            "Applied group filtering:\n- running: %s\n- discarded: %s",
            ", ".join(sorted(kept)),
            ", ".join(sorted(discarded)),
        )
        self._registry = {k: self._registry[k] for k in kept}
        return self

    def __deepcopy__(self, memodict={}) -> Self:
        # pylint: disable=dangerous-default-value
        app = AsyncApp(
            name=self.name,
            config=deepcopy(self.config),
            dependencies=list(self._dependencies),
            routing_strategy=self.routing_strategy,
        )
        app._registry = deepcopy(self._registry)
        app._groups = deepcopy(self._groups)
        return app


def supports_progress(task_fn) -> bool:
    return any(
        param.name == PROGRESS_HANDLER_ARG
        for param in signature(task_fn).parameters.values()
    )
