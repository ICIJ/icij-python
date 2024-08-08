from __future__ import annotations

import functools
import logging
from contextlib import asynccontextmanager
from copy import deepcopy
from inspect import iscoroutinefunction, signature
from typing import Callable, Dict, List, Optional, Tuple, Type, final

from icij_common.pydantic_utils import ICIJModel, ICIJSettings
from icij_worker.namespacing import Namespacing
from icij_worker.typing_ import Dependency
from icij_worker.utils import run_deps
from icij_worker.utils.imports import import_variable

logger = logging.getLogger(__name__)

PROGRESS_HANDLER_ARG = "progress"


class AsyncAppConfig(ICIJSettings):
    late_ack: bool = False
    max_task_queue_size: Optional[int] = None

    class Config:
        env_prefix = "ICIJ_APP_"


class RegisteredTask(ICIJModel):
    task: Callable
    recover_from: Tuple[Type[Exception], ...] = tuple()
    max_retries: Optional[int]
    namespace: Optional[str]


class AsyncApp:
    def __init__(
        self,
        name: str,
        config: AsyncAppConfig = None,
        dependencies: Optional[List[Dependency]] = None,
        namespacing: Optional[Namespacing] = None,
    ):
        self._name = name
        if config is None:
            config = AsyncAppConfig()  # This will load from the env
        self._config = config
        self._registry = dict()
        if dependencies is None:
            dependencies = []
        self._dependencies = dependencies
        if namespacing is None:
            namespacing = Namespacing()
        self._namespacing = namespacing

    @property
    def config(self) -> AsyncAppConfig:
        return self._config

    def with_config(self, value: AsyncAppConfig) -> AsyncApp:
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
    def namespacing(self) -> Namespacing:
        return self._namespacing

    def with_namespacing(self, ns: Namespacing) -> AsyncApp:
        self._namespacing = ns
        return self

    def task(
        self,
        name: Optional[str] = None,
        recover_from: Tuple[Type[Exception]] = tuple(),
        max_retries: Optional[int] = None,
        *,
        namespace: Optional[str] = None,
    ) -> Callable:
        if callable(name) and not recover_from and max_retries is None:
            f = name
            return functools.partial(
                self._register_task, name=f.__name__, namespace=namespace
            )(f)
        if max_retries is None:
            max_retries = 3
        return functools.partial(
            self._register_task,
            name=name,
            recover_from=recover_from,
            max_retries=max_retries,
            namespace=namespace,
        )

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
        namespace: Optional[str],
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
        self._registry[name] = RegisteredTask(
            task=f,
            max_retries=max_retries,
            recover_from=recover_from,
            namespace=namespace,
        )

        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    @classmethod
    def load(cls, app_path: str, config: Optional[AsyncAppConfig] = None) -> AsyncApp:
        app = deepcopy(import_variable(app_path))
        if config is not None:
            app.with_config(config)
        return app

    def filter_tasks(self, namespace: str) -> AsyncApp:
        kept = {
            t_name
            for t_name, t in self._registry.items()
            if self._namespacing.app_tasks_filter(
                task_namespace=t.namespace, app_namespace=namespace
            )
        }
        discarded = set(self._registry) - kept
        logger.info(
            "Applied namespace filtering:\n- running: %s\n- discarded: %s",
            ", ".join(sorted(kept)),
            ", ".join(sorted(discarded)),
        )
        self._registry = {k: self._registry[k] for k in kept}
        return self

    def __deepcopy__(self, memodict={}) -> AsyncApp:
        # pylint: disable=dangerous-default-value
        app = AsyncApp(
            name=self.name,
            config=deepcopy(self.config),
            dependencies=list(self._dependencies),
            namespacing=self.namespacing,
        )
        app._registry = deepcopy(self._registry)
        return app


def supports_progress(task_fn) -> bool:
    return any(
        param.name == PROGRESS_HANDLER_ARG
        for param in signature(task_fn).parameters.values()
    )
