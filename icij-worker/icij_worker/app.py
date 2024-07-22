from __future__ import annotations

import functools
import logging
from contextlib import asynccontextmanager
from typing import Callable, Dict, List, Optional, Tuple, Type, final

from pydantic import Field

from icij_common.pydantic_utils import ICIJModel, ICIJSettings
from icij_worker.namespacing import Namespacing
from icij_worker.typing_ import Dependency
from icij_worker.utils import run_deps
from icij_worker.utils.imports import import_variable

logger = logging.getLogger(__name__)


class AsyncAppConfig(ICIJSettings):
    late_ack: bool = False
    max_task_queue_size: Optional[int] = None

    class Config:
        env_prefix = "ICIJ_APP_"


class RegisteredTask(ICIJModel):
    task: Callable
    recover_from: Tuple[Type[Exception], ...] = tuple()
    # TODO: enable max retries
    max_retries: Optional[int] = Field(const=True, default=None)


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
    def namespacing(self) -> Optional[Namespacing]:
        return self._namespacing

    def with_namespacing(self, ns: Namespacing) -> AsyncApp:
        self._namespacing = ns
        return self

    def task(
        self,
        key: Optional[str] = None,
        recover_from: Tuple[Type[Exception]] = tuple(),
        max_retries: Optional[int] = None,
    ) -> Callable:
        if callable(key) and not recover_from and max_retries is None:
            f = key
            return functools.partial(self._register_task, name=f.__name__)(f)
        return functools.partial(
            self._register_task,
            name=key,
            recover_from=recover_from,
            max_retries=max_retries,
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
    ) -> Callable:
        if name is None:
            name = f.__name__
        registered = self._registry.get(name)
        if registered is not None:
            raise ValueError(f'Task "{name}" is already registered: {registered}')
        self._registry[name] = RegisteredTask(
            task=f, max_retries=max_retries, recover_from=recover_from
        )

        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    @classmethod
    def load(cls, app_path: str, config: Optional[AsyncAppConfig] = None) -> AsyncApp:
        app = import_variable(app_path)
        if config is not None:
            app.with_config(config)
        return app

    def filter_tasks(self, namespace: str) -> AsyncApp:
        kept = {
            task_name
            for task_name in self._registry
            if self._namespacing.app_tasks_filter(
                task_key=task_name, app_namespace=namespace
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
