from typing import ClassVar

from pydantic import Discriminator, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

import icij_worker
from icij_common.pydantic_utils import (
    icij_config,
    make_enum_discriminator,
    merge_configs,
    tagged_union,
)
from icij_worker import AsyncBackend, TaskManager
from icij_worker.utils.logging_ import LogWithWorkerIDMixin

_PLACEHOLDER = ""
_ALL_LOGGERS = [icij_worker.__name__]


def _get_available_tm_configs() -> tuple:
    try:
        from icij_worker.task_manager.neo4j_ import Neo4JTaskManagerConfig
    except ImportError:
        Neo4JTaskManagerConfig = None
    try:
        from icij_worker.task_manager.amqp import AMQPTaskManagerConfig
    except ImportError:
        AMQPTaskManagerConfig = None

    configs = [AMQPTaskManagerConfig, Neo4JTaskManagerConfig]
    configs = tuple(c for c in configs if c is not None)
    return configs


TMConfig = _get_available_tm_configs()
tm_config_discriminator = None
if len(TMConfig) > 1:
    TMConfig = tagged_union(TMConfig, lambda x: x.backend.default)
    tm_config_discriminator = Discriminator(
        make_enum_discriminator("backend", AsyncBackend)
    )


class HttpServiceConfig(BaseSettings, LogWithWorkerIDMixin):
    model_config = merge_configs(
        icij_config(),
        SettingsConfigDict(
            env_prefix=_PLACEHOLDER, env_nested_delimiter="__", validate_default=True
        ),
    )

    app_title: str = "HTTP service"
    loggers: ClassVar[list[str]] = Field(
        _ALL_LOGGERS + ["uvicorn", "__main__"], frozen=True
    )
    host: str = "localhost"
    log_level: str = Field(default="INFO")
    n_workers: int = 1
    port: int = 8080

    task_manager: TMConfig = Field(discriminator=tm_config_discriminator)

    def to_task_manager(self) -> TaskManager:
        return TaskManager.from_config(self.task_manager)
