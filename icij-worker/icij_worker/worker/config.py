from __future__ import annotations

from abc import ABC
from pathlib import Path
from typing import ClassVar

from pydantic import Field
from pydantic_settings import SettingsConfigDict

from icij_common.pydantic_utils import icij_config, merge_configs
from icij_common.registrable import RegistrableSettings
from icij_worker import AsyncBackend


class WorkerConfig(RegistrableSettings, ABC):
    model_config = merge_configs(
        icij_config(),
        SettingsConfigDict(env_prefix="ICIJ_WORKER_", env_nested_delimiter="_"),
    )

    registry_key: ClassVar[str] = Field(frozen=True, default="type")
    type: AsyncBackend = Field(frozen=True)

    # TODO: is app_dependencies_path better ?
    app_bootstrap_config_path: Path | None = None
    inactive_after_s: float | None = None
    log_level: str = "INFO"
    task_queue_poll_interval_s: float = 1.0
