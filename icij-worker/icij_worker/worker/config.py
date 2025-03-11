from __future__ import annotations

from abc import ABC
from pathlib import Path
from typing import ClassVar

from pydantic import ConfigDict, Field

from icij_common.registrable import RegistrableSettings


class WorkerConfig(RegistrableSettings, ABC):
    model_config = ConfigDict(env_prefix="ICIJ_WORKER_")

    registry_key: ClassVar[str] = Field(frozen=True, default="type")

    # TODO: is app_dependencies_path better ?
    app_bootstrap_config_path: Path | None = None
    inactive_after_s: float | None = None
    log_level: str = "INFO"
    task_queue_poll_interval_s: float = 1.0
    type: ClassVar[str]
