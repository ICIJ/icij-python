from __future__ import annotations

from abc import ABC
from pathlib import Path
from typing import ClassVar, Optional

from pydantic import Field

from icij_worker.utils.registrable import RegistrableSettings


class WorkerConfig(RegistrableSettings, ABC):
    registry_key: ClassVar[str] = Field(const=True, default="type")

    # TODO: is app_dependencies_path better ?
    app_bootstrap_config_path: Optional[Path] = None
    inactive_after_s: Optional[float] = None
    log_level: str = "INFO"
    task_queue_poll_interval_s: float = 1.0
    type: ClassVar[str]

    class Config:
        env_prefix = "ICIJ_WORKER_"
