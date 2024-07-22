from __future__ import annotations

from abc import ABC
from pathlib import Path
from typing import ClassVar, Optional, Union

from pydantic import Field, Protocol
from pydantic.parse import load_file

from icij_worker.utils.registrable import RegistrableConfig


class WorkerConfig(RegistrableConfig, ABC):
    registry_key: ClassVar[str] = Field(const=True, default="type")

    # TODO: is app_dependencies_path better ?
    app_bootstrap_config_path: Optional[Path] = None
    inactive_after_s: Optional[float] = None
    log_level: str = "INFO"
    task_queue_poll_interval_s: float = 1.0
    type: ClassVar[str]

    class Config:
        env_prefix = "ICIJ_WORKER_"

    @classmethod
    def parse_file(
        cls: WorkerConfig,
        path: Union[str, Path],
        *,
        content_type: str = None,
        encoding: str = "utf8",
        proto: Protocol = None,
        allow_pickle: bool = False,
    ) -> WorkerConfig:
        obj = load_file(
            path,
            proto=proto,
            content_type=content_type,
            encoding=encoding,
            allow_pickle=allow_pickle,
            json_loads=cls.__config__.json_loads,
        )
        worker_type = obj.pop(WorkerConfig.registry_key.default)
        subcls = WorkerConfig.resolve_class_name(worker_type)
        return subcls(**obj)
