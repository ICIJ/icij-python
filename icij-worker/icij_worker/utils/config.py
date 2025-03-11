import os
from abc import ABC
from copy import deepcopy
from typing import ClassVar, Generic, TypeVar

from pydantic import BaseModel, field_validator
from pydantic.fields import FieldInfo

from icij_common.pydantic_utils import safe_copy
from icij_common.registrable import find_variable_loc_in_env
from icij_worker.task_manager import TaskManagerConfig
from pydantic_settings import BaseSettings, SettingsConfigDict

TM = TypeVar("TM", bound=TaskManagerConfig)  # pylint: disable=invalid-name


class SettingsWithTM(BaseSettings, BaseModel, Generic[TM], ABC):
    """Helper which lets you configure your app which wraps a task manager and read the
     app settings from env like this:

    MY_OTHER_APP_SETTING=ANOTHER_SETTING
    TASK_MANAGER__APP=icij_worker.utils.tests.APP
    TASK_MANAGER__BACKEND=amqp
    TASK_MANAGER__STORAGE__MAX_CONNECTIONS=28
    TASK_MANAGER__RABBITMQ_HOST=localhost
    TASK_MANAGER__RABBITMQ_PORT=15752
    """

    app_path: ClassVar[str]

    task_manager: TM
    model_config = SettingsConfigDict(
        env_prefix="", env_nested_delimiter="__", validate_default=True
    )

    @classmethod
    def from_env(cls):
        backend_field_name = TaskManagerConfig.registry_key.default
        tm_backend_env_key = (
            f"{cls.model_config['env_prefix']}task_manager"
            f"{cls.model_config['env_nested_delimiter']}{backend_field_name}"
        )
        backend_key, backend = find_variable_loc_in_env(
            tm_backend_env_key, cls.model_config["case_sensitive"]
        )
        tm_config_cls = TaskManagerConfig.resolve_class_name(backend)
        old_env = deepcopy(os.environ)
        try:
            os.environ.pop(backend_key)
            return cls[tm_config_cls]()
        finally:
            os.environ.clear()
            os.environ.update(old_env)

    @field_validator("task_manager", mode="before")
    def set_task_manager_app_path(cls, v: TM):
        # pylint: disable=no-self-argument
        app_path = cls.app_path
        if isinstance(app_path, FieldInfo):
            app_path = app_path.default
        if isinstance(v, dict):
            v["app_path"] = app_path
        elif isinstance(v, TaskManagerConfig):
            v = safe_copy(v, update={"app_path": app_path})
        return v

    def to_task_manager(self) -> "TaskManager":
        from icij_worker import TaskManager

        return TaskManager.from_config(self.task_manager)
