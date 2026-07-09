import os
from pydantic_settings import SettingsConfigDict

from icij_common.pydantic_utils import merge_configs
from icij_worker import AMQPTaskManagerConfig
from icij_worker.http_.config import HttpServiceConfig


class _MockServiceConfig(HttpServiceConfig):
    model_config = merge_configs(
        HttpServiceConfig.model_config,
        SettingsConfigDict(
            env_prefix="MY_SERVICE_", env_nested_delimiter="__", validate_default=True
        ),
    )


def test_http_config_from_env(reset_env) -> None:  # noqa: ANN001, ARG001
    # Given
    env = {
        "MY_SERVICE_TASK_MANAGER__BACKEND": "amqp",
        "MY_SERVICE_TASK_MANAGER__STORAGE__MAX_CONNECTIONS": "28",
        "MY_SERVICE_TASK_MANAGER__APP_PATH": "some_path",
    }
    os.environ.update(env)
    # When
    config = _MockServiceConfig()
    # Then
    assert isinstance(config.task_manager, AMQPTaskManagerConfig)
