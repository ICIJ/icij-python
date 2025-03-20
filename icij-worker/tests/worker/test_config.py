# pylint: disable=redefined-outer-name
import os

import pytest
from pydantic import Field

from icij_common.test_utils import reset_env  # pylint: disable=unused-import
from icij_worker import WorkerConfig


@pytest.fixture()
def env_log_level(reset_env, request):  # pylint: disable=unused-argument
    log_level = request.param
    if log_level is not None:
        os.environ["ICIJ_WORKER_LOG_LEVEL"] = log_level


@WorkerConfig.register()
class WorkerImplConfig(WorkerConfig):
    type: str = Field(frozen=True, default="worker_impl")


@pytest.fixture()
def mock_worker_in_env(tmp_path, reset_env):  # pylint: disable=unused-argument
    os.environ["ICIJ_WORKER_TYPE"] = "worker_impl"
    os.environ["ICIJ_WORKER_DB_PATH"] = str(tmp_path / "mock-db.json")


@pytest.mark.parametrize(
    "env_log_level,expected_level",
    [(None, "INFO"), ("DEBUG", "DEBUG"), ("INFO", "INFO")],
    indirect=["env_log_level"],
)
def test_config_from_env(
    env_log_level: str | None,
    expected_level: str,
    # pylint: disable=unused-argument
    mock_worker_in_env,
):
    # pylint: disable=unused-argument
    # When
    config = WorkerConfig.from_env()
    # Then
    assert isinstance(config, WorkerImplConfig)
    assert config.log_level == expected_level
