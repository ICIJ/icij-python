# pylint: disable=redefined-outer-name
import os

import pytest
from typer.testing import CliRunner

from icij_worker.cli import cli_app


@pytest.mark.parametrize("help_command", ["-h", "--help"])
def test_workers_help(cli_runner: CliRunner, help_command: str):
    # When
    result = cli_runner.invoke(
        cli_app, ["workers", help_command], catch_exceptions=False
    )
    # Then
    assert result.exit_code == 0
    output = result.stdout
    assert "icij-worker workers [OPTIONS] COMMAND [ARGS]..." in output
    assert "start  Start a pool of workers running the provided app" in output


@pytest.fixture()
def mock_worker_in_env(tmp_path):  # pylint: disable=unused-argument
    os.environ["ICIJ_WORKER_TYPE"] = "mock"
    db_path = tmp_path
    os.environ["ICIJ_WORKER_DB_PATH"] = str(db_path)


def test_workers_start(
    mock_worker_in_env,  # pylint: disable=unused-argument
    cli_runner: CliRunner,
    capsys,
):
    # Working around Pytest log capture and cli runner log capture
    with capsys.disabled():
        # Given
        test_app = "icij_worker.utils.tests.APP"
        # When
        result = cli_runner.invoke(cli_app, ["workers", "start", test_app])
        # Then
        # Here the program will fail because the DB for the worker is not initialized,
        # since the CLI is running forever, launching a failing worker enables returning
        # and not hanging forever. Another option would have been to use different
        # threads here
        assert "starting 1 worker for app icij_worker.utils.tests.APP" in result.stderr


@pytest.mark.parametrize("help_command", ["-h", "--help"])
def test_workers_start_help(cli_runner: CliRunner, help_command: str):
    # When
    result = cli_runner.invoke(cli_app, ["workers", "start", help_command])
    # Then
    assert result.exit_code == 0
    output = result.stdout
    assert "icij-worker workers start [OPTIONS] APP" in output
    assert "-n" in output
    assert "-n-workers" in output
    assert "-c" in output
    assert "--config" in output
    assert " --backend" in output
