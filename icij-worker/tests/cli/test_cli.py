import pytest
from packaging.version import Version
from typer.testing import CliRunner

from icij_common.test_utils import fail_if_exception
from icij_worker.cli import cli_app


@pytest.mark.parametrize("help_command", ["-h", "--help"])
def test_help(cli_runner: CliRunner, help_command: str):
    result = cli_runner.invoke(cli_app, [help_command])
    assert result.exit_code == 0
    output = result.stdout
    assert "icij-worker [OPTIONS] COMMAND [ARGS]..." in output
    assert "--version" in output


def test_version(cli_runner: CliRunner):
    result = cli_runner.invoke(cli_app, ["--version"])
    assert result.exit_code == 0
    version = result.stdout
    with fail_if_exception(f"CLI app returned an invalid version: {version}"):
        Version(version)
