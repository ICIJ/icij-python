import pytest
import typer
from typer.testing import CliRunner


@pytest.fixture(scope="session")
def cli_runner() -> CliRunner:
    # disable rich to avoid colors to show up in test string:
    # https://github.com/tiangolo/typer/pull/647#issuecomment-1868190451
    # rich and pytest capture don't play well together:
    # https://github.com/Textualize/rich/issues/317
    typer.core.rich = None
    return CliRunner(mix_stderr=False)
