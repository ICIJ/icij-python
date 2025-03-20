import importlib.metadata
import os
from typing import Annotated

import typer

import icij_worker
from icij_common.logging_utils import setup_loggers
from icij_worker.cli.tasks import task_app
from icij_worker.cli.utils import AsyncTyper
from icij_worker.cli.workers import worker_app

cli_app = AsyncTyper(context_settings={"help_option_names": ["-h", "--help"]})
cli_app.add_typer(worker_app)
cli_app.add_typer(task_app)


def version_callback(value: bool):
    if value:
        package_version = importlib.metadata.version(icij_worker.__name__)
        print(package_version)
        raise typer.Exit()


def pretty_exc_callback(value: bool):
    if not value:
        os.environ["_TYPER_STANDARD_TRACEBACK"] = "1"


@cli_app.callback(name="icij-worker")
def main(
    version: Annotated[  # pylint: disable=unused-argument
        bool | None,
        typer.Option(  # pylint: disable=unused-argument
            "--version", callback=version_callback, is_eager=True
        ),
    ] = None,
    pretty_exceptions: Annotated[  # pylint: disable=unused-argument
        bool,
        typer.Option(  # pylint: disable=unused-argument
            "--pretty-exceptions", callback=pretty_exc_callback, is_eager=True
        ),
    ] = False,
):
    """Python async worker pool CLI 🧑‍🏭"""
    setup_loggers(["__main__", icij_worker.__name__])
