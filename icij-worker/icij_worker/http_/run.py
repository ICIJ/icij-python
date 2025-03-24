from __future__ import annotations

import argparse
import logging
import sys
import traceback
from pathlib import Path

import uvicorn

import icij_worker
from icij_common.import_utils import import_variable
from icij_common.logging_utils import setup_loggers
from icij_worker.http_.config import HttpServiceConfig
from icij_worker.http_.service import create_service


class Formatter(argparse.ArgumentDefaultsHelpFormatter):
    def __init__(self, prog: str):
        super().__init__(prog, max_help_position=35, width=150)


def _start_app_(ns: argparse.Namespace) -> None:
    config_cls = None
    if ns.config_class is not None:
        config_cls = import_variable(ns.config_class)
    _start_app(config_cls=config_cls, config_path=ns.config_path)


def _start_app(
    config_cls: type[HttpServiceConfig] | None, config_path: str | None = None
) -> None:
    if config_cls is None:
        config_cls = HttpServiceConfig
    if config_path is not None:
        config = config_cls.model_validate_json(Path(config_path).read_text())
    else:
        config = config_cls()
    fast_api = create_service(config)
    log_level = logging.getLevelName(config.log_level)
    uvicorn.run(
        fast_api,
        host=config.host,
        port=config.port,
        workers=config.n_workers,
        log_level=log_level,
    )


def get_arg_parser() -> argparse.ArgumentParser:
    arg_parser = argparse.ArgumentParser(
        description="HTTP server start CLI", formatter_class=Formatter
    )
    arg_parser.add_argument("--config-class", type=str)
    arg_parser.add_argument("--config-path", type=str)
    arg_parser.set_defaults(func=_start_app_)
    return arg_parser


def main() -> None:
    # Setup loggers temporarily before loggers init using the app configuration
    setup_loggers(["__main__", icij_worker.__name__])
    logger = logging.getLogger(__name__)
    try:
        arg_parser = get_arg_parser()
        args = arg_parser.parse_args()

        if hasattr(args, "func"):
            args.func(args)
        else:
            arg_parser.print_help()
            sys.exit(1)
    except KeyboardInterrupt as e:
        logger.error("Application shutdown...")
        raise e
    except Exception as e:
        error_with_trace = "".join(traceback.format_exception(None, e, e.__traceback__))
        logger.error("Error occurred at application startup:\n%s", error_with_trace)
        raise e
