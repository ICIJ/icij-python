import contextlib
import logging
import sys
from abc import ABC
from collections.abc import Callable, Generator
from datetime import UTC, datetime
from functools import wraps
from typing import final

TRACE = 5
logging.TRACE = TRACE
logging.addLevelName(logging.TRACE, "TRACE")
logging.Logger.trace = lambda inst, msg, *args, **kwargs: inst.log(
    logging.TRACE, msg, *args, **kwargs
)


class DifferedLoggingMessage:
    def __init__(self, fn: Callable, *args, **kwargs):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        return str(self.fn(*self.args, **self.kwargs))


class LogWithNameMixin(ABC):  # noqa: B024
    def __init__(self, logger: logging.Logger):
        self._logger = logger

    @final
    def info(self, msg: str, *args: object, **kwargs: object) -> None:
        self._logger.info(msg, *args, **kwargs)

    @final
    def debug(self, msg: str, *args, **kwargs) -> None:
        self._logger.debug(msg, *args, **kwargs)

    @final
    def error(self, msg: str, *args, **kwargs) -> None:
        self._logger.error(msg, *args, **kwargs)

    @final
    def exception(self, msg: str, *args, **kwargs) -> None:
        self._logger.exception(msg, *args, **kwargs)  # noqa: LOG004

    @final
    def warning(self, msg: str, *args, **kwargs) -> None:
        self._logger.warning(msg, *args, **kwargs)


class WorkerIdFilter(logging.Filter):
    def __init__(self, worker_id: str):
        super().__init__()
        self._worker_id = worker_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.workerid = self._worker_id
        return True


def log_elapsed_time(
    logger: logging.Logger, level: int, output_msg: str | None = None
) -> Callable:
    if output_msg is None:
        output_msg = "Elapsed time ->:\n{elapsed_time}"

    def get_wrapper[I, R](fn: Callable[I, R]) -> Callable[I, R]:
        @wraps(fn)
        def wrapped(*args, **kwargs) -> R:
            start = datetime.now(UTC)
            msg_fmt = dict()
            res = fn(*args, **kwargs)
            if "elapsed_time" in output_msg:
                msg_fmt["elapsed_time"] = datetime.now(UTC) - start
            logger.log(level, output_msg.format(**msg_fmt))
            return res

        return wrapped

    return get_wrapper


@contextlib.contextmanager
def log_elapsed_time_cm(
    logger: logging.Logger, level: int, output_msg: str | None = None
) -> Generator[None]:
    if output_msg is None:
        output_msg = "Elapsed time ->:\n{elapsed_time}"
    start = datetime.now(UTC)
    yield
    end = datetime.now(UTC) - start
    msg_fmt = dict()
    if "elapsed_time" in output_msg:
        msg_fmt["elapsed_time"] = end
    logger.log(level, output_msg.format(**msg_fmt))


STREAM_HANDLER_FMT = "[%(levelname)s][%(asctime)s.%(msecs)03d][%(name)s]: %(message)s"
STREAM_HANDLER_FMT_WITH_WORKER_ID = (
    "[%(levelname)s][%(asctime)s.%(msecs)03d][%(workerid)s][%(name)s]: %(message)s"
)
DATE_FMT = "%H:%M:%S"


def setup_loggers(loggers: list[str], level: int = logging.INFO) -> None:
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setLevel(level)
    stream_handler.setFormatter(logging.Formatter(STREAM_HANDLER_FMT, DATE_FMT))
    for logger in loggers:
        logger = logging.getLogger(logger)  # noqa: PLW2901
        logger.setLevel(level)
        logger.handlers = []
        logger.addHandler(stream_handler)
