from __future__ import annotations

import contextlib
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from functools import cached_property, wraps
from typing import Any, MutableMapping, Optional, final


class DifferedLoggingMessage:
    def __init__(self, fn, *args, **kwargs):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        return str(self.fn(*self.args, **self.kwargs))


class LogWithNameMixin(ABC):
    @property
    @abstractmethod
    def _logger(self) -> logging.Logger:
        pass

    @cached_property
    def _adapter(self) -> logging.LoggerAdapter:
        extra = {"name": self.logged_named}
        return _LogWithNameAdapter(self._logger, extra)

    @property
    @abstractmethod
    def logged_named(self) -> str:
        pass

    @final
    def info(self, msg, *args, **kwargs):
        self._adapter.info(msg, *args, **kwargs)

    @final
    def debug(self, msg, *args, **kwargs):
        self._adapter.debug(msg, *args, **kwargs)

    @final
    def error(self, msg, *args, **kwargs):
        self._adapter.error(msg, *args, **kwargs)


class _LogWithNameAdapter(logging.LoggerAdapter):
    def process(
        self, msg: Any, kwargs: MutableMapping[str, Any]
    ) -> tuple[Any, MutableMapping[str, Any]]:
        return f"[{self.extra['name']}] {msg}", kwargs


def log_elapsed_time(
    logger: logging.Logger, level: int, output_msg: Optional[str] = None
):
    if output_msg is None:
        output_msg = "Elapsed time ->:\n{elapsed_time}"

    def get_wrapper(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            start = datetime.now()
            msg_fmt = dict()
            res = fn(*args, **kwargs)
            if "elapsed_time" in output_msg:
                msg_fmt["elapsed_time"] = datetime.now() - start
            logger.log(level, output_msg.format(**msg_fmt))
            return res

        return wrapped

    return get_wrapper


@contextlib.contextmanager
def log_elapsed_time_cm(
    logger: logging.Logger, level: int, output_msg: Optional[str] = None
):
    if output_msg is None:
        output_msg = "Elapsed time ->:\n{elapsed_time}"
    start = datetime.now()
    yield
    end = datetime.now() - start
    msg_fmt = dict()
    if "elapsed_time" in output_msg:
        msg_fmt["elapsed_time"] = end
    logger.log(level, output_msg.format(**msg_fmt))


STREAM_HANDLER_FMT = "[%(levelname)s][%(asctime)s.%(msecs)03d][%(name)s]: %(message)s"
DATE_FMT = "%H:%M:%S"
