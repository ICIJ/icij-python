import asyncio
import logging

logger = logging.getLogger(__name__)


class Seq:
    pass


def stop_other_tasks_when_exc(t: asyncio.Task, others: list[asyncio.Task]) -> None:
    try:
        exc = t.exception()
    except asyncio.CancelledError:
        msg = (
            "%s was cancelled, probably due to an exception in a concurrent infinite"
            " loops"
        )
        logger.debug(msg, t)
        return
    if exc is None:
        msg = "attempted to cancel %s, which was already done"
        logger.debug(msg, t)
        return
    loop = t.get_loop()
    others = [task for task in others if t is not task]
    for task in others:
        if not task.done():
            task.cancel(f"cancelled by {t}")
    loop.call_soon(_raise(exc))


def _raise(exc: Exception):
    logger.debug("exception occurred in one of the running infinite loops: %s", exc)
    raise exc
