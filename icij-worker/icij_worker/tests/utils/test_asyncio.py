import asyncio
import logging
from functools import partial

import pytest

from icij_worker.utils.asyncio_ import stop_other_tasks_when_exc

logger = logging.getLogger(__name__)


def test_run_loops_should_raise_exceptions():
    class SomeFailure(Exception): ...  # pylint: disable=multiple-statements

    async def _infinite_loop():
        while True:
            await asyncio.sleep(0.1)

    async def _failing_infinite_loop():
        for _ in range(3):
            await asyncio.sleep(0.1)
        raise SomeFailure("some failure")

    loop = asyncio.get_event_loop()
    tasks = [
        loop.create_task(_infinite_loop()),
        loop.create_task(_failing_infinite_loop()),
    ]
    callback = partial(stop_other_tasks_when_exc, others=tasks)
    for t in tasks:
        t.add_done_callback(callback)
    done, pending = loop.run_until_complete(
        asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
    )
    assert not pending
    for t in done:
        with pytest.raises((asyncio.CancelledError, SomeFailure)):
            t.result()
