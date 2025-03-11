import asyncio
from abc import ABC
from typing import Awaitable, Callable, TypeVar

import neo4j

from icij_common.neo4j_.migrate import retrieve_dbs
from icij_worker.objects import TaskEvent
from icij_worker.task_storage.neo4j_ import Neo4jStorage

T = TypeVar("T", bound=TaskEvent)
ConsumeT = Callable[[neo4j.AsyncTransaction, ...], Awaitable[T | None]]


class Neo4jConsumerMixin(Neo4jStorage, ABC):
    _driver: neo4j.AsyncDriver
    _db_driver: neo4j.AsyncDriver
    _task_meta: dict[str, tuple[str, str]] = dict()

    async def _consume_(
        self,
        consume_tx: ConsumeT,
        refresh_interval_s: float,
        db_filter: Callable[[str], bool] | None,
    ) -> T:
        dbs = []
        refresh_dbs_i = 0
        while "i'm waiting until I find something interesting":
            # Refresh project list once in an while
            refresh_dbs = (refresh_dbs_i % 10) == 0
            if refresh_dbs:
                dbs = await retrieve_dbs(self._driver)
                if db_filter is not None:
                    dbs = [db for db in dbs if db_filter(db.name)]
            for db in dbs:
                async with self._db_session(db.name) as sess:
                    received = await sess.execute_write(consume_tx)
                    if "group" in received:
                        self._task_meta[received.id] = (db.name, received["group"])
                    if received is not None:
                        return received
            await asyncio.sleep(refresh_interval_s)
            refresh_dbs_i += 1
