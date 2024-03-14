# pylint: disable=redefined-outer-name
from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from typing import AsyncGenerator, List, Optional, Tuple, Type

import aiohttp
import neo4j
import pika
import pytest
import pytest_asyncio
from aiohttp import ClientResponseError
from pika.channel import Channel
from pika.spec import Basic, BasicProperties

import icij_worker
from icij_common.logging_utils import (
    DATE_FMT,
    STREAM_HANDLER_FMT,
)
from icij_common.neo4j.migrate import (
    Migration,
    init_project,
)
from icij_common.neo4j.projects import add_project_support_migration_tx

# noinspection PyUnresolvedReferences
from icij_common.neo4j.test_utils import (  # pylint: disable=unused-import
    neo4j_test_driver,
)
from icij_common.test_utils import TEST_PROJECT
from icij_worker import AsyncApp, Task
from icij_worker.event_publisher.amqp import Routing
from icij_worker.task_manager.neo4j import add_support_for_async_task_tx
from icij_worker.typing_ import PercentProgress

# noinspection PyUnresolvedReferences
from icij_worker.utils.tests import (  # pylint: disable=unused-import
    DBMixin,
    test_async_app,
)
from icij_worker.worker.amqp.consumer import (
    AMQPMessageConsumer,
    OnMessage,
    _AMQPMessageConsumer,
)

_RABBITMQ_TEST_PORT = 5673
_RABBITMQ_MANAGEMENT_PORT = 15673
TEST_MANAGEMENT_URL = f"http://localhost:{_RABBITMQ_MANAGEMENT_PORT}"
DEFAULT_VHOST = "%2F"

_DEFAULT_BROKER_URL = (
    f"amqp://guest:guest@localhost:{_RABBITMQ_TEST_PORT}/{DEFAULT_VHOST}"
)
_DEFAULT_AUTH = aiohttp.BasicAuth(login="guest", password="guest", encoding="utf-8")


def rabbit_mq_test_session() -> aiohttp.ClientSession:
    return aiohttp.ClientSession(raise_for_status=True, auth=_DEFAULT_AUTH)


async def migration_v_0_1_0_tx(tx: neo4j.AsyncTransaction):
    await add_project_support_migration_tx(tx)
    await add_support_for_async_task_tx(tx)


TEST_MIGRATIONS = [
    Migration(
        version="0.1.0",
        label="create migration and project and constraints as well as task"
        " related stuff",
        migration_fn=migration_v_0_1_0_tx,
    )
]


@pytest.fixture(scope="session")
def amqp_loggers():
    loggers = [pika.__name__, icij_worker.__name__]
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(STREAM_HANDLER_FMT, datefmt=DATE_FMT))
    for logger_ in loggers:
        logger_ = logging.getLogger(logger_)
        if logger_.name == pika.__name__:
            logger_.setLevel(logging.INFO)
        else:
            logger_.setLevel(logging.DEBUG)
        logger_.handlers = []
        logger_.addHandler(handler)


@pytest_asyncio.fixture(scope="function")
async def populate_tasks(neo4j_async_app_driver: neo4j.AsyncDriver) -> List[Task]:
    query_0 = """CREATE (task:_Task:QUEUED {
    id: 'task-0', 
    type: 'hello_world',
    createdAt: $now,
    inputs: '{"greeted": "0"}'
 }) 
RETURN task"""
    recs_0, _, _ = await neo4j_async_app_driver.execute_query(
        query_0, now=datetime.now()
    )
    t_0 = Task.from_neo4j(recs_0[0])
    query_1 = """CREATE (task:_Task:RUNNING {
    id: 'task-1', 
    type: 'hello_world',
    progress: 66.6,
    createdAt: $now,
    retries: 1,
    inputs: '{"greeted": "1"}'
 }) 
RETURN task"""
    recs_1, _, _ = await neo4j_async_app_driver.execute_query(
        query_1, now=datetime.now()
    )
    t_1 = Task.from_neo4j(recs_1[0])
    return [t_0, t_1]


class Recoverable(ValueError):
    pass


@pytest.fixture(scope="function")
def test_failing_async_app() -> AsyncApp:
    # TODO: add log deps here if it helps to debug
    app = AsyncApp(name="test-app", dependencies=[])
    already_failed = False

    @app.task("recovering_task", recover_from=(Recoverable,))
    def _recovering_task() -> str:
        nonlocal already_failed
        if already_failed:
            return "i told you i could recover"
        already_failed = True
        raise Recoverable("i can recover from this")

    @app.task("fatal_error_task")
    async def _fatal_error_task(progress: Optional[PercentProgress] = None):
        if progress is not None:
            await progress(0.1)
        raise ValueError("this is fatal")

    return app


@pytest.fixture()
async def neo4j_async_app_driver(
    neo4j_test_driver: neo4j.AsyncDriver,
) -> neo4j.AsyncDriver:
    await init_project(
        neo4j_test_driver,
        name=TEST_PROJECT,
        registry=TEST_MIGRATIONS,
        timeout_s=0.001,
        throttle_s=0.001,
    )
    return neo4j_test_driver


@pytest_asyncio.fixture(scope="session")
async def rabbit_mq_session() -> AsyncGenerator[str, None]:
    await _wipe_rabbit_mq()
    yield _DEFAULT_BROKER_URL


@pytest_asyncio.fixture()
async def rabbit_mq() -> AsyncGenerator[str, None]:
    await _wipe_rabbit_mq()
    yield _DEFAULT_BROKER_URL


def get_test_management_url(url: str) -> str:
    return f"{TEST_MANAGEMENT_URL}{url}"


async def _wipe_rabbit_mq():
    async with rabbit_mq_test_session() as session:
        await _delete_all_connections(session)
        tasks = [_delete_all_exchanges(session), _delete_all_queues(session)]
        await asyncio.gather(*tasks)


async def _delete_all_connections(session: aiohttp.ClientSession):
    async with session.get(get_test_management_url("/api/connections")) as res:
        connections = await res.json()
        tasks = [_delete_connection(session, conn["name"]) for conn in connections]
    await asyncio.gather(*tasks)


async def _delete_connection(session: aiohttp.ClientSession, name: str):
    async with session.delete(get_test_management_url(f"/api/connections/{name}")):
        pass


async def _delete_all_exchanges(session: aiohttp.ClientSession):
    url = f"/api/exchanges/{DEFAULT_VHOST}"
    async with session.get(get_test_management_url(url)) as res:
        exchanges = list(await res.json())
        exchanges = (
            ex for ex in exchanges if ex["user_who_performed_action"] == "guest"
        )
        tasks = [_delete_exchange(session, ex["name"]) for ex in exchanges]
    await asyncio.gather(*tasks)


async def _delete_exchange(session: aiohttp.ClientSession, name: str):
    url = f"/api/exchanges/{DEFAULT_VHOST}/{name}"
    async with session.delete(get_test_management_url(url)):
        pass


async def _delete_all_queues(session: aiohttp.ClientSession):
    url = f"/api/queues/{DEFAULT_VHOST}"
    async with session.get(get_test_management_url(url)) as res:
        queues = await res.json()
    tasks = [_delete_queue(session, q["name"]) for q in queues]
    await asyncio.gather(*tasks)


async def _delete_queue(session: aiohttp.ClientSession, name: str):
    url = f"/api/queues/{DEFAULT_VHOST}/{name}"
    async with session.delete(get_test_management_url(url)) as res:
        res.raise_for_status()


async def queue_exists(name: str) -> bool:
    url = get_test_management_url(f"/api/queues/vhost/{name}")
    try:
        async with aiohttp.ClientSession(raise_for_status=True) as sess:
            async with sess.get(url):
                return True
    except ClientResponseError:
        return False


@contextmanager
def shutdown_nowait(executor: ThreadPoolExecutor):
    try:
        yield executor
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


class TestConsumer__(_AMQPMessageConsumer):  # pylint: disable=invalid-name
    n_failures: int = 0
    consumed = 0

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        *,
        on_message: OnMessage,
        broker_url: str,
        routing: Routing,
        app_id: Optional[str] = None,
        recover_from: Tuple[Type[Exception], ...] = tuple(),
    ):
        super().__init__(
            logger,
            on_message=on_message,
            broker_url=broker_url,
            routing=routing,
            app_id=app_id,
            recover_from=recover_from,
        )
        self._declare_and_bind = True

    def on_message(
        self,
        _: Channel,
        basic_deliver: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ):
        # pylint: disable=arguments-renamed
        super().on_message(_, basic_deliver, properties, body)
        self.consumed += 1


def consumer_factory(consumer_cls: Type[TestConsumer__], n_failures: int) -> Type:
    consumer_cls.n_failures = n_failures

    class TestConsumer(AMQPMessageConsumer):
        def n_consumed(self) -> int:
            return self._consumer.consumed

        @property
        def consumer(self) -> TestConsumer__:
            return self._consumer

        def _create_consumer(self, logger: logging.Logger) -> TestConsumer__:
            return consumer_cls(
                logger=logger,
                on_message=self._on_message,
                broker_url=self._broker_url,
                routing=self._routing,
                app_id=self._app_id,
                recover_from=self._recover_from,
            )

    return TestConsumer
