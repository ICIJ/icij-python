# pylint: disable=redefined-outer-name
from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Optional

import aio_pika
import aiohttp
import neo4j
import pika
import pytest
from aio_pika.abc import AbstractRobustChannel
from aiohttp import ClientResponseError, ClientTimeout

import icij_worker
from icij_common.logging_utils import (
    DATE_FMT,
    STREAM_HANDLER_FMT,
)
from icij_common.neo4j.db import (
    NEO4J_COMMUNITY_DB,
    add_multidatabase_support_migration_tx,
    db_specific_session,
)
from icij_common.neo4j.migrate import (
    Migration,
    init_database,
)

# noinspection PyUnresolvedReferences
from icij_common.neo4j.test_utils import (  # pylint: disable=unused-import
    neo4j_test_driver,
)
from icij_worker import AsyncApp, Neo4JTaskManager, Task
from icij_worker.app import AsyncAppConfig
from icij_worker.event_publisher.amqp import AMQPPublisher
from icij_worker.objects import CancelEvent, ManagerEvent, TaskState
from icij_worker.task_manager.amqp import AMQPTaskManager
from icij_worker.task_storage import TaskStorage
from icij_worker.task_storage.fs import FSKeyValueStorage
from icij_worker.task_storage.neo4j_ import (
    add_support_for_async_task_tx,
    migrate_add_index_to_task_namespace_v0_tx,
    migrate_cancelled_event_created_at_v0_tx,
    migrate_index_event_dates_v0_tx,
    migrate_task_errors_v0_tx,
    migrate_task_inputs_to_arguments_v0_tx,
    migrate_task_progress_v0_tx,
    migrate_task_retries_and_error_v0_tx,
    migrate_task_type_to_name_v0,
)
from icij_worker.typing_ import PercentProgress

# noinspection PyUnresolvedReferences
from icij_worker.utils.tests import (  # pylint: disable=unused-import
    DBMixin,
    MockWorker,
    app_config,
    late_ack_app_config,
    mock_db,
    mock_db_session,
    test_async_app,
    test_async_app_late,
)

RABBITMQ_TEST_PORT = 5673
RABBITMQ_TEST_HOST = "localhost"
_RABBITMQ_MANAGEMENT_PORT = 15673
TEST_MANAGEMENT_URL = f"http://localhost:{_RABBITMQ_MANAGEMENT_PORT}"
DEFAULT_VHOST = "%2F"

_DEFAULT_BROKER_URL = (
    f"amqp://guest:guest@{RABBITMQ_TEST_HOST}:{RABBITMQ_TEST_PORT}/{DEFAULT_VHOST}"
)
_DEFAULT_AUTH = aiohttp.BasicAuth(login="guest", password="guest", encoding="utf-8")


def rabbit_mq_test_session() -> aiohttp.ClientSession:
    return aiohttp.ClientSession(
        raise_for_status=True, auth=_DEFAULT_AUTH, timeout=ClientTimeout(total=2)
    )


async def migration_v_0_1_0_tx(tx: neo4j.AsyncTransaction):
    await add_multidatabase_support_migration_tx(tx)
    await add_support_for_async_task_tx(tx)


TEST_MIGRATIONS = [
    Migration(
        version="0.1.0",
        label="create migration and project and constraints as well as task"
        " related stuff",
        migration_fn=migration_v_0_1_0_tx,
    ),
    Migration(
        version="0.2.0",
        label="migrate tasks errors",
        migration_fn=migrate_task_errors_v0_tx,
    ),
    Migration(
        version="0.3.0",
        label="rename cancelled event createdAt",
        migration_fn=migrate_cancelled_event_created_at_v0_tx,
    ),
    Migration(
        version="0.4.0",
        label="add index on task namespace",
        migration_fn=migrate_add_index_to_task_namespace_v0_tx,
    ),
    Migration(
        version="0.5.0",
        label="rename inputs into arguments",
        migration_fn=migrate_task_inputs_to_arguments_v0_tx,
    ),
    Migration(
        version="0.6.0",
        label="rename task type into name",
        migration_fn=migrate_task_type_to_name_v0,
    ),
    Migration(
        version="0.7.0",
        label="scale task progress",
        migration_fn=migrate_task_progress_v0_tx,
    ),
    Migration(
        version="0.8.0",
        label="index events by dates",
        migration_fn=migrate_index_event_dates_v0_tx,
    ),
    Migration(
        version="0.9.0",
        label="task retries and error occurred_at",
        migration_fn=migrate_task_retries_and_error_v0_tx,
    ),
]


@pytest.fixture(scope="session")
def amqp_loggers():
    loggers = [aio_pika.__name__, icij_worker.__name__]
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


_NOW = datetime.now()


@pytest.fixture(scope="function")
async def populate_tasks(
    neo4j_async_app_driver: neo4j.AsyncDriver, request
) -> List[Task]:
    namespace = getattr(request, "param", None)
    query_0 = """CREATE (task:_Task:QUEUED {
    namespace: $namespace,
    id: 'task-0', 
    name: 'hello_world',
    createdAt: $now,
    arguments: '{"greeted": "0"}'
 }) 
RETURN task"""
    recs_0, _, _ = await neo4j_async_app_driver.execute_query(
        query_0, now=_NOW, namespace=namespace
    )
    t_0 = Task.from_neo4j(recs_0[0])
    query_1 = """CREATE (task:_Task:RUNNING {
    id: 'task-1', 
    namespace: $namespace,
    name: 'hello_world',
    progress: 0.66,
    createdAt: $now,
    retriesLeft: 1,
    arguments: '{"greeted": "1"}'
 }) 
RETURN task"""
    recs_1, _, _ = await neo4j_async_app_driver.execute_query(
        query_1, now=_NOW, namespace=namespace
    )
    t_1 = Task.from_neo4j(recs_1[0])
    return [t_0, t_1]


@pytest.fixture(scope="function")
async def populate_cancel_events(
    populate_tasks: List[Task], neo4j_async_app_driver: neo4j.AsyncDriver, request
) -> List[CancelEvent]:
    namespace = getattr(request, "param", None)
    query_0 = """MATCH (task:_Task { id: $taskId })
SET task.namespace = $namespace
CREATE (task)-[:_CANCELLED_BY]->(event:_CancelEvent { requeue: false, effective: false, cancelledAt: $now }) 
RETURN task, event"""
    recs_0, _, _ = await neo4j_async_app_driver.execute_query(
        query_0, now=datetime.now(), taskId=populate_tasks[0].id, namespace=namespace
    )
    return [CancelEvent.from_neo4j(recs_0[0])]


class Recoverable(ValueError):
    pass


@pytest.fixture(scope="function")
def test_failing_async_app() -> AsyncApp:
    return _make_app()


@pytest.fixture(scope="function")
def test_failing_async_app_late_ack(late_ack_app_config: AsyncAppConfig) -> AsyncApp:
    return _make_app(late_ack_app_config)


def _make_app(config: Optional[AsyncAppConfig] = None) -> AsyncApp:
    # TODO: add log deps here if it helps to debug
    app = AsyncApp(name="test-app", dependencies=[])
    if config is not None:
        app = app.with_config(config)
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
    await init_database(
        neo4j_test_driver,
        name=NEO4J_COMMUNITY_DB,
        registry=TEST_MIGRATIONS,
        timeout_s=0.001,
        throttle_s=0.001,
    )
    return neo4j_test_driver


@pytest.fixture(scope="session")
async def rabbit_mq_session() -> AsyncGenerator[str, None]:
    await _wipe_rabbit_mq()
    yield _DEFAULT_BROKER_URL


@pytest.fixture()
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


async def exchange_exists(name: str) -> bool:
    url = get_test_management_url(f"/api/exchanges/{DEFAULT_VHOST}/{name}")
    try:
        async with rabbit_mq_test_session() as sess:
            async with sess.get(url):
                return True
    except ClientResponseError:
        return False


async def get_queue(name: str) -> Dict:
    url = get_test_management_url(f"/api/queues/{DEFAULT_VHOST}/{name}")
    async with rabbit_mq_test_session() as sess:
        async with sess.get(url) as res:
            res.raise_for_status()
            return await res.json()


async def get_queue_size(name: str) -> int:
    q = await get_queue(name)
    return q.get("messages")


async def queue_exists(name: str) -> bool:
    try:
        await get_queue(name)
        return True
    except ClientResponseError:
        return False


@contextmanager
def shutdown_nowait(executor: ThreadPoolExecutor):
    try:
        yield executor
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


class TestableAMQPPublisher(AMQPPublisher):

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        *,
        broker_url: str,
        connection_timeout_s: float = 1.0,
        reconnection_wait_s: float = 5.0,
        app_id: Optional[str] = None,
    ):
        # declare and bind the queues
        super().__init__(
            logger,
            broker_url=broker_url,
            connection_timeout_s=connection_timeout_s,
            reconnection_wait_s=reconnection_wait_s,
            app_id=app_id,
        )
        self._declare_and_bind = True

    @property
    def can_publish(self) -> bool:
        if self._connection_ is None or self._connection.is_closed:
            return False
        if self._channel_ is None or self._channel.is_closed:
            return False
        return True

    @property
    def event_queue(self) -> str:
        return self.__class__.manager_evt_routing().queue_name


@pytest.fixture(scope="session")
def hello_world_task() -> Task:
    task = Task(
        id="some-id",
        name="hello_world",
        arguments={"greeted": "world"},
        state=TaskState.CREATED,
        created_at=datetime.now(),
    )
    return task


class TestableFSKeyValueStorage(FSKeyValueStorage):
    @property
    def db_path(self) -> str:
        return str(self._db_path)


@pytest.fixture()
def fs_storage_path(tmpdir: Path) -> Path:
    return Path(tmpdir) / "task-store.db"


@pytest.fixture()
async def fs_storage(fs_storage_path: Path) -> TestableFSKeyValueStorage:
    store = TestableFSKeyValueStorage(fs_storage_path)
    async with store:
        yield store


class TestableAMQPTaskManager(AMQPTaskManager):
    def __init__(self, app: AsyncApp, task_store: TaskStorage, *, broker_url: str):
        super().__init__(app, task_store, broker_url=broker_url)
        self.consumed: List[ManagerEvent] = []

    @cached_property
    def channel(self) -> AbstractRobustChannel:
        return self._channel

    async def _consume(self) -> ManagerEvent:
        event = await super()._consume()
        self.consumed.append(event)
        return event

    @property
    def app(self) -> AsyncApp:
        return self._app


@pytest.fixture()
async def test_amqp_task_manager(
    fs_storage: TestableFSKeyValueStorage, rabbit_mq: str, test_async_app: AsyncApp
) -> TestableAMQPTaskManager:
    task_manager = TestableAMQPTaskManager(
        test_async_app, fs_storage, broker_url=rabbit_mq
    )
    async with task_manager:
        yield task_manager


class TestableNeo4JTaskManager(Neo4JTaskManager):
    def __init__(self, app: AsyncApp, driver: neo4j.AsyncDriver):
        super().__init__(app, driver)
        self.consumed: List[ManagerEvent] = []

    async def _consume(self) -> ManagerEvent:
        event = await super()._consume()
        self.consumed.append(event)
        return event

    @property
    def app(self) -> AsyncApp:
        return self._app


@pytest.fixture
def neo4j_task_manager(
    neo4j_async_app_driver: neo4j.AsyncDriver, request
) -> TestableNeo4JTaskManager:
    app = getattr(request, "param", "test_async_app")
    app = request.getfixturevalue(app)
    return TestableNeo4JTaskManager(app, neo4j_async_app_driver)


async def count_locks(driver: neo4j.AsyncDriver, db: str) -> int:
    # Now let's check that no lock if left in the DB
    count_locks_query = "MATCH (lock:_TaskLock) RETURN count(*) as nLocks"
    async with db_specific_session(driver, db=db) as sess:
        recs = await sess.run(count_locks_query)
        counts = await recs.single(strict=True)
    return counts["nLocks"]


@pytest.fixture(
    scope="function",
    params=[{"app": "test_async_app"}, {"app": "test_async_app_late"}],
)
def mock_worker(mock_db: Path, request) -> MockWorker:
    param = getattr(request, "param", dict()) or dict()
    app = request.getfixturevalue(param.get("app", "test_async_app"))
    worker = MockWorker(
        app,
        "test-worker",
        namespace=param.get("namespace"),
        db_path=mock_db,
        poll_interval_s=0.1,
        teardown_dependencies=False,
    )
    return worker
