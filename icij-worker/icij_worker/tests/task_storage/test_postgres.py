# pylint: disable=redefined-outer-name
import functools
import json
import os
import re
from datetime import datetime, timezone
from typing import Dict, Generic, List, Optional, cast

import pytest
from psycopg import AsyncClientCursor, AsyncConnection, sql
from psycopg.rows import dict_row

from icij_common.pydantic_utils import safe_copy
from icij_common.test_utils import TEST_DB
from icij_worker import (
    Namespacing,
    PostgresConnectionInfo,
    PostgresStorage,
    PostgresStorageConfig,
    ResultEvent,
    Task,
    TaskState,
    init_postgres_database,
)
from icij_worker.exceptions import UnknownTask
from icij_worker.objects import ErrorEvent, StacktraceItem, TaskError
from icij_worker.task_storage.postgres.postgres import create_databases_registry_db
from icij_worker.utils.config import SettingsWithTM, TM

_TEST_DB = "test-db"
TEST_PG_HOST = "localhost"
TEST_PG_USER = "postgres"
TEST_PG_PORT = 5555
TEST_PG_PASSWORD = "changeme"


@pytest.fixture(scope="session")
async def test_postgres_config() -> PostgresStorageConfig:
    return PostgresStorageConfig(
        host=TEST_PG_HOST,
        port=TEST_PG_PORT,
        user=TEST_PG_USER,
        password=TEST_PG_PASSWORD,
        use_ssl=False,
        connect_timeout_s=2.0,
    )


class _TestNamespacing(Namespacing):
    @classmethod
    def postgres_db(cls, namespace: str) -> str:
        return TEST_DB


@pytest.fixture(scope="session")
async def test_postgres_storage_session(
    test_postgres_config: PostgresStorageConfig,
) -> PostgresStorage:
    namespacing = _TestNamespacing()
    storage = test_postgres_config.to_storage(namespacing=namespacing)
    storage = cast(PostgresStorage, storage)
    return storage


@pytest.fixture
async def test_postgres_storage(
    test_postgres_storage_session: PostgresStorage,
) -> PostgresStorage:
    storage = test_postgres_storage_session
    async with storage:
        yield storage


@functools.lru_cache
def task_0() -> Task:
    t = Task(
        id="task-0",
        name="task-type-0",
        created_at=datetime.now(timezone.utc),
        state=TaskState.CREATED,
        arguments={"greeted": "world"},
    ).with_max_retries(3)
    return t


_ = task_0()


@functools.lru_cache
def task_1() -> Task:
    t = Task(
        id="task-1",
        name="task-type-1",
        created_at=datetime.now(timezone.utc),
        state=TaskState.QUEUED,
        arguments={},
    ).with_max_retries(3)
    return t


_ = task_1()

_TASK_COLS = [
    "id",
    "name",
    "namespace",
    "state",
    "progress",
    "created_at",
    "completed_at",
    "cancelled_at",
    "retries_left",
    "max_retries",
    "arguments",
]


async def _wipe_tasks(conn: AsyncConnection):
    async with conn.cursor() as cur:
        delete_everything = """TRUNCATE TABLE tasks CASCADE;
TRUNCATE TABLE  results;
TRUNCATE TABLE  errors;
"""
        await cur.execute(delete_everything)


async def _wipe_registry(conn: AsyncConnection):
    async with conn.cursor() as cur:
        delete_everything = sql.SQL(
            "DROP DATABASE IF EXISTS {registry_db} WITH (FORCE);"
        ).format(registry_db=sql.Identifier(PostgresStorageConfig.registry_db_name))
        old_autocommit = conn.autocommit
        await conn.set_autocommit(True)
        await cur.execute(delete_everything)
        await conn.set_autocommit(old_autocommit)


@pytest.fixture(scope="session")
def test_connection_info() -> PostgresConnectionInfo:
    return PostgresConnectionInfo(
        host=TEST_PG_HOST,
        port=TEST_PG_PORT,
        user=TEST_PG_USER,
        password=TEST_PG_PASSWORD,
    )


@pytest.fixture(scope="session")
async def test_postgres_conn_session(
    test_connection_info: PostgresConnectionInfo,
) -> AsyncConnection:
    registry_db_name = PostgresStorageConfig.registry_db_name
    connection = await AsyncConnection.connect(
        autocommit=True, cursor_factory=AsyncClientCursor, **test_connection_info.kwargs
    )
    async with connection as conn:
        await _wipe_registry(conn)
        await create_databases_registry_db(conn, registry_db_name)

    connections: Dict[str, AsyncConnection] = dict()

    async def _factory(db_name: str) -> AsyncConnection:
        if db_name not in connections:
            conn = await AsyncConnection.connect(
                autocommit=True, dbname=db_name, **test_connection_info.kwargs
            )
            await conn.__aenter__()  # pylint: disable=unnecessary-dunder-call
            connections[db_name] = conn
        return connections[db_name]

    await init_postgres_database(
        TEST_DB,
        _factory,
        registry_db_name=registry_db_name,
        connection_info=test_connection_info,
        migration_timeout_s=20.0,
        migration_throttle_s=0.1,
    )
    connection = await AsyncConnection.connect(
        autocommit=True,
        cursor_factory=AsyncClientCursor,
        dbname=TEST_DB,
        **test_connection_info.kwargs,
    )
    async with connection:
        yield connection
    for c in connections.values():
        await c.close()


@pytest.fixture()
async def test_postgres_conn(
    test_postgres_conn_session: AsyncConnection,
):
    conn = test_postgres_conn_session
    await _wipe_tasks(conn)
    return conn


@pytest.fixture()
async def populate_task(test_postgres_conn: AsyncConnection) -> List[Task]:
    tasks = [task_0(), task_1()]
    task_tuples = [t.dict() for t in tasks]
    for i, t in enumerate(task_tuples):
        t["arguments"] = json.dumps(t["arguments"])
        t["namespace"] = "some-namespace" if i % 2 == 0 else None
    task_tuples = [tuple(t[col] for col in _TASK_COLS) for t in task_tuples]
    async with test_postgres_conn.cursor() as cur:
        query = f"INSERT INTO tasks ({', '.join(_TASK_COLS)}) VALUES\n"
        values_placeholder = f"({','.join('%s' for _ in range(len(_TASK_COLS)))})"
        query += ",\n".join(cur.mogrify(values_placeholder, t) for t in task_tuples)
        query += ";"
        await cur.execute(query)
    return tasks


async def test_save_task(
    test_postgres_storage: PostgresStorage, test_postgres_conn: AsyncConnection
) -> None:
    # Given
    storage = test_postgres_storage
    conn = test_postgres_conn
    task = task_0()
    # When
    is_new = await storage.save_task_(task, None)
    # Then
    assert is_new
    async with conn.cursor(row_factory=dict_row) as cur:
        query = "SELECT * FROM tasks AS t WHERE t.id = %s"
        await cur.execute(query, (task.id,))
        db_task = await cur.fetchone()
    db_task["arguments"] = json.loads(db_task["arguments"])
    namespace = db_task.pop("namespace")
    assert namespace is None
    db_task = Task(**db_task)
    assert db_task == task


async def test_save_existing_task(
    test_postgres_storage: PostgresStorage,
    test_postgres_conn: AsyncConnection,
    populate_task: List[Task],
) -> None:
    # pylint: disable=unused-argument
    # Given
    storage = test_postgres_storage
    conn = test_postgres_conn
    task = task_0()
    task = safe_copy(
        task, update={"state": TaskState.RUNNING, "progress": 0.66, "retries_left": 1}
    )

    # When
    is_new = await storage.save_task_(task, None)
    # Then
    assert not is_new
    async with conn.cursor(row_factory=dict_row) as cur:
        query = "SELECT * FROM tasks AS t WHERE t.id = %s"
        await cur.execute(query, (task.id,))
        db_task = await cur.fetchone()
    db_task["arguments"] = json.loads(db_task["arguments"])
    namespace = db_task.pop("namespace")
    assert namespace == "some-namespace"
    db_task = Task(**db_task)
    assert db_task == task


async def test_save_result(
    test_postgres_storage: PostgresStorage,
    test_postgres_conn: AsyncConnection,
    populate_task: List[Task],
) -> None:
    # pylint: disable=unused-argument
    # Given
    storage = test_postgres_storage
    conn = test_postgres_conn
    task = populate_task[0]
    result = ResultEvent.from_task(task, result="some-results-here")
    # When
    await storage.save_result(result)
    # Then
    async with conn.cursor(row_factory=ResultEvent.postgres_row_factory) as cur:
        query = "SELECT * FROM results AS r WHERE r.task_id = %s"
        await cur.execute(query, (task.id,))
        db_res = await cur.fetchone()
    assert db_res == result


async def test_save_result_should_raise_for_unknown_task(
    test_postgres_storage: PostgresStorage, test_postgres_conn: AsyncConnection
) -> None:
    # pylint: disable=unused-argument
    # Given
    storage = test_postgres_storage
    result = ResultEvent(
        task_id="i_dont_exists",
        result="some-results-here",
        created_at=datetime.now(timezone.utc),
    )
    # When
    expected = re.escape('Unknown task "i_dont_exists"')
    with pytest.raises(UnknownTask, match=expected):
        await storage.save_result(result)


async def test_save_error(
    test_postgres_storage: PostgresStorage,
    test_postgres_conn: AsyncConnection,
    populate_task: List[Task],
) -> None:
    # pylint: disable=unused-argument
    # Given
    storage = test_postgres_storage
    conn = test_postgres_conn
    task = populate_task[0]
    error = TaskError(
        name="error",
        message="with details",
        stacktrace=[StacktraceItem(name="SomeError", file="somefile", lineno=666)],
        cause="some cause",
    )
    error_event = ErrorEvent(
        task_id=task.id,
        error=error,
        retries_left=2,
        created_at=datetime.now(timezone.utc),
    )
    # When
    await storage.save_error(error_event)
    # Then
    async with conn.cursor(row_factory=ErrorEvent.postgres_row_factory) as cur:
        query = "SELECT * FROM errors AS r WHERE r.task_id = %s"
        await cur.execute(query, (task.id,))
        db_error = await cur.fetchone()
    assert db_error == error_event


async def test_save_error_should_raise_for_unknown_task(
    test_postgres_storage: PostgresStorage, test_postgres_conn: AsyncConnection
) -> None:
    # pylint: disable=unused-argument
    # Given
    storage = test_postgres_storage
    error = TaskError(
        name="error",
        message="with details",
        stacktrace=[StacktraceItem(name="SomeError", file="somefile", lineno=666)],
        cause="some cause",
    )
    error_event = ErrorEvent(
        task_id="i_dont_exists",
        error=error,
        retries_left=2,
        created_at=datetime.now(timezone.utc),
    )
    # When
    expected = re.escape('Unknown task "i_dont_exists"')
    with pytest.raises(UnknownTask, match=expected):
        await storage.save_error(error_event)


async def test_get_task(
    populate_task: List[Task], test_postgres_storage: PostgresStorage
) -> None:
    # Given
    storage = test_postgres_storage
    task = populate_task[0]
    # When
    db_task = await storage.get_task(task.id)
    # Then
    assert db_task == task


async def test_get_task_should_raise_for_unknown_task(
    test_postgres_conn: AsyncConnection, test_postgres_storage: PostgresStorage
) -> None:
    # pylint: disable=unused-argument
    # Given
    storage = test_postgres_storage
    task_id = "unknown_id"
    # When/Then
    expected = re.escape('Unknown task "unknown_id"')
    with pytest.raises(UnknownTask, match=expected):
        await storage.get_task(task_id)


@pytest.mark.parametrize(
    "namespace,task_name,state,expected_tasks",
    [
        (None, None, None, [task_1(), task_0()]),
        ("some-namespace", None, None, [task_0()]),
        (None, "task-type-1", None, [task_1()]),
        (None, None, TaskState.CREATED, [task_0()]),
    ],
)
async def test_get_tasks(
    populate_task: List[Task],
    test_postgres_storage: PostgresStorage,
    namespace: Optional[str],
    task_name: Optional[str],
    state: Optional[TaskState],
    expected_tasks: List[Task],
) -> None:
    # pylint: disable=unused-argument
    # Given
    storage = test_postgres_storage
    # When
    db_tasks = await storage.get_tasks(
        namespace=namespace, state=state, task_name=task_name
    )
    # Then
    assert db_tasks == expected_tasks


async def test_get_task_result(
    populate_task: List[Task],
    test_postgres_storage: PostgresStorage,
    test_postgres_conn: AsyncConnection,
) -> None:
    # pylint: disable=unused-argument
    # Given
    conn = test_postgres_conn
    storage = test_postgres_storage
    task = populate_task[0]
    task_id = task.id
    created_at = datetime.now(timezone.utc)
    result = "some-results-here"
    async with conn.cursor() as cur:
        await cur.execute(
            "INSERT INTO results (task_id, result, created_at) VALUES (%s, %s, %s)",
            (task_id, json.dumps(result), created_at),
        )

    # When
    db_res = await storage.get_task_result(task_id)
    expected = ResultEvent(task_id=task_id, created_at=created_at, result=result)
    assert db_res == expected


async def test_get_task_result_should_raise_for_unknown_task(
    test_postgres_storage: PostgresStorage,
) -> None:
    # Given
    storage = test_postgres_storage
    # When
    expected = re.escape('Unknown task "unknown_id"')
    with pytest.raises(UnknownTask, match=expected):
        await storage.get_task_result("unknown_id")


async def test_get_task_errors(
    populate_task: List[Task],
    test_postgres_storage: PostgresStorage,
    test_postgres_conn: AsyncConnection,
) -> None:
    # Given
    conn = test_postgres_conn
    storage = test_postgres_storage
    task = populate_task[0]
    task_id = task.id
    created_at = datetime.now(timezone.utc)
    retries_left = 2
    name = "some-name"
    message = "some message"
    cause = "some cause"
    item = StacktraceItem(name="SomeError", file="some details", lineno=666)
    stacktrace = json.dumps([item.dict()])
    async with conn.cursor() as cur:
        await cur.execute(
            """INSERT INTO errors
(task_id, retries_left, name, message, cause, stacktrace, created_at)
VALUES (%s, %s, %s, %s, %s, %s, %s);
""",
            (task_id, retries_left, name, message, cause, stacktrace, created_at),
        )

    # When
    db_errors = await storage.get_task_errors(task_id)
    error = TaskError(name=name, message=message, cause=cause, stacktrace=[item])
    error_event = ErrorEvent(
        task_id=task_id, error=error, created_at=created_at, retries_left=retries_left
    )
    assert db_errors == [error_event]


async def test_get_task_errors_should_raise_for_unknown_task(
    test_postgres_storage: PostgresStorage,
) -> None:
    # Given
    storage = test_postgres_storage
    # When
    expected = re.escape('Unknown task "unknown_id"')
    with pytest.raises(UnknownTask, match=expected):
        await storage.get_task_errors("unknown_id")


async def test_get_task_namespace(
    populate_task: List[Task],
    test_postgres_storage: PostgresStorage,
) -> None:
    # pylint: disable=unused-argument
    # Given
    storage = test_postgres_storage
    # When
    ns_0 = await storage.get_task_namespace("task-0")
    ns_1 = await storage.get_task_namespace("task-1")
    assert ns_0 == "some-namespace"
    assert ns_1 is None


def test_task_manager_with_postgres_storage_from_config(reset_env):
    # pylint: disable=unused-argument
    # Given
    class _MySettings(SettingsWithTM, Generic[TM]):
        some_other_app_setting: str

        class Config:
            env_prefix = "MY_APP_"
            env_nested_delimiter = "__"

    env_vars = {
        "MY_APP_SOME_OTHER_APP_SETTING": "ANOTHER_SETTING",
        "MY_APP_TASK_MANAGER__APP_PATH": "icij_worker.utils.tests.APP",
        "MY_APP_TASK_MANAGER__BACKEND": "amqp",
        "MY_APP_TASK_MANAGER__STORAGE__MAX_CONNECTIONS": "28",
        "MY_APP_TASK_MANAGER__RABBITMQ_HOST": "localhost",
        "MY_APP_TASK_MANAGER__RABBITMQ_PORT": str(15752),
    }
    os.environ.update(env_vars)

    # When
    settings = _MySettings.from_env()
    task_manager = settings.to_task_manager()
    # Then
    assert isinstance(
        task_manager._storage, PostgresStorage  # pylint: disable=protected-access
    )
