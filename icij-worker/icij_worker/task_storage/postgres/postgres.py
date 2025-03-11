# pylint: disable=c-extension-no-member
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from copy import copy
from functools import cached_property
from typing import AsyncGenerator, Awaitable, Callable, ClassVar, Protocol

import ujson
from psycopg import AsyncClientCursor, AsyncConnection, AsyncCursor, sql
from psycopg.conninfo import make_conninfo
from psycopg.errors import DuplicateDatabase
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from icij_worker import RoutingStrategy, ResultEvent, Task, TaskState
from icij_worker.constants import (
    POSTGRES_TASKS_GROUP,
    POSTGRES_TASKS_TABLE,
    POSTGRES_TASK_DBS_TABLE,
    POSTGRES_TASK_DB_IS_LOCKED,
    POSTGRES_TASK_DB_NAME,
    POSTGRES_TASK_ERRORS_TABLE,
    POSTGRES_TASK_RESULTS_TABLE,
    TASK_ARGS,
    TASK_ERRORS_TASK_ID,
    TASK_ID,
    TASK_NAME,
    TASK_RESULT_CREATED_AT,
    TASK_RESULT_RESULT,
    TASK_RESULT_RESULT_VALUE,
    TASK_RESULT_TASK_ID,
    TASK_STATE,
)
from icij_worker.exceptions import UnknownTask
from icij_worker.objects import ErrorEvent, TaskError, TaskUpdate
from icij_worker.task_storage import TaskStorage, TaskStorageConfig
from icij_worker.task_storage.postgres.connection_info import PostgresConnectionInfo
from icij_worker.task_storage.postgres.db_mate import migrate

logger = logging.getLogger(__name__)


class ConnectionPoolFactory(Protocol):
    async def __call__(
        self, key: str, *, min_size: int, max_size: int
    ) -> AsyncConnectionPool: ...


class PostgresStorageConfig(PostgresConnectionInfo, TaskStorageConfig):
    registry_db_name: ClassVar[str] = "task_dbs_registry"

    max_connections: int = 1
    migration_timeout_s: float = 60.0
    migration_throttle_s: float = 0.1

    def to_storage(  # pylint: disable=arguments-differ
        self, routing_strategy: RoutingStrategy | None
    ) -> PostgresStorage:
        storage = PostgresStorage(
            connection_info=self.as_connection_info,
            routing_strategy=routing_strategy,
            registry_db_name=self.registry_db_name,
            max_connections=self.max_connections,
            migration_timeout_s=self.migration_timeout_s,
            migration_throttle_s=self.migration_throttle_s,
        )
        return storage

    @cached_property
    def as_connection_info(self) -> PostgresConnectionInfo:
        self_as_connection_info = {
            k: v
            for k, v in self.model_dump().items()
            if k in PostgresConnectionInfo.__fields__
        }
        return PostgresConnectionInfo(**self_as_connection_info)


class PoolManager(AbstractAsyncContextManager):
    def __init__(
        self,
        pool_factory: ConnectionPoolFactory,
        *,
        max_size: int | None,
        min_size: int | None,
    ):
        # TODO: limit the number of total pools if needed
        if min_size is not None:
            min_size = 1
        self._min_size = min_size
        if max_size is not None:
            max_size = min_size
        self._max_size = max_size
        self._pool_factory = pool_factory
        self._exit_stack = AsyncExitStack()
        self._pools = dict()

    async def get_pool(self, key: str) -> AsyncConnectionPool:
        if key not in self._pools:
            self._pools[key] = await self._pool_factory(
                key, max_size=self._max_size, min_size=self._min_size
            )
            await self._exit_stack.enter_async_context(self._pools[key])
        return self._pools[key]

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._exit_stack.__aexit__(exc_type, exc_value, traceback)


class PostgresStorage(TaskStorage):

    def __init__(
        self,
        connection_info: PostgresConnectionInfo,
        max_connections: int,
        registry_db_name: str,
        routing_strategy: RoutingStrategy = None,
        migration_timeout_s: float = 60,
        migration_throttle_s: float = 0.1,
    ):
        if routing_strategy is None:
            routing_strategy = RoutingStrategy()
        self._routing_strategy = routing_strategy
        self._connection_info = connection_info
        self._max_connections = max_connections
        self._registry_db_name = registry_db_name
        self._migration_timeout_s = migration_timeout_s
        self._migration_throttle_s = migration_throttle_s
        # Let's try to make the most of the pool while not opening to many connections
        self._pool_manager = PoolManager(
            self._pool_factory, min_size=1, max_size=self._max_connections
        )
        self._task_meta: dict[str, tuple[str, str]] = dict()
        self._known_dbs: set[str] = set()
        self._exit_stack = AsyncExitStack()

    async def __aenter__(self):
        await self._exit_stack.enter_async_context(self._pool_manager)
        await self._refresh_dbs()
        for db_name in self._known_dbs:
            await self.init_database(db_name)

    async def _refresh_dbs(self):
        base_pool = await self._pool_manager.get_pool("")
        async with base_pool.connection() as conn:
            await create_databases_registry_db(conn, self._registry_db_name)
        registry_pool = await self._pool_manager.get_pool(self._registry_db_name)
        async with registry_pool.connection() as conn:
            self._known_dbs.update(await retrieve_dbs(conn))

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    async def save_task_(self, task: Task, group: str | None) -> bool:
        db_name = self._routing_strategy.postgres_db(group)
        if db_name not in self._known_dbs:
            await self._refresh_dbs()
            await self._ensure_db(db_name)
        pool = await self._pool_manager.get_pool(db_name)
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                async with conn.transaction():
                    task_exists = await _task_exists(cur, task.id)
                    if task_exists:
                        await _update_task(cur, task)
                    else:
                        await _insert_task(cur, task, group)
        self._task_meta[task.id] = (db_name, group)
        is_new = not task_exists
        return is_new

    async def save_result(self, result: ResultEvent):
        task_db = await self._get_task_db(result.task_id)
        params = result.model_dump(
            include={TASK_RESULT_TASK_ID, TASK_RESULT_RESULT, TASK_RESULT_CREATED_AT},
            exclude={ResultEvent.registry_key.default},
        )
        pool = await self._pool_manager.get_pool(task_db)
        async with pool.connection() as conn:
            params[TASK_RESULT_RESULT] = ujson.dumps(
                params[TASK_RESULT_RESULT][TASK_RESULT_RESULT_VALUE]
            )
            async with conn.cursor() as cur:
                await cur.execute(_INSERT_RESULT_QUERY, params)

    async def save_error(self, error: ErrorEvent):
        task_db = await self._get_task_db(error.task_id)
        pool = await self._pool_manager.get_pool(task_db)
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await _insert_error(cur, error)

    async def get_task(self, task_id: str) -> Task:
        task_db = await self._get_task_db(task_id)
        pool = await self._pool_manager.get_pool(task_db)
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=Task.postgres_row_factory) as cur:
                await cur.execute(_GET_TASK_QUERY, (task_id,))
                tasks = await cur.fetchall()
        if not tasks:
            raise UnknownTask(task_id)
        if len(tasks) != 1:
            raise ValueError(f"found several task with id {task_id}")
        return tasks[0]

    async def get_tasks(
        self,
        group: str | None,
        *,
        task_name: str | None = None,
        state: list[TaskState] | TaskState | None = None,
        **kwargs,
    ) -> list[Task]:
        tasks_db = self._routing_strategy.postgres_db(group)
        pool = await self._pool_manager.get_pool(tasks_db)
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=Task.postgres_row_factory) as cur:
                tasks = [
                    t
                    async for t in _get_tasks(
                        cur, group=group, task_name=task_name, state=state
                    )
                ]
        return tasks

    async def get_task_errors(self, task_id: str) -> list[ErrorEvent]:
        tasks_db = await self._get_task_db(task_id)
        pool = await self._pool_manager.get_pool(tasks_db)
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=ErrorEvent.postgres_row_factory) as cur:
                await cur.execute(_GET_TASK_ERRORS_QUERY, (task_id,))
                errors = await cur.fetchall()
        return errors

    async def get_task_result(self, task_id: str) -> ResultEvent:
        tasks_db = await self._get_task_db(task_id)
        pool = await self._pool_manager.get_pool(tasks_db)
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=ResultEvent.postgres_row_factory) as cur:
                await cur.execute(_GET_TASK_RESULT_QUERY, (task_id,))
                res = await cur.fetchone()
        return res

    async def get_task_group(self, task_id: str) -> str | None:
        tasks_db = await self._get_task_db(task_id)
        pool = await self._pool_manager.get_pool(tasks_db)
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(_GET_TASK_GROUP_QUERY, (task_id,))
                ns = await cur.fetchone()
        if ns is None:
            raise UnknownTask(task_id)
        ns = ns["task_group"]
        return ns

    async def init_database(self, db_name: str):
        await init_database(
            db_name,
            self._pool_manager.get_pool,
            registry_db_name=self._registry_db_name,
            connection_info=self._connection_info,
            migration_timeout_s=self._migration_timeout_s,
            migration_throttle_s=self._migration_throttle_s,
        )

    async def get_health(self) -> bool:
        try:
            registry_pool = await self._pool_manager.get_pool(self._registry_db_name)
            async with registry_pool.connection() as conn:
                health_query = sql.SQL("SELECT 1;")
                async with conn.cursor() as cur:
                    await cur.execute(health_query)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("postgres health check failed: %s", e)
            return False
        return True

    async def _get_task_db(self, task_id: str) -> str:
        if task_id not in self._task_meta:
            await self._refresh_task_meta()
        try:
            return self._task_meta[task_id][0]
        except KeyError as e:
            raise UnknownTask(task_id) from e

    async def _refresh_task_meta(self):
        await self._refresh_dbs()
        for db_name in self._known_dbs:
            pool = await self._pool_manager.get_pool(db_name)
            db_meta = dict()
            async with pool.connection() as conn:
                async for task_meta in _tasks_meta(conn):
                    db_meta[task_meta[TASK_ID]] = (
                        db_name,
                        task_meta[POSTGRES_TASKS_GROUP],
                    )
            self._task_meta.update(db_meta)

    async def _pool_factory(
        self, key: str, *, min_size: int, max_size: int
    ) -> AsyncConnectionPool:
        kwargs = copy(self._connection_info.kwargs)
        if key is not None:
            kwargs["dbname"] = key
        kwargs["cursor_factory"] = AsyncClientCursor
        # Use autocommit transactions to avoid psycopg creating transactions at each
        # statement and keeping them open forever:
        # https://www.psycopg.org/psycopg3/docs/basic/transactions.html
        kwargs["autocommit"] = True
        pool = AsyncConnectionPool(
            kwargs=kwargs,
            check=AsyncConnectionPool.check_connection,
            min_size=min_size,
            max_size=max_size,
            num_workers=1,
        )
        return pool

    async def _ensure_db(self, db_name):
        if db_name not in self._known_dbs:
            await self.init_database(db_name)
            self._known_dbs.add(db_name)

    @classmethod
    def _from_config(cls, config: PostgresStorageConfig, **extras) -> PostgresStorage:
        as_dict = config.model_dump()
        as_dict["registry_db_name"] = config.registry_db_name
        conn_info = {
            k: v for k, v in as_dict.items() if k in PostgresConnectionInfo.__fields__
        }
        kwargs = {k: v for k, v in as_dict.items() if k not in conn_info}
        kwargs.update(extras)
        connection_info = PostgresConnectionInfo(**conn_info)
        return cls(connection_info=connection_info, **kwargs)


async def _task_exists(cur: AsyncCursor, task_id: str) -> bool:
    await cur.execute(_TASK_EXISTS_QUERY, (task_id,))
    count = await cur.fetchone()
    count = count["n_tasks"]
    return count > 0


async def _insert_task(cur: AsyncClientCursor, task: Task, group: str | None):
    task_as_dict = task.model_dump(exclude={Task.registry_key.default})
    task_as_dict[POSTGRES_TASKS_GROUP] = group
    task_as_dict[TASK_ARGS] = ujson.dumps(task.args)
    col_names = sql.SQL(", ").join(sql.Identifier(n) for n in task_as_dict)
    col_value_placeholders = sql.SQL(", ").join(
        sql.Placeholder(n) for n in task_as_dict
    )
    query = sql.SQL("INSERT INTO {tasks} ({}) VALUES ({})").format(
        col_names,
        col_value_placeholders,
        tasks=sql.Identifier(POSTGRES_TASKS_TABLE),
    )
    await cur.execute(query, task_as_dict)


async def _get_tasks(
    cur: AsyncCursor,
    *,
    group: str | None,
    task_name: str | None,
    state: list[TaskState] | TaskState | None,
    chunk_size=10,
) -> AsyncGenerator[Task, None]:
    # pylint: disable=unused-argument
    where = []
    if group is not None:
        where_group = sql.SQL("task.{} = {}").format(
            sql.Identifier(POSTGRES_TASKS_GROUP), sql.Literal(group)
        )
        where.append(where_group)
    if task_name is not None:
        where_task_name = sql.SQL("task.{} = {}").format(
            sql.Identifier(TASK_NAME), sql.Literal(task_name)
        )
        where.append(where_task_name)
    if state is not None:
        if isinstance(state, TaskState):
            state = [state]
        where_state = sql.SQL("task.{} IN ({})").format(
            sql.Identifier(TASK_STATE),
            sql.SQL(", ").join(sql.Literal(s.value) for s in state),
        )
        where.append(where_state)
    order_by = sql.SQL("ORDER BY task.{} DESC").format(
        sql.Identifier(TASK_RESULT_CREATED_AT)
    )
    query = [_BASE_GET_TASKS_QUERY]
    if where:
        query.append(sql.SQL("WHERE {}").format(sql.SQL(" AND ").join(where)))
    query.append(order_by)
    query = sql.SQL("\n").join(query)
    # TODO: pass the above chunksize, when upgrading to a new version of psycopg
    async for task in cur.stream(query, size=1):
        yield task


async def _insert_error(cur: AsyncClientCursor, error: ErrorEvent):
    error_as_dict = error.model_dump(exclude_none=True)
    error_as_dict.update(error_as_dict.pop("error"))
    error_as_dict.pop(TaskError.registry_key.default)
    error_as_dict["stacktrace"] = ujson.dumps(error_as_dict["stacktrace"])
    col_names = sql.SQL(", ").join(sql.Identifier(n) for n in error_as_dict)
    col_value_placeholders = sql.SQL(", ").join(
        sql.Placeholder(n) for n in error_as_dict
    )
    query = sql.SQL("INSERT INTO {errors} ({}) VALUES ({})").format(
        col_names,
        col_value_placeholders,
        errors=sql.Identifier(POSTGRES_TASK_ERRORS_TABLE),
    )
    await cur.execute(query, error_as_dict)


async def _update_task(cur: AsyncCursor, task: Task):
    task_update = TaskUpdate.from_task(task).model_dump(exclude_none=True)
    updates = sql.SQL(", ").join(
        sql.SQL("{} = {}").format(sql.Identifier(col), sql.Placeholder(col))
        for col in task_update
    )
    update_query = sql.SQL(
        "UPDATE {task_table} SET {} WHERE {task_id_col} = {task_id}"
    ).format(
        updates,
        task_table=sql.Identifier(POSTGRES_TASKS_TABLE),
        task_id_col=sql.Identifier(TASK_ID),
        task_id=sql.Literal(task.id),
    )
    await cur.execute(update_query, task_update)


async def retrieve_dbs(conn: AsyncConnection) -> list[str]:
    list_dbs = sql.SQL("SELECT db.{} AS db_name FROM {} AS db;").format(
        sql.Identifier(POSTGRES_TASK_DB_NAME), sql.Identifier(POSTGRES_TASK_DBS_TABLE)
    )
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(list_dbs)
        dbs = [row["db_name"] async for row in cur]
    return dbs


async def _tasks_meta(conn: AsyncConnection) -> AsyncGenerator[dict, None]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_TASK_META_QUERY)
        async for row in cur:
            yield row


@asynccontextmanager
async def _migration_lock(
    registry_conn: AsyncConnection,
    db_name: str,
    *,
    timeout_s: float,
    throttle_s: float,
):
    start = time.time()
    while time.time() - start < timeout_s:
        async with registry_conn.cursor() as cur:
            async with registry_conn.transaction():
                await cur.execute(_ACQUIRE_MIGRATION_LOCK, (db_name,))
                locked = await cur.fetchone()
            try:
                if locked:
                    yield
                    return
            finally:
                await cur.execute(_RELEASE_MIGRATION_LOCK, (db_name,))
            logger.debug("failed to acquire lock for %s, sleeping...", db_name)
            await asyncio.sleep(throttle_s)

    raise RuntimeError(
        f"Failed to acquire migration lock in less than {timeout_s} seconds."
        f"Another migration might be in progress, if it's not the case please remove"
        f" the migration lock from the {registry_conn.info.dbname} DB"
    )


async def init_database(
    db_name: str,
    pool_factory: Callable[[str], Awaitable[AsyncConnectionPool]],
    *,
    registry_db_name: str,
    connection_info: PostgresConnectionInfo,
    migration_timeout_s: float,
    migration_throttle_s: float,
):
    # Create DB
    default_db = ""
    base_pool = await pool_factory(default_db)
    async with base_pool.connection() as base_conn:
        async with base_conn.cursor() as cur:
            old_autocommit = base_conn.autocommit
            await base_conn.set_autocommit(True)
            try:
                await cur.execute(
                    sql.SQL("CREATE DATABASE {table};").format(
                        table=sql.Identifier(db_name)
                    )
                )
            except DuplicateDatabase:
                pass
            await base_conn.set_autocommit(old_autocommit)
    registry_pool = await pool_factory(registry_db_name)
    async with registry_pool.connection() as registry_conn:
        await _insert_db_into_registry(registry_conn, db_name=db_name)
        async with _migration_lock(
            registry_conn,
            db_name,
            timeout_s=migration_timeout_s,
            throttle_s=migration_throttle_s,
        ):
            # Migrate it
            migrate(connection_info, db_name, timeout_s=migration_timeout_s)
    logger.info("database %s successfully initialized", db_name)


async def _insert_db_into_registry(registry_con: AsyncConnection, db_name: str):
    async with registry_con.cursor() as cur:
        query = sql.SQL(
            """INSERT INTO {} ({}, {}) VALUES (%s, %s)
ON CONFLICT DO NOTHING;"""
        ).format(
            sql.Identifier(POSTGRES_TASK_DBS_TABLE),
            sql.Identifier(POSTGRES_TASK_DB_NAME),
            sql.Identifier(POSTGRES_TASK_DB_IS_LOCKED),
        )
        await cur.execute(query, (db_name, False))


async def create_databases_registry_db(conn: AsyncConnection, registry_db_name: str):
    async with conn.cursor() as cur:
        old_autocommit = conn.autocommit
        await conn.set_autocommit(True)
        try:
            await cur.execute(
                sql.SQL("CREATE DATABASE {};").format(sql.Identifier(registry_db_name))
            )
        except DuplicateDatabase:
            return
        finally:
            await conn.set_autocommit(old_autocommit)
    params = conn.info.get_parameters()
    params["dbname"] = registry_db_name
    params["password"] = conn.info.password
    conn_info = make_conninfo(**params)
    db_conn = await AsyncConnection.connect(conn_info)
    async with db_conn:
        async with db_conn.cursor() as cur:
            await cur.execute(_CREATE_REGISTRY_TASK_TABLE)


_GET_TASK_QUERY = sql.SQL("SELECT * FROM {} AS task WHERE task.{task_id} = %s").format(
    sql.Identifier(POSTGRES_TASKS_TABLE), task_id=sql.Identifier(TASK_ID)
)

_BASE_GET_TASKS_QUERY = sql.SQL("SELECT * FROM {} AS task").format(
    sql.Identifier(POSTGRES_TASKS_TABLE)
)

_GET_TASK_RESULT_QUERY = sql.SQL(
    "SELECT * FROM {} AS res WHERE res.{task_id} = %s"
).format(
    sql.Identifier(POSTGRES_TASK_RESULTS_TABLE),
    task_id=sql.Identifier(TASK_RESULT_TASK_ID),
)

_GET_TASK_ERRORS_QUERY = sql.SQL(
    "SELECT * FROM {} AS error WHERE error.{task_id} = %s"
).format(
    sql.Identifier(POSTGRES_TASK_ERRORS_TABLE),
    task_id=sql.Identifier(TASK_ERRORS_TASK_ID),
)

_TASK_EXISTS_QUERY = sql.SQL(
    """SELECT COUNT(t.{task_id}) AS n_tasks
FROM {task_table} AS t
WHERE t.{task_id} = %s
"""
).format(
    task_id=sql.Identifier(TASK_ID), task_table=sql.Identifier(POSTGRES_TASKS_TABLE)
)

_TASK_META_QUERY = sql.SQL(
    "SELECT t.{task_id}, t.{task_group} FROM {task_table} AS t;"
).format(
    task_id=sql.Identifier(TASK_ID),
    task_group=sql.Identifier(POSTGRES_TASKS_GROUP),
    task_table=sql.Identifier(POSTGRES_TASKS_TABLE),
)

_GET_TASK_GROUP_QUERY = sql.SQL(
    "SELECT t.{} AS task_group FROM {} AS t WHERE t.{} = %s;"
).format(
    sql.Identifier(POSTGRES_TASKS_GROUP),
    sql.Identifier(POSTGRES_TASKS_TABLE),
    sql.Identifier(TASK_ID),
)

_INSERT_RESULT_QUERY = sql.SQL(
    """INSERT INTO {res_table} ({task_id_col}, {res_col}, {res_created_at_col})
VALUES ({task_id}, {res}, {res_created_at});
"""
).format(
    res_table=sql.Identifier(POSTGRES_TASK_RESULTS_TABLE),
    task_id_col=sql.Identifier(TASK_RESULT_TASK_ID),
    res_created_at_col=sql.Identifier(TASK_RESULT_CREATED_AT),
    res_col=sql.Identifier(TASK_RESULT_RESULT),
    task_id=sql.Placeholder(TASK_RESULT_TASK_ID),
    res=sql.Placeholder(TASK_RESULT_RESULT),
    res_created_at=sql.Placeholder(TASK_RESULT_CREATED_AT),
)

_CREATE_REGISTRY_TASK_TABLE = sql.SQL(
    """CREATE TABLE {task_table} (
    {name} varchar(128),
    {is_locked} boolean,
    PRIMARY KEY({name})
);
"""
).format(
    task_table=sql.Identifier(POSTGRES_TASK_DBS_TABLE),
    name=sql.Identifier(POSTGRES_TASK_DB_NAME),
    is_locked=sql.Identifier(POSTGRES_TASK_DB_IS_LOCKED),
)

_ACQUIRE_MIGRATION_LOCK = sql.SQL(
    """UPDATE {task_table} AS t SET {is_locked} = TRUE
WHERE t.{name} = %s
RETURNING t.{name};"""
).format(
    task_table=sql.Identifier(POSTGRES_TASK_DBS_TABLE),
    name=sql.Identifier(POSTGRES_TASK_DB_NAME),
    is_locked=sql.Identifier(POSTGRES_TASK_DB_IS_LOCKED),
)

_RELEASE_MIGRATION_LOCK = sql.SQL(
    "UPDATE {task_table} SET {is_locked} = FALSE WHERE {name} = %s;"
).format(
    task_table=sql.Identifier(POSTGRES_TASK_DBS_TABLE),
    name=sql.Identifier(POSTGRES_TASK_DB_NAME),
    is_locked=sql.Identifier(POSTGRES_TASK_DB_IS_LOCKED),
)
