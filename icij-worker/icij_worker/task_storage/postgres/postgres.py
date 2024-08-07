# pylint: disable=c-extension-no-member
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from copy import copy
from typing import (
    AsyncGenerator,
    Awaitable,
    Callable,
    ClassVar,
    Dict,
    Generic,
    List,
    Optional,
    OrderedDict,
    Set,
    Tuple,
    TypeVar,
    Union,
)

import ujson
from psycopg import AsyncClientCursor, AsyncConnection, AsyncCursor, sql
from psycopg.conninfo import make_conninfo
from psycopg.errors import DuplicateDatabase
from psycopg.rows import dict_row

from constants import (
    POSTGRES_TASKS_TABLE,
    POSTGRES_TASK_DBS_TABLE,
    POSTGRES_TASK_DB_IS_LOCKED,
    POSTGRES_TASK_DB_NAME,
    POSTGRES_TASK_ERRORS_TABLE,
    POSTGRES_TASK_RESULTS_TABLE,
    TASK_ARGUMENTS,
    TASK_ERRORS_TASK_ID,
    TASK_ID,
    TASK_NAME,
    TASK_NAMESPACE,
    TASK_RESULT_CREATED_AT,
    TASK_RESULT_RESULT,
    TASK_RESULT_TASK_ID,
    TASK_STATE,
)
from icij_worker import Namespacing, ResultEvent, Task, TaskState
from icij_worker.exceptions import UnknownTask
from icij_worker.objects import ErrorEvent, TaskError, TaskUpdate
from icij_worker.task_storage import TaskStorage, TaskStorageConfig
from icij_worker.task_storage.postgres.connection_info import PostgresConnectionInfo
from icij_worker.task_storage.postgres.db_mate import migrate

logger = logging.getLogger(__name__)


C = TypeVar("C", bound="AsyncContextManager")
ConnectionFactory = Callable[[str], C]


class PostgresStorageConfig(PostgresConnectionInfo, TaskStorageConfig):
    registry_db_name: ClassVar[str] = "task_dbs_registry"

    max_connections: int = 1
    migration_timeout_s: float = 60.0
    migration_throttle_s: float = 0.1

    def to_storage(self, namespacing: Optional[Namespacing]) -> PostgresStorage:
        if namespacing is None:
            namespacing = Namespacing()
        self_as_connection_info = {
            k: v
            for k, v in self.dict().items()
            if k in PostgresConnectionInfo.__fields__
        }
        storage = PostgresStorage(
            connection_info=PostgresConnectionInfo(**self_as_connection_info),
            namespacing=namespacing,
            registry_db_name=self.registry_db_name,
            max_connections=self.max_connections,
            migration_timeout_s=self.migration_timeout_s,
            migration_throttle_s=self.migration_throttle_s,
        )
        return storage


class ConnectionManager(OrderedDict[str, C], Generic[C], AbstractAsyncContextManager):
    def __init__(self, conn_factory: ConnectionFactory, max_connections: int):
        self._max_connections = max_connections
        self._conn_factory = conn_factory
        super().__init__()

    async def get_connection(self, key: str) -> C:
        create_conn = False
        conn = None
        try:
            conn = self[key]
        except KeyError:
            create_conn = True
        if create_conn:
            while self._max_connections - len(self) < 1:
                old_key = next(iter(self))
                old_conn = super().__getitem__(old_key)
                await old_conn.__aexit__(None, None, None)
                super().__delitem__(old_key)
            conn = await self._conn_factory(key)
            conn = await conn.__aenter__()  # pylint: disable=unnecessary-dunder-call
            super().__setitem__(key, conn)
        super().move_to_end(key)
        return conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for conn in self.values():
            await conn.__aexit__(exc_type, exc_val, exc_tb)
        for k in self:
            del self[k]


class PostgresStorage(TaskStorage):
    def __init__(
        self,
        connection_info: PostgresConnectionInfo,
        max_connections: int,
        registry_db_name: str,
        namespacing: Namespacing = None,
        migration_timeout_s: float = 60,
        migration_throttle_s: float = 0.1,
    ):
        if namespacing is None:
            namespacing = Namespacing()
        self._namespacing = namespacing
        self._connection_info = connection_info
        self._max_connections = max_connections
        self._registry_db_name = registry_db_name
        self._migration_timeout_s = migration_timeout_s
        self._migration_throttle_s = migration_throttle_s
        # Let's try to make the most of the pool while not opening to many connections
        self._conn_manager = ConnectionManager[AsyncConnection](
            self._conn_factory, max_connections=self._max_connections
        )
        self._task_meta: Dict[str, Tuple[str, str]] = dict()
        self._known_dbs: Set[str] = set()
        self._exit_stack = AsyncExitStack()

    async def __aenter__(self):
        await self._exit_stack.enter_async_context(self._conn_manager)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    async def save_task_(self, task: Task, namespace: Optional[str]) -> bool:
        db_name = self._namespacing.postgres_db(namespace)
        if db_name not in self._known_dbs:
            await self._ensure_db(db_name)
        conn = await self._conn_manager.get_connection(db_name)
        async with conn.cursor(row_factory=dict_row) as cur:
            async with conn.transaction():
                task_exists = await _task_exists(cur, task.id)
                if task_exists:
                    await _update_task(cur, task)
                else:
                    await _insert_task(cur, task, namespace)
        self._task_meta[task.id] = (db_name, namespace)
        return task_exists

    async def save_result(self, result: ResultEvent):
        task_db = await self._get_task_db(result.task_id)
        conn = await self._conn_manager.get_connection(task_db)
        params = result.dict(
            include={TASK_RESULT_TASK_ID, TASK_RESULT_RESULT, TASK_RESULT_CREATED_AT},
            exclude={ResultEvent.registry_key.default},
        )
        params[TASK_RESULT_RESULT] = ujson.dumps(params[TASK_RESULT_RESULT])
        async with conn.cursor() as cur:
            await cur.execute(_INSERT_RESULT_QUERY, params)

    async def save_error(self, error: ErrorEvent):
        task_db = await self._get_task_db(error.task_id)
        conn = await self._conn_manager.get_connection(task_db)
        async with conn.cursor(row_factory=dict_row) as cur:
            await _insert_error(cur, error)

    async def get_task(self, task_id: str) -> Task:
        task_db = await self._get_task_db(task_id)
        conn = await self._conn_manager.get_connection(task_db)
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
        namespace: Optional[str],
        *,
        task_name: Optional[str] = None,
        state: Optional[Union[List[TaskState], TaskState]] = None,
        **kwargs,
    ) -> List[Task]:
        tasks_db = self._namespacing.postgres_db(namespace)
        conn = await self._conn_manager.get_connection(tasks_db)
        async with conn.cursor(row_factory=Task.postgres_row_factory) as cur:
            tasks = [
                t
                async for t in _get_tasks(
                    cur, namespace=namespace, task_name=task_name, state=state
                )
            ]
            return tasks

    async def get_task_errors(self, task_id: str) -> List[ErrorEvent]:
        tasks_db = await self._get_task_db(task_id)
        conn = await self._conn_manager.get_connection(tasks_db)
        async with conn.cursor(row_factory=ErrorEvent.postgres_row_factory) as cur:
            await cur.execute(_GET_TASK_ERRORS_QUERY, (task_id,))
            errors = await cur.fetchall()
        return errors

    async def get_task_result(self, task_id: str) -> ResultEvent:
        tasks_db = await self._get_task_db(task_id)
        conn = await self._conn_manager.get_connection(tasks_db)
        async with conn.cursor(row_factory=ResultEvent.postgres_row_factory) as cur:
            await cur.execute(_GET_TASK_RESULT_QUERY, (task_id,))
            res = await cur.fetchone()
        return res

    async def get_task_namespace(self, task_id: str) -> Optional[str]:
        tasks_db = await self._get_task_db(task_id)
        conn = await self._conn_manager.get_connection(tasks_db)
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(_GET_TASK_NAMESPACE_QUERY, (task_id,))
            ns = await cur.fetchone()
        if ns is None:
            raise UnknownTask(task_id)
        ns = ns["task_ns"]
        return ns

    async def init_database(self, db_name: str):
        await init_database(
            db_name,
            self._conn_manager.get_connection,
            registry_db_name=self._registry_db_name,
            connection_info=self._connection_info,
            migration_timeout_s=self._migration_timeout_s,
            migration_throttle_s=self._migration_throttle_s,
        )

    async def _get_task_db(self, task_id: str) -> str:
        if task_id not in self._task_meta:
            await self._refresh_task_meta()
        try:
            return self._task_meta[task_id][0]
        except KeyError as e:
            raise UnknownTask(task_id) from e

    async def _refresh_task_meta(self):
        some_conn = await self._conn_manager.get_connection(self._registry_db_name)
        self._known_dbs.update(await retrieve_dbs(some_conn))
        for db_name in self._known_dbs:
            conn = await self._conn_manager.get_connection(db_name)
            db_meta = dict()
            async for task_meta in _tasks_meta(conn):
                db_meta[task_meta[TASK_ID]] = (db_name, task_meta[TASK_NAMESPACE])
            self._task_meta.update(db_meta)

    async def _conn_factory(self, db_name: str) -> AsyncConnection:
        kwargs = copy(self._connection_info.kwargs)
        if db_name is not None:
            kwargs["dbname"] = db_name
        # Use autocommit transactions to avoid psycopg creating transactions at each
        # statement and keeping them open forever:
        # https://www.psycopg.org/psycopg3/docs/basic/transactions.html
        conn = await AsyncConnection.connect(
            autocommit=True, cursor_factory=AsyncClientCursor, **kwargs
        )
        return conn

    async def _ensure_db(self, db_name):
        some_conn = await self._conn_manager.get_connection(self._registry_db_name)
        self._known_dbs.update(await retrieve_dbs(some_conn))
        if db_name not in self._known_dbs:
            await self.init_database(db_name)
            self._known_dbs.add(db_name)

    @classmethod
    def _from_config(cls, config: PostgresStorageConfig, **extras) -> PostgresStorage:
        as_dict = config.dict()
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


async def _insert_task(cur: AsyncClientCursor, task: Task, namespace: Optional[str]):
    task_as_dict = task.dict(exclude={Task.registry_key.default})
    task_as_dict[TASK_NAMESPACE] = namespace
    task_as_dict[TASK_ARGUMENTS] = ujson.dumps(task.arguments)
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
    namespace: Optional[str],
    task_name: Optional[str],
    state: Optional[Union[List[TaskState], TaskState]],
    chunk_size=10,
) -> AsyncGenerator[Task, None]:
    # pylint: disable=unused-argument
    where = []
    if namespace is not None:
        where_ns = sql.SQL("task.{} = {}").format(
            sql.Identifier(TASK_NAMESPACE), sql.Literal(namespace)
        )
        where.append(where_ns)
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
    error_as_dict = error.dict()
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
    task_update = TaskUpdate.from_task(task).dict(exclude_none=True)
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


async def retrieve_dbs(conn: AsyncConnection) -> List[str]:
    list_dbs = sql.SQL("SELECT db.{} AS db_name FROM {} AS db;").format(
        sql.Identifier(POSTGRES_TASK_DB_NAME), sql.Identifier(POSTGRES_TASK_DBS_TABLE)
    )
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(list_dbs)
        dbs = [row["db_name"] async for row in cur]
    return dbs


async def _tasks_meta(conn: AsyncConnection) -> AsyncGenerator[Dict, None]:
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
    conn_factory: Callable[[str], Awaitable[AsyncConnection]],
    *,
    registry_db_name: str,
    connection_info: PostgresConnectionInfo,
    migration_timeout_s: float,
    migration_throttle_s: float,
):
    # Create DB
    default_db = ""
    base_conn = await conn_factory(default_db)
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
    await create_databases_registry_db(base_conn, registry_db_name)
    registry_conn = await conn_factory(registry_db_name)
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
    "SELECT t.{task_id}, t.{task_ns} FROM {task_table} AS t;"
).format(
    task_id=sql.Identifier(TASK_ID),
    task_ns=sql.Identifier(TASK_NAMESPACE),
    task_table=sql.Identifier(POSTGRES_TASKS_TABLE),
)

_GET_TASK_NAMESPACE_QUERY = sql.SQL(
    "SELECT t.{} AS task_ns FROM {} AS t WHERE t.{} = %s;"
).format(
    sql.Identifier(TASK_NAMESPACE),
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
