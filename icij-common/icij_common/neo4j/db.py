from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from distutils.version import StrictVersion
from typing import AsyncGenerator, List, Optional, Tuple

import neo4j

from icij_common.neo4j.constants import (
    MIGRATION_NODE,
    MIGRATION_DB,
    MIGRATION_VERSION,
    DATABASE_NAME,
    DATABASE_NODE,
    DATABASE_REGISTRY_DB,
)
from icij_common.pydantic_utils import ICIJModel

logger = logging.getLogger(__name__)

NEO4J_COMMUNITY_DB = "neo4j"
_IS_ENTERPRISE: Optional[bool] = None
_NEO4J_VERSION: Optional[StrictVersion] = None
_SUPPORTS_PARALLEL: Optional[bool] = None

_COMPONENTS_QUERY = """CALL dbms.components() YIELD versions, edition
RETURN versions, edition"""


class Database(ICIJModel):
    name: str

    @classmethod
    def from_neo4j(cls, record: neo4j.Record, key="db") -> Database:
        db = dict(record.value(key))
        return Database(**db)


async def create_databases_registry_db(neo4j_driver: neo4j.AsyncDriver):
    if await is_enterprise(neo4j_driver):
        logger.info("Creating databases registry DB...")
        query = "CREATE DATABASE $registry_db IF NOT EXISTS"
        await neo4j_driver.execute_query(query, registry_db=DATABASE_REGISTRY_DB)
    else:
        logger.info("Using default db as registry DB !")


async def databases_tx(tx: neo4j.AsyncTransaction) -> List[Database]:
    query = f"MATCH (db:{DATABASE_NODE}) RETURN db"
    res = await tx.run(query)
    dbs = [Database.from_neo4j(p) async for p in res]
    return dbs


async def create_db(neo4j_driver: neo4j.AsyncDriver, db: str):
    if await is_enterprise(neo4j_driver):
        db = await db_name(neo4j_driver, db=db)
        query = "CREATE DATABASE $db_name IF NOT EXISTS"
        await neo4j_driver.execute_query(query, db_name=db)


async def add_multidatabase_support_migration_tx(tx: neo4j.AsyncTransaction):
    await create_database_unique_name_constraint_tx(tx)
    await create_migration_unique_database_and_version_constraint_tx(tx)


async def create_database_unique_name_constraint_tx(tx: neo4j.AsyncTransaction):
    # TODO: rename that into constraint_database_unique_name
    constraint_query = f"""CREATE CONSTRAINT constraint_project_unique_name
IF NOT EXISTS
FOR (p:{DATABASE_NODE})
REQUIRE (p.{DATABASE_NAME}) IS UNIQUE
"""
    await tx.run(constraint_query)


async def create_migration_unique_database_and_version_constraint_tx(
    tx: neo4j.AsyncTransaction,
):
    # TODO: rename that into constraint_migration_unique_database_and_version
    constraint_query = f"""CREATE CONSTRAINT
     constraint_migration_unique_project_and_version
IF NOT EXISTS 
FOR (m:{MIGRATION_NODE})
REQUIRE (m.{MIGRATION_VERSION}, m.{MIGRATION_DB}) IS UNIQUE
"""
    await tx.run(constraint_query)


async def create_database_tx(
    tx: neo4j.AsyncTransaction, name: str
) -> Tuple[Database, bool]:
    if name == DATABASE_REGISTRY_DB:
        raise ValueError(
            f'Bad luck, name "{DATABASE_REGISTRY_DB}" is reserved for internal use.'
            f" Can't initialize database"
        )
    query = f"""MERGE (db:{DATABASE_NODE} {{ {DATABASE_NAME}: $name }})
RETURN db"""
    res = await tx.run(query, name=name)
    rec = await res.single()
    summary = await res.consume()
    existed = summary.counters.nodes_created == 0
    database = Database.from_neo4j(rec)
    return database, existed


async def database_registry_db(neo4j_driver: neo4j.AsyncDriver) -> str:
    if await is_enterprise(neo4j_driver):
        return DATABASE_REGISTRY_DB
    return NEO4J_COMMUNITY_DB


async def db_name(neo4j_driver: neo4j.AsyncDriver, db: str) -> str:
    if await is_enterprise(neo4j_driver):
        return db
    return NEO4J_COMMUNITY_DB


def databases_index(db: str) -> str:
    return db


@asynccontextmanager
async def db_specific_session(
    neo4j_driver: neo4j.AsyncDriver, db: str
) -> AsyncGenerator[neo4j.AsyncSession, None]:
    sess_ctx = neo4j_driver.session(database=db)
    async with sess_ctx as sess:
        yield sess


@asynccontextmanager
async def registry_db_session(neo4j_driver: neo4j.AsyncDriver) -> neo4j.AsyncSession:
    session = neo4j_driver.session(database=await database_registry_db(neo4j_driver))
    async with session as sess:
        yield sess


async def _get_components(neo4j_driver: neo4j.AsyncDriver):
    async with neo4j_driver.session(database=neo4j.SYSTEM_DATABASE) as sess:
        res = await sess.run(_COMPONENTS_QUERY)
        res = await res.single()
    global _IS_ENTERPRISE
    global _NEO4J_VERSION
    _IS_ENTERPRISE = res["edition"] != "community"
    _NEO4J_VERSION = StrictVersion(res["versions"][0])


async def server_version(neo4j_driver: neo4j.AsyncDriver) -> StrictVersion:
    if _NEO4J_VERSION is None:
        await _get_components(neo4j_driver)
    return _NEO4J_VERSION


async def supports_parallel_runtime(neo4j_driver: neo4j.AsyncDriver) -> bool:
    global _SUPPORTS_PARALLEL
    if _SUPPORTS_PARALLEL is None:
        version = await server_version(neo4j_driver)
        _SUPPORTS_PARALLEL = version >= StrictVersion("5.13")
    return _SUPPORTS_PARALLEL


async def is_enterprise(neo4j_driver: neo4j.AsyncDriver) -> bool:
    if _IS_ENTERPRISE is None:
        await _get_components(neo4j_driver)
    return _IS_ENTERPRISE
