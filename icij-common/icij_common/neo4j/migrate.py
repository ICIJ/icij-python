from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Coroutine
from datetime import datetime
from distutils.version import StrictVersion
from enum import Enum, unique
from inspect import signature
from typing import Any, Callable, List, Optional, Sequence, Union

import neo4j
from neo4j.exceptions import ConstraintError
from pydantic import Field

from icij_common.neo4j.constants import (
    MIGRATION_COMPLETED,
    MIGRATION_LABEL,
    MIGRATION_NODE,
    MIGRATION_DB,
    MIGRATION_STARTED,
    MIGRATION_STATUS,
    MIGRATION_VERSION,
)
from icij_common.pydantic_utils import NoEnumModel
from .db import (
    Database,
    create_db,
    create_database_tx,
    db_specific_session,
    databases_tx,
    registry_db_session,
)

logger = logging.getLogger(__name__)

TransactionFn = Callable[[neo4j.AsyncTransaction], Coroutine]
ExplicitTransactionFn = Callable[[neo4j.Session], Coroutine]
MigrationFn = Union[TransactionFn, ExplicitTransactionFn]

_MIGRATION_TIMEOUT_MSG = """Migration timeout expired !
Please check that a migration is indeed in progress. If the application is in a \
deadlock restart it forcing the migration index cleanup."""


class MigrationError(RuntimeError):
    pass


@unique
class MigrationStatus(str, Enum):
    IN_PROGRESS = "IN_PROGRESS"
    DONE = "DONE"


class MigrationVersion(StrictVersion):
    @classmethod
    def __get_validators__(cls):
        def validator(v: Any) -> MigrationVersion:
            if isinstance(v, (str, MigrationVersion)):
                return MigrationVersion(v)
            raise ValueError(
                f"Must be a {MigrationVersion.__name__} or a {str.__name__}, "
                f"found {type(v)}"
            )

        yield validator


class _BaseMigration(NoEnumModel):
    version: MigrationVersion
    label: str
    status: MigrationStatus = MigrationStatus.IN_PROGRESS


class Neo4jMigration(_BaseMigration):
    # It would have been cleaner to create
    # (p:_Project)-[:_RUNS { id: p.name + m.version }]->(m:_Migration)
    # relationships. However, neo4j < 5.7 doesn't support unique constraint on
    # relationships properties which prevents from implementing the locking mechanism
    # properly. We hence enforce unique constraint on
    # (_Migration.version, _Migration.project)
    db: str = Field(alias="project")
    started: datetime
    completed: Optional[datetime] = None
    status: MigrationStatus = MigrationStatus.IN_PROGRESS

    @classmethod
    def from_neo4j(cls, record: neo4j.Record, key="migration") -> Neo4jMigration:
        migration = dict(record.value(key))
        if "started" in migration:
            migration["started"] = migration["started"].to_native()
        if "completed" in migration:
            migration["completed"] = migration["completed"].to_native()
        return Neo4jMigration(**migration)


class Migration(_BaseMigration):
    migration_fn: MigrationFn


MigrationRegistry: Sequence[Migration]


async def _migrate_with_lock(
    *,
    db_session: neo4j.AsyncSession,
    registry_session: neo4j.AsyncSession,
    db: str,
    migration: Migration,
):
    # Note: all migrations.py should be carefully tested otherwise they will lock
    # the DB...

    # Lock the DB first, raising in case a migration already exists
    logger.debug("Trying to run migration to %s...", migration.label)
    await registry_session.execute_write(
        create_migration_tx,
        db=db,
        migration_version=str(migration.version),
        migration_label=migration.label,
    )
    # Then run to migration
    logger.debug("Acquired write lock to %s !", migration.label)
    sig = signature(migration.migration_fn)
    first_param = list(sig.parameters)[0]
    if first_param == "tx":
        await db_session.execute_write(migration.migration_fn)
    elif first_param == "sess":
        await migration.migration_fn(db_session)
    else:
        raise ValueError(f"Invalid migration function: {migration.migration_fn}")
    # Finally free the lock
    await registry_session.execute_write(
        complete_migration_tx,
        db=db,
        migration_version=str(migration.version),
    )
    logger.debug("Marked %s as complete !", migration.label)


async def create_migration_tx(
    tx: neo4j.AsyncTransaction,
    *,
    db: str,
    migration_version: str,
    migration_label: str,
) -> Neo4jMigration:
    query = f"""CREATE (m:{MIGRATION_NODE} {{
    {MIGRATION_DB}: $db,
    {MIGRATION_LABEL}: $label,
    {MIGRATION_VERSION}: $version,
    {MIGRATION_STATUS}: $status,
    {MIGRATION_STARTED}: $started
}})
RETURN m as migration"""
    res = await tx.run(
        query,
        label=migration_label,
        version=migration_version,
        db=db,
        status=MigrationStatus.IN_PROGRESS.value,
        started=datetime.now(),
    )
    migration = await res.single()
    if migration is None:
        raise ValueError(f"Couldn't find migration {migration_version} for {db}")
    migration = Neo4jMigration.from_neo4j(migration)
    return migration


async def complete_migration_tx(
    tx: neo4j.AsyncTransaction, *, db: str, migration_version: str
) -> Neo4jMigration:
    query = f"""MATCH (m:{MIGRATION_NODE} {{
        {MIGRATION_VERSION}: $version,
        {MIGRATION_DB}: $db
     }})
SET m += {{ {MIGRATION_STATUS}: $status, {MIGRATION_COMPLETED}: $completed }} 
RETURN m as migration"""
    res = await tx.run(
        query,
        version=migration_version,
        db=db,
        status=MigrationStatus.DONE.value,
        completed=datetime.now(),
    )
    migration = await res.single()
    migration = Neo4jMigration.from_neo4j(migration)
    return migration


async def db_migrations_tx(tx: neo4j.AsyncTransaction, db: str) -> List[Neo4jMigration]:
    query = f"""MATCH (m:{MIGRATION_NODE} {{ {MIGRATION_DB}: $db }})
RETURN m as migration
"""
    res = await tx.run(query, db=db)
    migrations = [Neo4jMigration.from_neo4j(rec) async for rec in res]
    return migrations


async def delete_all_migrations(driver: neo4j.AsyncDriver):
    query = f"""MATCH (m:{MIGRATION_NODE})
DETACH DELETE m"""
    async with registry_db_session(driver) as sess:
        await sess.run(query)


async def retrieve_dbs(neo4j_driver: neo4j.AsyncDriver) -> List[Database]:
    async with registry_db_session(neo4j_driver) as sess:
        dbs = await sess.execute_read(databases_tx)
    return dbs


async def migrate_db_schemas(
    neo4j_driver: neo4j.AsyncDriver,
    registry: MigrationRegistry,
    *,
    timeout_s: float,
    throttle_s: float,
):
    dbs = await retrieve_dbs(neo4j_driver)
    tasks = [
        migrate_db_schema(
            neo4j_driver,
            registry,
            db=p.name,
            timeout_s=timeout_s,
            throttle_s=throttle_s,
        )
        for p in dbs
    ]
    await asyncio.gather(*tasks)


async def migrate_db_schema(
    neo4j_driver: neo4j.AsyncDriver,
    registry: MigrationRegistry,
    db: str,
    *,
    timeout_s: float,
    throttle_s: float,
):
    logger.info("Migrating DB %s", db)
    start = time.monotonic()
    if not registry:
        return
    todo = sorted(registry, key=lambda m: m.version)
    async with registry_db_session(neo4j_driver) as registry_sess:
        async with db_specific_session(neo4j_driver, db=db) as db_session:
            while "Waiting for DB to be migrated or for a timeout":
                migrations = await registry_sess.execute_read(db_migrations_tx, db=db)
                in_progress = [
                    m for m in migrations if m.status is MigrationStatus.IN_PROGRESS
                ]
                if len(in_progress) > 1:
                    raise MigrationError(
                        f"Found several migration in progress: {in_progress}"
                    )
                if in_progress:
                    logger.info(
                        "Found that %s is in progress, waiting for %s seconds...",
                        in_progress[0].label,
                        throttle_s,
                    )
                    await asyncio.sleep(throttle_s)
                else:
                    done = [m for m in migrations if m.status is MigrationStatus.DONE]
                    if done:
                        current_version = max((m.version for m in done))
                        todo = [m for m in todo if m.version > current_version]
                    if not todo:
                        break
                    try:
                        await _migrate_with_lock(
                            db_session=db_session,
                            registry_session=registry_sess,
                            db=db,
                            migration=todo[0],
                        )
                        todo = todo[1:]
                        continue
                    except ConstraintError:
                        logger.info(
                            "Migration %s has just started somewhere else, "
                            " waiting for %s seconds...",
                            todo[0].label,
                            throttle_s,
                        )
                        await asyncio.sleep(throttle_s)
                elapsed = time.monotonic() - start
                if elapsed > timeout_s:
                    logger.error(_MIGRATION_TIMEOUT_MSG)
                    raise MigrationError(_MIGRATION_TIMEOUT_MSG)
                continue


async def init_database(
    neo4j_driver: neo4j.AsyncDriver,
    name: str,
    registry: MigrationRegistry,
    *,
    timeout_s: float,
    throttle_s: float,
) -> bool:
    # Create DB
    await create_db(neo4j_driver, db=name)

    # Record the DB in the registry
    async with registry_db_session(neo4j_driver) as sess:
        db, already_exists = await sess.execute_write(create_database_tx, name=name)

    # Migrate it
    await migrate_db_schema(
        neo4j_driver,
        registry=registry,
        db=db.name,
        timeout_s=timeout_s,
        throttle_s=throttle_s,
    )

    return already_exists
