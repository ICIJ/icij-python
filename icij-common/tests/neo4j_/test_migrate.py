import logging

from datetime import datetime
from unittest import mock

import neo4j
import pytest

from neo4j.exceptions import ClientError
from packaging.version import Version
from pydantic import BaseModel, ConfigDict

import icij_common
from icij_common.neo4j_ import migrate
from icij_common.neo4j_.constants import DATABASE_REGISTRY_DB
from icij_common.neo4j_.db import (
    Database,
    NEO4J_COMMUNITY_DB,
    add_multidatabase_support_migration_tx,
)
from icij_common.neo4j_.migrate import (
    Migration,
    MigrationError,
    MigrationStatus,
    MigrationVersion,
    Neo4jMigration,
    init_database,
    migrate_db_schema,
    migrate_db_schemas,
    retrieve_dbs,
)
from icij_common.neo4j_.test_utils import (
    mock_enterprise_,
    mocked_is_enterprise,
    wipe_db,
)
from icij_common.test_utils import TEST_DB, fail_if_exception

V_0_1_0 = Migration(
    version="0.1.0",
    label="Create databases",
    migration_fn=add_multidatabase_support_migration_tx,
)
_BASE_REGISTRY = [V_0_1_0]


@pytest.fixture(scope="function")
async def _migration_index_and_constraint(
    neo4j_test_driver: neo4j.AsyncDriver,
) -> neo4j.AsyncDriver:
    await init_database(
        neo4j_test_driver,
        NEO4J_COMMUNITY_DB,
        _BASE_REGISTRY,
        timeout_s=30,
        throttle_s=0.1,
    )
    return neo4j_test_driver


async def _create_indexes_tx(tx: neo4j.AsyncTransaction):
    index_query_0 = "CREATE INDEX index0 IF NOT EXISTS FOR (n:Node) ON (n.attribute0)"
    await tx.run(index_query_0)
    index_query_1 = "CREATE INDEX index1 IF NOT EXISTS FOR (n:Node) ON (n.attribute1)"
    await tx.run(index_query_1)


async def _create_indexes(sess: neo4j.AsyncSession):
    index_query_0 = "CREATE INDEX index0 IF NOT EXISTS FOR (n:Node) ON (n.attribute0)"
    await sess.run(index_query_0)
    index_query_1 = "CREATE INDEX index1 IF NOT EXISTS FOR (n:Node) ON (n.attribute1)"
    await sess.run(index_query_1)


async def _drop_constraint_tx(tx: neo4j.AsyncTransaction):
    drop_index_query = "DROP INDEX index0 IF EXISTS"
    await tx.run(drop_index_query)


_MIGRATION_0 = Migration(
    version="0.2.0",
    label="create index and constraint",
    migration_fn=_create_indexes_tx,
)
_MIGRATION_0_EXPLICIT = Migration(
    version="0.2.0",
    label="create index and constraint",
    migration_fn=_create_indexes,
)
_MIGRATION_1 = Migration(
    version="0.3.0",
    label="drop constraint",
    migration_fn=_drop_constraint_tx,
)


def test_migration_version_from_str():
    # Given
    class SomeModel(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)

        version: MigrationVersion

    # When
    m = SomeModel(version="0.3.0")

    # Then
    assert m.version == Version("0.3.0")


@pytest.mark.parametrize(
    "registry,expected_indexes,not_expected_indexes",
    [
        # No migration
        ([], set(), set()),
        # Single
        ([_MIGRATION_0], {"index0", "index1"}, set()),
        # Single as explicit_transaction
        ([_MIGRATION_0_EXPLICIT], {"index0", "index1"}, set()),
        # Multiple ordered
        ([_MIGRATION_0, _MIGRATION_1], {"index1"}, {"index0"}),
        # Multiple unordered
        ([_MIGRATION_1, _MIGRATION_0], {"index1"}, {"index0"}),
    ],
)
async def test_migrate_db_schema(
    _migration_index_and_constraint: neo4j.AsyncDriver,
    # pylint: disable=invalid-name
    registry: list[Migration],
    expected_indexes: set[str],
    not_expected_indexes: set[str],
):
    # Given
    neo4j_driver = _migration_index_and_constraint

    # When
    await migrate_db_schemas(neo4j_driver, registry, timeout_s=10, throttle_s=0.1)

    # Then
    index_res, _, _ = await neo4j_driver.execute_query("SHOW INDEXES")
    existing_indexes = set()
    for rec in index_res:
        existing_indexes.add(rec["name"])
    missing_indexes = expected_indexes - existing_indexes
    assert not missing_indexes
    assert not not_expected_indexes.intersection(existing_indexes)

    if registry:
        db_migrations_recs, _, _ = await neo4j_driver.execute_query(
            "MATCH (m:_Migration) RETURN m as migration"
        )
        db_migrations = [
            Neo4jMigration.from_neo4j(rec, key="migration")
            for rec in db_migrations_recs
        ]
        assert len(db_migrations) == len(registry) + 1
        assert all(m.status is MigrationStatus.DONE for m in db_migrations)
        max_version = max(m.version for m in registry)
        db_version = max(m.version for m in db_migrations)
        assert db_version == max_version


async def test_migrate_db_schema_should_raise_after_timeout(
    _migration_index_and_constraint: neo4j.AsyncDriver,
    # pylint: disable=invalid-name
):
    # Given
    neo4j_driver = _migration_index_and_constraint
    registry = [_MIGRATION_0]

    # When
    query = """CREATE (:_Migration {
    version: $version,
    project: $db,
    label: $label,
    started: $started 
 })"""

    await neo4j_driver.execute_query(
        query,
        version=str(_MIGRATION_0.version),
        db=NEO4J_COMMUNITY_DB,
        label=_MIGRATION_0.label,
        started=datetime.now(),
    )
    expected_msg = "Migration timeout expired"
    with pytest.raises(MigrationError, match=expected_msg):
        await migrate_db_schemas(neo4j_driver, registry, timeout_s=0, throttle_s=0.1)


async def test_migrate_db_schema_should_wait_when_other_migration_in_progress(
    caplog,
    monkeypatch,
    _migration_index_and_constraint: neo4j.AsyncDriver,
    # pylint: disable=invalid-name
):
    # Given
    neo4j_driver_0 = _migration_index_and_constraint
    caplog.set_level(logging.INFO, logger=icij_common.__name__)

    async def mocked_get_migrations(
        sess: neo4j.AsyncSession,
        db: str,
    ) -> list[Neo4jMigration]:
        # pylint: disable=unused-argument
        return [
            Neo4jMigration(
                db=TEST_DB,
                version="0.1.0",
                label="migration in progress",
                status=MigrationStatus.IN_PROGRESS,
                started=datetime.now(),
            )
        ]

    monkeypatch.setattr(migrate, "db_migrations_tx", mocked_get_migrations)

    # When/Then
    expected_msg = "Migration timeout expired "
    with pytest.raises(MigrationError, match=expected_msg):
        timeout_s = 0.5
        wait_s = 0.1
        await migrate_db_schemas(
            neo4j_driver_0,
            [_MIGRATION_0, _MIGRATION_1],
            timeout_s=timeout_s,
            throttle_s=wait_s,
        )
    # Check that we've slept at least once otherwise timeout must be increased...
    assert any(
        rec.name == "icij_common.neo4j_.migrate"
        and f"waiting for {wait_s}" in rec.message
        for rec in caplog.records
    )


async def test_migrate_db_schema_should_wait_when_other_migration_just_started(
    monkeypatch,
    caplog,
    _migration_index_and_constraint: neo4j.AsyncDriver,
    # pylint: disable=invalid-name
):
    # Given
    neo4j_driver = _migration_index_and_constraint
    caplog.set_level(logging.INFO, logger=icij_common.__name__)

    async def mocked_get_migrations(
        sess: neo4j.AsyncSession, db: str
    ) -> list[Neo4jMigration]:
        # pylint: disable=unused-argument
        return []

    # No migration in progress
    monkeypatch.setattr(migrate, "db_migrations_tx", mocked_get_migrations)

    # However we simulate _MIGRATION_0 being running just before our migrate_db_schema
    # by inserting it in progress
    query = """CREATE (m:_Migration {
    project: $db,
    version: $version, 
    label: 'someLabel', 
    started: $started
})
"""
    await neo4j_driver.execute_query(
        query,
        db=NEO4J_COMMUNITY_DB,
        version=str(_MIGRATION_0.version),
        label=str(_MIGRATION_0.label),
        started=datetime.now(),
        status=MigrationStatus.IN_PROGRESS.value,
    )
    try:
        # When/Then
        expected_msg = "Migration timeout expired "
        with pytest.raises(MigrationError, match=expected_msg):
            timeout_s = 0.5
            wait_s = 0.1
            await migrate_db_schemas(
                neo4j_driver,
                [_MIGRATION_0],
                timeout_s=timeout_s,
                throttle_s=wait_s,
            )
        # Check that we've slept at least once otherwise timeout must be increased...
        assert any(
            rec.name == "icij_common.neo4j_.migrate" and "just started" in rec.message
            for rec in caplog.records
        )
    finally:
        # Don't forget to cleanup other the DB will be locked
        async with neo4j_driver.session(database="neo4j") as sess:
            await wipe_db(sess)


@pytest.mark.parametrize("enterprise", [True, False])
async def test_retrieve_dbs(
    _migration_index_and_constraint: neo4j.AsyncDriver,
    # pylint: disable=invalid-name
    enterprise: bool,
    monkeypatch,
):
    # Given
    neo4j_driver = _migration_index_and_constraint

    if enterprise:
        mock_enterprise_(monkeypatch)

    dbs = await retrieve_dbs(neo4j_driver)

    # Then
    assert dbs == [Database(name=NEO4J_COMMUNITY_DB)]


async def test_migrate_should_use_registry_db_when_with_enterprise_support(
    _migration_index_and_constraint: neo4j.AsyncDriver,
    # pylint: disable=invalid-name
    monkeypatch,
):
    # Given
    registry = _BASE_REGISTRY

    monkeypatch.setattr(icij_common.neo4j_.db, "is_enterprise", mocked_is_enterprise)
    neo4j_driver = _migration_index_and_constraint

    # When/Then
    expected = (
        "Unable to get a routing table for database 'datashare-project-registry'"
        " because this database does not exist"
    )
    with pytest.raises(ClientError, match=expected):
        await migrate_db_schemas(neo4j_driver, registry, timeout_s=10, throttle_s=0.1)


@pytest.mark.parametrize("is_enterprise", [True, False])
async def test_init_database(
    neo4j_test_driver: neo4j.AsyncDriver, is_enterprise: bool, monkeypatch
):
    # Given
    neo4j_driver = neo4j_test_driver

    registry = [V_0_1_0]

    if is_enterprise:
        db = "test-db"
        mock_enterprise_(monkeypatch)
        with pytest.raises(ClientError) as ctx:
            await init_database(neo4j_driver, db, registry, timeout_s=1, throttle_s=1)
        expected_code = "Neo.ClientError.Statement.UnsupportedAdministrationCommand"
        assert ctx.value.code == expected_code
    else:
        db = NEO4J_COMMUNITY_DB
        # When
        existed = await init_database(
            neo4j_driver, db, registry, timeout_s=1, throttle_s=1
        )
        assert not existed

        # Then
        dbs = await retrieve_dbs(neo4j_driver)
        assert dbs == [Database(name=db)]
        db_migrations_recs, _, _ = await neo4j_driver.execute_query(
            "MATCH (m:_Migration) RETURN m as migration"
        )
        db_migrations = [
            Neo4jMigration.from_neo4j(rec, key="migration")
            for rec in db_migrations_recs
        ]
        assert len(db_migrations) == 1
        migration = db_migrations[0]
        assert migration.version == V_0_1_0.version


async def test_init_database_should_be_idempotent(neo4j_test_driver: neo4j.AsyncDriver):
    # Given
    neo4j_driver = neo4j_test_driver
    db = NEO4J_COMMUNITY_DB
    registry = [V_0_1_0]
    await init_database(neo4j_driver, db, registry, timeout_s=1, throttle_s=1)

    # When
    with fail_if_exception("init_project is not idempotent"):
        existed = await init_database(
            neo4j_driver, db, registry, timeout_s=1, throttle_s=1
        )

    # Then
    assert existed

    dbs = await retrieve_dbs(neo4j_driver)
    assert dbs == [Database(name=db)]
    db_migrations_recs, _, _ = await neo4j_driver.execute_query(
        "MATCH (m:_Migration) RETURN m as migration"
    )
    db_migrations = [
        Neo4jMigration.from_neo4j(rec, key="migration") for rec in db_migrations_recs
    ]
    assert len(db_migrations) == 1
    migration = db_migrations[0]
    assert migration.version == V_0_1_0.version


async def test_init_database_should_raise_for_reserved_name(
    neo4j_test_driver_session: neo4j.AsyncDriver,
):
    # Given
    neo4j_driver = neo4j_test_driver_session
    db = DATABASE_REGISTRY_DB

    # When/then
    expected = (
        'Bad luck, name "datashare-project-registry" is reserved for'
        " internal use. Can't initialize database"
    )
    with pytest.raises(ValueError, match=expected):
        await init_database(neo4j_driver, db, registry=[], timeout_s=1, throttle_s=1)


@pytest.mark.pull("131")
async def test_migrate_project_db_schema_should_read_migrations_from_registry(
    neo4j_test_driver_session: neo4j.AsyncDriver,
    monkeypatch,
):
    # Given
    registry = [V_0_1_0.model_copy(update={"status": MigrationStatus.DONE})]
    monkeypatch.setattr(icij_common.neo4j_.db, "is_enterprise", mocked_is_enterprise)
    with mock.patch(
        "icij_common.neo4j_.migrate.registry_db_session"
    ) as mocked_registry_sess:
        with mock.patch("icij_common.neo4j_.migrate.db_specific_session"):
            mocked_sess = mock.AsyncMock()
            mocked_registry_sess.return_value.__aenter__.return_value = mocked_sess
            mocked_sess.execute_read.return_value = registry
            await migrate_db_schema(
                neo4j_test_driver_session,
                _BASE_REGISTRY,
                TEST_DB,
                timeout_s=1,
                throttle_s=1,
            )
        mocked_sess.execute_read.assert_called_once()
