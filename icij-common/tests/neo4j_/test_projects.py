from unittest.mock import AsyncMock

import pytest

from icij_common.neo4j_.db import create_databases_registry_db


@pytest.mark.usefixtures("mock_enterprise")
async def test_should_create_databases_registry_db_with_enterprise_distribution() -> (
    None
):
    # Given
    mocked_driver = AsyncMock()

    # When
    await create_databases_registry_db(mocked_driver)

    # Then
    mocked_driver.execute_query.assert_called_once_with(
        "CREATE DATABASE $registry_db IF NOT EXISTS",
        registry_db="datashare-project-registry",
    )
