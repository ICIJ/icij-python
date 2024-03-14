# pylint: disable=redefined-outer-name
from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional

import neo4j
import pytest
import pytest_asyncio
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
from icij_worker.task_manager.neo4j import add_support_for_async_task_tx
from icij_worker.typing_ import PercentProgress

# noinspection PyUnresolvedReferences
from icij_worker.utils.tests import (  # pylint: disable=unused-import
    DBMixin,
    test_async_app,
)

logger = logging.getLogger(__name__)


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
