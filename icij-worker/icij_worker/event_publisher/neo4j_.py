from datetime import datetime

import neo4j

from icij_worker.constants import (
    NEO4J_TASK_MANAGER_EVENT_EVENT,
    NEO4J_TASK_MANAGER_EVENT_NODE,
    NEO4J_TASK_MANAGER_EVENT_NODE_CREATED_AT,
)
from icij_worker.event_publisher.event_publisher import EventPublisher
from icij_worker.objects import ManagerEvent
from icij_worker.task_storage.neo4j_ import Neo4jStorage


class Neo4jEventPublisher(Neo4jStorage, EventPublisher):

    async def _publish_event(self, event: ManagerEvent):
        async with self._task_session(event.task_id) as sess:
            await _publish_event(sess, event)

    @property
    def driver(self) -> neo4j.AsyncDriver:
        return self._driver


async def _publish_event(sess: neo4j.AsyncSession, event: ManagerEvent):
    as_json = event.model_dump_json(exclude_none=True)
    await sess.execute_write(
        _publish_manager_event_tx, as_json, created_at=event.created_at
    )


async def _publish_manager_event_tx(
    tx: neo4j.AsyncTransaction, event_as_json: str, created_at: datetime
):
    create_manager_event = f"""
CREATE (event:{NEO4J_TASK_MANAGER_EVENT_NODE} {{
    {NEO4J_TASK_MANAGER_EVENT_EVENT}: $eventAsJson,
    {NEO4J_TASK_MANAGER_EVENT_NODE_CREATED_AT}: $createdAt 
}})
RETURN event"""
    await tx.run(create_manager_event, eventAsJson=event_as_json, createdAt=created_at)
