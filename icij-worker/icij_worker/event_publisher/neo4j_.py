from datetime import datetime

import neo4j

from icij_common.neo4j.constants import (
    TASK_MANAGER_EVENT_EVENT,
    TASK_MANAGER_EVENT_NODE,
    TASK_MANAGER_EVENT_NODE_CREATED_AT,
)
from icij_worker.event_publisher.event_publisher import EventPublisher
from icij_worker.objects import (
    CancelledEvent,
    ErrorEvent,
    ManagerEvent,
    ProgressEvent,
    ResultEvent,
)
from icij_worker.task_storage.neo4j_ import Neo4jStorage


class Neo4jEventPublisher(Neo4jStorage, EventPublisher):

    async def _publish_event(self, event: ManagerEvent):
        async with self._task_session(event.task_id) as sess:
            await _publish_event(sess, event)

    @property
    def driver(self) -> neo4j.AsyncDriver:
        return self._driver


async def _publish_event(sess: neo4j.AsyncSession, event: ManagerEvent):
    as_json = event.json(exclude_none=True)
    if isinstance(event, ProgressEvent):
        stamp = datetime.now()
    elif isinstance(event, ErrorEvent):
        stamp = event.error.occurred_at
    elif isinstance(event, ResultEvent):
        stamp = event.completed_at
    elif isinstance(event, CancelledEvent):
        stamp = event.cancelled_at
    else:
        raise TypeError(f"unexpected event type: {event}")
    await sess.execute_write(_publish_manager_event_tx, as_json, stamp=stamp)


async def _publish_manager_event_tx(
    tx: neo4j.AsyncTransaction, event_as_json: str, stamp: datetime
):
    create_manager_event = f"""
CREATE (event:{TASK_MANAGER_EVENT_NODE} {{
    {TASK_MANAGER_EVENT_EVENT}: $eventAsJson,
    {TASK_MANAGER_EVENT_NODE_CREATED_AT}: $createdAt 
}})
RETURN event"""
    await tx.run(create_manager_event, eventAsJson=event_as_json, createdAt=stamp)
