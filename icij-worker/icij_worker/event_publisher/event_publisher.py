from abc import ABC, abstractmethod
from typing import final

from icij_worker import Task, TaskEvent


class EventPublisher(ABC):
    @final
    async def publish_event(self, event: TaskEvent, task: Task):
        event = event.with_project_id(task)
        await self._publish_event(event)

    @abstractmethod
    async def _publish_event(self, event: TaskEvent):
        pass
