from abc import ABC, abstractmethod
from typing import final

from icij_worker import ManagerEvent


class EventPublisher(ABC):
    @final
    async def publish_event(self, event: ManagerEvent):
        await self._publish_event(event)

    @abstractmethod
    async def _publish_event(self, event: ManagerEvent):
        pass
