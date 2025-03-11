from __future__ import annotations

import logging
from contextlib import AsyncExitStack
from functools import cached_property

from aio_pika import (
    Exchange as AioPikaExchange,
    RobustChannel,
    connect_robust,
)
from aio_pika.abc import AbstractRobustConnection

from icij_common.logging_utils import LogWithNameMixin
from icij_worker import ManagerEvent
from . import EventPublisher
from ..routing_strategy import Routing
from ..utils.amqp import AMQPMixin, RobustConnection


class AMQPPublisher(AMQPMixin, EventPublisher, LogWithNameMixin):
    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        *,
        broker_url: str,
        connection_timeout_s: float = 1.0,
        reconnection_wait_s: float = 5.0,
        is_qpid: bool = False,
        app_id: str | None = None,
        connection: Optional[AbstractRobustConnection] = None,
    ):
        super().__init__(
            broker_url,
            connection_timeout_s=connection_timeout_s,
            reconnection_wait_s=reconnection_wait_s,
            is_qpid=is_qpid,
        )
        if logger is None:
            logger = logging.getLogger(__name__)
        LogWithNameMixin.__init__(self, logger)
        self._app_id = app_id
        self._broker_url = broker_url
        self._connection_ = connection
        self._channel_: Optional[RobustChannel] = None
        self._manager_evt_x: Optional[AioPikaExchange] = None
        self._connection_timeout_s = connection_timeout_s
        self._reconnection_wait_s = reconnection_wait_s
        self._exit_stack = AsyncExitStack()
        self._declare_and_bind = True

    async def __aenter__(self) -> AMQPPublisher:
        self.info("starting publisher connection workflow...")
        await self._exit_stack.__aenter__()
        await self._connection_workflow()
        self.info("publisher connected !")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    @cached_property
    def _routings(self) -> list[Routing]:
        return [self.manager_evt_routing()]

    async def _publish_event(self, event: ManagerEvent):
        await self._publish_message(
            event,
            exchange=self._manager_evt_x,
            routing_key=self.manager_evt_routing().routing_key,
            mandatory=False,
        )

    async def _connection_workflow(self):
        self.debug("creating connection...")
        if self._connection_ is None:
            self._connection_ = await connect_robust(
                self._broker_url,
                timeout=self._connection_timeout_s,
                reconnect_interval=self._reconnection_wait_s,
                connection_class=RobustConnection,
            )
            await self._exit_stack.enter_async_context(self._connection)
        self.debug("creating channel...")
        self._channel_ = await self._connection.channel(
            publisher_confirms=self._publisher_confirms,
            on_return_raises=False,
        )
        await self._exit_stack.enter_async_context(self._channel)
        await self._channel_.set_qos(1, global_=False)
        if self._declare_and_bind:
            await self._create_routing(self.manager_evt_routing())
        self._manager_evt_x = await self._channel.get_exchange(
            self.manager_evt_routing().exchange.name, ensure=True
        )
        self.info("channel opened !")
