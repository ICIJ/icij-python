from __future__ import annotations

import logging
from contextlib import AsyncExitStack
from functools import cached_property
from typing import List, Optional

from aio_pika import (
    Exchange as AioPikaExchange,
    RobustChannel,
    RobustConnection as _RobustConnection,
    connect_robust,
)
from aio_pika.abc import AbstractRobustConnection

from icij_common.logging_utils import LogWithNameMixin
from icij_worker import TaskError, TaskEvent, TaskResult
from . import EventPublisher
from ..namespacing import Routing
from ..utils.amqp import AMQPMixin


# TODO: move these to a upper level


class RobustConnection(_RobustConnection):
    # Defined async context manager attributes to be able to enter and exit this
    # in ExitStack
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class AMQPPublisher(AMQPMixin, EventPublisher, LogWithNameMixin):
    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        *,
        broker_url: str,
        connection_timeout_s: float = 1.0,
        reconnection_wait_s: float = 5.0,
        app_id: Optional[str] = None,
        connection: Optional[AbstractRobustConnection] = None,
    ):
        super().__init__(
            broker_url,
            connection_timeout_s=connection_timeout_s,
            reconnection_wait_s=reconnection_wait_s,
        )
        if logger is None:
            logger = logging.getLogger(__name__)
        LogWithNameMixin.__init__(self, logger)
        self._app_id = app_id
        self._broker_url = broker_url
        self._connection_ = connection
        self._channel_: Optional[RobustChannel] = None
        self._evt_ex: Optional[AioPikaExchange] = None
        self._res_ex: Optional[AioPikaExchange] = None
        self._err_ex: Optional[AioPikaExchange] = None
        self._connection_timeout_s = connection_timeout_s
        self._reconnection_wait_s = reconnection_wait_s
        self._exit_stack = AsyncExitStack()
        # We don't declare and bind anything here, the task manager is in charge of it.
        # We use this flag only for testing where we want to set everything up easily
        self._declare_and_bind = False

    async def __aenter__(self) -> AMQPPublisher:
        self.info("starting publisher connection workflow...")
        await self._exit_stack.__aenter__()
        await self._connection_workflow()
        self.info("publisher connected !")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    @cached_property
    def _routings(self) -> List[Routing]:
        return [self.evt_routing(), self.res_routing(), self.err_routing()]

    async def _publish_event(self, event: TaskEvent):
        await self._publish_message(
            event,
            exchange=self._evt_ex,
            routing_key=self.evt_routing().routing_key,
            mandatory=False,
        )

    publish_event_ = _publish_event

    async def publish_result(self, result: TaskResult):
        # TODO: for now task project information is not leverage on the AMQP side which
        #  is not very convenient as clients will won't know from which project the
        #  result is coming. This is limitating as for instance when as result must
        #  probably be saved in separate DBs in order to avoid project data leaking to
        #  other projects through the DB
        await self._publish_message(
            result,
            exchange=self._res_ex,
            routing_key=self.res_routing().routing_key,
            mandatory=True,  # This is important
        )

    async def publish_error(self, error: TaskError):
        # TODO: for now task project information is not leverage on the AMQP side which
        #  is not very convenient as clients will won't know from which project the
        #  result is coming. This is limitating as for instance when as result must
        #  probably be saved in separate DBs in order to avoid project data leaking to
        #  other projects through the DB

        await self._publish_message(
            error,
            exchange=self._err_ex,
            routing_key=self.err_routing().routing_key,
            mandatory=True,  # This is important
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
            publisher_confirms=True, on_return_raises=False
        )
        await self._exit_stack.enter_async_context(self._channel)
        await self._channel.set_qos(prefetch_count=1, global_=True)
        await self._declare_exchanges()
        await self._declare_and_bind_queues()
        self.info("channel opened !")

    async def _declare_exchanges(self):
        if self._declare_and_bind:
            self.debug("(re)declaring %s...", self.evt_routing().exchange)
            self._evt_ex = await self._channel.declare_exchange(
                name=self.evt_routing().exchange.name,
                type=self.evt_routing().exchange.type,
                timeout=self._connection_timeout_s,
                durable=True,
            )
            self.debug("(re)declaring %s...", self.res_routing().exchange)
            self._res_ex = await self._channel.declare_exchange(
                name=self.res_routing().exchange.name,
                type=self.res_routing().exchange.type,
                timeout=self._connection_timeout_s,
                durable=True,
            )
            self.debug("(re)declaring %s...", self.err_routing().exchange)
            self._err_ex = await self._channel.declare_exchange(
                name=self.err_routing().exchange.name,
                type=self.err_routing().exchange.type,
                timeout=self._connection_timeout_s,
                durable=True,
            )
        else:
            self.debug("publisher will use existing exchanges...")
            self._evt_ex = await self._channel.get_exchange(
                self.evt_routing().exchange.name
            )
            self._res_ex = await self._channel.get_exchange(
                self.res_routing().exchange.name
            )
            self._res_ex = await self._channel.get_exchange(
                self.res_routing().exchange.name
            )

    async def _declare_and_bind_queues(self):
        if self._declare_and_bind:
            for routing in self._routings:
                self.debug("(re)declaring queue %s...", routing.queue_name)
                queue = await self._channel.declare_queue(
                    routing.queue_name, durable=True
                )
                self.debug(
                    "binding queue %s on %s...", routing.queue_name, routing.routing_key
                )
                await queue.bind(routing.exchange.name, routing_key=routing.routing_key)
