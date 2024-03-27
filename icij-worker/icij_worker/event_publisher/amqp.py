from __future__ import annotations

import logging
from contextlib import AsyncExitStack
from functools import cached_property, lru_cache
from typing import List, Optional

from aio_pika import (
    DeliveryMode,
    Exchange as AioPikaExchange,
    ExchangeType,
    Message,
    RobustChannel,
    RobustConnection as _RobustConnection,
    connect_robust,
)

from icij_common.logging_utils import LogWithNameMixin
from icij_common.pydantic_utils import LowerCamelCaseModel, NoEnumModel
from icij_worker import TaskError, TaskEvent, TaskResult
from . import EventPublisher


# TODO: move these to a upper level
class Exchange(NoEnumModel, LowerCamelCaseModel):
    name: str
    type: ExchangeType


class Routing(LowerCamelCaseModel):
    exchange: Exchange
    routing_key: str
    default_queue: str
    dead_letter_routing: Optional[Routing] = None


class RobustConnection(_RobustConnection):
    # Defined async context manager attributes to be able to enter and exit this
    # in ExitStack
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class AMQPPublisher(EventPublisher, LogWithNameMixin):
    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        *,
        broker_url: str,
        connection_timeout_s: float = 1.0,
        reconnection_wait_s: float = 5.0,
        app_id: Optional[str] = None,
    ):
        if logger is None:
            logger = logging.getLogger(__name__)
        LogWithNameMixin.__init__(self, logger)
        self._app_id = app_id
        self._broker_url = broker_url
        self._connection_: Optional[RobustConnection] = None
        self._channel_: Optional[RobustChannel] = None
        self._evt_exchange: Optional[AioPikaExchange] = None
        self._res_exchange: Optional[AioPikaExchange] = None
        self._err_exchange: Optional[AioPikaExchange] = None
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

    @classmethod
    @lru_cache(maxsize=1)
    def evt_routing(cls) -> Routing:
        return Routing(
            exchange=Exchange(name="exchangeMainEvents", type=ExchangeType.FANOUT),
            routing_key="routingKeyMainEvents",
            default_queue="queueMainEvents",
        )

    @classmethod
    @lru_cache(maxsize=1)
    def res_routing(cls) -> Routing:
        return Routing(
            exchange=Exchange(name="exchangeTaskResults", type=ExchangeType.DIRECT),
            routing_key="routingKeyMainTaskResults",
            default_queue="queueMainTaskResults",
        )

    @classmethod
    @lru_cache(maxsize=1)
    def err_routing(cls) -> Routing:
        return Routing(
            exchange=Exchange(name="exchangeMainErrors", type=ExchangeType.DIRECT),
            routing_key="routingKeyMainErrors",
            default_queue="queueMainErrors",
        )

    @cached_property
    def _routings(self) -> List[Routing]:
        return [self.evt_routing(), self.res_routing(), self.err_routing()]

    async def _publish_event(self, event: TaskEvent):
        message = event.json().encode()
        await self._publish_message(
            message,
            exchange=self._evt_exchange,
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
        message = result.json().encode()
        await self._publish_message(
            message,
            exchange=self._res_exchange,
            routing_key=self.res_routing().routing_key,
            mandatory=True,  # This is important
        )

    async def publish_error(self, error: TaskError):
        # TODO: for now task project information is not leverage on the AMQP side which
        #  is not very convenient as clients will won't know from which project the
        #  result is coming. This is limitating as for instance when as result must
        #  probably be saved in separate DBs in order to avoid project data leaking to
        #  other projects through the DB
        message = error.json().encode()
        await self._publish_message(
            message,
            exchange=self._err_exchange,
            routing_key=self.err_routing().routing_key,
            mandatory=True,  # This is important
        )

    async def _publish_message(
        self,
        message: bytes,
        *,
        exchange: AioPikaExchange,
        delivery_mode: DeliveryMode = DeliveryMode.PERSISTENT,
        routing_key: Optional[str],
        mandatory: bool,
    ):
        message = Message(message, delivery_mode=delivery_mode, app_id=self._app_id)

        await exchange.publish(
            message,
            routing_key,
            mandatory=mandatory,
        )

    @property
    def _connection(self) -> RobustConnection:
        if self._connection_ is None:
            msg = (
                f"Publisher has no connection, please call"
                f" {AMQPPublisher.__aenter__.__name__}"
            )
            raise ValueError(msg)
        return self._connection_

    @property
    def _channel(self) -> RobustChannel:
        if self._channel_ is None:
            msg = (
                f"Publisher has no channel, please call"
                f" {AMQPPublisher.__aenter__.__name__}"
            )
            raise ValueError(msg)
        return self._channel_

    async def _connection_workflow(self):
        self.debug("creating connection...")
        self._connection_ = await connect_robust(
            self._broker_url,
            timeout=self._connection_timeout_s,
            reconnect_interval=self._reconnection_wait_s,
            connection_class=RobustConnection,
        )
        await self._exit_stack.enter_async_context(self._connection)
        self.debug("creating channel...")
        self._channel_ = await self._connection.channel()
        await self._exit_stack.enter_async_context(self._channel)
        await self._channel.set_qos(prefetch_count=1, global_=True)
        await self._declare_exchanges()
        await self._declare_and_bind_queues()
        self.info("channel opened !")

    async def _declare_exchanges(self):
        if self._declare_and_bind:
            self.debug("(re)declaring %s...", self.evt_routing().exchange)
            self._evt_exchange = await self._channel.declare_exchange(
                name=self.evt_routing().exchange.name,
                type=self.evt_routing().exchange.type,
                timeout=self._connection_timeout_s,
                durable=True,
            )
            self.debug("(re)declaring %s...", self.res_routing().exchange)
            self._res_exchange = await self._channel.declare_exchange(
                name=self.res_routing().exchange.name,
                type=self.res_routing().exchange.type,
                timeout=self._connection_timeout_s,
                durable=True,
            )
            self.debug("(re)declaring %s...", self.err_routing().exchange)
            self._err_exchange = await self._channel.declare_exchange(
                name=self.err_routing().exchange.name,
                type=self.err_routing().exchange.type,
                timeout=self._connection_timeout_s,
                durable=True,
            )
        else:
            self.debug("publisher will use existing exchanges...")
            self._evt_exchange = await self._channel.get_exchange(
                self.evt_routing().exchange.name
            )
            self._res_exchange = await self._channel.get_exchange(
                self.res_routing().exchange.name
            )
            self._res_exchange = await self._channel.get_exchange(
                self.res_routing().exchange.name
            )

    async def _declare_and_bind_queues(self):
        if self._declare_and_bind:
            for routing in self._routings:
                self.debug("(re)declaring queues %s...", routing.default_queue)
                queue = await self._channel.declare_queue(
                    routing.default_queue, durable=True
                )
                self.debug("binding queues, %s...", routing.default_queue)
                await queue.bind(routing.exchange.name, routing_key=routing.routing_key)
