from contextlib import AbstractAsyncContextManager, AsyncExitStack
from copy import deepcopy
from functools import lru_cache
from typing import Dict, Optional, Tuple, cast

from aio_pika import DeliveryMode, Message as AioPikaMessage
from aio_pika.abc import (
    AbstractExchange,
    AbstractQueueIterator,
    AbstractRobustChannel,
    AbstractRobustConnection,
    ExchangeType,
)
from aiormq.abc import ConfirmationFrameType
from pamqp.commands import Basic

from icij_worker import Message
from icij_worker.constants import (
    AMQP_EVENTS_QUEUE,
    AMQP_EVENTS_ROUTING_KEY,
    AMQP_EVENTS_X,
    AMQP_RESULTS_DL_QUEUE,
    AMQP_RESULTS_DL_ROUTING_KEY,
    AMQP_RESULTS_DL_X,
    AMQP_RESULTS_QUEUE,
    AMQP_RESULTS_ROUTING_KEY,
    AMQP_RESULTS_X,
    AMQP_TASKS_DL_QUEUE,
    AMQP_TASKS_DL_ROUTING_KEY,
    AMQP_TASKS_DL_X,
    AMQP_TASKS_QUEUE,
    AMQP_TASKS_ROUTING_KEY,
    AMQP_TASKS_X,
    AMQP_WORKER_EVENTS_QUEUE,
    AMQP_WORKER_EVENTS_ROUTING_KEY,
    AMQP_WORKER_EVENTS_X,
)
from icij_worker.namespacing import Exchange, Routing


class AMQPMixin:
    _app_id: str
    _channel_: AbstractRobustChannel

    def __init__(
        self,
        broker_url: str,
        *,
        connection_timeout_s: Optional[float] = None,
        reconnection_wait_s: Optional[float] = None,
        inactive_after_s: Optional[float] = None,
    ):
        self._broker_url = broker_url
        self._reconnection_wait_s = reconnection_wait_s
        self._connection_timeout_s = connection_timeout_s
        self._inactive_after_s = inactive_after_s
        self._connection_: Optional[AbstractRobustConnection] = None
        self._exit_stack = AsyncExitStack()

    async def _publish_message(
        self,
        message: Message,
        *,
        exchange: AbstractExchange,
        delivery_mode: DeliveryMode = DeliveryMode.PERSISTENT,
        routing_key: Optional[str],
        mandatory: bool,
    ) -> Optional[ConfirmationFrameType]:
        message = message.json(exclude_unset=True, by_alias=True).encode()
        message = AioPikaMessage(
            message, delivery_mode=delivery_mode, app_id=self._app_id
        )
        confirmation = await exchange.publish(message, routing_key, mandatory=mandatory)
        if not isinstance(confirmation, Basic.Ack):
            msg = f"Failed to deliver {message.body}, received {confirmation}"
            raise RuntimeError(msg)
        return confirmation

    @property
    def _connection(self) -> AbstractRobustConnection:
        if self._connection_ is None:
            msg = (
                f"Publisher has no connection, please call"
                f" {self.__class__.__aenter__.__name__}"
            )
            raise ValueError(msg)
        return self._connection_

    @property
    def _channel(self) -> AbstractRobustChannel:
        if self._channel_ is None:
            msg = (
                f"Publisher has no channel, please call"
                f" {self.__class__.__aenter__.__name__}"
            )
            raise ValueError(msg)
        return self._channel_

    @property
    def channel(self) -> AbstractRobustChannel:
        return self._channel

    @classmethod
    @lru_cache(maxsize=1)
    def default_task_routing(cls) -> Routing:
        return Routing(
            exchange=Exchange(name=AMQP_TASKS_X, type=ExchangeType.DIRECT),
            routing_key=AMQP_TASKS_ROUTING_KEY,
            queue_name=AMQP_TASKS_QUEUE,
            dead_letter_routing=Routing(
                exchange=Exchange(name=AMQP_TASKS_DL_X, type=ExchangeType.DIRECT),
                routing_key=AMQP_TASKS_DL_ROUTING_KEY,
                queue_name=AMQP_TASKS_DL_QUEUE,
            ),
        )

    @classmethod
    @lru_cache(maxsize=1)
    def evt_routing(cls) -> Routing:
        return Routing(
            exchange=Exchange(name=AMQP_EVENTS_X, type=ExchangeType.FANOUT),
            routing_key=AMQP_EVENTS_ROUTING_KEY,
            queue_name=AMQP_EVENTS_QUEUE,
        )

    @classmethod
    @lru_cache(maxsize=1)
    def res_and_err_routing(cls) -> Routing:
        return Routing(
            exchange=Exchange(name=AMQP_RESULTS_X, type=ExchangeType.DIRECT),
            routing_key=AMQP_RESULTS_ROUTING_KEY,
            queue_name=AMQP_RESULTS_QUEUE,
            dead_letter_routing=Routing(
                exchange=Exchange(name=AMQP_RESULTS_DL_X, type=ExchangeType.DIRECT),
                routing_key=AMQP_RESULTS_DL_ROUTING_KEY,
                queue_name=AMQP_RESULTS_DL_QUEUE,
            ),
        )

    @classmethod
    @lru_cache(maxsize=1)
    def worker_evt_routing(cls) -> Routing:
        return Routing(
            exchange=Exchange(name=AMQP_WORKER_EVENTS_X, type=ExchangeType.FANOUT),
            routing_key=AMQP_WORKER_EVENTS_ROUTING_KEY,
            queue_name=AMQP_WORKER_EVENTS_QUEUE,
        )

    async def _get_queue_iterator(
        self,
        routing: Routing,
        *,
        declare_exchanges: bool,
        declare_queues: bool = True,
        durable_queues: bool = True,
    ) -> Tuple[AbstractQueueIterator, AbstractExchange, Optional[AbstractExchange]]:
        await self._exit_stack.enter_async_context(
            cast(AbstractAsyncContextManager, self._channel)
        )
        dlq_ex = None
        if declare_exchanges:
            await self._create_routing(
                routing, declare_queues=declare_queues, durable_queues=durable_queues
            )
        ex = await self._channel.get_exchange(routing.exchange.name, ensure=True)
        queue = await self._channel.get_queue(routing.queue_name, ensure=True)
        kwargs = dict()
        if self._inactive_after_s is not None:
            kwargs["timeout"] = self._inactive_after_s
        return queue.iterator(**kwargs), ex, dlq_ex

    async def _create_routing(
        self,
        routing: Routing,
        declare_queues: bool = True,
        durable_queues: bool = True,
        queue_args: Optional[Dict] = None,
    ):
        x = await self._channel.declare_exchange(
            routing.exchange.name, type=routing.exchange.type, durable=True
        )
        if queue_args is not None:
            queue_args = deepcopy(queue_args)
        if routing.dead_letter_routing:
            await self._create_routing(
                routing.dead_letter_routing,
                declare_queues=declare_queues,
                durable_queues=durable_queues,
            )
            if queue_args is None:
                queue_args = dict()
            dlx_name = routing.dead_letter_routing.exchange.name
            dl_routing_key = routing.dead_letter_routing.routing_key
            update = {
                "x-dead-letter-exchange": dlx_name,
                "x-dead-letter-routing-key": dl_routing_key,
            }
            queue_args.update(update)
        if declare_queues:
            queue = await self._channel.declare_queue(
                routing.queue_name,
                durable=durable_queues,
                arguments=queue_args,
            )
        else:
            queue = await self._channel.get_queue(routing.queue_name, ensure=True)
        await queue.bind(x, routing_key=routing.routing_key)
