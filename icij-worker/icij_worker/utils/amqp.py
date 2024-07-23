from contextlib import AbstractAsyncContextManager, AsyncExitStack
from copy import deepcopy
from functools import lru_cache
from typing import Dict, Optional, Tuple, cast

from aio_pika import (
    DeliveryMode,
    Message as AioPikaMessage,
)
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
    AMQP_MANAGER_EVENTS_DL_QUEUE,
    AMQP_MANAGER_EVENTS_DL_ROUTING_KEY,
    AMQP_MANAGER_EVENTS_DL_X,
    AMQP_MANAGER_EVENTS_QUEUE,
    AMQP_MANAGER_EVENTS_ROUTING_KEY,
    AMQP_MANAGER_EVENTS_X,
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
from icij_worker.namespacing import Exchange, Namespacing, Routing


class AMQPMixin:
    _app_id: str
    _channel_: AbstractRobustChannel
    _namespacing: Namespacing
    _task_x: AbstractExchange
    _max_task_queue_size: Optional[int]

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
        self._task_queues = set()

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
                f"{self} has no connection, please call"
                f" {self.__class__.__aenter__.__name__}"
            )
            raise ValueError(msg)
        return self._connection_

    @property
    def _channel(self) -> AbstractRobustChannel:
        if self._channel_ is None:
            msg = (
                f"{self} has no channel, please call"
                f" {self.__class__.__aenter__.__name__}"
            )
            raise ValueError(msg)
        return self._channel_

    @property
    def channel(self) -> AbstractRobustChannel:
        return self._channel

    @property
    def connection(self) -> AbstractRobustChannel:
        return self._connection

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
    def manager_evt_routing(cls) -> Routing:
        return Routing(
            exchange=Exchange(name=AMQP_MANAGER_EVENTS_X, type=ExchangeType.DIRECT),
            routing_key=AMQP_MANAGER_EVENTS_ROUTING_KEY,
            queue_name=AMQP_MANAGER_EVENTS_QUEUE,
            dead_letter_routing=Routing(
                exchange=Exchange(
                    name=AMQP_MANAGER_EVENTS_DL_X, type=ExchangeType.DIRECT
                ),
                routing_key=AMQP_MANAGER_EVENTS_DL_ROUTING_KEY,
                queue_name=AMQP_MANAGER_EVENTS_DL_QUEUE,
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

    async def _ensure_task_queue(self, namespace: Optional[str]):
        if namespace not in self._task_queues:
            self._task_queues.add(namespace)
            routing = self._namespacing.amqp_task_routing(namespace)
            task_routing = self.default_task_routing()
            dlx_name = task_routing.dead_letter_routing.exchange.name
            dl_routing_key = task_routing.dead_letter_routing.routing_key
            arguments = {
                "x-dead-letter-exchange": dlx_name,
                "x-dead-letter-routing-key": dl_routing_key,
            }
            if self._max_task_queue_size is not None:
                arguments["x-max-length"] = self._max_task_queue_size
                arguments["x-overflow"] = "reject-publish"
            queue = await self._channel.declare_queue(
                routing.queue_name, durable=True, arguments=arguments
            )
            await queue.bind(routing.exchange.name, routing.routing_key)

    async def _get_queue_iterator(
        self,
        routing: Routing,
        *,
        declare_exchanges: bool,
        declare_queues: bool = True,
        durable_queues: bool = True,
        queue_args: Optional[Dict] = None,
    ) -> Tuple[AbstractQueueIterator, AbstractExchange, Optional[AbstractExchange]]:
        await self._exit_stack.enter_async_context(
            cast(AbstractAsyncContextManager, self._channel)
        )
        dlq_ex = None
        await self._create_routing(
            routing,
            declare_exchanges=declare_exchanges,
            declare_queues=declare_queues,
            durable_queues=durable_queues,
            queue_args=queue_args,
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
        declare_exchanges: bool = True,
        declare_queues: bool = True,
        durable_queues: bool = True,
        queue_args: Optional[Dict] = None,
    ):
        if declare_exchanges:
            x = await self._channel.declare_exchange(
                routing.exchange.name, type=routing.exchange.type, durable=True
            )
        else:
            x = await self._channel.get_exchange(routing.exchange.name, ensure=True)
        if queue_args is not None:
            queue_args = deepcopy(queue_args)
        if routing.dead_letter_routing:
            await self._create_routing(
                routing.dead_letter_routing,
                declare_exchanges=declare_exchanges,
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
