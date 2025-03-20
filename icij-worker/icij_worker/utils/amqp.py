from __future__ import annotations

import asyncio
import logging
import re
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from copy import deepcopy
from enum import Enum, unique
from functools import cached_property, lru_cache
from typing import Any, cast
from urllib import parse

import aiormq
from aio_pika import (
    DeliveryMode,
    Message as AioPikaMessage,
    RobustChannel as RobustChannel_,
    RobustConnection as RobustConnection_,
)
from aio_pika.abc import (
    AbstractExchange,
    AbstractQueueIterator,
    AbstractRobustChannel,
    AbstractRobustConnection,
    ExchangeType,
    TimeoutType,
    UnderlayConnection,
)
from aio_pika.exceptions import ChannelPreconditionFailed
from aiohttp import BasicAuth
from aiormq import Connection as AiormqConnection
from aiormq.abc import ArgumentsType, ConfirmationFrameType, URLorStr
from icij_common.pydantic_utils import icij_config, merge_configs, no_enum_values_config
from mdurl import URL
from pamqp.commands import Basic
from pamqp.common import FieldTable
from pydantic import BaseModel

from icij_worker import Message
from icij_worker.app import TaskGroup
from icij_worker.constants import (
    AMQP_HEALTH_POLICY_PRIORITY,
    AMQP_HEALTH_QUEUE,
    AMQP_HEALTH_ROUTING_KEY,
    AMQP_HEALTH_X,
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
    AMQP_TASK_QUEUE_PRIORITY,
    AMQP_WORKER_EVENTS_QUEUE,
    AMQP_WORKER_EVENTS_ROUTING_KEY,
    AMQP_WORKER_EVENTS_X,
)
from icij_worker.exceptions import WorkerTimeoutError
from icij_worker.routing_strategy import Exchange, Routing, RoutingStrategy
from icij_worker.utils.http import AiohttpClient

logger = logging.getLogger(__name__)

_DELIVERY_ACK_TIMEOUT_RE = re.compile(
    r"delivery acknowledgement on channel .+ timed out", re.MULTILINE
)


@unique
class ApplyTo(str, Enum):
    EXCHANGES = "exchanges"
    QUEUES = "queues"
    CLASSIC_QUEUES = "classic_queues"
    QUORUM_QUEUES = "quorum_queues"
    STREAMS = "streams"
    ALL = "all"


class AMQPPolicy(BaseModel):
    model_config = merge_configs(icij_config(), no_enum_values_config())

    name: str
    pattern: str
    definition: dict[str, Any]
    apply_to: ApplyTo | None = None
    priority: int | None = None


_DELIVERY_ACK_TIMEOUT_RE = re.compile(
    r"delivery acknowledgement on channel .+ timed out", re.MULTILINE
)


class AMQPConfigMixin(BaseModel):
    model_config = icij_config()

    connection_timeout_s: float = 5.0
    reconnection_wait_s: float = 5.0
    rabbitmq_host: str = "127.0.0.1"
    rabbitmq_password: str = "guest"
    rabbitmq_port: int | None = 5672
    rabbitmq_management_port: int | None = 15672
    rabbitmq_user: str | None = "guest"
    rabbitmq_vhost: str | None = "%2F"
    rabbitmq_is_qpid: bool = False

    @cached_property
    def broker_url(self) -> str:
        amqp_userinfo = self.rabbitmq_user
        if self.rabbitmq_password:
            amqp_userinfo += f":{self.rabbitmq_password}"
        if amqp_userinfo:
            amqp_userinfo += "@"
        amqp_authority = (
            f"{amqp_userinfo or ''}{self.rabbitmq_host}"
            f"{f':{self.rabbitmq_port}' or ''}"
        )
        amqp_uri = f"amqp://{amqp_authority}"
        if self.rabbitmq_vhost is not None:
            amqp_uri += f"/{self.rabbitmq_vhost}"
        return amqp_uri

    @cached_property
    def management_url(self) -> str:
        management_url = f"http://{self.rabbitmq_host}:{self.rabbitmq_management_port}"
        return management_url

    @cached_property
    def basic_auth(self) -> BasicAuth:
        return BasicAuth(self.rabbitmq_user, self.rabbitmq_password)

    def to_management_client(self) -> AMQPManagementClient:
        client = AMQPManagementClient(
            self.management_url,
            rabbitmq_vhost=self.rabbitmq_vhost,
            rabbitmq_auth=self.basic_auth,
        )
        return client


class AMQPMixin:
    _app_id: str
    _channel_: AbstractRobustChannel
    _routing_strategy: RoutingStrategy
    _task_x: AbstractExchange
    max_task_queue_size: int | None
    _always_include = {"createdAt", "retriesLeft"}

    def __init__(
        self,
        broker_url: str,
        *,
        connection_timeout_s: float = 1.0,
        reconnection_wait_s: float = 5.0,
        inactive_after_s: float = None,
        is_qpid: bool = False,
    ):
        self._is_qpid = is_qpid
        self._broker_url = broker_url
        self._reconnection_wait_s = reconnection_wait_s
        self._connection_timeout_s = connection_timeout_s
        self._inactive_after_s = inactive_after_s
        self._publisher_confirms = not is_qpid
        self._connection_: AbstractRobustConnection | None = None
        self._exit_stack = AsyncExitStack()

    async def _publish_message(
        self,
        message: Message | bytes,
        *,
        exchange: AbstractExchange,
        delivery_mode: DeliveryMode = DeliveryMode.PERSISTENT,
        routing_key: str | None,
        mandatory: bool,
    ) -> ConfirmationFrameType | None:
        if isinstance(message, Message):
            message = message.model_dump_json(
                exclude_unset=True, by_alias=True, exclude_none=True
            ).encode()
        message = AioPikaMessage(
            message, delivery_mode=delivery_mode, app_id=self._app_id
        )
        confirmation = await exchange.publish(message, routing_key, mandatory=mandatory)
        if not isinstance(confirmation, Basic.Ack) and self._publisher_confirms:
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
            queue_args={"x-queue-type": "quorum"},
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

    @classmethod
    @lru_cache(maxsize=1)
    def health_routing(cls) -> Routing:
        return Routing(
            exchange=Exchange(name=AMQP_HEALTH_X, type=ExchangeType.DIRECT),
            routing_key=AMQP_HEALTH_ROUTING_KEY,
            queue_name=AMQP_HEALTH_QUEUE,
        )

    async def _get_queue_iterator(
        self,
        routing: Routing,
        *,
        declare_exchanges: bool,
        declare_queues: bool = True,
    ) -> tuple[AbstractQueueIterator, AbstractExchange, AbstractExchange | None]:
        await self._exit_stack.enter_async_context(
            cast(AbstractAsyncContextManager, self._channel)
        )
        dlq_ex = None
        await self._create_routing(
            routing, declare_exchanges=declare_exchanges, declare_queues=declare_queues
        )
        ex = await self._channel.get_exchange(routing.exchange.name, ensure=False)
        queue = await self._channel.get_queue(routing.queue_name, ensure=False)
        kwargs = dict()
        if self._inactive_after_s is not None:
            kwargs["timeout"] = self._inactive_after_s
        return queue.iterator(**kwargs), ex, dlq_ex

    async def _create_routing(
        self,
        routing: Routing,
        *,
        declare_exchanges: bool = True,
        declare_queues: bool = True,
    ):
        if declare_exchanges:
            x = await self._channel.declare_exchange(
                routing.exchange.name, type=routing.exchange.type, durable=True
            )
        else:
            x = await self._channel.get_exchange(routing.exchange.name, ensure=True)
        queue_args = None
        if routing.queue_args is not None:
            queue_args = deepcopy(routing.queue_args)
        if routing.dead_letter_routing:
            await self._create_routing(
                routing.dead_letter_routing,
                declare_exchanges=declare_exchanges,
                declare_queues=declare_queues,
            )
            if queue_args is None:
                queue_args = dict()
            dlx_name = routing.dead_letter_routing.exchange.name
            dl_routing_key = routing.dead_letter_routing.routing_key
            # TODO: this could be passed through policy
            update = {
                "x-dead-letter-exchange": dlx_name,
                "x-dead-letter-routing-key": dl_routing_key,
            }
            queue_args.update(update)
        if declare_queues:
            if self._is_qpid:
                queue_args = dict()
            queue = await self._channel.declare_queue(
                routing.queue_name, durable=True, arguments=queue_args
            )
        else:
            queue = await self._channel.get_queue(routing.queue_name, ensure=True)
        await queue.bind(x, routing_key=routing.routing_key)

    async def _get_amqp_health(self) -> bool:
        health_routing = self.health_routing()
        try:
            health_x = await self.channel.get_exchange(
                health_routing.exchange.name, ensure=True
            )
            await self._publish_message(
                b"",
                exchange=health_x,
                routing_key=health_routing.routing_key,
                mandatory=False,
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.exception("amqp health failed: %s", e)
            return False
        return True


class AMQPManagementClient(AiohttpClient):
    def __init__(
        self,
        rabbitmq_management_url: str,
        *,
        rabbitmq_vhost: str,
        rabbitmq_auth: BasicAuth,
    ):
        super().__init__(rabbitmq_management_url, rabbitmq_auth)
        self._vhost = rabbitmq_vhost

    async def set_policy(self, policy: AMQPPolicy):
        url = f"/api/policies/{self._vhost}/{parse.quote(policy.name)}"
        data = {"pattern": policy.pattern, "definition": policy.definition}
        if policy.apply_to is not None:
            data["apply-to"] = policy.apply_to.value
        if policy.priority is not None:
            data["priority"] = policy.priority
        async with self._put(url, json=data):
            pass

    async def list_policies(self) -> list[dict]:
        url = f"/api/policies/{self._vhost}"
        async with self._get(url) as res:
            return await res.json()

    async def clear_policies(self):
        policies = await self.list_policies()
        for p in policies:
            url = f"/api/policies/{self._vhost}/{p['name']}"
            async with self._delete(url):
                pass


def amqp_task_group_policy(
    routing: Routing,
    group: TaskGroup | None,
    app_max_task_queue_size: int | None,
) -> AMQPPolicy:
    pattern = rf"^{re.escape(routing.queue_name)}$"
    name = f"task-group-policy-{routing.queue_name}"
    definition = {"overflow": "reject-publish", "delivery-limit": 10}
    max_task_queue_size = app_max_task_queue_size
    if group is not None and group.max_task_queue_size is not None:
        max_task_queue_size = group.max_task_queue_size
    if max_task_queue_size is not None:
        definition["max-length"] = max_task_queue_size
    return AMQPPolicy(
        name=name,
        pattern=pattern,
        definition=definition,
        apply_to=ApplyTo.QUEUES,
        priority=AMQP_TASK_QUEUE_PRIORITY,
    )


def worker_events_policy(routing: Routing) -> AMQPPolicy:
    pattern = rf"{re.escape(routing.queue_name)}-*"
    name = "worker-events-policy"
    definition = {"expires": 10 * 60 * 1000}
    return AMQPPolicy(
        name=name,
        pattern=pattern,
        definition=definition,
        apply_to=ApplyTo.QUEUES,
        priority=AMQP_TASK_QUEUE_PRIORITY,
    )


def health_policy(routing: Routing) -> AMQPPolicy:
    pattern = rf"^{re.escape(routing.queue_name)}$"
    name = f"health-policy-{routing.queue_name}"
    definition = {"message-ttl": 5000, "expires": 3600 * 1000}
    return AMQPPolicy(
        name=name,
        pattern=pattern,
        definition=definition,
        apply_to=ApplyTo.QUEUES,
        priority=AMQP_HEALTH_POLICY_PRIORITY,
    )


class RobustChannel(RobustChannel_):
    async def __close_callback(self, _: Any, exc: BaseException) -> None:
        # pylint: disable=unused-private-member
        timeout_exc = parse_consumer_timeout(exc)
        if timeout_exc is not None:
            logger.error("channel closing due to consumer timeout: %s", exc)
            raise timeout_exc from exc


class RobustConnection(RobustConnection_):
    CHANNEL_CLASS: type[RobustChannel] = RobustChannel

    # Defined async context manager attributes to be able to enter and exit this
    # in ExitStack
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def connect(self, timeout: TimeoutType = None) -> None:
        self.transport = await QpidUnderlayConnection.connect(
            self.url,
            self._on_connection_close,
            timeout=timeout,
            **self.kwargs,
        )
        await self._on_connected()


class QpidUnderlayConnection(UnderlayConnection):
    @classmethod
    async def make_connection(
        cls,
        url: URL,
        timeout: TimeoutType = None,
        **kwargs: Any,
    ) -> aiormq.abc.AbstractConnection:
        connection: aiormq.abc.AbstractConnection = await asyncio.wait_for(
            aiormq_qpid_connect(url, **kwargs), timeout=timeout
        )
        await connection.ready()
        return connection


async def aiormq_qpid_connect(
    url: URLorStr,
    *args: Any,
    client_properties: FieldTable | None = None,
    **kwargs: Any,
) -> QpidConnection:
    connection = QpidConnection(url, *args, **kwargs)
    await connection.connect(client_properties or {})
    return connection


class QpidConnection(AiormqConnection):
    QPID_CAPABILITIES = {"publisher_confirms": False}

    @property
    def server_capabilities(self) -> ArgumentsType:
        # QPid doesn't seem to expose server properties so we mock empty ones
        return self.server_properties.get("capabilities", self.QPID_CAPABILITIES)


def parse_consumer_timeout(exc: BaseException) -> WorkerTimeoutError | None:
    if not isinstance(exc, ChannelPreconditionFailed):
        return None
    if not exc.args:
        return None
    msg = exc.args[0]
    if _DELIVERY_ACK_TIMEOUT_RE.search(msg):
        return WorkerTimeoutError(msg)
    return None
