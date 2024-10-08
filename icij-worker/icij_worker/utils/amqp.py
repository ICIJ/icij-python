from __future__ import annotations

import re
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from copy import deepcopy
from enum import Enum, unique
from functools import cached_property, lru_cache
from typing import Any, AsyncContextManager, Dict, List, Optional, Tuple, Type, cast
from urllib import parse

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
)
from aio_pika.exceptions import ChannelPreconditionFailed
from aiohttp import (
    BasicAuth,
    ClientResponse,
    ClientResponseError,
    ClientSession,
)
from aiohttp.client import _RequestOptions
from aiohttp.typedefs import StrOrURL
from aiormq.abc import ConfirmationFrameType
from pamqp.commands import Basic
from typing_extensions import Unpack

from icij_common.pydantic_utils import ICIJModel, NoEnumModel
from icij_worker import Message
from icij_worker.app import TaskGroup
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
    AMQP_TASK_QUEUE_PRIORITY,
    AMQP_WORKER_EVENTS_QUEUE,
    AMQP_WORKER_EVENTS_ROUTING_KEY,
    AMQP_WORKER_EVENTS_X,
)
from icij_worker.exceptions import WorkerTimeoutError
from icij_worker.routing_strategy import Exchange, Routing, RoutingStrategy
from icij_worker.task_storage.postgres.postgres import logger

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


class AMQPPolicy(NoEnumModel):
    name: str
    pattern: str
    definition: Dict[str, Any]
    apply_to: Optional[ApplyTo] = None
    priority: Optional[int] = None


_DELIVERY_ACK_TIMEOUT_RE = re.compile(
    r"delivery acknowledgement on channel .+ timed out", re.MULTILINE
)


class AMQPConfigMixin(ICIJModel):
    connection_timeout_s: float = 5.0
    reconnection_wait_s: float = 5.0
    rabbitmq_host: str = "127.0.0.1"
    rabbitmq_password: str = "guest"
    rabbitmq_port: Optional[int] = 5672
    rabbitmq_management_port: Optional[int] = 15672
    rabbitmq_user: Optional[str] = "guest"
    rabbitmq_vhost: Optional[str] = "%2F"

    @cached_property
    def broker_url(self) -> str:
        amqp_userinfo = self.rabbitmq_user
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
    max_task_queue_size: Optional[int]
    _always_include = {"createdAt", "retriesLeft"}

    def __init__(
        self,
        broker_url: str,
        *,
        connection_timeout_s: float = 1.0,
        reconnection_wait_s: float = 5.0,
        inactive_after_s: float = None,
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
        message = message.json(
            exclude_unset=True, by_alias=True, exclude_none=True
        ).encode()
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
        await self._create_routing(
            routing,
            declare_exchanges=declare_exchanges,
            declare_queues=declare_queues,
            durable_queues=durable_queues,
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
        *,
        declare_exchanges: bool = True,
        declare_queues: bool = True,
        durable_queues: bool = True,
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
                durable_queues=durable_queues,
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
            queue = await self._channel.declare_queue(
                routing.queue_name, durable=durable_queues, arguments=queue_args
            )
        else:
            queue = await self._channel.get_queue(routing.queue_name, ensure=True)
        await queue.bind(x, routing_key=routing.routing_key)


class AMQPManagementClient(AsyncContextManager):
    def __init__(
        self,
        rabbitmq_management_url: str,
        *,
        rabbitmq_vhost: str,
        rabbitmq_auth: BasicAuth,
    ):
        self._management_url = rabbitmq_management_url
        self._vhost = rabbitmq_vhost
        self._auth = rabbitmq_auth
        self._session: Optional[ClientSession]

    async def __aenter__(self):
        self._session = ClientSession(self._management_url, auth=self._auth)
        await self._session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._session.__aexit__(exc_type, exc_value, traceback)

    @asynccontextmanager
    async def _put(self, url: StrOrURL, *, data: Any = None, **kwargs: Any):
        async with self._session.put(url, data=data, **kwargs) as res:
            _raise_for_status(res)
            yield res

    @asynccontextmanager
    async def _get(self, url: StrOrURL, *, allow_redirects: bool = True, **kwargs: Any):
        async with self._session.get(
            url, allow_redirects=allow_redirects, **kwargs
        ) as res:
            _raise_for_status(res)
            yield res

    @asynccontextmanager
    async def _delete(self, url: StrOrURL, **kwargs: Unpack[_RequestOptions]):
        async with self._session.delete(url, **kwargs) as res:
            _raise_for_status(res)
            yield res

    async def set_policy(self, policy: AMQPPolicy):
        url = f"/api/policies/{self._vhost}/{parse.quote(policy.name)}"
        data = {"pattern": policy.pattern, "definition": policy.definition}
        if policy.apply_to is not None:
            data["apply-to"] = policy.apply_to.value
        if policy.priority is not None:
            data["priority"] = policy.priority
        async with self._put(url, json=data):
            pass

    async def list_policies(self) -> List[Dict]:
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
    task_routing: Routing,
    group: Optional[TaskGroup],
    app_max_task_queue_size: Optional[int],
) -> AMQPPolicy:
    pattern = rf"^{re.escape(task_routing.queue_name)}$"
    name = f"task-group-policy-{task_routing.queue_name}"
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


def _raise_for_status(res: ClientResponse):
    try:
        res.raise_for_status()
    except ClientResponseError as e:
        msg = "request to %s, failed with reason %s"
        logger.exception(msg, res.request_info, res.reason)
        raise e


class RobustChannel(RobustChannel_):
    async def __close_callback(self, _: Any, exc: BaseException) -> None:
        # pylint: disable=unused-private-member
        timeout_exc = parse_consumer_timeout(exc)
        if timeout_exc is not None:
            logger.error("channel closing due to consumer timeout: %s", exc)
            raise timeout_exc from exc


class RobustConnection(RobustConnection_):
    CHANNEL_CLASS: Type[RobustChannel] = RobustChannel

    # Defined async context manager attributes to be able to enter and exit this
    # in ExitStack
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


def parse_consumer_timeout(exc: BaseException) -> Optional[WorkerTimeoutError]:
    if not isinstance(exc, ChannelPreconditionFailed):
        return None
    if not exc.args:
        return None
    msg = exc.args[0]
    if _DELIVERY_ACK_TIMEOUT_RE.search(msg):
        return WorkerTimeoutError(msg)
    return None
