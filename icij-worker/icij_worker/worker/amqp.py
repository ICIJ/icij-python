from __future__ import annotations

from contextlib import AbstractAsyncContextManager, AsyncExitStack
from datetime import datetime
from typing import ClassVar, Dict, Optional, Type, cast

from aio_pika import RobustQueue, connect_robust
from aio_pika.abc import (
    AbstractIncomingMessage,
    AbstractQueueIterator,
    AbstractRobustChannel,
    AbstractRobustConnection,
    ExchangeType,
)
from pydantic import Field

from icij_common.pydantic_utils import safe_copy
from icij_worker import (
    AsyncApp,
    Task,
    TaskError,
    TaskEvent,
    TaskResult,
    TaskState,
    Worker,
    WorkerConfig,
    WorkerType,
)
from icij_worker.event_publisher.amqp import (
    AMQPPublisher,
    RobustConnection,
)
from icij_worker.namespacing import Exchange, Namespacing, Routing
from icij_worker.objects import CancelledTaskEvent
from icij_worker.utils.from_config import T

# TODO: remove this when project information is inside the tasks
_PROJECT_PLACEHOLDER = "placeholder_project_amqp"


@WorkerConfig.register()
class AMQPWorkerConfig(WorkerConfig):
    type: ClassVar[str] = Field(const=True, default=WorkerType.amqp.value)

    amqp_connection_timeout_s: float = 5.0
    amqp_reconnection_wait_s: float = 5.0
    rabbitmq_host: str = "127.0.0.1"
    rabbitmq_password: Optional[str] = None
    rabbitmq_port: Optional[int] = 5672
    rabbitmq_user: Optional[str] = None
    rabbitmq_vhost: Optional[str] = "%2F"

    @property
    def broker_url(self) -> str:
        amqp_userinfo = None
        if self.rabbitmq_user is not None:
            amqp_userinfo = self.rabbitmq_user
            if self.rabbitmq_password is not None:
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


@Worker.register(WorkerType.amqp)
class AMQPWorker(Worker):
    def __init__(
        self,
        app: AsyncApp,
        worker_id: str,
        *,
        namespace: Optional[str],
        broker_url: str,
        connection_timeout_s: Optional[float] = None,
        reconnection_wait_s: Optional[float] = None,
        inactive_after_s: Optional[float] = None,
        handle_signals: bool = True,
        teardown_dependencies: bool = False,
    ):
        super().__init__(
            app,
            worker_id,
            namespace=namespace,
            handle_signals=handle_signals,
            teardown_dependencies=teardown_dependencies,
        )
        self._cancel_event_queue_name = (
            f"{self._cancel_event_routing().queue_name}" f"-{self._id}"
        )
        self._broker_url = broker_url
        self._reconnection_wait_s = reconnection_wait_s
        self._connection_timeout_s = connection_timeout_s
        self._inactive_after_s = inactive_after_s
        self._publisher: Optional[AMQPPublisher] = None
        self._connection_: Optional[AbstractRobustConnection] = None
        self._task_queue_: Optional[RobustQueue] = None
        self._task_queue_iterator: Optional[AbstractQueueIterator] = None
        self._cancel_evt_queue_: Optional[RobustQueue] = None
        self._cancel_evt_queue_iterator: Optional[AbstractQueueIterator] = None
        self._task_routing: Optional[Routing] = None
        self._delivered: Dict[str, AbstractIncomingMessage] = dict()
        self._exit_stack = AsyncExitStack()

        self._declare_exchanges = False

    async def _aenter__(self):
        await self._exit_stack.__aenter__()  # pylint: disable=unnecessary-dunder-call
        await self._make_connection()
        await self._bind_task_queue()
        await self._bind_cancel_event_queue()
        # TODO: the publisher is fanout so no need to bind the namespace here,
        #  make updates if that changes
        self._publisher = self._create_publisher()
        await self._exit_stack.enter_async_context(self._publisher)

    async def _bind_task_queue(self):
        self._task_routing = self.task_routing(self._namespacing, self._namespace)
        self._task_queue_iterator = await self._get_queue_iterator(
            self._connection,
            self._task_routing,
            create_queue=self._declare_exchanges,
            exchange_type=ExchangeType.DIRECT,
        )
        self._task_queue_iterator = cast(
            AbstractAsyncContextManager, self._task_queue_iterator
        )
        await self._exit_stack.enter_async_context(self._task_queue_iterator)

    async def _bind_cancel_event_queue(self):
        self._cancel_evt_queue_iterator = await self._get_queue_iterator(
            self._connection,
            self._cancel_event_routing(),
            create_queue=True,  # The worker always creates its own transient queue
            durable_queue=False,
            created_queue_name=self._cancel_event_queue_name,
            exchange_type=ExchangeType.FANOUT,
        )
        self._cancel_evt_queue_iterator = cast(
            AbstractAsyncContextManager, self._cancel_evt_queue_iterator
        )
        await self._exit_stack.enter_async_context(self._cancel_evt_queue_iterator)

    async def _make_connection(self):
        if self._connection_ is None:
            self._connection_ = await connect_robust(
                self._broker_url,
                timeout=self._connection_timeout_s,
                reconnect_interval=self._reconnection_wait_s,
                connection_class=RobustConnection,
            )
            await self._exit_stack.enter_async_context(self._connection_)

    async def _aexit__(self, exc_type, exc_val, exc_tb):
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    async def _get_queue_iterator(
        self,
        connection: AbstractRobustConnection,
        routing: Routing,
        *,
        create_queue: bool = True,
        created_queue_name: Optional[str] = None,
        exchange_type: ExchangeType,
        durable_queue: bool = True,
    ) -> AbstractQueueIterator:
        channel: AbstractRobustChannel = await connection.channel()
        await self._exit_stack.enter_async_context(
            cast(AbstractAsyncContextManager, channel)
        )
        arguments = None
        if self._declare_exchanges:
            if routing.dead_letter_routing is not None:
                dl_routing = routing.dead_letter_routing
                dlq_ex = await channel.declare_exchange(
                    dl_routing.exchange.name, type=exchange_type, durable=True
                )
                dl_queue = await channel.declare_queue(
                    dl_routing.queue_name, durable=True
                )
                await dl_queue.bind(dlq_ex, routing_key=routing.routing_key)
                arguments = {"x-dead-letter-exchange": dlq_ex.name}
            ex = await channel.declare_exchange(
                routing.exchange.name, type=exchange_type, durable=True
            )
        else:
            ex = await channel.get_exchange(routing.exchange.name)
        if create_queue:
            if created_queue_name is None:
                created_queue_name = routing.queue_name
            queue = await channel.declare_queue(
                created_queue_name, durable=durable_queue, arguments=arguments
            )
        else:
            queue = await channel.get_queue(routing.queue_name)
        await queue.bind(ex, routing_key=routing.routing_key)
        kwargs = dict()
        if self._inactive_after_s is not None:
            kwargs["timeout"] = self._inactive_after_s
        return queue.iterator(**kwargs)

    async def _consume(self) -> Task:
        message: AbstractIncomingMessage = await self._task_messages_it.__anext__()
        task = Task.parse_raw(message.body)
        self._delivered[task.id] = message
        return task

    async def _consume_cancelled(self) -> CancelledTaskEvent:
        message: AbstractIncomingMessage = await self._cancel_events_it.__anext__()
        # TODO: handle project deserialization here
        event = CancelledTaskEvent.parse_raw(message.body)
        return event

    @property
    def _task_messages_it(self) -> AbstractQueueIterator:
        if self._task_queue_iterator is None:
            msg = (
                f"Worker as no task message iterator, "
                f"please call {AMQPWorker.__aenter__} first"
            )
            raise ValueError(msg)
        return self._task_queue_iterator

    @property
    def _cancel_events_it(self) -> AbstractQueueIterator:
        if self._cancel_evt_queue_iterator is None:
            msg = (
                f"Worker as no cancel events iterator, "
                f"please call {AMQPWorker.__aenter__} first"
            )
            raise ValueError(msg)
        return self._cancel_evt_queue_iterator

    async def _acknowledge(self, task: Task, completed_at: datetime) -> Task:
        message = self._delivered[task.id]
        await message.ack()
        acked = safe_copy(
            task,
            update={
                "state": TaskState.DONE,
                "progress": 100,
                "completed_at": completed_at,
            },
        )
        return acked

    async def _negatively_acknowledge(self, nacked: Task, *, cancelled: bool):
        # pylint: disable=unused-argument
        message = self._delivered[nacked.id]
        requeue = nacked.state is TaskState.QUEUED
        await message.nack(requeue=requeue)

    async def _publish_event(self, event: TaskEvent):
        await self._publisher.publish_event_(event)

    async def _save_result(self, result: TaskResult):
        await self._publisher.publish_result(result)

    async def _save_error(self, error: TaskError):
        await self._publisher.publish_error(error)

    @staticmethod
    def task_routing(namespacing: Namespacing, namespace: Optional[str]) -> Routing:
        routing = namespacing.amqp_task_routing(namespace)
        return routing

    @classmethod
    def _cancel_event_routing(cls) -> Routing:
        return Routing(
            exchange=Exchange(
                name="exchangeMainCancelledEvents", type=ExchangeType.FANOUT
            ),
            routing_key="routingKeyMainCancelledEvents",
            queue_name="queueCancelledEvents",
        )

    @property
    def _connection(self) -> AbstractRobustConnection:
        if self._connection_ is None:
            msg = (
                f"Publisher has no connection, please call"
                f" {AMQPPublisher.__aenter__.__name__}"
            )
            raise ValueError(msg)
        return self._connection_

    def _create_publisher(self) -> AMQPPublisher:
        return AMQPPublisher(
            self._logger,
            broker_url=self._broker_url,
            connection_timeout_s=self._reconnection_wait_s,
            reconnection_wait_s=self._reconnection_wait_s,
            app_id=self._app.name,
            connection=self._connection,
        )

    @classmethod
    def _from_config(cls: Type[T], config: AMQPWorkerConfig, **extras) -> AMQPWorker:
        worker = cls(
            broker_url=config.broker_url,
            connection_timeout_s=config.amqp_connection_timeout_s,
            reconnection_wait_s=config.amqp_reconnection_wait_s,
            inactive_after_s=config.inactive_after_s,
            **extras,
        )
        worker.set_config(config)
        return worker
