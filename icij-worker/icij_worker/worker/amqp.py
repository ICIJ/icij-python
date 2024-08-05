from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from copy import deepcopy
from functools import lru_cache
from typing import ClassVar, Dict, Optional, Type, cast

from aio_pika import RobustQueue
from aio_pika.abc import (
    AbstractIncomingMessage,
    AbstractQueueIterator,
    AbstractRobustConnection,
)
from pydantic import Field

from icij_common.pydantic_utils import safe_copy
from icij_worker import (
    AsyncApp,
    ManagerEvent,
    ResultEvent,
    Task,
    TaskError,
    Worker,
    WorkerConfig,
    WorkerType,
)
from icij_worker.event_publisher.amqp import (
    AMQPPublisher,
)
from icij_worker.namespacing import Routing
from icij_worker.objects import Message, TaskState, WorkerEvent
from icij_worker.utils.amqp import AMQPMixin
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
class AMQPWorker(Worker, AMQPMixin):

    def __init__(
        self,
        app: AsyncApp,
        worker_id: Optional[str] = None,
        *,
        namespace: Optional[str],
        broker_url: str,
        connection_timeout_s: float = 1.0,
        reconnection_wait_s: float = 5.0,
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
        AMQPMixin.__init__(
            self,
            broker_url,
            connection_timeout_s=connection_timeout_s,
            reconnection_wait_s=reconnection_wait_s,
            inactive_after_s=inactive_after_s,
        )
        cancel_evt_queue_name = f"{self.worker_evt_routing().queue_name}-{self._id}"
        self._worker_evt_routing = safe_copy(
            self.worker_evt_routing(), update={"queue_name": cancel_evt_queue_name}
        )

        self._publisher: Optional[AMQPPublisher] = None
        self._connection_: Optional[AbstractRobustConnection] = None
        self._task_queue_: Optional[RobustQueue] = None
        self._task_queue_iterator: Optional[AbstractQueueIterator] = None
        self._worker_evt_queue_iterator: Optional[AbstractQueueIterator] = None
        self._task_routing: Optional[Routing] = None
        self._delivered: Dict[str, AbstractIncomingMessage] = dict()

        self._declare_exchanges = False

    @property
    def _app_id(self) -> str:
        return self._app.name

    async def _aenter__(self):
        await self._exit_stack.__aenter__()  # pylint: disable=unnecessary-dunder-call
        self._publisher = self._create_publisher()
        await self._exit_stack.enter_async_context(self._publisher)
        self._connection_ = self._publisher.connection
        self._channel_ = await self._connection.channel(
            publisher_confirms=True,
            on_return_raises=False,
        )
        await self._channel.set_qos(prefetch_count=1, global_=False)
        await self._exit_stack.enter_async_context(self._channel)
        await self._bind_task_queue()
        await self._bind_worker_event_queue()

    async def _aexit__(self, exc_type, exc_val, exc_tb):
        event_transient_queue = await self._channel.get_queue(
            self._worker_evt_routing.queue_name
        )
        await event_transient_queue.delete(if_empty=False, if_unused=False)
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    async def _bind_task_queue(self):
        task_routing = self.task_routing(self._namespace)
        if self._app.config.max_task_queue_size is not None:
            queue_args = deepcopy(task_routing.queue_args)
            queue_args["x-max-length"] = self._app.config.max_task_queue_size
            task_routing = safe_copy(task_routing, update={"queue_args": queue_args})
        self._task_routing = task_routing
        self._task_queue_iterator, _, _ = await self._get_queue_iterator(
            self._task_routing,
            declare_exchanges=self._declare_exchanges,
            declare_queues=self._declare_exchanges,
        )
        self._task_queue_iterator = cast(
            AbstractAsyncContextManager, self._task_queue_iterator
        )
        await self._exit_stack.enter_async_context(self._task_queue_iterator)

    async def _bind_worker_event_queue(self):
        self._worker_evt_queue_iterator, _, _ = await self._get_queue_iterator(
            self._worker_evt_routing,
            declare_queues=True,  # The worker always creates its own transient queue
            durable_queues=False,
            declare_exchanges=self._declare_exchanges,
        )
        self._worker_evt_queue_iterator = cast(
            AbstractAsyncContextManager, self._worker_evt_queue_iterator
        )
        await self._exit_stack.enter_async_context(self._worker_evt_queue_iterator)

    async def _consume(self) -> Task:
        while "I'm waiting to get a known task":
            message: AbstractIncomingMessage = await self._task_messages_it.__anext__()
            task = Task.parse_raw(message.body)
            self._delivered[task.id] = message
            # This behavior is not shared with other implems, it's AMQP specific and
            # avoid a worker consuming in loops tasks which it can't handle.
            # This also bypasses the normal/tested/share7 behavior of the Worker class
            if not await self._ensure_can_consume(task):
                continue
            return task

    async def _consume_worker_events(self) -> WorkerEvent:
        message: AbstractIncomingMessage = await self._worker_events_it.__anext__()
        await message.ack()
        event = cast(WorkerEvent, Message.parse_raw(message.body))
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
    def _worker_events_it(self) -> AbstractQueueIterator:
        if self._worker_evt_queue_iterator is None:
            msg = (
                f"Worker as no cancel events iterator, "
                f"please call {AMQPWorker.__aenter__} first"
            )
            raise ValueError(msg)
        return self._worker_evt_queue_iterator

    async def _acknowledge(self, task: Task):
        message = self._delivered[task.id]
        await message.ack()

    async def _negatively_acknowledge(self, nacked: Task):
        # pylint: disable=unused-argument
        message = self._delivered[nacked.id]
        requeue = nacked.state is not TaskState.ERROR
        await message.nack(requeue=requeue)

    async def _publish_event(self, event: ManagerEvent):
        await self._publisher.publish_event(event)

    async def _save_result(self, result: ResultEvent):
        await self._publisher.publish_result(result)

    async def _save_error(self, error: TaskError):
        await self._publisher.publish_error(error)

    @lru_cache()
    def task_routing(self, namespace: Optional[str]) -> Routing:
        routing = self._namespacing.amqp_task_routing(namespace)
        return routing

    def _create_publisher(self) -> AMQPPublisher:
        return AMQPPublisher(
            self._logger,
            broker_url=self._broker_url,
            connection_timeout_s=self._reconnection_wait_s,
            reconnection_wait_s=self._reconnection_wait_s,
            app_id=self._app.name,
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

    async def _ensure_can_consume(self, task: Task) -> bool:
        if task.name not in self._app.registry:
            msg = (
                f'Task(id="%s") has unknown name "{task.name}" for worker. '
                f"Nacking it with requeue."
            )
            self.error(msg)
            task_msg = self._delivered[task.id]
            await task_msg.nack(requeue=True)
            return False
        return True
