from __future__ import annotations

from contextlib import AsyncExitStack
from datetime import datetime
from functools import lru_cache
from typing import ClassVar, Dict, Optional, Tuple, Type

from aio_pika import RobustQueue, connect_robust
from aio_pika.abc import (
    AbstractIncomingMessage,
    AbstractQueueIterator,
    AbstractRobustChannel,
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
    TaskStatus,
    Worker,
    WorkerConfig,
    WorkerType,
)
from icij_worker.event_publisher import AMQPPublisher
from icij_worker.event_publisher.amqp import Exchange, RobustConnection, Routing
from icij_worker.utils.from_config import T


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
            f"{amqp_userinfo}{self.rabbitmq_host}"
            f"{f':{self.rabbitmq_port}' if self.rabbitmq_port else ''}"
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
        broker_url: str,
        connection_timeout_s: Optional[float] = None,
        reconnection_wait_s: Optional[float] = None,
        inactive_after_s: Optional[float] = None,
        handle_signals: bool = True,
        teardown_dependencies: bool = False,
        **kwargs,
    ):
        super().__init__(
            app,
            worker_id,
            handle_signals=handle_signals,
            teardown_dependencies=teardown_dependencies,
            **kwargs,
        )
        self._broker_url = broker_url
        self._reconnection_wait_s = reconnection_wait_s
        self._connection_timeout_s = connection_timeout_s
        self._inactive_after_s = inactive_after_s
        self._publisher = self._create_publisher()
        self._consumer_connection: Optional[RobustConnection] = None
        self._consumer_queue_: Optional[RobustQueue] = None
        self._queue_iterator: Optional[AbstractQueueIterator] = None
        self._delivered: Dict[str, AbstractIncomingMessage] = dict()
        self._exit_stack = AsyncExitStack()

        self._declare_and_bind = False

    async def _aenter__(self):
        await self._exit_stack.__aenter__()  # pylint: disable=unnecessary-dunder-call
        await self._exit_stack.enter_async_context(self._publisher)
        self._consumer_connection = await connect_robust(
            self._broker_url,
            timeout=self._connection_timeout_s,
            reconnect_interval=self._reconnection_wait_s,
            connection_class=RobustConnection,
        )
        await self._exit_stack.enter_async_context(self._consumer_connection)
        # This one will automatically get destroyed with the connection
        self._queue_iterator = await self._get_queue_iterator()
        await self._exit_stack.enter_async_context(self._queue_iterator)

    async def _aexit__(self, exc_type, exc_val, exc_tb):
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    async def _get_queue_iterator(self) -> AbstractQueueIterator:
        channel: AbstractRobustChannel = await self._consumer_connection.channel()
        await self._exit_stack.enter_async_context(channel)
        routing = self._task_routing()
        if self._declare_and_bind:
            ex = await channel.declare_exchange(
                routing.exchange.name, type=ExchangeType.DIRECT, durable=True
            )
            queue = await channel.declare_queue(routing.default_queue, durable=True)
            await queue.bind(ex, routing_key=routing.routing_key)
        else:
            queue = await channel.get_queue(routing.default_queue)
        return queue.iterator(timeout=self._inactive_after_s)

    async def _consume(self) -> Tuple[Task, str]:
        message: AbstractIncomingMessage = await self._message_it.__anext__()
        # TODO: handle project deserialization here
        task = Task.parse_raw(message.body)
        self._delivered[task.id] = message
        return task, "placeholder_project_amqp"

    @property
    def _message_it(self) -> AbstractQueueIterator:
        if self._queue_iterator is None:
            msg = (
                f"Worker as not queue iterator, "
                f"please call {AMQPWorker.__aenter__} first"
            )
            raise ValueError(msg)
        return self._queue_iterator

    async def _acknowledge(
        self, task: Task, project: str, completed_at: datetime
    ) -> Task:
        message = self._delivered[task.id]
        await message.ack()
        acked = safe_copy(
            task,
            update={
                "status": TaskStatus.DONE,
                "progress": 100,
                "completed_at": completed_at,
            },
        )
        return acked

    async def _negatively_acknowledge(
        self, task: Task, project: str, *, requeue: bool
    ) -> Task:
        message = self._delivered[task.id]
        await message.nack(requeue=requeue)
        if requeue:
            update = {
                "status": TaskStatus.QUEUED,
                "progress": 0.0,
                "retries": task.retries or 0 + 1,
            }
        else:
            update = {"status": TaskStatus.ERROR}
        nacked = safe_copy(task, update=update)
        return nacked

    async def _refresh_cancelled(self, project: str):
        # TODO: implement AMQP cancellation
        pass

    async def publish_event(self, event: TaskEvent, project: str):
        await self._publisher.publish_event(event, project)

    async def _save_result(self, result: TaskResult, project: str):
        await self._publisher.publish_result(result, project)

    async def _save_error(self, error: TaskError, task: Task, project: str):
        await self._publisher.publish_error(error, project, project)

    @classmethod
    @lru_cache(maxsize=1)
    def _task_routing(cls) -> Routing:
        # TODO: routing must be improved here, as a worker should not listen for all
        #  tasks but only for a subset (task should be group by semantic / duration /
        #  resource consumption)
        return Routing(
            exchange=Exchange(name="exchangeMainTasks", type=ExchangeType.DIRECT),
            routing_key="routingKeyMainTasks",
            default_queue="queueMainTasks",
        )

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
