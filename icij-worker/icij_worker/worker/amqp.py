from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from functools import lru_cache
from typing import cast

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
    AsyncBackend,
    ManagerEvent,
    ResultEvent,
    Task,
    TaskError,
    Worker,
    WorkerConfig,
)
from icij_worker.event_publisher.amqp import (
    AMQPPublisher,
)
from icij_worker.exceptions import MessageDeserializationError
from icij_worker.objects import Message, WorkerEvent
from icij_worker.routing_strategy import Routing
from icij_worker.utils.amqp import (
    AMQPConfigMixin,
    AMQPManagementClient,
    AMQPMixin,
    amqp_task_group_policy,
    worker_events_policy,
)

# TODO: remove this when project information is inside the tasks
_PROJECT_PLACEHOLDER = "placeholder_project_amqp"


@WorkerConfig.register()
class AMQPWorkerConfig(WorkerConfig, AMQPConfigMixin):
    type: str = Field(frozen=True, default=AsyncBackend.amqp.value)


@Worker.register(AsyncBackend.amqp)
class AMQPWorker(Worker, AMQPMixin):
    def __init__(
        self,
        app: AsyncApp,
        management_client: AMQPManagementClient,
        worker_id: str | None = None,
        *,
        group: str | None,
        broker_url: str,
        connection_timeout_s: float = 1.0,
        reconnection_wait_s: float = 5.0,
        inactive_after_s: float | None = None,
        is_qpid: bool = False,
        handle_signals: bool = True,
        teardown_dependencies: bool = False,
    ):
        super().__init__(
            app,
            worker_id,
            group=group,
            handle_signals=handle_signals,
            teardown_dependencies=teardown_dependencies,
        )
        AMQPMixin.__init__(
            self,
            broker_url,
            connection_timeout_s=connection_timeout_s,
            reconnection_wait_s=reconnection_wait_s,
            inactive_after_s=inactive_after_s,
            is_qpid=is_qpid,
        )
        self._management_client = management_client
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
        self._delivered: dict[str, AbstractIncomingMessage] = dict()

        self._declare_exchanges = True

    @property
    def _app_id(self) -> str:
        return self._app.name

    async def _aenter__(self):
        await self._exit_stack.__aenter__()  # pylint: disable=unnecessary-dunder-call
        if not self._is_qpid:
            await self._exit_stack.enter_async_context(self._management_client)
        self._publisher = self._create_publisher()
        await self._exit_stack.enter_async_context(self._publisher)
        self._connection_ = self._publisher.connection
        self._channel_ = await self._connection.channel(
            publisher_confirms=self._publisher_confirms, on_return_raises=False
        )
        await self._channel.set_qos(prefetch_count=1, global_=False)
        await self._exit_stack.enter_async_context(self._channel)
        await self._bind_task_queue()
        await self._bind_worker_event_queue()
        await self._create_routing(
            self.manager_evt_routing(), declare_queues=True, declare_exchanges=True
        )

    async def _aexit__(self, exc_type, exc_val, exc_tb):
        event_transient_queue = await self._channel.get_queue(
            self._worker_evt_routing.queue_name
        )
        await event_transient_queue.delete(if_empty=True, if_unused=False)
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    async def _bind_task_queue(self):
        task_routing = self.task_routing(self._group)
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
        if not self._is_qpid:
            task_group = self._app.task_group(self._group)
            group_policy = amqp_task_group_policy(
                self._task_routing, task_group, self._app.config.max_task_queue_size
            )
            await self._management_client.set_policy(group_policy)
            worker_event_policy = worker_events_policy(
                self.__class__.worker_evt_routing()
            )
            await self._management_client.set_policy(worker_event_policy)

    async def _bind_worker_event_queue(self):
        self._worker_evt_queue_iterator, _, _ = await self._get_queue_iterator(
            self._worker_evt_routing,
            declare_queues=True,
            declare_exchanges=self._declare_exchanges,
        )
        self._worker_evt_queue_iterator = cast(
            AbstractAsyncContextManager, self._worker_evt_queue_iterator
        )
        await self._exit_stack.enter_async_context(self._worker_evt_queue_iterator)

    async def _consume(self) -> Task:
        # TODO: remove this now that task groups exist
        while "I'm waiting to get a known task":
            # pylint: disable=unnecessary-dunder-call
            message: AbstractIncomingMessage = await self._task_messages_it.__anext__()
            try:
                task = Task.model_validate_json(message.body)
            except Exception as e:
                msg = f"invalid task body {message.body}"
                raise MessageDeserializationError(msg) from e
            self._delivered[task.id] = message
            # This behavior is not shared with other implems, it's AMQP specific and
            # avoid a worker consuming in loops tasks which it can't handle.
            # This also bypasses the normal/tested/share7 behavior of the Worker class
            if not await self._ensure_can_consume(task):
                continue
            return task

    async def _consume_worker_events(self) -> WorkerEvent:
        # pylint: disable=unnecessary-dunder-call
        message: AbstractIncomingMessage = await self._worker_events_it.__anext__()
        await message.ack()
        try:
            event = cast(WorkerEvent, Message.model_validate_json(message.body))
        except Exception as e:
            msg = f"invalid event body {message.body}"
            raise MessageDeserializationError(msg) from e
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
        # We don't actually nack the task message here, otherwise the task message
        # would be redistributed as is. Instead, since a ErrorEvent was sent earlier,
        # the TM will requeue the task as needed (with the updated retry count and
        # so on). So we just mark the current task message as correctly processed
        message = self._delivered[nacked.id]
        await message.ack()

    async def _publish_event(self, event: ManagerEvent):
        await self._publisher.publish_event(event)

    async def _save_result(self, result: ResultEvent):
        await self._publisher.publish_result(result)

    async def _save_error(self, error: TaskError):
        await self._publisher.publish_error(error)

    @lru_cache()
    def task_routing(self, group: str | None) -> Routing:
        routing = self._routing_strategy.amqp_task_routing(group)
        return routing

    def _create_publisher(self) -> AMQPPublisher:
        return AMQPPublisher(
            self._logger,
            broker_url=self._broker_url,
            connection_timeout_s=self._reconnection_wait_s,
            reconnection_wait_s=self._reconnection_wait_s,
            is_qpid=self._is_qpid,
            app_id=self._app.name,
        )

    @classmethod
    def _from_config(cls, config: AMQPWorkerConfig, **extras) -> AMQPWorker:
        management_client = config.to_management_client()
        worker = cls(
            management_client=management_client,
            broker_url=config.broker_url,
            connection_timeout_s=config.connection_timeout_s,
            reconnection_wait_s=config.reconnection_wait_s,
            is_qpid=config.rabbitmq_is_qpid,
            inactive_after_s=config.inactive_after_s,
            **extras,
        )
        return worker

    async def _ensure_can_consume(self, task: Task) -> bool:
        if task.name not in self._app.registry:
            msg = (
                f'Task(id="{task.id}") has unknown name "{task.name}" for worker. '
                f"Nacking it with requeue."
            )
            self.error(msg)
            task_msg = self._delivered[task.id]
            await task_msg.nack(requeue=True)
            return False
        return True
