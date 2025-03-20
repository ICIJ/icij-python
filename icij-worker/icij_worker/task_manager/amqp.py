from __future__ import annotations

import logging
from datetime import datetime, timezone
from functools import cached_property
from typing import TypeVar, cast

from aio_pika import connect_robust
from aio_pika.abc import AbstractExchange, AbstractQueueIterator
from aiormq import DeliveryError
from pydantic import Field

from icij_worker.app import AsyncApp
from icij_worker.exceptions import MessageDeserializationError, TaskQueueIsFull
from icij_worker.objects import (
    AsyncBackend,
    CancelEvent,
    ErrorEvent,
    ManagerEvent,
    Message,
    ResultEvent,
    ShutdownEvent,
    Task,
    TaskState,
)
from icij_worker.routing_strategy import Routing
from icij_worker.task_manager import TaskManager, TaskManagerConfig
from icij_worker.task_storage import TaskStorage
from icij_worker.task_storage.fs import FSKeyValueStorageConfig
from icij_worker.task_storage.postgres import PostgresStorageConfig
from icij_worker.utils.amqp import (
    AMQPConfigMixin,
    AMQPManagementClient,
    AMQPMixin,
    RobustConnection,
    amqp_task_group_policy,
    health_policy,
)

logger = logging.getLogger(__name__)

S = TypeVar("S", bound=TaskStorage)


@TaskManagerConfig.register()
class AMQPTaskManagerConfig(TaskManagerConfig, AMQPConfigMixin):
    backend: AsyncBackend = Field(frozen=True, default=AsyncBackend.amqp)
    storage: FSKeyValueStorageConfig | PostgresStorageConfig


@TaskManager.register(AsyncBackend.amqp)
class AMQPTaskManager(TaskManager, AMQPMixin):
    def __init__(
        self,
        app: AsyncApp,
        task_store: TaskStorage,
        management_client: AMQPManagementClient,
        *,
        broker_url: str,
        connection_timeout_s: float = 1.0,
        reconnection_wait_s: float = 5.0,
        inactive_after_s: float | None = None,
        is_qpid: bool = False,
    ):
        super().__init__(app)
        super(TaskManager, self).__init__(  # pylint: disable=bad-super-call
            broker_url,
            connection_timeout_s=connection_timeout_s,
            reconnection_wait_s=reconnection_wait_s,
            inactive_after_s=inactive_after_s,
            is_qpid=is_qpid,
        )
        self._management_client = management_client
        self._storage = task_store
        self._task_x: AbstractExchange | None = None
        self._worker_evt_x: AbstractExchange | None = None
        self._health_x: AbstractExchange | None = None
        self._manager_messages_it: AbstractQueueIterator | None = None

        self._task_groups: dict[str, str | None] = dict()

    @classmethod
    def _from_config(cls, config: AMQPTaskManagerConfig, **extras) -> AMQPTaskManager:
        # pylint: disable=unused-argument
        app = config.app
        storage = config.storage.to_storage(app.routing_strategy)
        management_client = config.to_management_client()
        task_manager = cls(
            app,
            storage,
            management_client,
            broker_url=config.broker_url,
            connection_timeout_s=config.connection_timeout_s,
            reconnection_wait_s=config.reconnection_wait_s,
            is_qpid=config.rabbitmq_is_qpid,
        )
        return task_manager

    async def _aenter__(self) -> AMQPTaskManager:
        logger.info("starting task manager connection workflow...")
        await self._exit_stack.__aenter__()  # pylint: disable=unnecessary-dunder-call
        await self._exit_stack.enter_async_context(self._storage)
        await self._connection_workflow()
        self._manager_messages_it = (
            await self._get_queue_iterator(
                self.manager_evt_routing(),
                declare_exchanges=False,
                declare_queues=False,
            )
        )[0]
        return self

    async def _aexit__(self, exc_type, exc_val, exc_tb):
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    @cached_property
    def _app_id(self) -> str:
        return self.app_name

    async def get_task(self, task_id: str) -> Task:
        return await self._storage.get_task(task_id)

    async def get_task_group(self, task_id: str) -> str | None:
        return await self._storage.get_task_group(task_id)

    async def get_task_errors(self, task_id: str) -> list[ErrorEvent]:
        return await self._storage.get_task_errors(task_id)

    async def get_task_result(self, task_id: str) -> ResultEvent:
        return await self._storage.get_task_result(task_id)

    async def get_tasks(
        self,
        group: str | None,
        *,
        task_name: str | None = None,
        state: list[TaskState] | TaskState | None = None,
        **kwargs,
    ) -> list[Task]:
        # pylint: disable=unused-argument
        return await self._storage.get_tasks(group, task_name=task_name, state=state)

    async def save_task_(self, task: Task, group: str | None) -> bool:
        return await self._storage.save_task_(task, group)

    async def save_result(self, result: ResultEvent):
        await self._storage.save_result(result)

    async def save_error(self, error: ErrorEvent):
        await self._storage.save_error(error)

    async def _consume(self) -> ManagerEvent:
        # pylint: disable=unnecessary-dunder-call
        msg = await self._manager_messages_it.__anext__()
        try:
            message = cast(ManagerEvent, Message.model_validate_json(msg.body))
        except Exception as e:
            msg = f"invalid manager event body {msg.body}"
            raise MessageDeserializationError(msg) from e
        await msg.ack()
        return message

    async def _enqueue(self, task: Task):
        group = await self._storage.get_task_group(task.id)
        routing = self._routing_strategy.amqp_task_routing(group)
        try:
            await self._publish_message(
                task,
                exchange=self._task_x,
                routing_key=routing.routing_key,
                mandatory=True,  # This is important
            )
        except DeliveryError as e:
            raise TaskQueueIsFull(self.max_task_queue_size) from e

    async def _requeue(self, task: Task):
        group = await self._storage.get_task_group(task.id)
        routing = self._routing_strategy.amqp_task_routing(group)
        try:
            await self._publish_message(
                task,
                exchange=self._task_x,
                routing_key=routing.routing_key,
                mandatory=True,  # This is important
            )
        except DeliveryError as e:
            raise TaskQueueIsFull(self.max_task_queue_size) from e

    async def cancel(self, task_id: str, *, requeue: bool):
        cancel_event = CancelEvent(
            task_id=task_id, requeue=requeue, created_at=datetime.now(timezone.utc)
        )
        # TODO: for now cancellation is not grouped, workers from other group
        #  are responsible to ignoring the broadcast. That could be easily implemented
        #  in the future but will need sync with Java
        routing = self.worker_evt_routing().routing_key
        await self._publish_message(
            cancel_event,
            exchange=self._worker_evt_x,
            routing_key=routing,
            mandatory=True,  # This is important
        )

    async def shutdown_workers(self):
        shutdown_event = ShutdownEvent(created_at=datetime.now(timezone.utc))
        # TODO: for now cancellation is not grouped, workers from other group
        #  are responsible to ignoring the broadcast. That could be easily implemented
        #  in the future but will need sync with Java
        routing = self.worker_evt_routing().routing_key
        await self._publish_message(
            shutdown_event,
            exchange=self._worker_evt_x,
            routing_key=routing,
            mandatory=True,  # This is important
        )

    async def _connection_workflow(self):
        await self._exit_stack.enter_async_context(self._management_client)
        logger.debug("creating connection...")
        self._connection_ = await connect_robust(
            self._broker_url,
            timeout=self._connection_timeout_s,
            reconnect_interval=self._reconnection_wait_s,
            connection_class=RobustConnection,
        )
        await self._exit_stack.enter_async_context(self._connection)
        logger.debug("creating channel...")
        self._channel_ = await self._connection.channel(
            publisher_confirms=self._publisher_confirms, on_return_raises=False
        )
        await self._exit_stack.enter_async_context(self._channel)
        await self._channel.set_qos(prefetch_count=1, global_=False)
        logger.info("channel opened !")
        logger.info("creating task queues opened !")
        await self._ensure_task_queues()
        for routing in self._other_routings:
            logger.debug("(re)declaring routing %s...", routing)
            await self._create_routing(
                routing, declare_exchanges=True, declare_queues=True
            )
        await self._create_routing(
            self.worker_evt_routing(), declare_exchanges=True, declare_queues=False
        )
        await self._create_routing(
            self.manager_evt_routing(), declare_exchanges=True, declare_queues=True
        )
        await self._create_routing(
            self.health_routing(), declare_exchanges=True, declare_queues=True
        )
        self._task_x = await self._channel.get_exchange(
            self.default_task_routing().exchange.name, ensure=True
        )
        self._manager_evt_x = await self._channel.get_exchange(
            self.manager_evt_routing().exchange.name, ensure=True
        )
        self._worker_evt_x = await self._channel.get_exchange(
            self.worker_evt_routing().exchange.name, ensure=True
        )
        self._health_x = await self._channel.get_exchange(
            self.health_routing().exchange.name, ensure=True
        )
        healthz_policy = health_policy(self.health_routing())
        await self._management_client.set_policy(healthz_policy)
        logger.info("connection workflow complete")

    @cached_property
    def _other_routings(self) -> list[Routing]:
        worker_events_routing = AMQPMixin.worker_evt_routing()
        manager_events_routing = AMQPMixin.manager_evt_routing()
        return [worker_events_routing, manager_events_routing]

    async def _ensure_task_queues(self):
        default_routing = AMQPMixin.default_task_routing()
        group_policy = amqp_task_group_policy(
            default_routing, None, self.max_task_queue_size
        )
        await self._create_routing(
            default_routing, declare_exchanges=True, declare_queues=True
        )
        await self._management_client.set_policy(group_policy)
        for group in self._app.task_groups:
            routing = self._routing_strategy.amqp_task_routing(group.name)
            group_policy = amqp_task_group_policy(
                routing, group, self.max_task_queue_size
            )
            await self._management_client.set_policy(group_policy)
            await self._create_routing(
                routing, declare_exchanges=True, declare_queues=True
            )

    async def get_health(self) -> dict[str, bool]:
        storage_health = await self._storage.get_health()
        amqp_health = await self._get_amqp_health()
        return {"storage": storage_health, "amqp": amqp_health}
