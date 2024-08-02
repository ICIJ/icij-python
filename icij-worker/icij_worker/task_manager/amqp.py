from __future__ import annotations

import logging
from datetime import datetime, timezone
from functools import cached_property
from typing import Dict, List, Optional, TypeVar, Union, cast

from aio_pika import connect_robust
from aio_pika.abc import AbstractExchange, AbstractQueueIterator
from aiormq import DeliveryError

from icij_worker import AsyncApp, Task, TaskManager
from icij_worker.event_publisher.amqp import RobustConnection
from icij_worker.exceptions import TaskQueueIsFull
from icij_worker.namespacing import Routing
from icij_worker.objects import (
    CancelEvent,
    ErrorEvent,
    ManagerEvent,
    Message,
    ResultEvent,
    TaskState,
)
from icij_worker.task_storage import TaskStorage
from icij_worker.utils.amqp import AMQPMixin

S = TypeVar("S", bound=TaskStorage)

logger = logging.getLogger(__name__)


class AMQPTaskManager(TaskManager, AMQPMixin):

    def __init__(
        self,
        app: AsyncApp,
        task_store: TaskStorage,
        *,
        broker_url: str,
        connection_timeout_s: float = 1.0,
        reconnection_wait_s: float = 5.0,
        inactive_after_s: Optional[float] = None,
    ):
        super().__init__(app)
        super(TaskManager, self).__init__(
            broker_url,
            connection_timeout_s=connection_timeout_s,
            reconnection_wait_s=reconnection_wait_s,
            inactive_after_s=inactive_after_s,
        )
        self._storage = task_store

        self._loops = set()

        self._task_x: Optional[AbstractExchange] = None
        self._worker_evt_x: Optional[AbstractExchange] = None
        self._manager_messages_it: Optional[AbstractQueueIterator] = None

        self._task_namespaces: Dict[str, Optional[str]] = dict()

    async def _aenter__(self) -> AMQPTaskManager:
        logger.info("starting task manager connection workflow...")
        await self._exit_stack.__aenter__()  # pylint: disable=unnecessary-dunder-call
        await self._exit_stack.enter_async_context(self._storage)
        await self._connection_workflow()
        await self._ensure_task_queues()
        self._manager_messages_it = (
            await self._get_queue_iterator(
                self.manager_evt_routing(), declare_exchanges=False
            )
        )[0]
        logger.info("starting consume loops..")
        return self

    async def _aexit__(self, exc_type, exc_val, exc_tb):
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    @cached_property
    def _app_id(self) -> str:
        return self.app_name

    async def get_task(self, task_id: str) -> Task:
        return await self._storage.get_task(task_id)

    async def get_task_namespace(self, task_id: str) -> Optional[str]:
        return await self._storage.get_task_namespace(task_id)

    async def get_task_errors(self, task_id: str) -> List[ErrorEvent]:
        return await self._storage.get_task_errors(task_id)

    async def get_task_result(self, task_id: str) -> ResultEvent:
        return await self._storage.get_task_result(task_id)

    async def get_tasks(
        self,
        namespace: Optional[str],
        *,
        task_name: Optional[str] = None,
        state: Optional[Union[List[TaskState], TaskState]] = None,
        **kwargs,
    ) -> List[Task]:
        return await self._storage.get_tasks(
            namespace, task_name=task_name, state=state
        )

    async def save_task_(self, task: Task, namespace: Optional[str]) -> bool:
        return await self._storage.save_task_(task, namespace)

    async def save_result(self, result: ResultEvent):
        await self._storage.save_result(result)

    async def save_error(self, error: ErrorEvent):
        await self._storage.save_error(error)

    async def _consume(self) -> ManagerEvent:
        msg = await self._manager_messages_it.__anext__()
        await msg.ack()
        return cast(ManagerEvent, Message.parse_raw(msg.body))

    async def _enqueue(self, task: Task):
        namespace = await self._storage.get_task_namespace(task.id)
        await self._ensure_task_queues()
        routing = self._namespacing.amqp_task_routing(namespace)
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
        namespace = await self._storage.get_task_namespace(task.id)
        routing = self._namespacing.amqp_task_routing(namespace)
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
        # TODO: for now cancellation is not namespaced, workers from other namespace
        #  are responsible to ignoring the broadcast. That could be easily implemented
        #  in the future but will need sync with Java
        routing = self.worker_evt_routing().routing_key
        await self._publish_message(
            cancel_event,
            exchange=self._worker_evt_x,
            routing_key=routing,
            mandatory=True,  # This is important
        )

    async def _connection_workflow(self):
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
            publisher_confirms=True, on_return_raises=False
        )
        await self._exit_stack.enter_async_context(self._channel)
        await self._channel.set_qos(prefetch_count=1, global_=True)
        logger.info("channel opened !")
        task_routing = self.default_task_routing()
        logger.debug("(re)declaring routing %s...", task_routing)
        task_queue_args = None
        if self.max_task_queue_size is not None:
            task_queue_args = {
                "x-overflow": "reject-publish",
                "x-max-length": self.max_task_queue_size,
            }
        await self._create_routing(
            task_routing,
            declare_exchanges=True,
            declare_queues=True,
            durable_queues=True,
            queue_args=task_queue_args,
        )
        for routing in self._other_routings:
            logger.debug("(re)declaring routing %s...", routing)
            await self._create_routing(
                routing,
                declare_exchanges=True,
                declare_queues=True,
                durable_queues=True,
            )
        await self._create_routing(
            AMQPMixin.worker_evt_routing(), declare_exchanges=True, declare_queues=False
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
        logger.info("connection workflow complete")

    @cached_property
    def _other_routings(self) -> List[Routing]:
        worker_events_routing = AMQPMixin.worker_evt_routing()
        manager_events_routing = AMQPMixin.manager_evt_routing()
        return [worker_events_routing, manager_events_routing]

    async def _ensure_task_queues(self):
        supported_ns = set(t.namespace for t in self._app.registry.values())
        for namespace in supported_ns:
            routing = self._namespacing.amqp_task_routing(namespace)
            task_routing = self.default_task_routing()
            dlx_name = task_routing.dead_letter_routing.exchange.name
            dl_routing_key = task_routing.dead_letter_routing.routing_key
            arguments = {
                "x-dead-letter-exchange": dlx_name,
                "x-dead-letter-routing-key": dl_routing_key,
            }
            if self.max_task_queue_size is not None:
                arguments["x-max-length"] = self.max_task_queue_size
                arguments["x-overflow"] = "reject-publish"
            queue = await self._channel.declare_queue(
                routing.queue_name, durable=True, arguments=arguments
            )
            await queue.bind(routing.exchange.name, routing.routing_key)
