from __future__ import annotations

import asyncio
import functools
import logging
from datetime import datetime
from functools import cached_property
from typing import Dict, List, Optional, TypeVar, Union, cast

from aio_pika import Exchange as AioPikaExchange, connect_robust
from aio_pika.abc import AbstractIncomingMessage, AbstractQueueIterator
from aiormq import DeliveryError

from icij_common.pydantic_utils import safe_copy
from icij_worker import Namespacing, Task, TaskManager
from icij_worker.event_publisher.amqp import RobustConnection
from icij_worker.exceptions import TaskQueueIsFull
from icij_worker.namespacing import Routing
from icij_worker.objects import (
    CancelledTaskEvent,
    Message,
    TaskError,
    TaskEvent,
    TaskResult,
    TaskState,
)
from icij_worker.task_storage import TaskStorage
from icij_worker.utils.amqp import AMQPMixin
from icij_worker.utils.asyncio_ import stop_other_tasks_when_exc

S = TypeVar("S", bound=TaskStorage)

logger = logging.getLogger(__name__)


class AMQPTaskManager(TaskManager, AMQPMixin):

    def __init__(
        self,
        task_store: TaskStorage,
        app_name: str,
        namespacing: Optional[Namespacing] = None,
        *,
        broker_url: str,
        max_task_queue_length: int = int(1e6),
        connection_timeout_s: Optional[float] = None,
        reconnection_wait_s: Optional[float] = None,
        inactive_after_s: Optional[float] = None,
    ):
        super().__init__(app_name, namespacing=namespacing)
        super(TaskManager, self).__init__(
            broker_url,
            connection_timeout_s=connection_timeout_s,
            reconnection_wait_s=reconnection_wait_s,
            inactive_after_s=inactive_after_s,
        )
        self._max_task_queue_length = max_task_queue_length
        self._storage = task_store

        self._loop = asyncio.get_event_loop()
        self._loops = set()

        self._task_queues = set()

        self._task_x: Optional[AioPikaExchange] = None
        self._worker_evt_x: Optional[AioPikaExchange] = None
        self._res_and_err_x: Optional[AioPikaExchange] = None

        self._evt_messages_it: Optional[AbstractQueueIterator] = None
        self._err_messages_it: Optional[AbstractQueueIterator] = None
        self._res_and_err_messages_it: Optional[AbstractQueueIterator] = None

        self._task_namespaces: Dict[str, Optional[str]] = dict()

    async def __aenter__(self) -> AMQPTaskManager:
        logger.info("starting task manager connection workflow...")
        await self._exit_stack.__aenter__()
        await self._connection_workflow()
        self._evt_messages_it = (
            await self._get_queue_iterator(self.evt_routing(), declare_exchanges=False)
        )[0]
        self._res_and_err_messages_it = (
            await self._get_queue_iterator(
                self.res_and_err_routing(), declare_exchanges=False
            )
        )[0]
        logger.info("starting consume loops..")
        self._start_loops()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._stop_loops()
        await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)

    @cached_property
    def _app_id(self) -> str:
        # TODO: we could do better if needed
        return self._app_name

    async def get_task(self, task_id: str) -> Task:
        return await self._storage.get_task(task_id)

    async def get_task_namespace(self, task_id: str) -> Optional[str]:
        return await self._storage.get_task_namespace(task_id)

    async def get_task_errors(self, task_id: str) -> List[TaskError]:
        return await self._storage.get_task_errors(task_id)

    async def get_task_result(self, task_id: str) -> TaskResult:
        return await self._storage.get_task_result(task_id)

    async def get_tasks(
        self,
        namespace: Optional[str],
        *,
        task_type: Optional[str] = None,
        state: Optional[Union[List[TaskState], TaskState]] = None,
        **kwargs,
    ) -> List[Task]:
        return await self._storage.get_tasks(
            namespace, task_type=task_type, state=state
        )

    async def save_task(self, task: Task, namespace: Optional[str]):
        await self._storage.save_task(task, namespace)

    async def save_result(self, result: TaskResult):
        await self._storage.save_result(result)

    async def save_error(self, error: TaskError):
        await self._storage.save_error(error)

    async def _enqueue(self, task: Task, **kwargs) -> Task:
        namespace = await self._storage.get_task_namespace(task.id)
        await self._ensure_task_queue(namespace)
        routing = self._namespacing.amqp_task_routing(namespace)
        try:
            await self._publish_message(
                task,
                exchange=self._task_x,
                routing_key=routing.routing_key,
                mandatory=True,  # This is important
            )
        except DeliveryError as e:
            raise TaskQueueIsFull(self._max_task_queue_length) from e
        # TODO: all of this wouldn't be need if the task manager would return the task
        #  ID instead of the task state
        queued = safe_copy(task, update={"state": TaskState.QUEUED})
        return queued

    async def _cancel(self, *, task_id: str, requeue: bool):
        cancelled_at = datetime.now()
        cancel_event = CancelledTaskEvent(
            task_id=task_id, requeue=requeue, cancelled_at=cancelled_at
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
        if self._max_task_queue_length is not None:
            task_queue_args = {
                "x-overflow": "reject-publish",
                "x-max-length": self._max_task_queue_length,
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
        self._task_x = await self._channel.get_exchange(
            self.default_task_routing().exchange.name, ensure=True
        )
        self._res_and_err_x = await self._channel.get_exchange(
            self.res_and_err_routing().exchange.name, ensure=True
        )
        self._worker_evt_x = await self._channel.get_exchange(
            self.worker_evt_routing().exchange.name, ensure=True
        )
        logger.info("connection workflow complete")

    @cached_property
    def _other_routings(self) -> List[Routing]:
        worker_events_routing = AMQPMixin.worker_evt_routing()
        events_routing = AMQPMixin.evt_routing()
        res_and_err_routing = AMQPMixin.res_and_err_routing()
        return [events_routing, worker_events_routing, res_and_err_routing]

    async def _ensure_task_queue(self, namespace: Optional[str]):
        if namespace not in self._task_queues:
            self._task_queues.add(namespace)
            routing = self._namespacing.amqp_task_routing(namespace)
            logger.debug(
                "(re)declaring queue %s for namespace %s", routing.queue_name, namespace
            )
            task_routing = self.default_task_routing()
            dlx_name = task_routing.dead_letter_routing.exchange.name
            dl_routing_key = task_routing.dead_letter_routing.routing_key
            arguments = {
                "x-overflow": "reject-publish",
                "x-max-length": self._max_task_queue_length,
                "x-dead-letter-exchange": dlx_name,
                "x-dead-letter-routing-key": dl_routing_key,
            }
            queue = await self._channel.declare_queue(
                routing.queue_name, durable=True, arguments=arguments
            )
            logger.debug("binding queues %s...", routing.queue_name)
            await queue.bind(routing.exchange.name, routing.routing_key)

    async def _consume_events(self):
        while True:
            message: AbstractIncomingMessage = await self._evt_messages_it.__anext__()
            event = cast(TaskEvent, Message.parse_raw(message.body))
            logger.debug("saving event for task: %s", event.task_id)
            await self._storage.save_event(event)

    async def _consume_result_and_errors(self):
        while True:
            message: AbstractIncomingMessage = (
                await self._res_and_err_messages_it.__anext__()
            )
            msg = Message.parse_raw(message.body)
            if isinstance(msg, TaskResult):
                logger.debug("saving result for task: %s", msg.task_id)
                await self._storage.save_result(msg)
            elif isinstance(msg, TaskError):
                logger.debug("saving error: %s", msg)
                await self._storage.save_error(msg)
            else:
                raise TypeError(f"unexpected message type {msg.__class__}")

    def _start_loops(self):
        self._loops = [self._consume_result_and_errors(), self._consume_events()]
        self._loops = [self._loop.create_task(t) for t in self._loops]
        callback = functools.partial(stop_other_tasks_when_exc, others=self._loops)
        for loop in self._loops:
            loop.add_done_callback(callback)

    async def _stop_loops(self):
        for loop in self._loops:
            loop.cancel()
        await asyncio.wait(self._loops, return_when=asyncio.ALL_COMPLETED)
        del self._loops
        self._loops = []