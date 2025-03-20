from __future__ import annotations

from asyncio import TimerHandle

import asyncio
import functools
import inspect
import logging
import os
import socket
import threading
import traceback
from abc import abstractmethod
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from copy import deepcopy
from datetime import datetime, timezone
from inspect import isawaitable
from typing import Any, Callable, Dict, Optional, TypeVar, final
from typing_extensions import Self

from icij_common.pydantic_utils import safe_copy, to_lower_snake_case
from icij_common.registrable import RegistrableFromConfig
from icij_worker import AsyncApp, ResultEvent, Task, TaskError, TaskState
from icij_worker.app import RegisteredTask, supports_progress
from icij_worker.event_publisher.event_publisher import EventPublisher
from icij_worker.exceptions import (
    MaxRetriesExceeded,
    MessageDeserializationError,
    RecoverableError,
    TaskAlreadyCancelled,
    TaskAlreadyReserved,
    UnknownTask,
    UnregisteredTask,
    WorkerTimeoutError,
)
from icij_worker.objects import (
    CancelEvent,
    CancelledEvent,
    ErrorEvent,
    ProgressEvent,
    ShutdownEvent,
    TaskUpdate,
    WorkerEvent,
)
from icij_worker.routing_strategy import RoutingStrategy
from icij_worker.worker.process import HandleSignalsMixin

logger = logging.getLogger(__name__)

C = TypeVar("C", bound="WorkerConfig")
WE = TypeVar("WE", bound=WorkerEvent)


class Worker(
    RegistrableFromConfig,
    EventPublisher,
    HandleSignalsMixin,
    AbstractAsyncContextManager,
):
    def __init__(
        self,
        app: AsyncApp,
        worker_id: str | None = None,
        *,
        group: str | None,
        handle_signals: bool = True,
        teardown_dependencies: bool = False,
    ):
        # If worker are run using a thread backend then signal handling might not be
        # required, in this case the signal handling mixing will just do nothing
        HandleSignalsMixin.__init__(self, logger, handle_signals=handle_signals)
        self._app = app
        if worker_id is None:
            worker_id = self._create_worker_id()
        self._id = worker_id
        self._group = group
        self._teardown_dependencies = teardown_dependencies
        self._graceful_shutdown = True
        self._loop = asyncio.get_event_loop()
        self._work_forever_task: asyncio.Task | None = None
        self._work_once_task: asyncio.Task | None = None
        self._watch_events: asyncio.Task | None = None
        self._already_exiting: bool = False
        self._shutdown_asked: bool = False
        self._current: Optional[Task] = None
        self._worker_events: dict[type[WE], dict[str, WE]] = {
            CancelEvent: dict(),
            ShutdownEvent: dict(),
        }
        self._config: Optional[C] = None
        self._event_lock = asyncio.Lock()
        self._current_lock = asyncio.Lock()
        self._started_task_consumption_evt = asyncio.Event()  # useful for tests
        self._successful_exit = False
        self._timeout_exc: Optional[WorkerTimeoutError] = None
        self._timeout_callback_handle: Optional[TimerHandle] = None

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def event_lock(self) -> asyncio.Lock:
        return self._event_lock

    @property
    def _routing_strategy(self) -> RoutingStrategy:
        return self._app.routing_strategy

    @functools.cached_property
    def id(self) -> str:
        return self._id

    @final
    def work_forever(self):
        # This is a bit cosmetic but the sync one is useful to be run inside Python
        # worker multiprocessing Pool, while async one is more convenient for testing
        # Start watching cancelled tasks
        self._work_forever_task = self._loop.create_task(self._work_forever_async())
        self._loop.run_until_complete(self._work_forever_task)

    @final
    async def _work_forever_async(self):
        async with self:
            self.info("started working...")
            try:
                await self._work_forever()
            except asyncio.CancelledError:  # Shutdown let's not reraise
                self.info("worker cancelled, shutting down...")
            except KeyboardInterrupt:  # Shutdown let's not reraise
                pass
            except Exception as e:
                self.exception("error occurred while consuming: %s", _format_error(e))
                self.info("will try to shutdown gracefully...")
                raise e
            finally:
                self.info(
                    "finally stopped working, nothing lasts forever, "
                    "i'm out of this busy life !"
                )

    @final
    async def _work_forever(self):
        while True:
            try:
                self._work_once_task = asyncio.create_task(self._work_once())
                await self._work_once_task
            except TaskAlreadyReserved:
                # This part is won't happen with AMQP since it will take care to
                # correctly forward one task to one worker
                self.info("tried to consume an already reserved task, skipping...")
                continue
            except TaskAlreadyCancelled:
                # This part is won't happen with AMQP since it will take care to
                # correctly forward one task to one worker
                self.info("tried to consume a cancelled task, skipping...")
                continue

    def _get_worker_event(self, task: Task, type: type[WE]) -> WE:
        try:
            return self._worker_events[type][task.id]
        except KeyError as e:
            raise UnknownTask(task_id=task.id, worker_id=self._id) from e

    @final
    def logged_name(self) -> str:
        return self.id

    @property
    def graceful_shutdown(self) -> bool:
        return self._graceful_shutdown

    @final
    async def _work_once(self):
        async with self.ack_cm:
            if self._current is None:  # Consumption failed, skipping
                return
            self._current = await task_wrapper(self, self._current)

    @final
    async def consume(self) -> Task:
        self._started_task_consumption_evt.set()
        task = await self._consume()
        msg = 'Task(id="%s") locked'
        if task.max_retries is not None:
            msg += (
                f", tentative ({task.max_retries - task.retries_left}"
                f"/{task.max_retries})"
            )
        self.info(msg, task.id)
        async with self._current_lock:
            self._current = task
        progress = 0.0
        update = {"progress": progress, "state": TaskState.RUNNING}
        self._current = safe_copy(task, update=update)
        event = ProgressEvent.from_task(self._current)
        await self.publish_event(event)
        return self._current

    @final
    @property
    def ack_cm(self):
        if self._late_ack:
            return self._late_ack_cm()
        return self._early_ack_cm()

    @final
    @asynccontextmanager
    async def _early_ack_cm(self):
        current_id = None
        try:
            await self.consume()
            await self.acknowledge(self._current)
            yield
            current_id = self._current.id
            async with self._current_lock:
                self._clear_current()
            self.info('Task(id="%s") successful !', current_id)
        except asyncio.CancelledError as e:
            async with self.event_lock, self._current_lock:
                await self._handle_task_cancellation(e)
        except (TaskAlreadyCancelled, TaskAlreadyReserved) as e:
            # Let this bubble up and the worker continue without recording anything
            raise e
        except RecoverableError as e:
            async with self.event_lock, self._current_lock:
                if self._current is not None:
                    await self._handle_error(e, fatal=False)
                else:
                    self.exception(
                        'Task(id="%s") recoverable error while publishing success',
                        current_id,
                    )
        except MessageDeserializationError as e:
            self._handle_deser_consumption_error(e)
            # TODO: change this function an AsyncContentManager
            # We have to yield here, otherwise we get a
            # RuntimeError("generator didn't yield")
            yield
        except Exception as fatal_error:  # pylint: disable=broad-exception-caught
            async with self._current_lock, self.event_lock:
                if self._current is not None:
                    # The error is due to the current task, other tasks might success,
                    # let's fail this task and keep working
                    await self._handle_error(fatal_error, fatal=True)
                    return
            msg = (
                f"current task is expected to be known when fatal error occur,"
                f" otherwise a {MessageDeserializationError.__name__} is expected"
            )
            raise RuntimeError(msg) from fatal_error

    @final
    @asynccontextmanager
    async def _late_ack_cm(self):
        current_id = None
        try:
            await self.consume()
            yield
            current_id = self._current.id
            async with self._current_lock:
                await self.acknowledge(self._current)
                self._clear_current()
            self.info('Task(id="%s") successful !', current_id)
        except asyncio.CancelledError as e:
            async with self.event_lock, self._current_lock:
                await self._handle_task_cancellation(e)
        except (TaskAlreadyCancelled, TaskAlreadyReserved) as e:
            # Let this bubble up and the worker continue without recording anything
            raise e
        except RecoverableError as e:
            async with self._current_lock, self.event_lock:
                if self._current is not None:
                    await self._handle_error(e, fatal=False)
                else:
                    self.exception(
                        'Task(id="%s") recoverable error while publishing success',
                        current_id,
                    )
        except MessageDeserializationError as e:
            self._handle_deser_consumption_error(e)
            # TODO: change this function an AsyncContentManager
            # We have to yield here, otherwise we get a
            # RuntimeError("generator didn't yield")
            yield
        except Exception as fatal_error:  # pylint: disable=broad-exception-caught
            async with self._current_lock, self.event_lock:
                if self._current is not None:
                    # The error is due to the current task, other tasks might success,
                    # let's fail this task and keep working
                    await self._handle_error(fatal_error, fatal=True)
                    return
            msg = (
                f"current task is expected to be known when fatal error occur,"
                f" otherwise a {MessageDeserializationError.__name__} is expected"
            )
            raise RuntimeError(msg) from fatal_error

    async def _handle_error(self, error: BaseException, fatal: bool):
        task = self._current
        if isinstance(error, RecoverableError):
            error = error.args[0]
        exceeded_max_retries = isinstance(error, MaxRetriesExceeded)
        retries_left = self._current.retries_left - 1 if not fatal else 0
        if exceeded_max_retries:
            self.exception('Task(id="%s") exceeded max retries', task.id)
        elif fatal:
            self.exception('Task(id="%s") fatal error during execution', task.id)
        else:
            self.exception('Task(id="%s") encountered error', task.id)
        task_error = TaskError.from_exception(error)
        await self.publish_error_event(task_error, self._current, retries_left)
        if self._late_ack:
            if fatal:
                await self._negatively_acknowledge_(self._current)
            else:
                await self._acknowledge(self._current)
        self._clear_current()

    def _handle_deser_consumption_error(self, e: MessageDeserializationError):
        cause = e.__cause__
        error_with_trace = _format_error(cause)
        self.error("failed to deserialize incoming task: %s", error_with_trace)
        self.error("skipping...")

    async def _handle_cancel_event(self, cancel_event: CancelEvent):
        async with self.event_lock, self._current_lock:
            if self._current is None:
                self.info(
                    'Task(id="%s") completed before cancellation was effective, '
                    "skipping cancellation!",
                    cancel_event.task_id,
                )
                return
            if self._current.id != cancel_event.task_id:
                self.info(
                    'worker switched to Task(id="%s") before it could cancel'
                    ' Task(id="%s"), skipping cancellation!',
                    self._current.id,
                    cancel_event.task_id,
                )
                return
            self.info(
                'received cancel event for Task(id="%s"), cancelling it !',
                cancel_event.task_id,
            )
            self._work_once_task.cancel()
        await self._work_once_task

    async def _handle_task_cancellation(self, e: asyncio.CancelledError):
        if self._shutdown_asked or self._shutdown_signal:
            # we just reraise and the Worker.__aexit__ will take care of
            # handling worker shutdown gracefully
            raise e
        if self._timeout_exc is not None:
            self.exception(
                'Task(id="%s") consumer exceeded allocated timeout', self._current.id
            )
            fatal = not self._app.config.recover_from_worker_timeout
            await self._handle_error(self._timeout_exc, fatal=fatal)
            return
        if self._current is None:
            logger.info(
                "task cancellation was ask but task was acked or nacked"
                " in between, discarding cancel event !"
            )
            return
        event = self._get_worker_event(self._current, CancelEvent)
        update = {"state": TaskState.QUEUED if event.requeue else TaskState.CANCELLED}
        self._current = safe_copy(self._current, update=update)
        self.info('Task(id="%s") cancellation requested !', self._current.id)
        await self._publish_cancelled_event(event.requeue)

    async def _handle_shutdown_event(self):
        self.info("shutdown requested...")
        async with self.event_lock, self._current_lock:
            self._shutdown_asked = True
            if self._current is not None:
                self.info(
                    'Task(id="%s") cancelling task...',
                    self._current,
                )
        self._work_once_task.cancel()
        await self._work_once_task

    @property
    def _late_ack(self) -> bool:
        return self._app.config.late_ack

    @final
    async def acknowledge(self, task: Task):
        self.info('Task(id="%s") acknowledging...', task.id)
        await self._acknowledge(task)
        self.info('Task(id="%s") acknowledged', task.id)

    @abstractmethod
    async def _acknowledge(self, task: Task): ...

    @final
    async def _negatively_acknowledge_(self, task: Task):
        if not self._late_ack:
            raise ValueError("can't negatively acknowledge with early ack")
        self.info("negatively acknowledging Task(id=%s)...", task.id)
        await self._negatively_acknowledge(task)
        self._clear_current()
        self.info("Task(id=%s) negatively acknowledged !", task.id)

    @abstractmethod
    async def _negatively_acknowledge(self, nacked: Task): ...

    @abstractmethod
    async def _consume(self) -> Task: ...

    @abstractmethod
    async def _consume_worker_events(self) -> WorkerEvent: ...

    @final
    async def publish_result_event(self, result: Any, task: Task):
        self.debug('Task(id="%s") publish result event', task.id)
        result_event = ResultEvent.from_task(task, result)
        await self.publish_event(result_event)

    @final
    async def publish_error_event(self, error: TaskError, task: Task, retries: int):
        self.debug('Task(id="%s") publish error event', task.id)
        error_event = ErrorEvent.from_task(
            task, error, retries_left=retries, created_at=datetime.now(timezone.utc)
        )
        await self.publish_event(error_event)

    @final
    async def _publish_progress(self, progress: float, task: Task):
        async with self._current_lock:
            self._current = safe_copy(task, update={"progress": progress})
        event = ProgressEvent.from_task(self._current)
        await self.publish_event(event)

    @final
    def _publish_progress_sync(self, progress: float, task: Task):
        self._loop.call_soon(self._publish_progress, progress, task)

    async def _publish_cancelled_event(self, requeue: bool):
        update = {"state": TaskState.QUEUED if requeue else TaskState.CANCELLED}
        self._current = safe_copy(self._current, update=update)
        cancelled_event = CancelledEvent.from_task(self._current, requeue=requeue)
        if not requeue and self._late_ack:
            await self.acknowledge(self._current)
        self._clear_current()
        await self.publish_event(cancelled_event)

    @final
    def parse_task(self, task: Task) -> tuple[Callable, tuple[type[Exception], ...]]:
        registered = _retrieve_registered_task(task, self._app)
        recoverable = registered.recover_from
        task_fn = registered.task
        if supports_progress(task_fn):
            publish_progress = functools.partial(self._publish_progress, task=task)
            task_fn = functools.partial(task_fn, progress=publish_progress)
        return task_fn, recoverable

    @final
    async def _watch_worker_events(self):
        try:
            while True:
                try:
                    worker_event = await self._consume_worker_events()
                except MessageDeserializationError as e:
                    msg = (
                        "failed to deserialize incoming worker event due to a "
                        "deserialization error: %s"
                    )
                    self.exception(msg, _format_error(e))
                    continue
                event_task_id = getattr(worker_event, "task_id", None)
                existing_events = self._worker_events[worker_event.__class__]
                if event_task_id is not None:
                    already_received = event_task_id in existing_events
                    if already_received:
                        continue
                    existing_events[event_task_id] = worker_event
                if isinstance(worker_event, ShutdownEvent):
                    await self._handle_shutdown_event()
                    return
                not_processing = self._current is None
                if not_processing:
                    continue
                if isinstance(worker_event, CancelEvent):
                    await self._handle_cancel_event(worker_event)
                else:
                    raise ValueError(f"unexpected event type {worker_event}")
        except Exception as fatal_error:
            async with self._current_lock, self.event_lock:
                if self._current is not None:
                    await self._handle_error(fatal_error, fatal=True)
            raise fatal_error
        except asyncio.CancelledError as e:
            logger.info("cancelling cancelled task watch !")
            raise e

    @final
    def check_retries(self, task: Task, original_exc: Exception):
        self.info(
            '%sTask(id="%s"): try %s/%s',
            task.name,
            task.id,
            task.retries_left,
            task.max_retries,
        )
        if task.retries_left <= 0:
            raise MaxRetriesExceeded(
                f"{task.name}(id={task.id}): max retries exceeded > {task.max_retries}"
            ) from original_exc

    @final
    def __enter__(self):
        self._loop.run_until_complete(self.__aenter__())

    @final
    async def __aenter__(self) -> Self:
        await self._aenter__()
        # Start watching worker events
        self._watch_events = self._loop.create_task(self._watch_worker_events())
        return self

    @final
    def bind_worker_timeout(self):
        registered = _retrieve_registered_task(self._current, self._app)
        group = registered.group
        if group is None:
            return
        group = self._app.task_group(group.name)
        if group is None:
            return
        timeout = group.timeout_s
        if timeout is None:
            return
        msg = f'Task(id="{self._current}") exceeded allocated timeout of {timeout}s'
        exc = WorkerTimeoutError(msg)
        self._timeout_callback_handle = self.loop.call_later(
            timeout, self.worker_timeout_callback, exc
        )

    @final
    def worker_timeout_callback(self, exc: WorkerTimeoutError):
        self._timeout_exc = exc
        if self._work_once_task is not None:
            self._work_once_task.cancel()

    async def _aenter__(self):
        pass

    @final
    def __exit__(self, exc_type, exc_value, tb):
        self._loop.run_until_complete(self.__aexit__(exc_type, exc_value, tb))

    @final
    async def __aexit__(self, exc_type, exc_value, tb):
        # dependencies might be closed while trying to gracefully shutdown
        if not self._already_exiting:
            self._already_exiting = True
            if self._watch_events is not None and not self._watch_events.cancelled():
                logger.info("terminating watch events loop ...")
                self._watch_events.cancel()
                # Wait for cancellation to be effective
                try:
                    await self._watch_events
                except asyncio.CancelledError:
                    pass
                self._watch_events = None
                logger.info("events loop terminated !")
            # Let's try to shut down gracefully
            await self.shutdown()
            # Clean worker dependencies only if needed, dependencies might be share in
            # which case we don't want to tear them down
            if self._teardown_dependencies:
                self.info("cleaning worker dependencies...")
                await self._aexit__(exc_type, exc_value, tb)
            self._successful_exit = True

    async def _aexit__(self, exc_type, exc_val, exc_tb):
        pass

    @final
    async def _shutdown_gracefully(self):
        if self._current is not None:
            requeue = self._shutdown_signal
            await self._publish_cancelled_event(requeue)

    @final
    async def shutdown(self):
        if self.graceful_shutdown:
            self.info("shutting down gracefully")
            async with self.event_lock, self._current_lock:
                await self._shutdown_gracefully()
            self.info("graceful shut down complete")
        else:
            self.info("shutting down the hard way, task might not be re-queued...")

    def _create_worker_id(self) -> str:
        pid = os.getpid()
        threadid = threading.get_ident()
        hostname = socket.gethostname()
        # TODO: this might not be unique when using asyncio
        return f"{self._app.name}-worker-{hostname}-{pid}-{threadid}"

    def _clear_current(self):
        if self._timeout_callback_handle is not None:
            self._timeout_callback_handle.cancel()
            self._timeout_callback_handle = None
        self._current = None


def _retrieve_registered_task(
    task: Task,
    app: AsyncApp,
) -> RegisteredTask:
    registered = app.registry.get(task.name)
    if registered is None:
        available_tasks = list(app.registry)
        raise UnregisteredTask(task.name, available_tasks)
    return registered


async def task_wrapper(worker: Worker, task: Task) -> Task:
    worker.bind_worker_timeout()
    # Skips if already reserved
    if task.state is TaskState.CANCELLED:
        worker.info('Task(id="%s") already cancelled skipping it !', task.id)
        raise TaskAlreadyCancelled(task_id=task.id)
    # Parse task to retrieve recoverable errors and max retries
    task_fn, recoverable_errors = worker.parse_task(task)
    task_args = {to_lower_snake_case(k): v for k, v in task.args.items()}
    task_inputs = add_missing_args(task_fn, task_args)
    # Retry task until success, fatal error or max retry exceeded
    return await _retry_task(worker, task, task_fn, task_inputs, recoverable_errors)


async def _retry_task(
    worker: Worker,
    task: Task,
    task_fn: Callable,
    task_args: Dict,
    recoverable_errors: tuple[type[Exception], ...],
) -> Task:
    retries = task.retries_left
    if retries != task.max_retries:
        # In the case of the retry, let's reset the progress
        event = ProgressEvent.from_task(task=task)
        await worker.publish_event(event)
    try:
        task_res = task_fn(**task_args)
        if isawaitable(task_res):
            task_res = await task_res
    except recoverable_errors as e:
        # This will throw a MaxRetriesExceeded when necessary
        worker.check_retries(task, e)
        raise RecoverableError(e) from e
    update = TaskUpdate.done(datetime.now(timezone.utc))
    task = safe_copy(task, update=update.model_dump(exclude_unset=True))
    worker.info('Task(id="%s") complete, saving result...', task.id)
    async with worker.event_lock:
        await worker.publish_result_event(task_res, task)
    return task


def add_missing_args(fn: Callable, args: dict[str, Any], **kwargs) -> dict[str, Any]:
    # We make the choice not to raise in case of missing argument here, the error will
    # be correctly raise when the function is called
    from_kwargs = dict()
    sig = inspect.signature(fn)
    for param_name in sig.parameters:
        if param_name in args:
            continue
        kwargs_value = kwargs.get(param_name)
        if kwargs_value is not None:
            from_kwargs[param_name] = kwargs_value
    if from_kwargs:
        args = deepcopy(args)
        args.update(from_kwargs)
    return args


def _format_error(error: BaseException) -> str:
    return "".join(traceback.format_exception(None, error, error.__traceback__))
