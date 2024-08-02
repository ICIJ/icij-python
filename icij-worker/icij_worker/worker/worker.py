from __future__ import annotations

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
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    Tuple,
    Type,
    TypeVar,
    final,
)

from icij_common.pydantic_utils import safe_copy
from icij_worker import AsyncApp, ResultEvent, Task, TaskError, TaskState
from icij_worker.app import RegisteredTask, supports_progress
from icij_worker.event_publisher.event_publisher import EventPublisher
from icij_worker.exceptions import (
    MaxRetriesExceeded,
    RecoverableError,
    TaskAlreadyCancelled,
    TaskAlreadyReserved,
    UnknownTask,
    UnregisteredTask,
)
from icij_worker.namespacing import Namespacing
from icij_worker.objects import (
    CancelEvent,
    CancelledEvent,
    ErrorEvent,
    ProgressEvent,
    TaskUpdate,
    WorkerEvent,
)
from icij_worker.utils import Registrable
from icij_worker.worker.process import HandleSignalsMixin

logger = logging.getLogger(__name__)

C = TypeVar("C", bound="WorkerConfig")
WE = TypeVar("WE", bound=WorkerEvent)


class Worker(
    EventPublisher, Registrable, HandleSignalsMixin, AbstractAsyncContextManager
):
    def __init__(
        self,
        app: AsyncApp,
        worker_id: Optional[str] = None,
        *,
        namespace: Optional[str],
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
        self._namespace = namespace
        self._teardown_dependencies = teardown_dependencies
        self._graceful_shutdown = True
        self._loop = asyncio.get_event_loop()
        self._work_forever_task: Optional[asyncio.Task] = None
        self._work_once_task: Optional[asyncio.Task] = None
        self._watch_cancelled_task: Optional[asyncio.Task] = None
        self._already_exiting = False
        self._current: Optional[Task] = None
        self._worker_events: Dict[Type[WE], Dict[str, WE]] = {CancelEvent: dict()}
        self._config: Optional[C] = None
        self._cancel_lock = asyncio.Lock()
        self._current_lock = asyncio.Lock()
        self._successful_exit = False

    def set_config(self, config: C):
        self._config = config

    def _to_config(self) -> C:
        if self._config is None:
            raise ValueError(
                "worker was initialized using a from_config, "
                "but the config was not attached using .set_config"
            )
        return self._config

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def cancel_lock(self) -> asyncio.Lock:
        return self._cancel_lock

    @property
    def _namespacing(self) -> Namespacing:
        return self._app.namespacing

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

    def _get_worker_event(self, task: Task, type: Type[WE]) -> WE:
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
            self._current = await task_wrapper(self, self._current)

    @final
    async def consume(self) -> Task:
        task = await self._consume()
        self.debug('Task(id="%s") locked', task.id)
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
                self._current = None
            self.info('Task(id="%s") successful !', current_id)
        except asyncio.CancelledError as e:
            await self._handle_task_cancellation(e)
        except (TaskAlreadyCancelled, TaskAlreadyReserved) as e:
            # Let this bubble up and the worker continue without recording anything
            raise e
        except RecoverableError as e:
            async with self._current_lock, self._cancel_lock:
                if self._current is not None:
                    await self._handle_error(e, fatal=False)
                else:
                    self.exception(
                        'Task(id="%s") recoverable error while publishing success',
                        current_id,
                    )
        except Exception as fatal_error:  # pylint: disable=broad-exception-caught
            async with self._current_lock, self._cancel_lock:
                if self._current is not None:
                    # The error is due to the current task, other tasks might success,
                    # let's fail this task and keep working
                    await self._handle_error(fatal_error, fatal=True)
                    return
            # The error was in the worker's code, something is wrong that won't change
            # at the next task, let's make the worker crash
            raise fatal_error

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
                self._current = None
            self.info('Task(id="%s") successful !', current_id)
        except asyncio.CancelledError as e:
            await self._handle_task_cancellation(e)
        except (TaskAlreadyCancelled, TaskAlreadyReserved) as e:
            # Let this bubble up and the worker continue without recording anything
            raise e
        except RecoverableError as e:
            async with self._current_lock, self._cancel_lock:
                if self._current is not None:
                    await self._handle_error(e, fatal=False)
                else:
                    self.exception(
                        'Task(id="%s") recoverable error while publishing success',
                        current_id,
                    )
        except Exception as fatal_error:  # pylint: disable=broad-exception-caught
            async with self._current_lock, self._cancel_lock:
                if self._current is not None:
                    # The error is due to the current task, other tasks might success,
                    # let's fail this task and keep working
                    await self._handle_error(fatal_error, fatal=True)
                    return
            # The error was in the worker's code, something is wrong that won't change
            # at the next task, let's make the worker crash
            raise fatal_error

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
        self._current = None

    async def _handle_cancel_event(self, cancel_event: CancelEvent):
        async with self.cancel_lock, self._current_lock:
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
        async with self.cancel_lock:
            if self._worker_cancelled:
                # we just raise and the Worker.__aexit__ will take care of
                # handling worker shutdown gracefully
                raise e
            if self._current is None:
                logger.info(
                    "task cancellation was ask but task was acked or nacked"
                    " in between, discarding cancel event !"
                )
                return
            event = self._get_worker_event(self._current, CancelEvent)
            update = {
                "state": TaskState.QUEUED if event.requeue else TaskState.CANCELLED
            }
            self._current = safe_copy(self._current, update=update)
            self.info('Task(id="%s") cancellation requested !', self._current.id)
            await self._publish_cancelled_event(event)
            self._current = None

    @functools.cached_property
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
        # This is ugly but makes tests easier
        # (shutdown doesn't try to requeue the task again)
        if task is self._current:
            self._current = None
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

    async def _publish_cancelled_event(self, cancel_event: CancelEvent):
        update = {
            "state": TaskState.QUEUED if cancel_event.requeue else TaskState.CANCELLED
        }
        self._current = safe_copy(self._current, update=update)
        cancelled_event = CancelledEvent.from_task(
            self._current, requeue=cancel_event.requeue
        )
        if not cancel_event.requeue and self._late_ack:
            await self.acknowledge(self._current)
        await self.publish_event(cancelled_event)

    @final
    def parse_task(self, task: Task) -> Tuple[Callable, Tuple[Type[Exception], ...]]:
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
                worker_event = await self._consume_worker_events()
                event_task_id = worker_event.task_id
                existing_events = self._worker_events[worker_event.__class__]
                already_received = event_task_id in existing_events
                not_processing = self._current is None
                if already_received or not_processing:
                    continue
                existing_events[event_task_id] = worker_event
                if isinstance(worker_event, CancelEvent):
                    await self._handle_cancel_event(worker_event)
                else:
                    raise ValueError(f"unexpected event type {worker_event}")
        except Exception as fatal_error:
            async with self._current_lock, self._cancel_lock:
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
    async def __aenter__(self):
        await self._aenter__()
        # Start watching cancelled tasks
        self._watch_cancelled_task = self._loop.create_task(self._watch_worker_events())

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
            if (
                self._watch_cancelled_task is not None
                and not self._watch_cancelled_task.cancelled()
            ):
                logger.info("terminating cancel event loop ...")
                self._watch_cancelled_task.cancel()
                # Wait for cancellation to be effective
                try:
                    await self._watch_cancelled_task
                except asyncio.CancelledError:
                    pass
                self._watch_cancelled_task = None
                logger.info("cancel event loop terminated !")
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
        async with self._current_lock, self._cancel_lock:
            if self._current is not None:
                cancel_event = CancelEvent.from_task(self._current, requeue=True)
                await self._publish_cancelled_event(cancel_event)
                self._current = None

    @final
    async def shutdown(self):
        if self.graceful_shutdown:
            self.info("shutting down gracefully")
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
    # Skips if already reserved
    if task.state is TaskState.CANCELLED:
        worker.info('Task(id="%s") already cancelled skipping it !', task.id)
        raise TaskAlreadyCancelled(task_id=task.id)
    # Parse task to retrieve recoverable errors and max retries
    task_fn, recoverable_errors = worker.parse_task(task)
    task_inputs = add_missing_args(task_fn, task.arguments, config=worker.config)
    # Retry task until success, fatal error or max retry exceeded
    return await _retry_task(worker, task, task_fn, task_inputs, recoverable_errors)


async def _retry_task(
    worker: Worker,
    task: Task,
    task_fn: Callable,
    task_args: Dict,
    recoverable_errors: Tuple[Type[Exception], ...],
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
    task = safe_copy(task, update=update.dict(exclude_unset=True))
    worker.info('Task(id="%s") complete, saving result...', task.id)
    async with worker.cancel_lock:
        await worker.publish_result_event(task_res, task)
    return task


def add_missing_args(
    fn: Callable, arguments: Dict[str, Any], **kwargs
) -> Dict[str, Any]:
    # We make the choice not to raise in case of missing argument here, the error will
    # be correctly raise when the function is called
    from_kwargs = dict()
    sig = inspect.signature(fn)
    for param_name in sig.parameters:
        if param_name in arguments:
            continue
        kwargs_value = kwargs.get(param_name)
        if kwargs_value is not None:
            from_kwargs[param_name] = kwargs_value
    if from_kwargs:
        arguments = deepcopy(arguments)
        arguments.update(from_kwargs)
    return arguments


def _format_error(error: Exception) -> str:
    return "".join(traceback.format_exception(None, error, error.__traceback__))
