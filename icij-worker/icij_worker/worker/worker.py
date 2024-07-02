from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import traceback
from abc import abstractmethod
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from copy import deepcopy
from datetime import datetime
from inspect import isawaitable, signature
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
from icij_worker import (
    AsyncApp,
    Task,
    TaskError,
    TaskEvent,
    TaskResult,
    TaskStatus,
)
from icij_worker.app import RegisteredTask
from icij_worker.exceptions import (
    MaxRetriesExceeded,
    RecoverableError,
    TaskAlreadyCancelled,
    TaskAlreadyReserved,
    UnknownTask,
    UnregisteredTask,
)
from icij_worker.task import CancelledTaskEvent
from icij_worker.utils import Registrable
from icij_worker.worker.process import HandleSignalsMixin
from icij_worker.event_publisher.event_publisher import EventPublisher

logger = logging.getLogger(__name__)

PROGRESS_HANDLER_ARG = "progress"

C = TypeVar("C", bound="WorkerConfig")


class Worker(
    EventPublisher,
    Registrable,
    HandleSignalsMixin,
    AbstractAsyncContextManager,
):
    def __init__(
        self,
        app: AsyncApp,
        worker_id: str,
        handle_signals: bool = True,
        teardown_dependencies: bool = False,
    ):
        # If worker are run using a thread backend then signal handling might not be
        # required, in this case the signal handling mixing will just do nothing
        HandleSignalsMixin.__init__(self, logger, handle_signals=handle_signals)
        self._app = app
        self._id = worker_id
        self._teardown_dependencies = teardown_dependencies
        self._graceful_shutdown = True
        self._loop = asyncio.get_event_loop()
        self._work_forever_task: Optional[asyncio.Task] = None
        self._work_once_task: Optional[asyncio.Task] = None
        self._watch_cancelled_task: Optional[asyncio.Task] = None
        self._already_exiting = False
        self._current: Optional[Task] = None
        self._cancelled: Dict[str, CancelledTaskEvent] = dict()
        self._cancelling: Optional[str] = None
        self._config: Optional[C] = None
        # We use asyncio lock, not thread lock, since the worker is supposed run in a
        # single thread (not thread-safe for now)
        self._cancellation_lock = asyncio.Lock()
        # Not sure if this is even necessary, since there might be a single loop
        # reading and writing the current stuf
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

    @functools.cached_property
    def id(self) -> str:
        return self._id

    @final
    def work_forever(self):
        # This is a bit cosmetic but the sync one is useful to be run inside Python
        # worker multiprocessing Pool, while async one is more convenient for testing
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
                self.error("error occurred while consuming: %s", _format_error(e))
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

    def _get_cancel_event(self, task: Task) -> CancelledTaskEvent:
        try:
            return self._cancelled[task.id]
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
        async with self.acknowledgment_cm():
            task = await self.consume()
            await task_wrapper(self, task)

    @final
    async def consume(self) -> Task:
        task = await self._consume()
        self.debug('Task(id="%s") locked', task.id)
        async with self._current_lock:
            self._current = task
        progress = 0.0
        update = {"progress": progress}
        task = safe_copy(task, update=update)
        event = TaskEvent(task_id=task.id, progress=progress, status=TaskStatus.RUNNING)
        await self.publish_event(event, task)
        return task

    @final
    @asynccontextmanager
    async def acknowledgment_cm(self):
        try:
            yield
            async with self._current_lock:
                await self.acknowledge(self._current)
        except asyncio.CancelledError as e:
            await self._handle_cancel_event(e)
        except (TaskAlreadyCancelled, TaskAlreadyReserved) as e:
            # Let this bubble up and the worker continue without recording anything
            raise e
        except RecoverableError:
            async with self._current_lock:
                self.error('Task(id="%s") encountered error', self._current.id)
                await self.negatively_acknowledge(self._current, requeue=True)
        except Exception as fatal_error:  # pylint: disable=broad-exception-caught
            async with self._current_lock:
                if self._current is not None:
                    # The error is due to the current task, other tasks might success,
                    # let's fail this task and keep working
                    await self._handle_fatal_error(fatal_error, self._current)
                    return
            # The error was in the worker's code, something is wrong that won't change
            # at the next task, let's make the worker crash
            raise fatal_error

    async def _handle_fatal_error(self, fatal_error: BaseException, task: Task):
        if isinstance(fatal_error, MaxRetriesExceeded):
            self.error('Task(id="%s") exceeded max retries, nacking it...', task.id)
        else:
            self.error(
                'fatal error during Task(id="%s") execution, nacking it...', task.id
            )
        task_error = TaskError.from_exception(fatal_error, task)
        await self.save_error(error=task_error)
        # Once the error has been saved, we notify the event consumers, they are
        # responsible for reflecting the fact that the error has occurred wherever
        # relevant. The source of truth will be error storage
        await self.publish_error_event(task_error, task)
        await self.negatively_acknowledge(task, requeue=False)

    async def _handle_cancel_event(self, e: asyncio.CancelledError):
        async with self._current_lock:
            async with self._cancellation_lock:
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
        event = self._get_cancel_event(self._current)
        self.info('Task(id="%s") cancellation requested !', self._current.id)
        async with self._current_lock:
            await self._negatively_acknowledge_running_task(event.requeue, cancel=True)

    @final
    async def acknowledge(self, task: Task):
        completed_at = datetime.now()
        self.info('Task(id="%s") acknowledging...', task.id)
        await self._acknowledge(task, completed_at)
        self._current = None
        self.info('Task(id="%s") acknowledged', task.id)
        self.debug('Task(id="%s") publishing acknowledgement event', task.id)
        task_event = TaskEvent(
            task_id=task.id,
            status=TaskStatus.DONE,
            progress=100,
            completed_at=completed_at,
            project_id=task.project_id,
        )
        event = task_event
        # Tell the listeners that the task succeeded
        await self.publish_event(event, task)
        self.info('Task(id="%s") successful !', task.id)

    @abstractmethod
    async def _acknowledge(self, task: Task, completed_at: datetime) -> Task: ...

    @final
    async def negatively_acknowledge(
        self, task: Task, *, requeue: bool, cancel: bool = False
    ):
        self.info(
            "negatively acknowledging Task(id=%s) with (requeue=%s)...",
            task.id,
            requeue,
        )
        cancelled_at = datetime.now()
        if requeue:
            update = {"status": TaskStatus.QUEUED, "progress": 0.0}
            if cancel:
                update["cancelled_at"] = cancelled_at
            else:
                update["retries"] = task.retries or 0 + 1
        elif cancel:
            update = {"status": TaskStatus.CANCELLED, "cancelled_at": cancelled_at}
        else:
            update = {"status": TaskStatus.ERROR}
        nacked = safe_copy(task, update=update)
        await self._negatively_acknowledge(nacked, cancelled=cancel)
        self.info(
            "Task(id=%s) negatively acknowledged (requeue=%s)!", nacked.id, requeue
        )
        self._current = None

    @abstractmethod
    async def _negatively_acknowledge(self, nacked: Task, *, cancelled: bool): ...

    # TODO: the cancelled parameter is needed in some implementation to know whether
    #  the cancellation happened, it which case the implem might want to clean
    #  cancellation events. Another alternative would be to add a _clean_cancelled
    #  method and use it in the  negatively_acknowledge method right after calling
    #  _negatively_acknowledge when cancel = True. However for some DB implems
    #  it's nice to nack and clean in a single transaction

    @abstractmethod
    async def _consume(self) -> Task: ...

    @abstractmethod
    async def _save_result(self, result: TaskResult):
        """Save the result in a safe place"""

    @abstractmethod
    async def _save_error(self, error: TaskError):
        """Save the error in a safe place"""

    @final
    async def save_result(self, result: TaskResult):
        self.info('Task(id="%s") saving result...', result.task_id)
        await self._save_result(result)
        self.info('Task(id="%s") result saved !', result.task_id)

    @final
    async def save_error(self, error: TaskError):
        self.error('Task(id="%s"): %s\n%s', error.task_id, error.title, error.detail)
        # Save the error in the appropriate location
        self.debug('Task(id="%s") saving error', error.task_id)
        await self._save_error(error)

    @final
    async def publish_error_event(
        self, error: TaskError, task: Task, retries: Optional[int] = None
    ):
        # Tell the listeners that the task failed
        self.debug('Task(id="%s") publish error event', task.id)
        event = TaskEvent.from_error(error, task.id, retries)
        await self.publish_event(event, task)

    @final
    async def _publish_progress(self, progress: float, task: Task):
        event = TaskEvent(progress=progress, task_id=task.id)
        await self.publish_event(event, task)

    @final
    def parse_task(self, task: Task) -> Tuple[Callable, Tuple[Type[Exception], ...]]:
        registered = _retrieve_registered_task(task, self._app)
        recoverable = registered.recover_from
        task_fn = registered.task
        supports_progress = any(
            param.name == PROGRESS_HANDLER_ARG
            for param in signature(task_fn).parameters.values()
        )
        if supports_progress:
            publish_progress = functools.partial(self._publish_progress, task=task)
            task_fn = functools.partial(task_fn, progress=publish_progress)
        return task_fn, recoverable

    @final
    async def _watch_cancelled(self):
        try:
            while True:
                cancel_event = await self._consume_cancelled()
                self._cancelled[cancel_event.task_id] = cancel_event
                async with self._current_lock, self._cancellation_lock:
                    if self._current is None:
                        continue
                    if (
                        self._cancelling is not None
                        and self._cancelling == cancel_event.task_id
                    ):
                        continue
                    self._cancelling = cancel_event.task_id
                    task = self._current
                    if task.id != cancel_event.task_id:
                        continue
                    self.info(
                        'received cancel event for Task(id="%s"), cancelling it !',
                        task.id,
                    )
                    self._work_once_task.cancel()
                await self._work_once_task
        except Exception as fatal_error:
            if self._current is not None:
                await self._handle_fatal_error(fatal_error, *self._current)
            raise fatal_error
        except asyncio.CancelledError as e:
            logger.info("cancelling cancelled task watch !")
            raise e

    @abstractmethod
    async def _consume_cancelled(self) -> CancelledTaskEvent: ...

    @final
    def check_retries(self, retries: int, task: Task):
        max_retries = self._app.registry[task.type].max_retries
        if max_retries is None:
            return
        self.info(
            '%sTask(id="%s"): try %s/%s', task.type, task.id, retries, max_retries
        )
        if retries is not None and retries > max_retries:
            raise MaxRetriesExceeded(
                f"{task.type}(id={task.id}): max retries exceeded > {max_retries}"
            )

    @final
    def __enter__(self):
        self._loop.run_until_complete(self.__aenter__())

    @final
    async def __aenter__(self):
        await self._aenter__()
        # Start watching cancelled tasks
        self._watch_cancelled_task = self._loop.create_task(self._watch_cancelled())

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
        await self._negatively_acknowledge_running_task(requeue=True)

    async def _negatively_acknowledge_running_task(
        self, requeue: bool, cancel: bool = False
    ):
        if self._current:
            task = self._current
            await self.negatively_acknowledge(task, requeue=requeue, cancel=cancel)

    @final
    async def shutdown(self):
        if self.graceful_shutdown:
            self.info("shutting down gracefully")
            await self._shutdown_gracefully()
            self.info("graceful shut down complete")
        else:
            self.info("shutting down the hard way, task might not be re-queued...")


def _retrieve_registered_task(
    task: Task,
    app: AsyncApp,
) -> RegisteredTask:
    registered = app.registry.get(task.type)
    if registered is None:
        available_tasks = list(app.registry)
        raise UnregisteredTask(task.type, available_tasks)
    return registered


async def task_wrapper(worker: Worker, task: Task):
    # Skips if already reserved
    if task.status is TaskStatus.CANCELLED:
        worker.info('Task(id="%s") already cancelled skipping it !', task.id)
        raise TaskAlreadyCancelled(task_id=task.id)
    # Parse task to retrieve recoverable errors and max retries
    task_fn, recoverable_errors = worker.parse_task(task)
    task_inputs = add_missing_args(task_fn, task.inputs, config=worker.config)
    # Retry task until success, fatal error or max retry exceeded
    await _retry_task(worker, task, task_fn, task_inputs, recoverable_errors)


async def _retry_task(
    worker: Worker,
    task: Task,
    task_fn: Callable,
    task_inputs: Dict,
    recoverable_errors: Tuple[Type[Exception], ...],
):
    retries = task.retries or 0
    if retries:
        # In the case of the retry, let's reset the progress
        event = TaskEvent(task_id=task.id, progress=0.0)
        await worker.publish_event(event, task)
    try:
        task_res = task_fn(**task_inputs)
        if isawaitable(task_res):
            task_res = await task_res
    except recoverable_errors as e:
        # This will throw a MaxRetriesExceeded when necessary
        worker.check_retries(retries, task)
        error = TaskError.from_exception(e, task)
        await worker.publish_error_event(error, task, retries=retries + 1)
        raise RecoverableError() from e
    worker.info('Task(id="%s") complete, saving result...', task.id)
    result = TaskResult.from_task(task=task, result=task_res)
    await worker.save_result(result)
    return


def add_missing_args(fn: Callable, inputs: Dict[str, Any], **kwargs) -> Dict[str, Any]:
    # We make the choice not to raise in case of missing argument here, the error will
    # be correctly raise when the function is called
    from_kwargs = dict()
    sig = inspect.signature(fn)
    for param_name in sig.parameters:
        if param_name in inputs:
            continue
        kwargs_value = kwargs.get(param_name)
        if kwargs_value is not None:
            from_kwargs[param_name] = kwargs_value
    if from_kwargs:
        inputs = deepcopy(inputs)
        inputs.update(from_kwargs)
    return inputs


def _format_error(error: Exception) -> str:
    return "".join(traceback.format_exception(None, error, error.__traceback__))
