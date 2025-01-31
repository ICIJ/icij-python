from asyncio import Task as AsyncIOTask

import asyncio
import logging
from abc import ABC, abstractmethod
from async_lru import alru_cache
from copy import deepcopy
from datetime import datetime, timezone
from functools import cached_property
from pydantic import Field
from typing import Any, ClassVar, Collection, Dict, Optional, Tuple, cast, final

from icij_common.pydantic_utils import safe_copy
from icij_worker import AsyncApp, ResultEvent, Task, TaskState
from icij_worker.app import AsyncAppConfig
from icij_worker.dag.dag import TaskDAG
from icij_worker.exceptions import (
    DAGError,
    TaskAlreadyQueued,
    UnknownTask,
    UnregisteredTask,
)
from icij_worker.objects import (
    AsyncBackend,
    CancelledEvent,
    ErrorEvent,
    ManagerEvent,
    ProgressEvent,
    READY_STATES,
    TaskError,
)
from icij_worker.routing_strategy import RoutingStrategy
from icij_worker.task_storage import DAGTaskStorage, TaskStorage
from icij_worker.utils import (
    CacheDict,
    RegistrableConfig,
    add_missing_args,
    find_missing_args,
)
from icij_worker.utils.registrable import RegistrableFromConfig

logger = logging.getLogger(__name__)


class TaskManagerConfig(RegistrableConfig):
    registry_key: ClassVar[str] = Field(const=True, default="backend")
    backend: ClassVar[AsyncBackend]

    app_path: str
    app_config: AsyncAppConfig = Field(default_factory=AsyncAppConfig)

    @property
    def app(self) -> AsyncApp:
        app = AsyncApp.load(self.app_path).with_config(self.app_config)
        return app


_TaskName = str
_TaskMeta = Tuple[_TaskName, TaskState, Optional[float]]


# TODO: try to isolate the DAG part to keep the main TM codebase light and simple
class TaskManager(TaskStorage, RegistrableFromConfig, ABC):
    def __init__(self, app: AsyncApp, caches_size: int = 1000):
        self._app = app
        self._loop = asyncio.get_event_loop()
        self._caches_size = caches_size
        self._task_metas: Dict[str, _TaskMeta] = CacheDict(cache_len=self._caches_size)
        self._consume_loop: Optional[AsyncIOTask] = None

    @final
    async def __aenter__(self):
        await self._aenter__()
        self._consume_loop = self._loop.create_task(self.consume_events())

    async def _aenter__(self):
        pass

    @final
    async def __aexit__(self, exc_type, exc_value, tb):
        await self._aexit__(exc_type, exc_value, tb)
        if self._consume_loop is not None and not self._consume_loop.done():
            logger.info("cancelling worker event loop...")
            self._consume_loop.cancel()
            await asyncio.wait([self._consume_loop])
            del self._consume_loop
            logger.info("worker event loop cancelled")

    async def _aexit__(self, exc_type, exc_val, exc_tb):
        pass

    @cached_property
    def late_ack(self) -> bool:
        return self._app.config.late_ack

    @cached_property
    def max_task_queue_size(self) -> int:
        return self._app.config.max_task_queue_size

    @cached_property
    def _routing_strategy(self) -> RoutingStrategy:
        return self._app.routing_strategy

    @cached_property
    def app_name(self) -> str:
        return self._app.name

    @final
    async def enqueue(self, task: Task) -> Task:
        if task.state is not TaskState.CREATED:
            msg = f"invalid state {task.state}, expected {TaskState.CREATED}"
            raise ValueError(msg)
        db_task = await self.get_task(task.id)
        if db_task.state is TaskState.QUEUED:
            raise TaskAlreadyQueued(db_task.id)
        await self._enqueue_(task)
        queued = safe_copy(task, update={"state": TaskState.QUEUED})
        await self.save_task(queued)
        self._update_metas(queued)
        return queued

    async def _enqueue_(self, task: Task):
        await self._enqueue(task)

    @final
    async def requeue(self, task: Task):
        logger.info("requeing Task(id=%s)", task.id)
        update = {"state": TaskState.QUEUED, "progress": 0.0}
        updated = safe_copy(task, update=update)
        await self._requeue(updated)
        self._update_metas(updated)
        logger.info("Task(id=%s) requeued", updated.id)

    async def save_task(self, task: Task) -> bool:
        try:
            group = await self.get_task_group(task_id=task.id)
        except UnknownTask as e:
            try:
                group = self._app.registry[task.name].group
                if group is not None:
                    group = group.name
                max_retries = self._app.registry[task.name].max_retries
                task = task.with_max_retries(max_retries)
            except KeyError:
                available_tasks = list(self._app.registry)
                raise UnregisteredTask(task.name, available_tasks) from e
        return await self.save_task_(task, group)

    async def _save_cancelled_event(self, event: CancelledEvent) -> Task:
        task = await self.get_task(event.task_id)
        task = task.as_resolved(event)
        if event.requeue and not self.late_ack:
            await self.requeue(task)
        self._update_metas(task)
        await self.save_task(task)
        return task

    @final
    async def consume_events(self):
        while True:
            msg = await self._consume()
            if isinstance(msg, ResultEvent):
                logger.debug("saving result for task: %s", msg.task_id)
                await self._save_result_event(msg)
            elif isinstance(msg, ErrorEvent):
                logger.debug("saving error: %s", msg)
                await self._save_error_event(msg)
            elif isinstance(msg, ProgressEvent):
                logger.debug("saving progress: %s", msg)
                await self._save_progress_event(msg)
            elif isinstance(msg, CancelledEvent):
                logger.debug("saving cancellation: %s", msg)
                await self._save_cancelled_event(msg)
            else:
                raise TypeError(f"unexpected message type {msg.__class__}")

    async def _save_result_event(self, result: ResultEvent) -> Task:
        await self.save_result(result)
        task = await self.get_task(result.task_id)
        task = task.as_resolved(result)
        await self.save_task(task)
        self._update_metas(task)
        return task

    async def _save_error_event(self, error: ErrorEvent) -> Task:
        # Update the task retries count
        task = await self.get_task(error.task_id)
        task = task.as_resolved(error)
        await self.save_error(error)
        if task.state is TaskState.QUEUED:
            await self.requeue(task)
        await self.save_task(task)
        self._update_metas(task)
        return task

    async def _save_progress_event(self, event: ProgressEvent) -> Task:
        # Might be better to be overridden to be performed in a transactional manner
        # when possible
        task = await self.get_task(event.task_id)
        task = task.as_resolved(event)
        await self.save_task(task)
        self._update_metas(task)
        return task

    async def cancel(self, task_id: str, *, requeue: bool):
        await self._cancel(task_id, requeue=requeue)

    @abstractmethod
    async def _cancel(self, task_id: str, *, requeue: bool): ...

    @abstractmethod
    async def shutdown_workers(self): ...

    @abstractmethod
    async def _consume(self) -> ManagerEvent: ...

    @abstractmethod
    async def _enqueue(self, task: Task) -> Task: ...

    @abstractmethod
    async def _requeue(self, task: Task): ...

    @abstractmethod
    async def get_health(self) -> Dict[str, bool]: ...

    def _update_metas(self, task: Task):
        self._task_metas[task.id] = (task.name, task.state, task.progress)

    async def _get_task_meta(self, task_id) -> _TaskMeta:
        meta = self._task_metas.get(task_id)
        if meta is None:
            task = await self.get_task(task_id)
            self._update_metas(task)
            meta = (task.name, task.state, task.progress)
            self._task_metas[task.id] = meta
        return meta

    async def _get_task_progress(self, task_id: str) -> Optional[float]:
        meta = await self._get_task_meta(task_id)
        return meta[2]


class DAGTaskManager(TaskManager, DAGTaskStorage, ABC):
    def __init__(self, app: AsyncApp, caches_size: int = 1000):
        super().__init__(app, caches_size)
        self._get_task_dag = alru_cache(maxsize=self._caches_size)(self.get_task_dag)

    async def save_task(self, task: Task) -> bool:
        is_new = await super().save_task(task)
        if is_new:
            task = task.with_max_retries(self._app)
            has_dag = bool(self._app.registry[task.name].parents)
            if has_dag:
                group = self._app.registry[task.name].group
                await self._save_task_dag(task, group)
        return is_new

    async def _save_task_dag(self, task: Task, group: Optional[str]):
        max_retries = self._app.registry[task.name].max_retries
        task = task.with_max_retries(max_retries)
        task_dag = TaskDAG.from_app(self._app, task)
        for t in task_dag.created_tasks:
            await self.save_task_(t, group)
        for child, parents in task_dag.arg_providers.items():
            for provided_arg, parent in parents.items():
                await self.save_dag_dependency(
                    child, parent_id=parent, provided_arg=provided_arg
                )

    async def _enqueue_(self, task: Task):
        dag = await self._get_task_dag(task.id)
        if dag is not None:
            if task.id == dag.dag_task_id:
                await self._enqueue_start_nodes(dag, task)
            else:
                await self._enqueue(task)
        else:
            await self._enqueue(task)

    async def _enqueue_start_nodes(self, dag: TaskDAG, dag_task: Task):
        start_nodes = dag.get_ready()
        for start_task_id in start_nodes:
            start_task = await self.get_task(start_task_id)
            args = await self._get_args_from_dag(
                start_task, dag, already_known=dag_task.args
            )
            start_task = start_task.with_args(args)
            await self.enqueue(start_task)

    async def _save_result_event(self, result: ResultEvent) -> Task:
        task = await super()._save_result_event(result)
        dag = await self._get_task_dag(task.id)
        if dag is not None:
            if result.task_id != dag.dag_task_id:
                await self._enqueue_next_tasks(task, dag)
        return task

    async def _save_error_event(self, error: ErrorEvent) -> Task:
        task = await super()._save_error_event(error)
        if task.state is TaskState.ERROR:
            dag = await self._get_task_dag(task.id)
            if dag is not None:
                logger.info(
                    'fatal error in DAG task Task(id="%s"), cancelling other tasks',
                    task.id,
                )
                already_known = {task.id, dag.dag_task_id}
                await self._cancel_dag_tasks(
                    dag, requeue=False, already_known=already_known
                )
                dag_error = DAGError(dag.dag_task_id, error)
                dag_error = TaskError.from_exception(dag_error)
                dag_task = await self.get_task(dag.dag_task_id)
                dag_error = ErrorEvent(
                    task_id=dag.dag_task_id,
                    error=dag_error,
                    created_at=error.created_at,
                    retries_left=error.retries_left,
                )
                dag_task = dag_task.as_resolved(dag_error)
                await self.save_error(dag_error)
                await self.save_task(dag_task)
        elif task.state is TaskState.QUEUED:
            dag = await self._get_task_dag(task.id)
            if dag is not None:
                await self._save_dag_task_progress(dag)
        else:
            raise ValueError(f"unexpected task state {task.state}")
        return task

    async def _save_progress_event(self, event: ProgressEvent) -> Task:
        task = await super()._save_progress_event(event)
        dag = await self._get_task_dag(event.task_id)
        if dag is not None:
            await self._save_dag_task_progress(dag)
        return task

    async def _save_cancelled_event(self, event: CancelledEvent):
        task = await super()._save_cancelled_event(event)
        dag = await self._get_task_dag(event.task_id)
        if dag is not None:
            await self._save_dag_task_progress(dag)
        return task

    async def _enqueue_next_tasks(self, task: Task, dag: TaskDAG):
        # Mark done task as done
        done = [(t_id, (await self._get_task_meta(t_id))[1]) for t_id in dag]
        done = {t_id for t_id, state in done if state is TaskState.DONE}
        while done:
            ready = set(dag.get_ready())
            for t_id in done:
                if t_id in ready:
                    dag.done(t_id)
            done -= ready
        # Enqueue next tasks
        already_known = deepcopy(task.args)
        dag_task = await self.get_task(dag.dag_task_id)
        already_known.update(deepcopy(dag_task.args))
        successors = dag.successors(task.id)
        for next_task_id in dag.get_ready():
            if next_task_id in successors:
                next_task = await self.get_task(next_task_id)
                args = await self._get_args_from_dag(next_task, dag, already_known)
                next_task = next_task.with_args(args)
                if next_task.id == dag.dag_task_id:
                    await self.save_task(next_task)
                    await self._enqueue(next_task)
                else:
                    await self.enqueue(next_task)

    async def _save_dag_task_progress(self, dag: TaskDAG):
        dag_task_progress = await self._get_dag_progress(dag)
        dag_task_event = ProgressEvent(
            task_id=dag.dag_task_id,
            progress=dag_task_progress,
            created_at=datetime.now(timezone.utc),
        )
        dag_task = await self.get_task(dag.dag_task_id)
        dag_task = dag_task.as_resolved(dag_task_event)
        await self.save_task(dag_task)

    async def _get_args_from_dag(
        self, task: Task, dag: TaskDAG, already_known: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        args = dict()
        task_fn = self._app.registry[task.name].task
        args.update(cast(dict, task.args))
        args.update(already_known)
        missing = find_missing_args(task_fn, args)
        for arg in missing:
            provider_id = dag.get_argument_provider(task.id, argument_name=arg)
            res = await self.get_task_result(provider_id)
            args[arg] = res.result
        args = add_missing_args(task_fn, dict(), **args)
        return args

    async def cancel(self, task_id: str, *, requeue: bool):
        dag = await self._get_task_dag(task_id)
        if dag is not None:
            logger.info(
                'Task(id="%s") was cancelled, cancelling its DAG tasks',
                task_id,
            )
            await self._cancel_dag_tasks(dag, requeue)
        await super().cancel(task_id, requeue=requeue)

    async def _cancel_dag_tasks(
        self,
        dag: TaskDAG,
        requeue: bool,
        already_known: Optional[Collection[str]] = None,
    ):
        if already_known is None:
            already_known = tuple()
        already_known = set(already_known)
        for t_id in dag:
            if t_id not in already_known:
                meta = await self._get_task_meta(t_id)
                dag_t_state = meta[1]
                if dag_t_state not in READY_STATES:
                    logger.info('cancelling Task(id="%s") as DAG was cancelled', t_id)
                    await self._cancel(t_id, requeue=requeue)

    async def _get_dag_progress(self, dag: TaskDAG) -> float:
        progress = 0.0
        meta = {
            dag_t: await self._get_task_meta(dag_t)
            for dag_t in dag
            if dag_t != dag.dag_task_id
        }
        weights = {
            dag_t: self._app.registry[task_name].progress_weight
            for dag_t, (task_name, _, _) in meta.items()
        }
        for dag_t in meta:
            progress += meta[dag_t][2] * weights[dag_t]
        progress /= sum(weights.values())
        return progress
