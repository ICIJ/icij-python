import asyncio
import functools
import logging
from abc import ABC, abstractmethod
from asyncio import Future
from copy import deepcopy
from functools import cached_property
from typing import (
    Any,
    ClassVar,
    Collection,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    cast,
    final,
)

from async_lru import alru_cache
from pydantic import Field

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
from icij_worker.namespacing import Namespacing
from icij_worker.objects import (
    AsyncBackend,
    CancelledEvent,
    ErrorEvent,
    ManagerEvent,
    ProgressEvent,
    READY_STATES,
    TaskError,
    TaskUpdate,
)
from icij_worker.task_storage import TaskStorage
from icij_worker.utils import CacheDict, RegistrableConfig, find_missing_args
from icij_worker.utils.asyncio_ import stop_other_tasks_when_exc
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


class TaskManager(TaskStorage, RegistrableFromConfig, ABC):
    def __init__(self, app: AsyncApp, caches_size: int = 1000):
        self._app = app
        self._loop = asyncio.get_event_loop()
        self._loops: List[Future] = []
        self._caches_size = caches_size
        self._task_metas: Dict[str, _TaskMeta] = CacheDict(cache_len=self._caches_size)
        self._get_task_dag = alru_cache(maxsize=self._caches_size)(self._get_task_dag)

    @final
    async def __aenter__(self):
        await self._aenter__()
        self._start_loops()

    async def _aenter__(self):
        pass

    @final
    async def __aexit__(self, exc_type, exc_value, tb):
        await self._aenter__()
        await self._stop_loops()

    async def _aexit__(self, exc_type, exc_val, exc_tb):
        pass

    @cached_property
    def late_ack(self) -> bool:
        return self._app.config.late_ack

    @cached_property
    def max_task_queue_size(self) -> int:
        return self._app.config.max_task_queue_size

    @cached_property
    def _namespacing(self) -> Namespacing:
        return self._app.namespacing

    @cached_property
    def app_name(self) -> str:
        return self._app.name

    @final
    async def enqueue(self, task: Task, with_dag: bool = True) -> Task:
        if task.state is not TaskState.CREATED:
            msg = f"invalid state {task.state}, expected {TaskState.CREATED}"
            raise ValueError(msg)
        task = await self.get_task(task.id)
        if task.state is TaskState.QUEUED:
            raise TaskAlreadyQueued(task.id)
        if with_dag:
            dag = await self._get_task_dag(task.id)
            if dag is not None:
                await self._enqueue_start_nodes(dag, task)
            else:
                await self._enqueue(task)
        else:
            await self._enqueue(task)
        queued = safe_copy(task, update={"state": TaskState.QUEUED})
        await self.save_task(queued)
        return queued

    async def _enqueue_start_nodes(self, dag: TaskDAG, dag_task: Task):
        for start_task_id in dag.start_nodes:
            start_task = await self.get_task(start_task_id)
            args = await self._get_args_from_dag(
                start_task, dag, already_known=dag_task.arguments
            )
            start_task = start_task.with_argument(args)
            await self.enqueue(start_task, with_dag=False)

    @final
    async def requeue(self, task: Task):
        logger.info("requeing Task(id=%s)", task.id)
        update = {"state": TaskState.QUEUED, "progress": 0.0, "cancelled_at": None}
        updated = safe_copy(task, update=update)
        await self._requeue(updated)
        logger.info("Task(id=%s) requeued", updated.id)

    @final
    async def save_task(self, task: Task) -> bool:
        is_new = False
        try:
            ns = await self.get_task_namespace(task_id=task.id)
        except UnknownTask as e:
            is_new = True
            try:
                ns = self._app.registry[task.name].namespace
            except KeyError:
                available_tasks = list(self._app.registry)
                raise UnregisteredTask(task.name, available_tasks) from e
        if is_new:
            task = task.with_max_retries(self._app)
            has_dag = bool(self._app.registry[task.name].parents)
            if has_dag:
                await self._save_task_dag(task, ns)
        self._update_metas(task)
        return await self.save_task_(task, ns)

    async def _save_task_dag(self, task: Task, namespace: Optional[str]):
        max_retries = self._app.registry[task.name].max_retries
        task = task.with_max_retries(max_retries)
        task_dag = TaskDAG.from_app(self._app, task)
        for t in task_dag.created_tasks:
            await self.save_task_(t, namespace)

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

    @final
    async def _save_result_event(self, result: ResultEvent):
        await self.save_result(result)
        task = await self.get_task(result.task_id)
        task = task.as_resolved(result)
        await self.save_task(task)
        dag = await self._get_task_dag(task.id)
        if dag is not None:
            has_next = await self._enqueue_next_tasks(task, dag)
            if not has_next:
                dag_task = await self.get_task(dag.dag_task_id)
                update = TaskUpdate.done(completed_at=result.created_at)
                dag_task = safe_copy(dag_task, update=update.dict(exclude_unset=True))
                await self.save_task(dag_task)

    @final
    async def _save_error_event(self, error: ErrorEvent):
        # Update the task retries count
        task = await self.get_task(error.task_id)
        task = task.as_resolved(error)
        await self.save_error(error)
        if task.state is TaskState.QUEUED:
            await self.requeue(task)
        await self.save_task(task)
        if task.state is TaskState.ERROR:
            dag = await self._get_task_dag(task.id)
            if dag is not None:
                logger.info(
                    'fatal error in DAG task Task(id="%s"), cancelling other tasks',
                    task.id,
                )
                already_known = {task.id, dag.dag_task_id}
                # TODO: here we don't really need the dag but just the list of DAG task,
                #  we might gain a little bit of time by not building the actual DAG
                await self._cancel_dag_tasks(
                    dag, requeue=False, already_known=already_known
                )
                dag_error = DAGError(dag.dag_task_id, error)
                dag_error = TaskError.from_exception(dag_error)
                dag_task = await self.get_task(dag.dag_task_id)
                dag_task = dag_task.as_resolved(dag_error)
                await self.save_error(dag_error)
                await self.save_task(dag_task)
        elif task.state is TaskState.QUEUED:
            dag = await self._get_task_dag(task.id)
            if dag is not None:
                # TODO: here we don't really need the dag but just the list of DAG task,
                #  we might gain a little bit of time by not building the actual DAG
                await self._save_dag_task_progress(dag)
        else:
            raise ValueError(f"unexpected task state {task.state}")

    async def _save_progress_event(self, event: ProgressEvent):
        # Might be better to be overridden to be performed in a transactional manner
        # when possible
        task = await self.get_task(event.task_id)
        task = task.as_resolved(event)
        await self.save_task(task)
        dag = await self._get_task_dag(event.task_id)
        if dag is not None:
            # TODO: here we don't really need the dag but just the list of DAG task,
            #  we might gain a little bit of time by not building the actual DAG
            await self._save_dag_task_progress(dag)

    async def _save_dag_task_progress(self, dag: TaskDAG):
        # TODO: here we don't really need the dag but just the list of DAG task,
        #  we might gain a little bit of time by not building the actual DAG
        dag_task_progress = self._get_dag_progress(dag)
        dag_task_event = ProgressEvent(progress=dag_task_progress)
        dag_task = await self.get_task(dag.dag_task_id)
        dag_task = dag_task.as_resolved(dag_task_event)
        await self.save_task(dag_task)

    async def _save_cancelled_event(self, event: CancelledEvent):
        task = await self.get_task(event.task_id)
        task = task.as_resolved(event)
        if event.requeue and not self.late_ack:
            await self.requeue(task)
        await self.save_task(task)

    async def cancel(self, task_id: str, *, requeue: bool):
        dag = await self._get_task_dag(task_id)
        if dag is not None:
            logger.info(
                'Task(id="%s") was cancelled, cancelling its DAG tasks',
                task_id,
            )
            # TODO: here we don't really need the dag but just the list of DAG task,
            #  we might gain a little bit of time by not building the actual DAG
            await self._cancel_dag_tasks(dag, requeue)
            cancelled_event = CancelledEvent(task_id, requeue)
            await self._save_cancelled_event(cancelled_event)
        else:
            await self._cancel(task_id, requeue=requeue)

    @abstractmethod
    async def _cancel(self, task_id: str, *, requeue: bool): ...

    @abstractmethod
    async def _consume(self) -> ManagerEvent: ...

    @abstractmethod
    async def _enqueue(self, task: Task) -> Task: ...

    @abstractmethod
    async def _requeue(self, task: Task): ...

    def _start_loops(self):
        self._loops = [self.consume_events()]
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

    async def _enqueue_next_tasks(self, task: Task, dag: TaskDAG) -> bool:
        # Mark done task as done
        dag.done(task.id)
        # Update task with other done tasks
        await self._mark_done_tasks(dag, already_done={task.id})
        # Enqueue next tasks
        already_known = deepcopy(task.arguments)
        dag_task = await self.get_task(dag.dag_task_id)
        already_known.update(deepcopy(dag_task.arguments))
        has_next = False
        if dag.is_active():
            successors = dag.successors(task.id)
            for next_task_id in dag.get_ready():
                has_next = True
                if next_task_id in successors:
                    next_task = await self.get_task(next_task_id)
                    args = await self._get_args_from_dag(next_task, dag, already_known)
                    next_task = next_task.with_arguments(args)
                    await self.enqueue(next_task, with_dag=False)
        return has_next

    async def _get_args_from_dag(
        self, task: Task, dag: TaskDAG, already_known: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        args = dict()
        task_fn = self._app.registry[task.name].task
        args.update(cast(dict, task.arguments))
        args.update(already_known)
        missing = find_missing_args(task_fn, args)
        for arg in missing:
            provider_id = dag.get_argument_provider(task.id, argument_name=arg)
            res = await self.get_task_result(provider_id)
            args[arg] = res
        return args

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
                dag_t_state = meta[0]
                if dag_t_state not in READY_STATES:
                    logger.info('cancelling Task(id="%s") as DAG was cancelled', t_id)
                    await self._cancel(t_id, requeue=requeue)

    async def _mark_done_tasks(self, dag: TaskDAG, already_done: Set[str]):
        for t_id in dag:
            if t_id not in already_done:
                meta = await self._get_task_meta(t_id)
                dag_t_state = meta[0]
                if dag_t_state is TaskState.DONE:
                    dag.done(t_id)

    def _update_metas(self, task: Task):
        self._task_metas[task.id] = (task.name, task.state, task.progress)

    async def _get_task_meta(self, task_id) -> _TaskMeta:
        meta = self._task_metas.get(task_id)
        if meta is None:
            task = await self.get_task(task_id)
            self._update_metas(task)
            meta = (task.name, task.state, task.progress)
            self._task_metas[task] = meta
        return meta

    async def _get_task_progress(self, task_id: str) -> Optional[float]:
        meta = await self._get_task_meta(task_id)
        return meta[2]

    async def _get_dag_progress(self, dag: TaskDAG) -> float:
        # TODO: here we don't really need the dag but just the list of DAG task,
        #  we might gain a little bit of time by not building the actual DAG
        progress = 0.0
        meta = {dag_t: await self._get_task_meta(dag_t) for dag_t in dag}
        weights = {
            dag_t: self._app.registry[task_name].progress_weight
            for dag_t, (task_name, _, _) in meta.values()
        }
        for dag_t in meta:
            progress += meta[dag_t][2] * weights[dag_t]
        progress /= sum(weights.values())
        return progress
