# pylint: disable=redefined-outer-name
# Test utils meant to be imported from clients libs to test their implem of workers
from __future__ import annotations

import asyncio
import logging
import signal
import uuid
from abc import ABC
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Annotated,
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
    cast,
)

from pydantic import Field

import icij_worker
from icij_common.pydantic_utils import ICIJModel, IgnoreExtraModel
from icij_worker import (
    AsyncApp,
    AsyncBackend,
    FSKeyValueStorage,
    ManagerEvent,
    RoutingStrategy,
    Task,
    TaskState,
    Worker,
    WorkerConfig,
)
from icij_worker.app import AsyncAppConfig, Chunked, Depends, TaskGroup
from icij_worker.dag.dag import TaskDAG
from icij_worker.event_publisher import EventPublisher
from icij_worker.exceptions import (
    MessageDeserializationError,
    TaskQueueIsFull,
    UnknownTask,
)
from icij_worker.objects import (
    CancelEvent,
    CancelledEvent,
    Message,
    ShutdownEvent,
    WorkerEvent,
)
from icij_worker.task_manager import DAGTaskManager, TaskManager, TaskManagerConfig
from icij_worker.typing_ import RateProgress
from icij_worker.utils.dependencies import DependencyInjectionError
from icij_worker.utils.logging_ import LogWithWorkerIDMixin
from icij_worker.utils.progress import to_raw_progress

logger = logging.getLogger(__name__)

_has_pytest = False  # necessary because of the pytest decorators which requires pytest
# to be defined
try:
    import pytest

    _has_pytest = True
except ImportError:
    pass

if _has_pytest:

    # TODO: make this one a MockStorage
    class DBMixin(FSKeyValueStorage, ABC):
        _locks_db_name = "locks"
        _manager_events_db_name = "manager_events"
        _worker_events_db_name = "worker_events"

        _routing_strategy: RoutingStrategy

        def __init__(self, db_path: Path) -> None:
            super().__init__(db_path)
            self._task_meta: Dict[str, Tuple[str, str]] = dict()

        def _make_group_dbs(self) -> Dict:
            dbs = super()._make_group_dbs()
            for k in [
                self._locks_db_name,
                self._manager_events_db_name,
                self._worker_events_db_name,
            ]:
                dbs[k] = self._make_db(self._db_path, name=k)
            return dbs

        @property
        def db_path(self) -> Path:
            return self._db_path

        @staticmethod
        def _order_events(events: List[Dict]) -> float:
            events = [
                datetime.fromisoformat(evt["createdAt"]).timestamp() for evt in events
            ]
            return -min(events)

        async def _consume_(
            self,
            db_name: str,
            sleep_interval: float,
            factory: Callable[[Any], R],
            group: Optional[str] = None,
            select: Optional[Callable[[R], bool]] = None,
            order: Optional[Callable[[R], Any]] = None,
        ) -> R:
            while "i'm waiting until I find something interesting":
                selected = dict(self._dbs[db_name].items())
                if db_name == self._tasks_db_name:
                    selected = {
                        k: v
                        for k, v in selected.items()
                        if k not in self._dbs[self._locks_db_name]
                    }
                if group is not None:
                    selected = [
                        (k, t) for k, t in selected.items() if t.get("group") == group
                    ]
                    for k, t in selected:
                        t.pop("group", None)
                else:
                    selected = selected.items()
                selected = [(k, factory(t)) for k, t in selected]
                if select is not None:
                    selected = [(k, t) for k, t in selected if select(t)]
                if selected:
                    if order is not None:
                        k, t = min(selected, key=lambda x: order(x[1]))
                    else:
                        k, t = selected[0]
                    return t
                await asyncio.sleep(sleep_interval)

    class MockAppConfig(ICIJModel, LogWithWorkerIDMixin):
        # Just provide logging stuff to be able to see nice logs while doing TDD
        log_level: str = "DEBUG"
        loggers: List[str] = [icij_worker.__name__]

    _MOCKED_CONFIG: Optional[MockAppConfig] = None

    async def mock_async_config_enter(**_):
        global _MOCKED_CONFIG
        _MOCKED_CONFIG = MockAppConfig()
        logger.info("Loading mocked configuration %s", _MOCKED_CONFIG.json(indent=2))

    def lifespan_config() -> MockAppConfig:
        if _MOCKED_CONFIG is None:
            raise DependencyInjectionError("config")
        return _MOCKED_CONFIG

    def loggers_enter(worker_id: str, **_):
        config = lifespan_config()
        config.setup_loggers(worker_id=worker_id)
        logger.info("worker loggers ready to log 💬")

    mocked_app_deps = [
        ("configuration loading", mock_async_config_enter, None),
        ("loggers setup", loggers_enter, None),
    ]

    APP = AsyncApp(name="test-app", dependencies=mocked_app_deps)

    async def _hello_world(
        greeted: str, progress: Optional[RateProgress] = None
    ) -> str:
        if progress is not None:
            await progress(0.1)
        greeting = f"Hello {greeted} !"
        if progress is not None:
            await progress(0.99)
        return greeting

    @APP.task
    async def hello_world(greeted: str, progress: Optional[RateProgress] = None) -> str:
        return await _hello_world(greeted, progress)

    @APP.task(group="hello")
    async def grouped_hello_world(
        greeted: str, progress: Optional[RateProgress] = None
    ) -> str:
        return await _hello_world(greeted, progress)

    @APP.task
    def hello_world_sync(greeted: str) -> str:
        greeting = f"Hello {greeted} !"
        return greeting

    async def _sleep_for(
        duration: float, s: float = 0.01, progress: Optional[RateProgress] = None
    ):
        start = datetime.now()
        elapsed = 0
        while elapsed < duration:
            elapsed = (datetime.now() - start).total_seconds()
            await asyncio.sleep(s)
            if progress is not None:
                p = min(elapsed / duration, 1.0)
                await progress(p)

    @APP.task(max_retries=1)
    async def sleep_for(
        duration: float, s: float = 0.01, progress: Optional[RateProgress] = None
    ):
        await _sleep_for(duration, s, progress)

    short_timeout = 1
    short_tasks_group = TaskGroup(name="short", timeout_s=short_timeout)

    @APP.task(max_retries=3, group=short_tasks_group)
    async def sleep_for_short(
        duration: float, s: float = 0.01, progress: Optional[RateProgress] = None
    ):
        await _sleep_for(duration, s, progress)

    @APP.task(max_retries=666)
    async def often_retriable() -> str:
        pass

    @APP.task(max_retries=5, progress_weight=3.0)
    async def preprocess(
        documents: Annotated[List[str], Chunked(size=2)],
        progress: Optional[RateProgress] = None,
    ) -> List[str]:
        if progress is not None:
            progress = to_raw_progress(progress, max_progress=len(documents))
        outputs = []
        for i, doc in enumerate(documents):
            processed = f"processed {doc}"
            outputs.append(processed)
            if progress is not None:
                await progress(i)
        return outputs

    @APP.task
    def detect(
        preprocessed: Annotated[List[str], Depends(on=preprocess), Chunked(size=2)],
        model: str,
    ) -> List[str]:
        detected = [f"Model {model} detected {doc}" for doc in preprocessed]
        return detected

    @APP.task
    def case_test_task(snake_case_arg: str):
        return snake_case_arg

    @pytest.fixture(scope="session")
    def test_async_app() -> AsyncApp:
        return AsyncApp.load(f"{__name__}.APP")

    @pytest.fixture(scope="session")
    def app_config() -> AsyncAppConfig:
        return AsyncAppConfig()

    @pytest.fixture(scope="session")
    def late_ack_app_config() -> AsyncAppConfig:
        return AsyncAppConfig(late_ack=True)

    @pytest.fixture(scope="session")
    def test_async_app_late(late_ack_app_config: AsyncAppConfig) -> AsyncApp:
        return AsyncApp.load(f"{__name__}.APP", config=late_ack_app_config)

    class MockManagerConfig(TaskManagerConfig):
        backend: ClassVar[AsyncBackend] = Field(const=True, default=AsyncBackend.mock)
        db_path: Path
        event_refresh_interval_s: float = 0.1

    @TaskManager.register(AsyncBackend.mock)
    class MockManager(DAGTaskManager, DBMixin):
        def __init__(
            self, app: AsyncApp, db_path: Path, event_refresh_interval_s: float = 0.1
        ):
            super().__init__(app)
            super(TaskManager, self).__init__(db_path)
            self._event_refresh_interval_s = event_refresh_interval_s

        @property
        def app(self) -> AsyncApp:
            return self._app

        async def _ensure_queue_size(self):
            tasks = await self.get_tasks(group=None, state=TaskState.QUEUED)
            n_queued = len(tasks)
            if (
                self.max_task_queue_size is not None
                and n_queued > self.max_task_queue_size
            ):
                raise TaskQueueIsFull(self.max_task_queue_size)

        async def _enqueue(self, task: Task):
            # pylint: disable=unused-argument
            await self._ensure_queue_size()

        async def _requeue(self, task: Task):
            await self._ensure_queue_size()
            group = await self.get_task_group(task.id)
            await self.save_task_(task, group)

        async def _consume(self) -> ManagerEvent:
            events = await self._consume_(
                self._manager_events_db_name,
                self._event_refresh_interval_s,
                list,
                select=lambda events: bool(  # pylint: disable=unnecessary-lambda
                    events
                ),
                order=self._order_events,
            )
            events = sorted(
                events,
                key=lambda e: datetime.fromisoformat(e["createdAt"]).timestamp(),
            )
            event = events[0]
            task_id = event["taskId"]
            key = self._key(task_id=task_id, obj_cls=ManagerEvent)
            manager_events_db = self._dbs[self._manager_events_db_name]
            manager_events_db[key] = events[1:]
            manager_events_db.commit(blocking=True)
            return cast(ManagerEvent, Message.parse_obj(event))

        async def _cancel(self, task_id: str, *, requeue: bool):
            cancel_event = CancelEvent(
                task_id=task_id, requeue=requeue, created_at=datetime.now(timezone.utc)
            )
            task_key = self._key(task_id=task_id, obj_cls=Task)
            tasks = self._dbs[self._tasks_db_name]
            task = tasks[task_key]
            task.pop("group", None)
            task = Task(**task)
            # If the task is in the queue we dequeue or requeue it
            if task.state <= TaskState.QUEUED:
                if not requeue:
                    event = CancelledEvent(
                        task_id=task_id,
                        requeue=False,
                        created_at=datetime.now(timezone.utc),
                    )
                    task = task.as_resolved(event)
                    tasks[task_key] = task.dict(exclude_unset=True, by_alias=True)
                    tasks.commit(blocking=True)
                return
            # Otherwise we notify the worker
            key = self._key(task_id=task_id, obj_cls=CancelEvent)
            worker_events = self._dbs[self._worker_events_db_name]
            events = worker_events.get(key, [])
            event_dict = cancel_event.dict(exclude_unset=True, by_alias=True)
            events.append(event_dict)
            worker_events[key] = events
            worker_events.commit(blocking=True)

        async def get_task_dag(self, task_id: str) -> Optional[TaskDAG]:
            return await super(DBMixin, self).get_task_dag(task_id)

        async def save_dag_dependency(
            self, task_id: str, *, parent_id: str, provided_arg: str
        ):
            return await super(DBMixin, self).save_dag_dependency(
                task_id=task_id, parent_id=parent_id, provided_arg=provided_arg
            )

        async def shutdown_workers(self):
            shutdown_event = ShutdownEvent(created_at=datetime.now(timezone.utc))
            key = self._key(task_id=f"shutdown-({uuid.uuid4()})", obj_cls=CancelEvent)
            worker_events = self._dbs[self._worker_events_db_name]
            events = worker_events.get(key, [])
            event_dict = shutdown_event.dict(by_alias=True, exclude_none=True)
            events.append(event_dict)
            worker_events[key] = events
            worker_events.commit(blocking=True)

        @classmethod
        def _from_config(cls, config: MockManagerConfig, **extras) -> MockManager:
            tm = cls(
                config.app,
                config.db_path,
                event_refresh_interval_s=config.event_refresh_interval_s,
            )
            return tm

        async def get_health(self) -> Dict[str, bool]:
            return {"db": True}

    R = TypeVar("R")

    class MockEventPublisher(DBMixin, EventPublisher):
        _excluded_from_event_update = {"error"}

        def __init__(self, db_path: Path):
            super().__init__(db_path)
            self.published_events = []

        async def _publish_event(self, event: ManagerEvent):
            key = self._key(event.task_id, ManagerEvent)
            manager_events_db = self._dbs[self._manager_events_db_name]
            event_dict = event.dict(exclude_unset=True, by_alias=True)
            events = manager_events_db.get(key, [])
            events.append(event_dict)
            manager_events_db[key] = events
            manager_events_db.commit(blocking=True)
            self.published_events.append(event)

    def _default_loggers() -> list[str]:
        return [icij_worker.__name__]

    @WorkerConfig.register()
    class MockWorkerConfig(WorkerConfig, IgnoreExtraModel):
        type: ClassVar[str] = Field(const=True, default=AsyncBackend.mock.value)

        db_path: Path
        log_level: str = "DEBUG"
        loggers: List[str] = Field(default_factory=_default_loggers)
        task_queue_poll_interval_s: float = 2.0

    @Worker.register(AsyncBackend.mock)
    class MockWorker(Worker, MockEventPublisher):
        def __init__(
            self,
            app: AsyncApp,
            worker_id: Optional[str] = None,
            *,
            group: Optional[str],
            db_path: Path,
            poll_interval_s: float,
            **kwargs,
        ):
            MockEventPublisher.__init__(self, db_path)
            Worker.__init__(self, app, worker_id, group=group, **kwargs)
            self._poll_interval_s = poll_interval_s
            self._worker_id = worker_id
            self._logger_ = logging.getLogger(__name__)
            self.terminated_cancelled_event_loop = False
            self._exited = False

        @property
        def app(self) -> AsyncApp:
            return self._app

        @property
        def watch_events(self) -> Optional[asyncio.Task]:
            return self._watch_events

        async def work_forever_async(self):
            await self._work_forever_async()

        @property
        def work_forever_task(self) -> Optional[asyncio.Task]:
            return self._work_forever_task

        @property
        def work_once_task(self) -> Optional[asyncio.Task]:
            return self._work_once_task

        @property
        def successful_exit(self) -> bool:
            return self._successful_exit

        @property
        def current(self) -> Optional[Task]:
            return self._current

        @current.setter
        def current(self, value: Optional[Task]):
            self._current = value

        @property
        def started_task_consumption(self) -> bool:
            return self._started_task_consumption_evt.is_set()

        async def signal_handler(self, signal_name: signal.Signals, *, graceful: bool):
            await self._signal_handler(signal_name, graceful=graceful)

        async def _aenter__(self):
            if not Path(self._db_path).exists():
                raise OSError(f"worker DB was not initialized ({self._db_path})")

        @classmethod
        def _from_config(cls, config: MockWorkerConfig, **extras) -> MockWorker:
            worker = cls(
                db_path=config.db_path,
                poll_interval_s=config.task_queue_poll_interval_s,
                **extras,
            )
            return worker

        async def _acknowledge(self, task: Task):
            key = self._key(task.id, Task)
            locks = self._dbs[self._locks_db_name]
            try:
                del locks[key]
            except KeyError as e:
                raise UnknownTask(task.id) from e
            locks.commit()

        async def _negatively_acknowledge(self, nacked: Task):
            key = self._key(nacked.id, Task)
            locks = self._dbs[self._locks_db_name]
            del locks[key]
            locks.commit()

        @staticmethod
        def _task_factory(obj: Dict) -> Task:
            obj.pop("group", None)
            try:
                task = Task.parse_obj(obj)
            except Exception as e:
                msg = f"invalid task object {obj}"
                raise MessageDeserializationError(msg) from e
            return task

        async def _consume(self) -> Task:
            task = await self._consume_(
                self._tasks_db_name,
                self._poll_interval_s,
                self._task_factory,
                group=self._group,
                select=lambda t: t.state is TaskState.QUEUED,
                order=lambda t: t.created_at,
            )
            locks = self._dbs[self._locks_db_name]
            key = self._key(task.id, Task)
            locks[key] = self._id
            locks.commit()
            return task

        async def consume_worker_events(self) -> WorkerEvent:
            return await self._consume_worker_events()

        async def _consume_worker_events(self) -> WorkerEvent:
            events = await self._consume_(
                self._worker_events_db_name,
                self._poll_interval_s,
                list,
                group=self._group,
                select=lambda evts: bool(evts),  # pylint: disable=unnecessary-lambda
                order=self._order_events,
            )
            events = sorted(
                events,
                key=lambda e: datetime.fromisoformat(e["createdAt"]).timestamp(),
            )
            event = events[0]
            try:
                event = cast(WorkerEvent, Message.parse_obj(event))
            except Exception as e:
                msg = f"invalid event object {event}"
                raise MessageDeserializationError(msg) from e
            if isinstance(event, CancelEvent):
                worker_events = self._dbs[self._worker_events_db_name]
                key = self._key(event.task_id, WorkerEvent)
                worker_events[key] = events[1:]
                worker_events.commit(blocking=True)
            return event

        async def work_once(self):
            await self._work_once()

        async def publish_cancelled_event(self, requeue: bool):
            await self._publish_cancelled_event(requeue)
