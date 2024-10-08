# pylint: disable=redefined-outer-name
# Test utils meant to be imported from clients libs to test their implem of workers
from __future__ import annotations

import asyncio
import json
import logging
import signal
import tempfile
from abc import ABC
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from pydantic import Field

import icij_worker
from icij_common.pydantic_utils import (
    ICIJModel,
    IgnoreExtraModel,
    jsonable_encoder,
    safe_copy,
)
from icij_common.test_utils import TEST_DB
from icij_worker import (
    AsyncApp,
    AsyncBackend,
    ManagerEvent,
    ResultEvent,
    RoutingStrategy,
    Task,
    TaskError,
    TaskState,
    Worker,
    WorkerConfig,
)
from icij_worker.app import AsyncAppConfig, TaskGroup
from icij_worker.event_publisher import EventPublisher
from icij_worker.exceptions import TaskQueueIsFull, UnknownTask
from icij_worker.objects import (
    CancelEvent,
    ErrorEvent,
    Message,
    TaskUpdate,
    WorkerEvent,
)
from icij_worker.task_manager import TaskManager, TaskManagerConfig
from icij_worker.typing_ import RateProgress
from icij_worker.utils.dependencies import DependencyInjectionError
from icij_worker.utils.logging_ import LogWithWorkerIDMixin

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
    class DBMixin(ABC):
        _task_collection = "tasks"
        _result_collection = "results"
        _error_collection = "errors"
        _lock_collection = "locks"
        _manager_event_collection = "manager_events"
        _worker_event_collection = "worker_events"

        _routing_strategy: RoutingStrategy

        def __init__(self, db_path: Path) -> None:
            self._db_path = db_path
            self._task_meta: Dict[str, Tuple[str, str]] = dict()

        @property
        def db_path(self) -> Path:
            return self._db_path

        def _write(self, data: Dict):
            self._db_path.write_text(json.dumps(jsonable_encoder(data)))

        def _read(self):
            return json.loads(self._db_path.read_text())

        @staticmethod
        def _task_key(task_id: str, db: Optional[str]) -> str:
            return str((task_id, db))

        def _get_task_db_name(self, task_id) -> str:
            if task_id not in self._task_meta:
                db = self._read()
                task_meta = dict()
                for k, v in db[self._task_collection].items():
                    (task_id, db) = eval(k)  # pylint: disable=eval-used
                    task_meta[task_id] = (db, v["group"])
                self._task_meta = task_meta
            try:
                return self._task_meta[task_id][0]
            except KeyError as e:
                raise UnknownTask(task_id) from e

        @classmethod
        def fresh_db(cls, db_path: Path):
            db = {
                cls._task_collection: dict(),
                cls._lock_collection: dict(),
                cls._error_collection: dict(),
                cls._result_collection: dict(),
                cls._manager_event_collection: dict(),
                cls._worker_event_collection: dict(),
            }
            db_path.write_text(json.dumps(db))

        async def get_task_group(self, task_id: str) -> Optional[str]:
            try:
                return self._task_meta[task_id][1]
            except KeyError as e:
                raise UnknownTask(task_id) from e

        async def save_result(self, result: ResultEvent):
            db = self._get_task_db_name(result.task_id)
            task_key = self._task_key(task_id=result.task_id, db=db)
            db = self._read()
            db[self._result_collection][task_key] = result
            self._write(db)

        async def save_error(self, error: ErrorEvent):
            db = self._get_task_db_name(task_id=error.task_id)
            task_key = self._task_key(task_id=error.task_id, db=db)
            db = self._read()
            errors = db[self._error_collection].get(task_key, [])
            errors.append(error)
            db[self._error_collection][task_key] = errors
            self._write(db)

        async def save_task_(self, task: Task, group: Optional[str]):
            new_task = False
            ns = None
            try:
                ns = await self.get_task_group(task_id=task.id)
            except UnknownTask:
                new_task = True
                if group is not None:
                    ns = group
                    db_name = self._routing_strategy.test_db(group)
                else:
                    db_name = TEST_DB
            else:
                if ns != group:
                    msg = (
                        f"DB task group ({ns}) differs from"
                        f" save task group: {group}"
                    )
                    raise ValueError(msg)
                db_name = self._get_task_db_name(task.id)
            update = TaskUpdate.from_task(task).dict(exclude_none=True, by_alias=True)
            update["group"] = group
            task_key = self._task_key(task_id=task.id, db=db_name)
            db = self._read()
            updated = db[self._task_collection].get(
                task_key, task.dict(exclude_none=True, by_alias=True)
            )
            updated.update(update)
            db[self._task_collection][task_key] = updated
            self._write(db)
            self._task_meta[task.id] = (db_name, ns)
            return new_task

        @staticmethod
        def _order_events(events: List[Dict]) -> float:
            events = [
                datetime.fromisoformat(evt["createdAt"]).timestamp() for evt in events
            ]
            return -min(events)

        async def _consume_(
            self,
            collection: str,
            sleep_interval: float,
            factory: Callable[[Any], R],
            group: Optional[str] = None,
            select: Optional[Callable[[R], bool]] = None,
            order: Optional[Callable[[R], Any]] = None,
        ) -> R:
            while "i'm waiting until I find something interesting":
                db = self._read()
                selected = deepcopy(db[collection])
                if collection == self._task_collection:
                    selected = {
                        k: v
                        for k, v in selected.items()
                        if k not in db[self._lock_collection]
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

    @pytest.fixture(scope="session")
    def mock_db_session() -> Path:
        with tempfile.NamedTemporaryFile(prefix="mock-db", suffix=".json") as f:
            db_path = Path(f.name)
            DBMixin.fresh_db(db_path)
            yield db_path

    @pytest.fixture
    def mock_db(mock_db_session: Path) -> Path:
        # Wipe the DB
        DBMixin.fresh_db(mock_db_session)
        return mock_db_session

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
    class MockManager(DBMixin, TaskManager):
        def __init__(
            self, app: AsyncApp, db_path: Path, event_refresh_interval_s: float = 0.1
        ):
            super().__init__(db_path)
            super(DBMixin, self).__init__(app)
            self._event_refresh_interval_s = event_refresh_interval_s

        @property
        def app(self) -> AsyncApp:
            return self._app

        async def _ensure_queue_size(self, task: Task):
            # pylint: disable=arguments-differ
            db_name = self._get_task_db_name(task.id)
            key = self._task_key(task.id, db=db_name)
            db = self._read()
            tasks = db[self._task_collection]
            n_queued = sum(
                1 for t in tasks.values() if t["state"] == TaskState.QUEUED.value
            )
            if (
                self.max_task_queue_size is not None
                and n_queued > self.max_task_queue_size
            ):
                raise TaskQueueIsFull(self.max_task_queue_size)
            db_task = tasks.get(key)
            if db_task is None:
                raise UnknownTask(task.id)

        async def _enqueue(self, task: Task):
            await self._ensure_queue_size(task)

        async def _requeue(self, task: Task):
            await self._ensure_queue_size(task)
            db_name = self._get_task_db_name(task.id)
            key = self._task_key(task.id, db=db_name)
            db = self._read()
            db_task = db[self._task_collection][key]
            db_task.pop("group")
            db_task = Task.parse_obj(db_task)
            update = TaskUpdate(progress=0.0, state=TaskState.QUEUED).dict(
                exclude_none=True, by_alias=True
            )
            requeued = safe_copy(db_task, update=update)
            db[self._task_collection][key] = requeued
            self._write(db)

        async def get_task(self, task_id: str) -> Task:
            db_name = self._get_task_db_name(task_id)
            key = self._task_key(task_id=task_id, db=db_name)
            db = self._read()
            try:
                tasks = db[self._task_collection]
            except KeyError as e:
                raise UnknownTask(task_id) from e
            task = deepcopy(tasks[key])
            task.pop("group")
            return Task.parse_obj(task)

        async def get_task_errors(self, task_id: str) -> List[ErrorEvent]:
            db = self._get_task_db_name(task_id)
            key = self._task_key(task_id=task_id, db=db)
            db = self._read()
            errors = db[self._error_collection]
            errors = errors.get(key, [])
            errors = [ErrorEvent.parse_obj(err) for err in errors]
            return errors

        async def get_task_result(self, task_id: str) -> ResultEvent:
            db = self._get_task_db_name(task_id)
            key = self._task_key(task_id=task_id, db=db)
            db = self._read()
            results = db[self._result_collection]
            try:
                return ResultEvent.parse_obj(results[key])
            except KeyError as e:
                raise UnknownTask(task_id) from e

        async def get_tasks(
            self,
            *,
            task_name: Optional[str] = None,
            state: Optional[Union[List[TaskState], TaskState]] = None,
            db: Optional[str] = None,
            **kwargs,
        ) -> List[Task]:
            # pylint: disable=arguments-differ
            db = self._read()
            tasks = db.values()
            if state:
                if isinstance(state, TaskState):
                    state = [state]
                state = set(state)
                tasks = (t for t in tasks if t.state in state)
            return list(tasks)

        async def _consume(self) -> ManagerEvent:
            events = await self._consume_(
                self._manager_event_collection,
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
            db = self._read()
            task_id = event["taskId"]
            db_name = self._get_task_db_name(task_id)
            key = self._task_key(task_id=task_id, db=db_name)
            db[self._manager_event_collection][key] = events[1:]
            self._write(db)
            return cast(ManagerEvent, Message.parse_obj(event))

        async def cancel(self, task_id: str, *, requeue: bool):
            cancel_event = CancelEvent(
                task_id=task_id, requeue=requeue, created_at=datetime.now(timezone.utc)
            )
            db_name = self._get_task_db_name(task_id)
            key = self._task_key(task_id=task_id, db=db_name)
            db = self._read()
            events = db[self._worker_event_collection]
            if key not in events:
                events[key] = []
            event_dict = cancel_event.dict(exclude_unset=True, by_alias=True)
            events[key].append(event_dict)
            self._write(db)

        @classmethod
        def _from_config(cls, config: MockManagerConfig, **extras) -> MockManager:
            tm = cls(
                config.app,
                config.db_path,
                event_refresh_interval_s=config.event_refresh_interval_s,
            )
            return tm

    R = TypeVar("R")

    class MockEventPublisher(DBMixin, EventPublisher):
        _excluded_from_event_update = {"error"}

        def __init__(self, db_path: Path):
            super().__init__(db_path)
            self.published_events = []

        async def _publish_event(self, event: ManagerEvent):
            db_name = self._get_task_db_name(event.task_id)
            key = self._task_key(task_id=event.task_id, db=db_name)
            db = self._read()
            event_dict = event.dict(exclude_unset=True, by_alias=True)
            if key not in db[self._manager_event_collection]:
                db[self._manager_event_collection][key] = []
            db[self._manager_event_collection][key].append(event_dict)
            self._write(db)
            self.published_events.append(event)

        def _get_db_task(self, db: Dict, *, task_id: str, db_name: str) -> Dict:
            tasks = db[self._task_collection]
            try:
                return tasks[self._task_key(task_id=task_id, db=db_name)]
            except KeyError as e:
                raise UnknownTask(task_id) from e

    @WorkerConfig.register()
    class MockWorkerConfig(WorkerConfig, IgnoreExtraModel):
        type: ClassVar[str] = Field(const=True, default=AsyncBackend.mock.value)

        db_path: Path
        log_level: str = "DEBUG"
        loggers: List[str] = [icij_worker.__name__]
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
            super().__init__(app, worker_id, group=group, **kwargs)
            MockEventPublisher.__init__(self, db_path)
            self._poll_interval_s = poll_interval_s
            self._worker_id = worker_id
            self._logger_ = logging.getLogger(__name__)
            self.terminated_cancelled_event_loop = False

        @property
        def app(self) -> AsyncApp:
            return self._app

        @property
        def watch_cancelled_task(self) -> Optional[asyncio.Task]:
            return self._watch_cancelled_task

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

        async def signal_handler(self, signal_name: signal.Signals, *, graceful: bool):
            await self._signal_handler(signal_name, graceful=graceful)

        async def _aenter__(self):
            if not self._db_path.exists():
                raise OSError(f"worker DB was not initialized ({self._db_path})")

        @classmethod
        def _from_config(cls, config: MockWorkerConfig, **extras) -> MockWorker:
            worker = cls(
                db_path=config.db_path,
                poll_interval_s=config.task_queue_poll_interval_s,
                **extras,
            )
            return worker

        def _get_db_errors(self, task_id: str, db_name: str) -> List[TaskError]:
            key = self._task_key(task_id=task_id, db=db_name)
            db = self._read()
            errors = db[self._error_collection]
            try:
                return errors[key]
            except KeyError as e:
                raise UnknownTask(task_id) from e

        def _get_db_result(self, task_id: str, db_name: str) -> ResultEvent:
            key = self._task_key(task_id=task_id, db=db_name)
            db = self._read()
            try:
                errors = db[self._result_collection]
                return errors[key]
            except KeyError as e:
                raise UnknownTask(task_id) from e

        async def _acknowledge(self, task: Task):
            db_name = self._get_task_db_name(task.id)
            key = self._task_key(task.id, db_name)
            db = self._read()
            try:
                db[self._lock_collection].pop(key)
            except KeyError as e:
                raise UnknownTask(task.id) from e
            self._write(db)

        async def _negatively_acknowledge(self, nacked: Task):
            db_name = self._get_task_db_name(nacked.id)
            key = self._task_key(nacked.id, db_name)
            db = self._read()
            db[self._lock_collection].pop(key)
            self._write(db)

        @staticmethod
        def _task_factory(obj: Dict) -> Task:
            obj.pop("group", None)
            return Task.parse_obj(obj)

        async def _consume(self) -> Task:
            task = await self._consume_(
                self._task_collection,
                self._poll_interval_s,
                self._task_factory,
                group=self._group,
                select=lambda t: t.state is TaskState.QUEUED,
                order=lambda t: t.created_at,
            )
            db = self._read()
            db_name = self._get_task_db_name(task.id)
            key = self._task_key(task.id, db_name)
            db[self._lock_collection][key] = self._id
            self._write(db)
            return task

        async def _consume_worker_events(self) -> WorkerEvent:
            events = await self._consume_(
                self._worker_event_collection,
                self._poll_interval_s,
                list,
                group=self._group,
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
            event = cast(WorkerEvent, Message.parse_obj(event))
            db = self._read()
            db_name = self._get_task_db_name(event.task_id)
            key = self._task_key(event.task_id, db_name)
            db[self._worker_event_collection][key] = events[1:]
            self._write(db)
            return event

        async def work_once(self):
            await self._work_once()

        async def publish_cancelled_event(self, cancel_event: CancelEvent):
            await self._publish_cancelled_event(cancel_event)
