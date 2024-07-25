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
from datetime import datetime
from pathlib import Path
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
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
    Namespacing,
    Task,
    TaskError,
    TaskEvent,
    TaskResult,
    TaskState,
    Worker,
    WorkerConfig,
    WorkerType,
)
from icij_worker.app import AsyncAppConfig
from icij_worker.event_publisher import EventPublisher
from icij_worker.exceptions import TaskQueueIsFull, UnknownTask
from icij_worker.objects import CancelEvent, TaskUpdate, WorkerEvent
from icij_worker.task_manager import TaskManager
from icij_worker.typing_ import PercentProgress
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
        _lock_collection = "locks"
        _error_collection = "errors"
        _result_collection = "results"
        _worker_event_collection = "cancel_events"

        _namespacing: Namespacing

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
                    task_meta[task_id] = (db, v["namespace"])
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
                cls._worker_event_collection: dict(),
            }
            db_path.write_text(json.dumps(db))

        async def get_task_namespace(self, task_id: str) -> Optional[str]:
            try:
                return self._task_meta[task_id][1]
            except KeyError as e:
                raise UnknownTask(task_id) from e

        async def save_result(self, result: TaskResult):
            db = self._get_task_db_name(result.task_id)
            task_key = self._task_key(task_id=result.task_id, db=db)
            db = self._read()
            db[self._result_collection][task_key] = result
            task = db[self._task_collection][task_key]
            ns = task.pop("namespace")
            task = Task.parse_obj(db[self._task_collection][task_key])
            update = {
                "completed_at": result.completed_at,
                "progress": 1.0,
                "state": TaskState.DONE.value,
            }
            task = safe_copy(task, update=update)
            task = task.dict(by_alias=True, exclude_unset=True)
            task["namespace"] = ns
            db[self._task_collection][task_key] = task
            self._write(db)

        async def save_error(self, error: TaskError):
            db = self._get_task_db_name(task_id=error.task_id)
            task_key = self._task_key(task_id=error.task_id, db=db)
            db = self._read()
            errors = db[self._error_collection].get(task_key)
            if errors is None:
                errors = []
            errors.append(error)
            db[self._error_collection][task_key] = errors
            self._write(db)

        async def save_task(self, task: Task, namespace: Optional[str]):
            new_task = False
            try:
                ns = await self.get_task_namespace(task_id=task.id)
            except UnknownTask:
                new_task = True
                if namespace is not None:
                    db_name = self._namespacing.test_db(namespace)
                else:
                    db_name = TEST_DB
                update = task.dict(exclude_unset=True, by_alias=True)
            else:
                if ns != namespace:
                    msg = (
                        f"DB task namespace ({ns}) differs from"
                        f" save task namespace: {namespace}"
                    )
                    raise ValueError(msg)
                db_name = self._get_task_db_name(task.id)
                update = TaskUpdate.from_task(task).dict(
                    exclude_none=True, by_alias=True
                )
            update["namespace"] = namespace
            task_key = self._task_key(task_id=task.id, db=db_name)
            db = self._read()
            updated = db[self._task_collection].get(task_key, dict())
            updated.update(update)
            db[self._task_collection][task_key] = updated
            self._write(db)
            self._task_meta[task.id] = (db_name, namespace)
            return new_task

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

    @APP.task
    async def hello_world(
        greeted: str, progress: Optional[PercentProgress] = None
    ) -> str:
        if progress is not None:
            await progress(0.1)
        greeting = f"Hello {greeted} !"
        if progress is not None:
            await progress(0.99)
        return greeting

    @APP.task
    def hello_world_sync(greeted: str) -> str:
        greeting = f"Hello {greeted} !"
        return greeting

    @APP.task
    async def sleep_for(
        duration: float, s: float = 0.01, progress: Optional[PercentProgress] = None
    ):
        start = datetime.now()
        elapsed = 0
        while elapsed < duration:
            elapsed = (datetime.now() - start).total_seconds()
            await asyncio.sleep(s)
            if progress is not None:
                p = min(elapsed / duration, 1.0)
                await progress(p)

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

    class MockManager(DBMixin, TaskManager):

        def __init__(
            self,
            db_path: Path,
            max_task_queue_size: int,
            app_name: str = "test-app",
            namespacing: Optional[Namespacing] = None,
        ):
            super().__init__(db_path)
            super(DBMixin, self).__init__(app_name, max_task_queue_size, namespacing)

        async def _enqueue(self, task: Task, **kwargs) -> Task:
            # pylint: disable=arguments-differ
            namespace = await self.get_task_namespace(task.id)
            if namespace is not None:
                db = self._namespacing.test_db(namespace)
            else:
                db = TEST_DB
            key = self._task_key(task.id, db=db)
            db = self._read()
            tasks = db[self._task_collection]
            n_queued = sum(
                1 for t in tasks.values() if t["state"] == TaskState.QUEUED.value
            )
            if n_queued > self._max_task_queue_size:
                raise TaskQueueIsFull(self._max_task_queue_size)
            updated = tasks.get(key)
            if updated is None:
                raise UnknownTask(task.id)
            update = {"state": TaskState.QUEUED, "progress": 0.0}
            updated.update(update)
            db[self._task_collection][key] = updated
            self._write(db)
            updated.pop("namespace")
            return Task.parse_obj(updated)

        async def _cancel(self, *, task_id: str, requeue: bool):
            db = self._get_task_db_name(task_id)
            key = self._task_key(task_id=task_id, db=db)
            event = CancelEvent(
                task_id=task_id, requeue=requeue, cancelled_at=datetime.now()
            )
            db = self._read()
            db[self._worker_event_collection][key] = event.dict()
            self._write(db)

        async def get_task(self, task_id: str) -> Task:
            db = self._get_task_db_name(task_id)
            key = self._task_key(task_id=task_id, db=db)
            db = self._read()
            try:
                tasks = db[self._task_collection]
                task = deepcopy(tasks[key])
                task.pop("namespace")
                return Task.parse_obj(task)
            except KeyError as e:
                raise UnknownTask(task_id) from e

        async def get_task_errors(self, task_id: str) -> List[TaskError]:
            db = self._get_task_db_name(task_id)
            key = self._task_key(task_id=task_id, db=db)
            db = self._read()
            errors = db[self._error_collection]
            errors = errors.get(key, [])
            errors = [TaskError.parse_obj(err) for err in errors]
            return errors

        async def get_task_result(self, task_id: str) -> TaskResult:
            db = self._get_task_db_name(task_id)
            key = self._task_key(task_id=task_id, db=db)
            db = self._read()
            results = db[self._result_collection]
            try:
                return TaskResult.parse_obj(results[key])
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

    R = TypeVar("R", bound=ICIJModel)

    class MockEventPublisher(DBMixin, EventPublisher):
        _excluded_from_event_update = {"error"}

        def __init__(self, db_path: Path):
            super().__init__(db_path)
            self.published_events = []

        async def _publish_event(self, event: TaskEvent):
            self.published_events.append(event)
            # Let's simulate that we have an event handler which will reflect some event
            # into the DB, we could not do it. In this case tests should not expect that
            # events are reflected in the DB. They would only be registered inside
            # published_events (which could be enough).
            # Here we choose to reflect the change in the DB since its closer to what
            # will happen IRL and test integration further
            db_name = self._get_task_db_name(event.task_id)
            ns = await self.get_task_namespace(event.task_id)
            key = self._task_key(task_id=event.task_id, db=db_name)
            db = self._read()
            try:
                task = self._get_db_task(db, task_id=event.task_id, db_name=db_name)
                task.pop("namespace")
                task = Task.parse_obj(task)
            except UnknownTask:
                task = Task.parse_obj(Task.mandatory_fields(event, keep_id=True))
            update = task.resolve_event(event)
            if update is not None:
                update = {
                    k: v for k, v in update.dict(by_alias=True).items() if v is not None
                }
                task = task.dict(exclude_unset=True, by_alias=True)
                task["namespace"] = ns
                task.update(update)
                db[self._task_collection][key] = task
                self._write(db)

        def _get_db_task(self, db: Dict, *, task_id: str, db_name: str) -> Dict:
            tasks = db[self._task_collection]
            try:
                return tasks[self._task_key(task_id=task_id, db=db_name)]
            except KeyError as e:
                raise UnknownTask(task_id) from e

    @WorkerConfig.register()
    class MockWorkerConfig(WorkerConfig, IgnoreExtraModel):
        type: ClassVar[str] = Field(const=True, default=WorkerType.mock.value)

        db_path: Path
        log_level: str = "DEBUG"
        loggers: List[str] = [icij_worker.__name__]
        task_queue_poll_interval_s: float = 2.0

    @Worker.register(WorkerType.mock)
    class MockWorker(Worker, MockEventPublisher):
        def __init__(
            self,
            app: AsyncApp,
            worker_id: Optional[str] = None,
            *,
            namespace: Optional[str],
            db_path: Path,
            task_queue_poll_interval_s: float,
            **kwargs,
        ):
            super().__init__(app, worker_id, namespace=namespace, **kwargs)
            MockEventPublisher.__init__(self, db_path)
            self._task_queue_poll_interval_s = task_queue_poll_interval_s
            self._worker_id = worker_id
            self._logger_ = logging.getLogger(__name__)
            self.terminated_cancelled_event_loop = False

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

        async def signal_handler(self, signal_name: signal.Signals, *, graceful: bool):
            await self._signal_handler(signal_name, graceful=graceful)

        async def _aenter__(self):
            if not self._db_path.exists():
                raise OSError(f"worker DB was not initialized ({self._db_path})")

        @classmethod
        def _from_config(cls, config: MockWorkerConfig, **extras) -> MockWorker:
            worker = cls(
                db_path=config.db_path,
                task_queue_poll_interval_s=config.task_queue_poll_interval_s,
                **extras,
            )
            return worker

        def _to_config(self) -> MockWorkerConfig:
            return MockWorkerConfig(db_path=self._db_path)

        async def _save_result(self, result: TaskResult):
            return await super(Worker, self).save_result(result)

        async def _save_error(self, error: TaskError):
            await super(Worker, self).save_error(error)

        def _get_db_errors(self, task_id: str, db_name: str) -> List[TaskError]:
            key = self._task_key(task_id=task_id, db=db_name)
            db = self._read()
            errors = db[self._error_collection]
            try:
                return errors[key]
            except KeyError as e:
                raise UnknownTask(task_id) from e

        def _get_db_result(self, task_id: str, db_name: str) -> TaskResult:
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
            if nacked.state is TaskState.QUEUED:
                placeholder = True
                await self._requeue(nacked, placeholder)
            elif nacked.state is TaskState.ERROR:
                db_name = self._get_task_db_name(nacked.id)
                key = self._task_key(nacked.id, db_name)
                db = self._read()
                update = TaskUpdate(state=TaskState.ERROR).dict(
                    by_alias=True, exclude_unset=True
                )
                task = db[self._task_collection][key]
                task.update(update)
                db[self._lock_collection].pop(key)
                self._write(db)
            elif nacked.state is TaskState.CANCELLED:
                db_name = self._get_task_db_name(nacked.id)
                key = self._task_key(nacked.id, db_name)
                db = self._read()
                db[self._lock_collection].pop(key)
                self._write(db)
            else:
                msg = (
                    f"expected {TaskState.QUEUED} or {TaskState.ERROR},"
                    f" found {nacked.state}"
                )
                raise ValueError(msg)

        async def _requeue(self, task: Task, acknowledge: bool):
            db_name = self._get_task_db_name(task.id)
            key = self._task_key(task.id, db_name)
            db = self._read()
            update = TaskUpdate(
                progress=0.0, state=TaskState.QUEUED, retries=task.retries
            ).dict(by_alias=True, exclude_unset=True)
            db[self._task_collection][key].update(update)
            db[self._lock_collection].pop(key, None)
            self._write(db)

        async def _consume(self) -> Task:
            task = await self._consume_(
                self._task_collection,
                Task,
                namespace=self._namespace,
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
            event = await self._consume_(
                self._worker_event_collection,
                WorkerEvent,
                namespace=self._namespace,
                order=lambda e: e.cancelled_at,
            )
            db = self._read()
            db_name = self._get_task_db_name(event.task_id)
            key = self._task_key(event.task_id, db_name)
            db[self._worker_event_collection].pop(key)
            self._write(db)
            return event

        async def _consume_(
            self,
            collection: str,
            consumed_cls: Type[R],
            namespace: Optional[str] = None,
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
                if namespace is not None:
                    selected = [
                        (k, t)
                        for k, t in selected.items()
                        if t.get("namespace") == namespace
                    ]
                else:
                    selected = selected.items()
                for k, t in selected:
                    t.pop("namespace", None)
                selected = [(k, consumed_cls.parse_obj(t)) for k, t in selected]
                if select is not None:
                    selected = [(k, t) for k, t in selected if select(t)]
                if selected:
                    if order is not None:
                        k, t = min(selected, key=lambda x: order(x[1]))
                    else:
                        k, t = selected[0]
                    return t
                await asyncio.sleep(self._task_queue_poll_interval_s)

        async def work_once(self):
            await self._work_once()
