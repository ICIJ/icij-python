# pylint: disable=redefined-outer-name
# Test utils meant to be imported from clients libs to test their implem of workers
from __future__ import annotations

import asyncio
import json
import logging
import signal
import tempfile
from abc import ABC
from datetime import datetime
from pathlib import Path
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Optional,
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
from icij_worker import (
    AsyncApp,
    Task,
    TaskError,
    TaskEvent,
    TaskResult,
    TaskStatus,
    Worker,
    WorkerConfig,
    WorkerType,
)
from icij_worker.event_publisher import EventPublisher
from icij_worker.exceptions import TaskAlreadyExists, TaskQueueIsFull, UnknownTask
from icij_worker.task import CancelledTaskEvent
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

    class DBMixin(ABC):
        _task_collection = "tasks"
        _error_collection = "errors"
        _result_collection = "results"
        _cancel_event_collection = "cancel_events"

        def __init__(self, db_path: Path):
            self._db_path = db_path
            self._task_projects: Dict[str, str] = dict()

        @property
        def db_path(self) -> Path:
            return self._db_path

        def _write(self, data: Dict):
            self._db_path.write_text(json.dumps(jsonable_encoder(data)))

        def _read(self):
            return json.loads(self._db_path.read_text())

        @staticmethod
        def _task_key(task_id: str, project: Optional[str]) -> str:
            return str((task_id, project))

        def _get_task_project_id(self, task_id) -> str:
            if task_id not in self._task_projects:
                db = self._read()
                self._task_projects = dict(
                    eval(k)  # pylint: disable=eval-used
                    for k in db[self._task_collection].keys()
                )
            try:
                return self._task_projects[task_id]
            except KeyError as e:
                raise UnknownTask(task_id) from e

        @classmethod
        def fresh_db(cls, db_path: Path):
            db = {
                cls._task_collection: dict(),
                cls._error_collection: {},
                cls._result_collection: {},
                cls._cancel_event_collection: {},
            }
            db_path.write_text(json.dumps(db))

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
                await progress(elapsed / duration * 100)

    @pytest.fixture(scope="session")
    def test_async_app() -> AsyncApp:
        return AsyncApp.load(f"{__name__}.APP")

    class MockManager(TaskManager, DBMixin):
        def __init__(self, db_path: Path, max_queue_size: int):
            super().__init__(db_path)
            self._max_queue_size = max_queue_size

        async def _enqueue(self, task: Task) -> Task:
            key = self._task_key(task_id=task.id, project=task.project_id)
            db = self._read()
            tasks = db[self._task_collection]
            n_queued = sum(
                1 for t in tasks.values() if t["status"] == TaskStatus.QUEUED.value
            )
            if n_queued > self._max_queue_size:
                raise TaskQueueIsFull(self._max_queue_size)
            if key in tasks:
                raise TaskAlreadyExists(task.id)
            update = {"status": TaskStatus.QUEUED}
            task = safe_copy(task, update=update)
            tasks[key] = task.dict()
            self._write(db)
            return task

        async def _cancel(self, *, task_id: str, requeue: bool):
            project_id = self._get_task_project_id(task_id)
            key = self._task_key(task_id=task_id, project=project_id)
            event = CancelledTaskEvent(
                task_id=task_id, requeue=requeue, created_at=datetime.now()
            )
            db = self._read()
            db[self._cancel_event_collection][key] = event.dict()
            self._write(db)

        async def get_task(self, *, task_id: str) -> Task:
            project_id = self._get_task_project_id(task_id)
            key = self._task_key(task_id=task_id, project=project_id)
            db = self._read()
            try:
                tasks = db[self._task_collection]
                return Task(**tasks[key])
            except KeyError as e:
                raise UnknownTask(task_id) from e

        async def get_task_errors(self, task_id: str) -> List[TaskError]:
            project_id = self._get_task_project_id(task_id)
            key = self._task_key(task_id=task_id, project=project_id)
            db = self._read()
            errors = db[self._error_collection]
            errors = errors.get(key, [])
            errors = [TaskError(**err) for err in errors]
            return errors

        async def get_task_result(self, task_id: str) -> TaskResult:
            project_id = self._get_task_project_id(task_id)
            key = self._task_key(task_id=task_id, project=project_id)
            db = self._read()
            results = db[self._result_collection]
            try:
                return TaskResult(**results[key])
            except KeyError as e:
                raise UnknownTask(task_id) from e

        async def get_tasks(
            self,
            project_id: Optional[str] = None,
            task_type: Optional[str] = None,
            status: Optional[Union[List[TaskStatus], TaskStatus]] = None,
        ) -> List[Task]:
            db = self._read()
            tasks = db.values()
            if status:
                if isinstance(status, TaskStatus):
                    status = [status]
                status = set(status)
                tasks = (t for t in tasks if t.status in status)
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
            project_id = self._get_task_project_id(event.task_id)
            key = self._task_key(task_id=event.task_id, project=project_id)
            db = self._read()
            try:
                task = self._get_db_task(db, task_id=event.task_id, project=project_id)
                task = Task(**task)
            except UnknownTask:
                task = Task(**Task.mandatory_fields(event, keep_id=True))
            update = task.resolve_event(event)
            if update is not None:
                task = task.dict(exclude_unset=True, by_alias=True)
                update = {
                    k: v
                    for k, v in event.dict(by_alias=True, exclude_unset=True).items()
                    if v is not None
                }
                if "taskId" in update:
                    update["id"] = update.pop("taskId")
                if "taskType" in update:
                    update["type"] = update.pop("taskType")
                if "error" in update:
                    update.pop("error")
                # The nack is responsible for bumping the retries
                if "retries" in update:
                    update.pop("retries")
                task.update(update)
                db[self._task_collection][key] = task
                self._write(db)

        def _get_db_task(self, db: Dict, task_id: str, project: str) -> Dict:
            tasks = db[self._task_collection]
            try:
                return tasks[self._task_key(task_id=task_id, project=project)]
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
            worker_id: str,
            db_path: Path,
            task_queue_poll_interval_s: float,
            **kwargs,
        ):
            super().__init__(app, worker_id, **kwargs)
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
            project_id = self._get_task_project_id(result.task_id)
            task_key = self._task_key(task_id=result.task_id, project=project_id)
            db = self._read()
            db[self._result_collection][task_key] = result
            self._write(db)

        async def _save_error(self, error: TaskError):
            project_id = self._get_task_project_id(task_id=error.task_id)
            task_key = self._task_key(task_id=error.task_id, project=project_id)
            db = self._read()
            errors = db[self._error_collection].get(task_key)
            if errors is None:
                errors = []
            errors.append(error)
            db[self._error_collection][task_key] = errors
            self._write(db)

        def _get_db_errors(self, task_id: str, project: str) -> List[TaskError]:
            key = self._task_key(task_id=task_id, project=project)
            db = self._read()
            errors = db[self._error_collection]
            try:
                return errors[key]
            except KeyError as e:
                raise UnknownTask(task_id) from e

        def _get_db_result(self, task_id: str, project: str) -> TaskResult:
            key = self._task_key(task_id=task_id, project=project)
            db = self._read()
            try:
                errors = db[self._result_collection]
                return errors[key]
            except KeyError as e:
                raise UnknownTask(task_id) from e

        async def _acknowledge(self, task: Task, completed_at: datetime):
            project_id = self._get_task_project_id(task.id)
            key = self._task_key(task.id, project_id)
            db = self._read()
            tasks = db[self._task_collection]
            try:
                saved_task = tasks[key]
            except KeyError as e:
                raise UnknownTask(task.id) from e
            saved_task = Task(**saved_task)
            update = {
                "completed_at": completed_at,
                "status": TaskStatus.DONE,
                "progress": 100.0,
            }
            tasks[key] = safe_copy(saved_task, update=update)
            self._write(db)

        async def _negatively_acknowledge(self, nacked: Task, *, cancelled: bool):
            project_id = self._get_task_project_id(nacked.id)
            key = self._task_key(nacked.id, project_id)
            db = self._read()
            tasks = db[self._task_collection]
            if cancelled:
                # Clean cancellation events
                db[self._cancel_event_collection].pop(
                    self._task_key(nacked.id, project_id)
                )
            update = {"status": nacked.status}
            if nacked.status is TaskStatus.QUEUED:
                update["progress"] = nacked.progress
                if not cancelled:
                    update["retries"] = nacked.retries
            if nacked.cancelled_at:
                update["cancelledAt"] = nacked.cancelled_at
            tasks[key].update(update)
            self._write(db)

        async def _consume(self) -> Task:
            return await self._consume_(
                self._task_collection,
                Task,
                select=lambda t: t.status is TaskStatus.QUEUED,
                order=lambda t: t.created_at,
            )

        async def _consume_cancelled(self) -> CancelledTaskEvent:
            return await self._consume_(
                self._cancel_event_collection,
                CancelledTaskEvent,
                order=lambda e: e.created_at,
            )

        async def _consume_(
            self,
            collection: str,
            consumed_cls: Type[R],
            select: Optional[Callable[[R], bool]] = None,
            order: Optional[Callable[[R], Any]] = None,
        ) -> R:
            while "i'm waiting until I find something interesting":
                db = self._read()
                selected = db[collection]
                selected = [(k, consumed_cls(**t)) for k, t in selected.items()]
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
