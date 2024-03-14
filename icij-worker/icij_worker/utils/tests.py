# pylint: disable=redefined-outer-name
# Test utils meant to be imported from clients libs to test their implem of workers
from __future__ import annotations

import asyncio
import json
import logging
import tempfile
from abc import ABC
from datetime import datetime
from pathlib import Path
from typing import ClassVar, Dict, List, Optional, Tuple, Union

from icij_common.pydantic_utils import (
    ICIJModel,
    IgnoreExtraModel,
    jsonable_encoder,
    safe_copy,
)
from pydantic import Field

import icij_worker
from icij_worker import (
    AsyncApp,
    EventPublisher,
    Task,
    TaskError,
    TaskEvent,
    TaskResult,
    TaskStatus,
    Worker,
    WorkerConfig,
    WorkerType,
)
from icij_worker.exceptions import TaskAlreadyExists, TaskQueueIsFull, UnknownTask
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

        def __init__(self, db_path: Path):
            self._db_path = db_path

        @property
        def db_path(self) -> Path:
            return self._db_path

        def _write(self, data: Dict):
            self._db_path.write_text(json.dumps(jsonable_encoder(data)))

        def _read(self):
            return json.loads(self._db_path.read_text())

        @staticmethod
        def _task_key(task_id: str, project: str) -> str:
            return str((task_id, project))

        @classmethod
        def fresh_db(cls, db_path: Path):
            db = {
                cls._task_collection: dict(),
                cls._error_collection: {},
                cls._result_collection: {},
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

        async def _enqueue(self, task: Task, project: str) -> Task:
            key = self._task_key(task_id=task.id, project=project)
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

        async def _cancel(self, *, task_id: str, project: str) -> Task:
            key = self._task_key(task_id=task_id, project=project)
            task_id = await self.get_task(task_id=task_id, project=project)
            update = {"status": TaskStatus.CANCELLED}
            task_id = safe_copy(task_id, update=update)
            db = self._read()
            db[self._task_collection][key] = task_id.dict()
            self._write(db)
            return task_id

        async def get_task(self, *, task_id: str, project: str) -> Task:
            key = self._task_key(task_id=task_id, project=project)
            db = self._read()
            try:
                tasks = db[self._task_collection]
                return Task(**tasks[key])
            except KeyError as e:
                raise UnknownTask(task_id) from e

        async def get_task_errors(self, task_id: str, project: str) -> List[TaskError]:
            key = self._task_key(task_id=task_id, project=project)
            db = self._read()
            errors = db[self._error_collection]
            errors = errors.get(key, [])
            errors = [TaskError(**err) for err in errors]
            return errors

        async def get_task_result(self, task_id: str, project: str) -> TaskResult:
            key = self._task_key(task_id=task_id, project=project)
            db = self._read()
            results = db[self._result_collection]
            try:
                return TaskResult(**results[key])
            except KeyError as e:
                raise UnknownTask(task_id) from e

        async def get_tasks(
            self,
            project: str,
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

    class MockEventPublisher(DBMixin, EventPublisher):
        _excluded_from_event_update = {"error"}

        def __init__(self, db_path: Path):
            super().__init__(db_path)
            self.published_events = []

        async def publish_event(self, event: TaskEvent, project: str):
            self.published_events.append(event)
            # Let's simulate that we have an event handler which will reflect some event
            # into the DB, we could not do it. In this case tests should not expect that
            # events are reflected in the DB. They would only be registered inside
            # published_events (which could be enough).
            # Here we choose to reflect the change in the DB since its closer to what
            # will happen IRL and test integration further
            key = self._task_key(task_id=event.task_id, project=project)
            db = self._read()
            try:
                task = self._get_db_task(db, task_id=event.task_id, project=project)
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
        log_level: str = "DEBUG"
        loggers: List[str] = [icij_worker.__name__]
        db_path: Path

    @Worker.register(WorkerType.mock)
    class MockWorker(Worker, MockEventPublisher):
        def __init__(
            self,
            app: AsyncApp,
            worker_id: str,
            db_path: Path,
            **kwargs,
        ):
            super().__init__(app, worker_id, **kwargs)
            MockEventPublisher.__init__(self, db_path)
            self._worker_id = worker_id
            self._logger_ = logging.getLogger(__name__)

        @classmethod
        def _from_config(cls, config: MockWorkerConfig, **extras) -> MockWorker:
            worker = cls(db_path=config.db_path, **extras)
            return worker

        def _to_config(self) -> MockWorkerConfig:
            return MockWorkerConfig(db_path=self._db_path)

        async def _save_result(self, result: TaskResult, project: str):
            task_key = self._task_key(task_id=result.task_id, project=project)
            db = self._read()
            db[self._result_collection][task_key] = result
            self._write(db)

        async def _save_error(self, error: TaskError, task: Task, project: str):
            task_key = self._task_key(task_id=task.id, project=project)
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

        async def _acknowledge(self, task: Task, project: str, completed_at: datetime):
            key = self._task_key(task.id, project)
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

        async def _negatively_acknowledge(
            self, task: Task, project: str, *, requeue: bool
        ) -> Task:
            key = self._task_key(task.id, project)
            db = self._read()
            tasks = db[self._task_collection]
            try:
                task = tasks[key]
            except KeyError as e:
                raise UnknownTask(task_id=task.id) from e
            task = Task(**task)
            if requeue:
                update = {
                    "status": TaskStatus.QUEUED,
                    "progress": 0.0,
                    "retries": task.retries or 0 + 1,
                }
            else:
                update = {"status": TaskStatus.ERROR}
            task = safe_copy(task, update=update)
            tasks[key] = task
            self._write(db)
            return task

        async def _refresh_cancelled(self, project: str):
            db = self._read()
            tasks = db[self._task_collection]
            tasks = [Task(**t) for t in tasks.values()]
            cancelled = [t.id for t in tasks if t.status is TaskStatus.CANCELLED]
            self._cancelled_[project] = set(cancelled)

        async def _consume(self) -> Tuple[Task, str]:
            while "waiting for some task to be available for some project":
                db = self._read()
                tasks = db[self._task_collection]
                tasks = [(k, Task(**t)) for k, t in tasks.items()]
                queued = [(k, t) for k, t in tasks if t.status is TaskStatus.QUEUED]
                if queued:
                    k, t = min(queued, key=lambda x: x[1].created_at)
                    project = eval(k)[1]  # pylint: disable=eval-used
                    return t, project
                await asyncio.sleep(self.config.task_queue_poll_interval_s)
