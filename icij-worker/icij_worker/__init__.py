from .app import AsyncApp, TaskGroup
from .objects import (
    Task,
    TaskError,
    ManagerEvent,
    ResultEvent,
    TaskState,
    Message,
    AsyncBackend,
)
from .task_manager import TaskManager
from .worker import Worker, WorkerConfig
from .routing_strategy import RoutingStrategy

try:
    from icij_worker.worker.amqp import AMQPWorker, AMQPWorkerConfig
    from icij_worker.event_publisher.amqp import AMQPPublisher
    from icij_worker.task_manager.amqp import AMQPTaskManager, AMQPTaskManagerConfig
except ImportError:
    pass

try:
    from icij_worker.worker.neo4j_ import Neo4jWorker, Neo4jWorkerConfig
    from icij_worker.event_publisher.neo4j_ import Neo4jEventPublisher
    from icij_worker.task_manager.neo4j_ import Neo4JTaskManager, Neo4JTaskManagerConfig
except ImportError:
    pass

try:
    from icij_worker.task_storage.fs import FSKeyValueStorage, FSKeyValueStorageConfig
except ImportError:
    pass

try:
    from icij_worker.task_storage.postgres import (
        PostgresStorage,
        PostgresStorageConfig,
        PostgresConnectionInfo,
        init_database as init_postgres_database,
    )
except ImportError:
    pass

from .backend import WorkerBackend
from .event_publisher import EventPublisher

# APP hook mean to be overridden with plugins
APP_HOOK = AsyncApp(name="app_hook")
