from .app import AsyncApp
from .objects import Task, TaskError, TaskEvent, TaskResult, TaskState, Message
from .task_manager import TaskManager
from .worker import Worker, WorkerConfig, WorkerType
from .namespacing import Namespacing

try:
    from icij_worker.worker.amqp import AMQPWorker, AMQPWorkerConfig
    from icij_worker.event_publisher.amqp import AMQPPublisher
    from icij_worker.task_manager.amqp import AMQPTaskManager
except ImportError:
    pass

try:
    from icij_worker.worker.neo4j_ import Neo4jWorker, Neo4jWorkerConfig
    from icij_worker.event_publisher.neo4j_ import Neo4jEventPublisher
    from icij_worker.task_manager.neo4j_ import Neo4JTaskManager
except ImportError:
    pass

try:
    from icij_worker.task_storage.fs import FSKeyValueStorage
except ImportError:
    pass

from .backend import WorkerBackend
from .event_publisher import EventPublisher
