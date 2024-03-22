from enum import Enum, unique

from .config import WorkerConfig
from .worker import Worker

try:
    from .neo4j import Neo4jWorker, Neo4jWorkerConfig, Neo4jEventPublisher
except ImportError:
    pass

try:
    from .amqp import AMQPWorker, AMQPWorkerConfig
except ImportError:
    pass


@unique
class WorkerType(str, Enum):
    # pylint: disable=invalid-name
    mock = "mock"
    neo4j = "neo4j"
    amqp = "amqp"
