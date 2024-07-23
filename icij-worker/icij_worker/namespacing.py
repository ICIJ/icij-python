from __future__ import annotations

from functools import lru_cache
from typing import Callable, Optional

from icij_common.pydantic_utils import LowerCamelCaseModel, NoEnumModel
from icij_common.test_utils import TEST_DB

try:
    from aio_pika import ExchangeType

    class Exchange(NoEnumModel, LowerCamelCaseModel):
        name: str
        type: ExchangeType

    class Routing(LowerCamelCaseModel):
        exchange: Exchange
        routing_key: str
        queue_name: str
        dead_letter_routing: Optional[Routing] = None

except ImportError:
    pass


class Namespacing:
    """Override this class to implement your own mapping between task namespace and
    DBs/amqp queues and so on..."""

    def app_tasks_filter(self, *, task_key: str, app_namespace: str) -> bool:
        """Used to filter app task so that the app can be started with a restricted
        namespace. Useful when tasks from the same app must be run by different workers
        """
        return task_key.startswith(app_namespace)

    @staticmethod
    @lru_cache
    def amqp_task_routing(task_namespace: Optional[str]) -> Routing:
        """Used to route task with the right AMQP routing key based on the namespace"""
        # Overriding this default might require overriding AMQPTaskManager/AMQPWorker
        # so that they communicate correctly
        from icij_worker.utils.amqp import AMQPMixin

        default_task_routing = AMQPMixin.default_task_routing()
        exchange_name = default_task_routing.exchange.name
        routing_key = default_task_routing.routing_key
        queue_name = default_task_routing.queue_name
        if task_namespace is not None:
            routing_key += f".{task_namespace}"
            queue_name += f".{task_namespace}"

        return Routing(
            exchange=Exchange(name=exchange_name, type=ExchangeType.DIRECT),
            routing_key=routing_key,
            queue_name=queue_name,
            # TODO: route DLQ by namespace ???
            dead_letter_routing=default_task_routing.dead_letter_routing,
        )

    @staticmethod
    def db_filter_factory(worker_namespace: str) -> Callable[[str], bool]:
        """Used during DB task polling to poll only from the DBs supported by the
        worker namespace.

        This factory should return a callable which will take the DB name and
         return whether the DB is supported for that worker namespace
        """
        # pylint: disable=unused-argument
        # By default, workers are allowed to listen to all DBs
        return lambda db_name: True

    @staticmethod
    def neo4j_db(namespace: str) -> str:
        # pylint: disable=unused-argument
        # By default, task from all namespaces are saved in the default neo4j db
        from icij_common.neo4j.db import NEO4J_COMMUNITY_DB

        return NEO4J_COMMUNITY_DB

    @staticmethod
    def test_db(namespace: str) -> str:
        # pylint: disable=unused-argument
        return TEST_DB
