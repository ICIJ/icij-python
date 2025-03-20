from __future__ import annotations

from functools import lru_cache
from typing import Callable

from icij_common.pydantic_utils import (
    icij_config,
    lowercamel_case_config,
    merge_configs,
    no_enum_values_config,
)
from pydantic import BaseModel

try:
    from aio_pika import ExchangeType

    class Exchange(BaseModel):
        model_config = merge_configs(
            icij_config(), no_enum_values_config(), lowercamel_case_config()
        )

        name: str
        type: ExchangeType

    class Routing(BaseModel):
        model_config = merge_configs(icij_config(), lowercamel_case_config())
        exchange: Exchange
        routing_key: str
        queue_name: str
        queue_args: dict | None = None
        dead_letter_routing: Routing | None = None

except ImportError:
    pass

POSTGRES_DEFAULT = "postgres"


class RoutingStrategy:
    """Override this class to implement your own routing strategy between task groups
    and DBs, amqp queues and so on..."""

    def app_tasks_filter(
        self, *, task_group: "TaskGroup" | None, app_group_name: str
    ) -> bool:
        """Used to filter app tasks so that the app can be started with a restricted
        group. Useful when tasks from the same app must be run by different workers
        """
        if task_group is None:
            return False
        return task_group.name.startswith(app_group_name)

    @classmethod
    @lru_cache
    def amqp_task_routing(cls, task_group: str | None) -> Routing:
        """Used to route task with the right AMQP routing key based on the group"""
        # Overriding this default might require overriding AMQPTaskManager/AMQPWorker
        # so that they communicate correctly
        from icij_worker.utils.amqp import AMQPMixin

        default_task_routing = AMQPMixin.default_task_routing()
        exchange_name = default_task_routing.exchange.name
        routing_key = default_task_routing.routing_key
        queue_name = default_task_routing.queue_name
        if task_group is not None:
            routing_key = task_group
            queue_name += f".{task_group}"

        return Routing(
            exchange=Exchange(name=exchange_name, type=ExchangeType.DIRECT),
            routing_key=routing_key,
            queue_name=queue_name,
            queue_args=default_task_routing.queue_args,
            # TODO: route DLQ by group ???
            dead_letter_routing=default_task_routing.dead_letter_routing,
        )

    @classmethod
    def db_filter_factory(cls, worker_group: str) -> Callable[[str], bool]:
        """Used during DB task polling to poll only from the DBs supported by the
        worker group.

        This factory should return a callable which will take the DB name and
         return whether the DB is supported for that worker group
        """
        # pylint: disable=unused-argument
        # By default, workers are allowed to listen to all DBs
        return lambda db_name: True

    @classmethod
    def neo4j_db(cls, group: str | None) -> str:
        # pylint: disable=unused-argument
        # By default, task from all groups are saved in the default neo4j db
        from icij_common.neo4j_.db import NEO4J_COMMUNITY_DB

        return NEO4J_COMMUNITY_DB

    @classmethod
    def postgres_db(cls, group: str | None) -> str:
        # pylint: disable=unused-argument
        return POSTGRES_DEFAULT

    @classmethod
    def test_db(cls, group: str | None) -> str:
        # pylint: disable=unused-argument
        from icij_common.test_utils import TEST_DB

        return TEST_DB
