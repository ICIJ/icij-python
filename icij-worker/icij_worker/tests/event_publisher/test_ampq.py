import pytest
from aio_pika import ExchangeType, connect_robust
from aiormq import ChannelNotFoundEntity

from icij_worker import Task, ManagerEvent
from icij_worker.event_publisher.amqp import (
    AMQPPublisher,
)
from icij_worker.routing_strategy import Exchange, Routing
from icij_worker.objects import ProgressEvent
from icij_worker.tests.conftest import (
    TestableAMQPPublisher,
)

_EVENT_ROUTING = Routing(
    exchange=Exchange(name="event-ex", type=ExchangeType.FANOUT),
    routing_key="event",
    queue_name="event-q",
)

_ERROR_ROUTING = Routing(
    exchange=Exchange(name="error-ex", type=ExchangeType.TOPIC),
    routing_key="error",
    queue_name="error-q",
)

_RESULT_ROUTING = Routing(
    exchange=Exchange(name="result-ex", type=ExchangeType.TOPIC),
    routing_key="result",
    queue_name="result-q",
)


async def test_publish_event(rabbit_mq: str, hello_world_task: Task):
    # Given
    task = hello_world_task
    broker_url = rabbit_mq
    publisher = TestableAMQPPublisher(
        broker_url=broker_url, connection_timeout_s=2, reconnection_wait_s=1
    )
    from datetime import datetime

    event = ProgressEvent(task_id=task.id, progress=0.0, created_at=datetime.now())

    # When
    async with publisher:
        await publisher.publish_event(event)

    # Then
    connection = await connect_robust(url=broker_url)
    channel = await connection.channel()
    queue = await channel.get_queue(publisher.event_queue)
    async with queue.iterator(timeout=2.0) as messages:
        async for message in messages:
            received_event = ManagerEvent.parse_raw(message.body)
            break
    assert received_event == event


async def test_publisher_should_not_create_and_bind_exchanges_and_queues(
    rabbit_mq: str,
):
    # Given
    broker_url = rabbit_mq
    publisher = AMQPPublisher(broker_url=broker_url)
    publisher._declare_and_bind = False  # pylint: disable=protected-access

    # When
    msg = "NOT_FOUND - no exchange 'exchangeManagerEvents' in vhost '/'"
    with pytest.raises(ChannelNotFoundEntity, match=msg):
        async with publisher:
            pass
