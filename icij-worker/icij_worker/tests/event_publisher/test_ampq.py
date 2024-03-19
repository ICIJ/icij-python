import pytest
from aio_pika import ExchangeType, connect_robust
from aiormq import ChannelNotFoundEntity

from icij_common.test_utils import TEST_PROJECT
from icij_worker import TaskEvent, TaskStatus
from icij_worker.event_publisher.amqp import (
    AMQPPublisher,
    Exchange,
    Routing,
)
from icij_worker.tests.conftest import (
    TestableAMQPPublisher,
)

_EVENT_ROUTING = Routing(
    exchange=Exchange(name="event-ex", type=ExchangeType.FANOUT),
    routing_key="event",
    default_queue="event-q",
)

_ERROR_ROUTING = Routing(
    exchange=Exchange(name="error-ex", type=ExchangeType.TOPIC),
    routing_key="error",
    default_queue="error-q",
)

_RESULT_ROUTING = Routing(
    exchange=Exchange(name="result-ex", type=ExchangeType.TOPIC),
    routing_key="result",
    default_queue="result-q",
)


@pytest.mark.asyncio
async def test_publish_event(rabbit_mq: str):
    # Given
    broker_url = rabbit_mq
    project = TEST_PROJECT
    publisher = TestableAMQPPublisher(
        broker_url=broker_url, connection_timeout_s=2, reconnection_wait_s=1
    )
    event = TaskEvent(
        task_id="task_id", task_type="hello_world", status=TaskStatus.CREATED
    )

    # When
    async with publisher:
        await publisher.publish_event(event, project)

    # Then
    connection = await connect_robust(url=broker_url)
    channel = await connection.channel()
    queue = await channel.get_queue(publisher.event_queue)
    async with queue.iterator(timeout=2.0) as messages:
        async for message in messages:
            received_event = TaskEvent.parse_raw(message.body)
            break
    assert received_event == event


async def test_publisher_not_create_and_bind_exchanges_and_queues(rabbit_mq: str):
    # Given
    broker_url = rabbit_mq
    publisher = AMQPPublisher(broker_url=broker_url)

    # When
    msg = "NOT_FOUND - no exchange 'exchangeMainEvents' in vhost '/'"
    with pytest.raises(ChannelNotFoundEntity, match=msg):
        async with publisher:
            pass
