import logging
from typing import Optional, Tuple, Type

import aiohttp
import pytest
from pika.adapters.blocking_connection import BlockingConnection
from pika.connection import URLParameters
from pika.exceptions import (
    ConnectionOpenAborted,
    NackError,
    StreamLostError,
    UnroutableError,
)
from pika.exchange_type import ExchangeType

from icij_common.test_utils import TEST_PROJECT
from icij_worker import TaskEvent, TaskStatus
from icij_worker.event_publisher.amqp import (
    AMQPEventPublisher,
    Exchange,
    Routing,
    parse_stream_lost_error,
)
from icij_worker.exceptions import ConnectionLostError
from icij_worker.tests.conftest import (
    DEFAULT_VHOST,
    rabbit_mq_test_session,
    get_test_management_url,
)

_EVENT_ROUTING = Routing(
    exchange=Exchange(name="event-ex", type=ExchangeType.fanout),
    routing_key="event",
    default_queue="event-q",
)

_ERROR_ROUTING = Routing(
    exchange=Exchange(name="error-ex", type=ExchangeType.topic),
    routing_key="error",
    default_queue="error-q",
)

_RESULT_ROUTING = Routing(
    exchange=Exchange(name="result-ex", type=ExchangeType.topic),
    routing_key="result",
    default_queue="result-q",
)


class _TestableAMQPPublisher(AMQPEventPublisher):
    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        *,
        broker_url: str,
        app_id: Optional[str] = None,
        recover_from: Tuple[Type[Exception], ...] = tuple(),
    ):
        super().__init__(
            logger, broker_url=broker_url, app_id=app_id, recover_from=recover_from
        )
        # declare and bind the queues
        self._declare_and_bind = True

    @property
    def can_publish(self) -> bool:
        if self._connection_ is None or not self._connection.is_open:
            return False
        if self._channel_ is None or not self._channel.is_open:
            return False
        return True

    @property
    def event_queue(self) -> str:
        return self._routing.default_queue


@pytest.mark.asyncio
async def test_publish_event(rabbit_mq: str):
    # Given
    broker_url = rabbit_mq
    project = TEST_PROJECT
    publisher = _TestableAMQPPublisher(broker_url=broker_url)
    event = TaskEvent(
        task_id="task_id", task_type="hello_world", status=TaskStatus.CREATED
    )

    # When
    with publisher:
        await publisher.publish_event(event, project)

    # Then
    connection = BlockingConnection(URLParameters(broker_url))
    channel = connection.channel()
    _, _, body = channel.basic_get(publisher.event_queue, auto_ack=True)
    received_event = TaskEvent.parse_raw(body)
    assert received_event == event


@pytest.mark.parametrize(
    "error,n_disconnects",
    [
        (
            StreamLostError(f"Stream connection lost: {ConnectionError('error')!r}"),
            2,
        ),
        (UnroutableError([]), 3),
        (NackError([]), 4),
    ],
)
@pytest.mark.asyncio
async def test_publisher_should_reconnect_for_recoverable_connection_error(
    rabbit_mq: str, error: Exception, n_disconnects: int
):
    # Given
    broker_url = rabbit_mq
    recover_from = (ConnectionError, UnroutableError, NackError)
    publisher = _TestableAMQPPublisher(broker_url=broker_url, recover_from=recover_from)
    max_attempt = 10

    # When
    success_i = None
    with publisher:
        for i, attempt in enumerate(
            publisher.reconnection_attempts(max_attempt=max_attempt, max_wait_s=0.1)
        ):
            with attempt:
                if i < n_disconnects:
                    publisher.close()
                    raise error
            success_i = i

        # Then
        assert publisher.can_publish
        assert success_i == n_disconnects


def test_publisher_should_not_reconnect_from_non_recoverable_error(rabbit_mq: str):
    # Given
    broker_url = rabbit_mq
    publisher = _TestableAMQPPublisher(broker_url=broker_url)
    max_attempt = 10

    class _MyNonRecoverableError(Exception):
        pass

    # When/Then
    with publisher:
        with pytest.raises(_MyNonRecoverableError):
            for attempt in publisher.reconnection_attempts(
                max_attempt=max_attempt, max_wait_s=0.1
            ):
                with attempt:
                    raise _MyNonRecoverableError()


def test_publisher_should_not_reconnect_too_many_times(rabbit_mq: str):
    # Given
    broker_url = rabbit_mq
    publisher = _TestableAMQPPublisher(broker_url=broker_url)
    max_attempt = 2

    # When/Then
    with publisher:
        with pytest.raises(ConnectionOpenAborted):
            for attempt in publisher.reconnection_attempts(
                max_attempt=max_attempt, max_wait_s=0.1
            ):
                with attempt:
                    raise ConnectionOpenAborted()


async def _list_all_exchanges(session: aiohttp.ClientSession):
    url = f"/api/exchanges/{DEFAULT_VHOST}"
    async with session.get(get_test_management_url(url)) as res:
        exchanges = await res.json()
        exchanges = [
            ex for ex in exchanges if ex["user_who_performed_action"] == "guest"
        ]
    return exchanges


async def _list_all_user_exchanges(session: aiohttp.ClientSession):
    url = f"/api/exchanges/{DEFAULT_VHOST}"
    async with session.get(get_test_management_url(url)) as res:
        exchanges = await res.json()
        exchanges = [
            ex for ex in exchanges if ex["user_who_performed_action"] == "guest"
        ]
    return exchanges


async def _list_all_user_queues(session: aiohttp.ClientSession):
    url = f"/api/queues/{DEFAULT_VHOST}"
    async with session.get(get_test_management_url(url)) as res:
        queues = await res.json()
    return queues


async def test_publisher_not_create_and_bind_exchanges_and_queues(rabbit_mq: str):
    # Given
    broker_url = rabbit_mq
    publisher = AMQPEventPublisher(broker_url=broker_url)

    # When
    with publisher:
        async with rabbit_mq_test_session() as session:
            exchanges = await _list_all_exchanges(session)
            queues = await _list_all_user_queues(session)
    exchanges = [ex for ex in exchanges if ex["user_who_performed_action"] == "guest"]
    assert not exchanges
    assert not queues


class _NotParsable(Exception):
    def __repr__(self) -> str:
        return f"_NotParsable__({self.args})"


@pytest.mark.parametrize(
    "stream_lost_error,expected_error",
    [
        (
            StreamLostError("Transport indicated EOF"),
            ConnectionLostError("Transport indicated EOF"),
        ),
        (
            StreamLostError(f"Stream connection lost: {ValueError('wrapped error')!r}"),
            ValueError("wrapped error"),
        ),
        (
            StreamLostError(
                f"Stream connection lost: {_NotParsable('can''t be parsed')!r}"
            ),
            StreamLostError(
                f"Stream connection lost: {_NotParsable('can''t be parsed')!r}"
            ),
        ),
    ],
)
def test_parse_stream_lost_error(
    stream_lost_error: StreamLostError, expected_error: Exception
):
    # When
    error = parse_stream_lost_error(stream_lost_error, namespace=globals())
    # Then
    assert isinstance(error, type(expected_error))
    assert error.args == expected_error.args


def test_parse_stream_lost_error_should_raise_for_invalid_stream_lost_error():
    # Given
    stream_lost_error = StreamLostError("I'm invalid")
    # When/Then
    with pytest.raises(ValueError) as exc:
        parse_stream_lost_error(stream_lost_error, namespace=globals())
        assert exc.match("pika version is supposed to be fixed at 1.3.2")
