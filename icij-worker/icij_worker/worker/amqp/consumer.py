# Mostly inspired by
# https://github.com/pika/pika/blob/main/examples/asyncio_consumer_example.py
import functools
import logging
from functools import cached_property
from typing import Dict, Optional, Protocol, Tuple, Type

import time
from pika.adapters.base_connection import BaseConnection
from pika.adapters.select_connection import SelectConnection
from pika.channel import Channel
from pika.connection import URLParameters
from pika.exceptions import (
    StreamLostError,
)
from pika.exchange_type import ExchangeType
from pika.frame import Method
from pika.spec import Basic, BasicProperties

from icij_common.logging_utils import LogWithNameMixin
from icij_worker.event_publisher.amqp import Routing, parse_stream_lost_error
from icij_worker.worker.amqp import MaxReconnectionExceeded

_IOLOOP_ALREADY_RUNNING_MSG = "is already running"


class OnMessage(Protocol):
    def __call__(
        self,
        basic_deliver: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ): ...


class OnMessageWithConsumer(Protocol):
    def __call__(
        self,
        consumer,
        basic_deliver: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ): ...


class _AMQPMessageConsumer(LogWithNameMixin):
    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        *,
        on_message: OnMessage,
        broker_url: str,
        routing: Routing,
        app_id: Optional[str] = None,
        recover_from: Tuple[Type[Exception], ...] = tuple(),
    ):
        if logger is None:
            logger = logging.getLogger(__name__)
        LogWithNameMixin.__init__(self, logger)

        self._on_message = on_message
        self._app_id = app_id
        self._broker_url = broker_url
        # TODO: routing must be improved here, as a worker should not listen for all
        #  tasks but only for a subset (task should be group by semantic / duration /
        #  resource consumption)
        self._routing = routing
        self._recover_from = recover_from

        self._connection_ = None
        self._channel_ = None
        self._reason = None

        self._should_reconnect = False
        self._closing = False
        self._consumer_tag = None
        self._consuming = False
        self._last_message_received_at = None
        # TODO: for now prefetch factor is disabled, for short tasks this might be
        #  helpful for a higher consumer throughput
        self._prefetch_count = 1
        # We don't declare and bind anything here, the task manager is in charge of it.
        # We use this flag only for testing where we want to set everything up easily
        self._declare_and_bind = False

    @property
    def should_reconnect(self) -> bool:
        return self._should_reconnect

    @property
    def reason(self) -> Optional[Exception]:
        return self._reason

    @property
    def last_message_received_at(self) -> Optional[float]:
        return self._last_message_received_at

    @property
    def consumer_tag(self) -> Optional[str]:
        return self._consumer_tag

    @property
    def _connection(self) -> SelectConnection:
        if self._connection_ is None:
            msg = (
                f"consumer has no connection, please call"
                f" {_AMQPMessageConsumer.connect.__name__}"
            )
            raise ValueError(msg)
        return self._connection_

    @property
    def _channel(self) -> Channel:
        if self._channel_ is None:
            msg = (
                f"consumer has no channel, please call"
                f" {_AMQPMessageConsumer.connect.__name__}"
            )
            raise ValueError(msg)
        return self._channel_

    @cached_property
    def _exception_namespace(self) -> Dict:
        ns = dict(globals())
        ns.update({exc_type.__name__: exc_type for exc_type in self._recover_from})
        return ns

    def connect(self):
        self.debug("connecting to %s", self._broker_url)
        # TODO: heartbeat ? blocked connection timeout ?
        self._connection_ = SelectConnection(
            parameters=URLParameters(self._broker_url),
            on_open_callback=self._on_connection_open,
            on_open_error_callback=self._on_connection_open_error,
            on_close_callback=self._on_connection_closed,
        )

    def on_message(
        self,
        _unused_channel: Channel,  # pylint: disable=invalid-name
        basic_deliver: Basic.Deliver,
        properties: BasicProperties,  # pylint: disable=invalid-name
        body: bytes,
    ):
        self._last_message_received_at = time.monotonic()
        msg = "received message # %s"
        if self._app_id is not None:
            msg = f"{msg} from {self._app_id}"
        self.debug(msg, basic_deliver.delivery_tag)
        self._on_message(basic_deliver=basic_deliver, properties=properties, body=body)
        self.acknowledge_message(basic_deliver.delivery_tag)

    def consume(self):
        self.connect()
        self._connection.ioloop.start()

    def reject_message(self, delivery_tag: int, requeue: bool):
        self._channel.basic_reject(delivery_tag=delivery_tag, requeue=requeue)

    def stop(self):
        if not self._closing:
            self._closing = True
            self.info("stopping...")
            if self._consuming:
                # The IOLoop is started again because this method is invoked  when
                # CTRL-C is pressed raising a KeyboardInterrupt exception. This
                # exception stops the IOLoop which needs to be running for pika to
                # communicate with RabbitMQ. All the commands issued prior to
                # starting the IOLoop will be buffered but not processed.
                self._stop_consuming()
                # Calling start will fail if the loop is already running
                try:
                    self._connection.ioloop.start()
                # not robust, but sadly pika does not provide a more catchable
                # exception type...
                except RuntimeError as e:
                    if _IOLOOP_ALREADY_RUNNING_MSG not in str(e):
                        raise e
            else:
                if self._connection_ is not None:
                    self._connection.ioloop.stop()
            self.info("stopped !")

    def _trigger_reconnect(self):
        self._should_reconnect = True

    def _stop_consuming(self):
        if self._channel:
            self.debug("sending a Basic.Cancel RPC command to RabbitMQ")
            self._channel.basic_cancel(self.consumer_tag, self._on_cancelok)

    def acknowledge_message(self, delivery_tag: int):
        self.debug("acknowledging message %s", delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def _close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            self.debug("connection is closing or already closed")
        else:
            self.debug("closing connection")
            self._connection.close()

    def _on_connection_open(self, _unused_connection: BaseConnection):
        # pylint: disable=invalid-name
        self.debug("connection opened !")
        self._open_channel()

    def _on_connection_open_error(
        self, _unused_connection: BaseConnection, err: Exception
    ):
        # pylint: disable=invalid-name
        self._parse_error(err)
        if isinstance(self._reason, StreamLostError):
            self.warning("failed to parse stream lost internal error: %s", err)
        self.error("connection open failed: %s", self._reason, exc_info=True)
        if isinstance(self._reason, self._recover_from):
            self.error("triggering reconnection !", exc_info=True)
            self._trigger_reconnect()
        self.stop()

    def _on_connection_closed(
        self, _unused_connection: BaseConnection, reason: Exception
    ):
        # pylint: disable=invalid-name
        self._parse_error(reason)
        self._channel_ = None
        if self._closing:  # The connection was closed on purpose
            self._connection.ioloop.stop()
        else:
            self.error(
                "connection was accidentally closed: %s",
                reason,
                exc_info=True,
            )
            if isinstance(self._reason, self._recover_from):
                self.error("triggering reconnection !", exc_info=True)
                self._trigger_reconnect()
            self.stop()

    def _open_channel(self):
        self.debug("creating a new channel")
        self._connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel: Channel):
        self.debug("channel opened !")
        self._channel_ = channel
        self._add_on_channel_close_callback()
        if self._declare_and_bind:
            self._setup_exchange()

    def _add_on_channel_close_callback(self):
        self._channel.add_on_close_callback(self._on_channel_closed)

    def _on_channel_closed(self, channel: Channel, reason: Exception):
        self.warning("channel %s was closed: %s", channel.channel_number, reason)
        self._close_connection()

    def _setup_exchange(self):
        self.debug("declaring exchange: %s", self._routing.exchange)
        if self._routing.exchange.type is not ExchangeType.topic:
            raise ValueError(f"task exchange must be {ExchangeType.topic}")
        self._channel.exchange_declare(
            exchange=self._routing.exchange.name,
            exchange_type=self._routing.exchange.type,
            callback=self._on_exchange_declareok,
            durable=True,
        )

    def _on_exchange_declareok(self, _unused_frame: Method):
        # pylint: disable=invalid-name
        self.debug("exchange %s declared", self._routing.exchange.name)
        self._setup_queue()

    def _setup_queue(self):
        self.debug("declaring queue %s", self._routing.default_queue)
        self._channel.queue_declare(
            queue=self._routing.default_queue,
            callback=self._on_queue_declareok,
            durable=True,
        )

    def _on_queue_declareok(self, _unused_frame: Method):
        # pylint: disable=invalid-name
        self.info(
            "binding %s to %s with %s",
            self._routing.exchange.name,
            self._routing.default_queue,
            self._routing.routing_key,
        )
        self._channel.queue_bind(
            self._routing.default_queue,
            self._routing.exchange.name,
            routing_key=self._routing.routing_key,
            callback=self._on_bindok,
        )

    def _on_bindok(self, _unused_frame: Method):
        # pylint: disable=invalid-name
        self.debug("queue %s bound", self._routing.default_queue)
        self._set_qos()

    def _set_qos(self):
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self._on_basic_qos_ok
        )

    def _on_basic_qos_ok(self, _unused_frame: Method):
        # pylint: disable=invalid-name
        self.debug("QOS set to: %d", self._prefetch_count)
        self._start_consuming()

    def _start_consuming(self):
        self.info("starting consuming...")
        self._add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self._routing.default_queue,
            self.on_message,
            consumer_tag=self.consumer_tag,
        )
        self._consuming = True

    def _add_on_cancel_callback(self):
        self.debug("adding consumer cancellation callback")
        self._channel.add_on_cancel_callback(self._on_consumer_cancelled)

    def _on_consumer_cancelled(self, method_frame: Method):
        # pylint: disable=invalid-name
        self.info(
            logging.ERROR,
            "consumer was cancelled remotely, shutting down: %r",
            method_frame,
        )
        if self._channel:
            self._channel.close()

    def _on_cancelok(self, _unused_frame: Method):
        # pylint: disable=invalid-name
        self._consuming = False
        self.info(
            "RabbitMQ acknowledged the cancellation of the consumer",
        )
        self._close_channel()
        self.info("exiting consumer execution after cancellation")

    def _close_channel(self):
        self.debug("closing the channel")
        self._channel.close()

    def _parse_error(self, error: Exception):
        if isinstance(error, StreamLostError):
            self._reason = parse_stream_lost_error(
                error, namespace=self._exception_namespace
            )
            if isinstance(self._reason, StreamLostError):
                self.warning(
                    "failed to parse internal stream lost error %s",
                    error,
                )
        else:
            self._reason = error


class AMQPMessageConsumer(LogWithNameMixin):

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        *,
        on_message: OnMessageWithConsumer,
        broker_url: str,
        routing: Routing,
        app_id: Optional[str] = None,
        recover_from: Tuple[Type[Exception], ...] = tuple(),
        max_connection_wait_s: float = 60.0,
        max_connection_attempts: int = 5,
        inactive_after_s: float = 60 * 60,
    ):
        if logger is None:
            logger = logging.getLogger(__name__)
        super().__init__(logger)
        self._on_message = functools.partial(on_message, consumer=self)
        self._broker_url = broker_url
        self._routing = routing
        self._app_id = app_id
        self._recover_from = recover_from

        self._consumer = self._create_consumer(logger)

        self._connection_attempt = 0
        self._max_wait_s = max_connection_wait_s
        self._max_attempts = max_connection_attempts
        # Before inactive_after_s second of activity the consumer is considered active,
        # and we'll try to reconnect immediately. After that delay, we'll try to
        # reconnect at most max_connection_attempts
        self._inactive_after_s = inactive_after_s

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def consumer_tag(self) -> Optional[str]:
        return self._consumer.consumer_tag

    @property
    def _is_active(self) -> bool:
        if self._consumer.last_message_received_at is not None:
            elapsed = time.monotonic() - self._consumer.last_message_received_at
            return elapsed < self._inactive_after_s
        return False

    def consume(self):
        try:
            while True:
                self._consumer.consume()
                # ioloop.stop() has been called, let's see if we should reconnect
                self._try_reconnect()
        except Exception as e:
            self.error("error occurred while consuming: %s", e, exc_info=True)
            self.info("will try to shutdown gracefully...")
            raise e
        finally:
            self.close()

    def acknowledge_message(self, delivery_tag: int):
        self._consumer.acknowledge_message(delivery_tag=delivery_tag)

    def reject_message(self, delivery_tag: int, requeue: bool):
        self._consumer.reject_message(delivery_tag=delivery_tag, requeue=requeue)

    def close(self):
        self.info("closing the consumer gracefully...")
        self._consumer.stop()

    def _create_consumer(
        self, logger: Optional[logging.Logger]
    ) -> _AMQPMessageConsumer:
        return _AMQPMessageConsumer(
            logger,
            on_message=self._on_message,
            broker_url=self._broker_url,
            routing=self._routing,
            app_id=self._app_id,
            recover_from=self._recover_from,
        )

    def _try_reconnect(self):
        # if the consumer was already consuming, many AMQP steps occurred, the
        # disconnect occurred after a while, we can reset the connection attempt. If
        # the consumer didn't to this point we don't want to retry connecting forever,
        # we hence increment the counter
        if self._is_active:
            self._connection_attempt = 0
        reason = self._consumer.reason
        if self._consumer.should_reconnect:
            self._connection_attempt += 1
            if self._connection_attempt > self._max_attempts:
                msg = f"consumer exceeded {self._max_attempts} reconnections"
                try:
                    raise MaxReconnectionExceeded(msg)
                except MaxReconnectionExceeded as max_retry_exc:
                    raise reason from max_retry_exc
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            self.info(
                "reconnection attempt %i, reconnecting after %ds",
                self._connection_attempt,
                reconnect_delay,
            )
            time.sleep(reconnect_delay)
            self._consumer = self._create_consumer(self._logger)
        else:
            self.error(
                "consumer encountered non recoverable error %s", reason, exc_info=True
            )
            raise reason

    def _get_reconnect_delay(self) -> float:
        try:
            exp = 2 ** (self._connection_attempt - 1)
            result = 1 * exp
        except OverflowError:
            return self._max_wait_s
        return max(0, min(result, self._max_wait_s))
