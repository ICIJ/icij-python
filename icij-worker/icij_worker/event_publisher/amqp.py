from __future__ import annotations

import logging
from functools import cached_property
from typing import Dict, Generator, Mapping, Optional, Tuple, Type

from pika.adapters.base_connection import BaseConnection
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.channel import Channel
from pika.connection import URLParameters
from pika.delivery_mode import DeliveryMode
from pika.exceptions import StreamLostError
from pika.exchange_type import ExchangeType
from pika.spec import Basic, BasicProperties
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from icij_common.logging_utils import LogWithNameMixin
from icij_common.pydantic_utils import LowerCamelCaseModel, NoEnumModel
from icij_worker import TaskEvent
from ..worker.amqp import ConnectionLostError
from . import EventPublisher


# TODO: move these to a upper level
class Exchange(NoEnumModel, LowerCamelCaseModel):
    name: str
    type: ExchangeType


class Routing(LowerCamelCaseModel):
    exchange: Exchange
    routing_key: str
    default_queue: str


class AMQPEventPublisher(EventPublisher, LogWithNameMixin):
    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        *,
        broker_url: str,
        max_connection_attempts: int = 1,
        max_reconnection_wait_s: int = 1.0,
        app_id: Optional[str] = None,
        recover_from: Tuple[Type[Exception], ...] = tuple(),
    ):
        if logger is None:
            logger = logging.getLogger(__name__)
        LogWithNameMixin.__init__(self, logger)
        self._app_id = app_id
        self._broker_url = broker_url
        self._routing = self._event_routing()
        self._recover_from = recover_from
        self._connection_: Optional[BaseConnection] = None
        self._channel_: Optional[BlockingChannel] = None
        self._max_connection_attempts = max_connection_attempts
        self._max_reconnection_wait_s = max_reconnection_wait_s
        # We don't declare and bind anything here, the task manager is in charge of it.
        # We use this flag only for testing where we want to set everything up easily
        self._declare_and_bind = False

    def _event_routing(self) -> Routing:
        return Routing(
            exchange=Exchange(name="exchangeMainEvents", type=ExchangeType.fanout),
            routing_key="routingKeyMainEvents",
            default_queue="queueMainEvents",
        )

    def __enter__(self) -> AMQPEventPublisher:
        if self._max_connection_attempts > 1:
            reconnect_wrapper = self.reconnection_attempts(
                self._max_connection_attempts, self._max_reconnection_wait_s
            )
            for attempt in reconnect_wrapper:
                with attempt:
                    attempt = attempt.attempt_number - 1
                    self._attempt_connect(attempt)
        else:
            self._attempt_connect(0)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._connection_ is not None and self._connection.is_open:
            self.info("closing connection...")
            self._connection_.close()  # This will close the channel too
            self.info("connection closed !")

    async def publish_event(self, event: TaskEvent, project: str):
        self._publish_event(event, project, mandatory=False)

    def _publish_event(
        self,
        event: TaskEvent,
        project: str,  # pylint: disable=unused-argument
        *,
        delivery_mode: DeliveryMode = DeliveryMode.Persistent,
        mandatory: bool,
        properties: Optional[Dict] = None,
    ):
        # TODO: for now project information is not leverage on the AMQP side which is
        #  not very convenient as clients will won't know from which project the event
        #  is coming. This is limitating as for instance when it comes to logs errors,
        #  such events must be save in separate DBs in order to avoid project data
        #  leaking to other projects through the DB
        self.debug("publishing task event %s for task %s", event, event.task_id)
        message = event.json().encode()
        self._publish_message(
            message,
            exchange=self._routing.exchange.name,
            routing_key=self._routing.routing_key,
            properties=properties,
            delivery_mode=delivery_mode,
            mandatory=mandatory,
        )
        self.debug("event published fort task %s", event.task_id)

    def _publish_message(
        self,
        message: bytes,
        *,
        exchange: str,
        routing_key: Optional[str],
        delivery_mode: DeliveryMode,
        mandatory: bool,
        properties: Optional[Dict] = None,
    ):
        if properties is None:
            properties = dict()
        properties = BasicProperties(
            app_id=self._app_id, delivery_mode=delivery_mode, **properties
        )
        self._channel.basic_publish(
            exchange,
            routing_key,
            message,
            properties,
            mandatory=mandatory,
        )

    @property
    def _connection(self) -> BaseConnection:
        if self._connection_ is None:
            msg = (
                f"Publisher has no connection, please call"
                f" {AMQPEventPublisher.connect.__name__}"
            )
            raise ValueError(msg)
        return self._connection_

    @property
    def _channel(self) -> BlockingChannel:
        if self._channel_ is None:
            msg = (
                f"Publisher has no channel, please call"
                f" {AMQPEventPublisher.connect.__name__}"
            )
            raise ValueError(msg)
        return self._channel_

    @cached_property
    def _exception_namespace(self) -> Dict:
        ns = dict(globals())
        ns.update({exc_type.__name__: exc_type for exc_type in self._recover_from})
        return ns

    def _attempt_connect(self, attempt: int):
        if self._connection_ is None or not self._connection.is_open:
            if attempt == 0:
                self.info("creating new connection...")
            else:
                self.info("recreating closed connection attempt #%s...", attempt)
            self._connection_ = BlockingConnection(URLParameters(self._broker_url))
            self.info("connection (re)created !")
        self.info("reopening channel...")
        self._open_channel()
        self.info("channel opened !")

    def reconnection_attempts(
        self, max_attempt: int, max_wait_s: float = 3600.0
    ) -> Generator[RetryCallState, None]:
        for i, attempt in enumerate(self._reconnect_retry(max_attempt, max_wait_s)):
            if i:
                self._attempt_connect(i)
            yield attempt

    def confirm_delivery(self) -> AMQPEventPublisher:
        self.debug("turning on delivery confirmation")
        self._channel.confirm_delivery()
        return self

    def close(self):
        if self._connection_ is not None and self._connection.is_open:
            self._connection_.close()

    def _reconnect_retry(self, max_attempts: int, max_wait_s: float) -> Retrying:
        retry = Retrying(
            wait=wait_exponential(max=max_wait_s),
            stop=stop_after_attempt(max_attempts),
            reraise=True,
            retry=retry_if_exception(self._should_reconnect),
        )
        return retry

    def _open_channel(self):
        self.debug("opening a new channel")
        self._channel_ = self._connection.channel()
        self._channel.add_on_return_callback(self._on_return_callback)
        self._channel.basic_qos(prefetch_count=1, global_qos=True)
        if self._declare_and_bind:
            self._declare_exchanges()
            self._declare_queues()
            self._bind_queues()

    def _declare_exchanges(self):
        self.debug("(re)declaring exchanges...")
        self._channel_.exchange_declare(
            exchange=self._routing.exchange.name,
            exchange_type=self._routing.exchange.type,
            durable=True,
        )

    def _declare_queues(self):
        self.debug("(re)declaring queues...")
        self._channel.queue_declare(self._routing.default_queue, durable=True)

    def _bind_queues(self):
        self.debug("binding queues...")
        self._channel.queue_bind(
            queue=self._routing.default_queue,
            exchange=self._routing.exchange.name,
            routing_key=self._routing.routing_key,
        )

    def _on_return_callback(
        self,
        _: Channel,
        __: Basic.Return,  # pylint: disable=invalid-name
        properties: BasicProperties,
        body: bytes,
    ):
        self.error(
            "published message was rejected by the broker."
            "\nProperties: %r"
            "\nBody: %.1000s",
            properties,
            body,
            exc_info=True,
        )

    def _should_reconnect(self, exception: BaseException) -> bool:
        if isinstance(exception, StreamLostError):
            exception = parse_stream_lost_error(
                exception, namespace=self._exception_namespace
            )
        return isinstance(exception, self._recover_from)

    def _on_disconnect_callback(self, retry_state: RetryCallState):
        exception = retry_state.outcome.exception()
        exception = f"({exception.__class__.__name__} {exception})"
        self.error(
            "recoverable exception occurred, trying to reconnect, attempt %s: %s",
            retry_state.attempt_number,
            exception,
            exc_info=True,
        )
        self._attempt_connect(retry_state.attempt_number)


_EOF_MSG = "Transport indicated EOF"
_OTHER_ERROR_MSG = "Stream connection lost: "
_PIKA_VERSION_ERROR_MSG = "pika version is supposed to be fixed at 1.3.2, this is not \
the case any longer, error handling should be updated accordingly"


# Ugly but necessary for now, see https://groups.google.com/g/pika-python/c/G4rzLB7s5E0
def parse_stream_lost_error(
    error: StreamLostError, namespace: Mapping[str, Type[Exception]]
) -> Exception:
    error_msg: str = error.args[0]
    if error_msg.startswith(_EOF_MSG):
        return ConnectionLostError(error_msg)
    if error_msg.startswith(_OTHER_ERROR_MSG):
        # In general eval(repr(o)) will allow to retrieve o
        error_type = error_msg.lstrip(_OTHER_ERROR_MSG)
        if not error_type:
            return error
        try:
            return eval(error_type, namespace)  # pylint: disable=eval-used
        except (NameError, SyntaxError):
            return error
    raise ValueError(_PIKA_VERSION_ERROR_MSG)
