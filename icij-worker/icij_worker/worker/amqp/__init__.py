# pylint: disable=multiple-statements
from icij_worker.exceptions import ICIJWorkerError


class MaxReconnectionExceeded(ICIJWorkerError, RuntimeError): ...


class ConnectionLostError(ICIJWorkerError, RuntimeError): ...
