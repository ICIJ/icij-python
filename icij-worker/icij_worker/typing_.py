from types import TracebackType
from typing import Any, Awaitable, Callable, Coroutine, Mapping, Protocol

from typing_extensions import AbstractSet

DependencyLabel = str | None
DependencySetup = Callable[..., None]
DependencyAsyncSetup = Callable[..., Coroutine[None, None, None]]

RateProgress = Callable[[float], Awaitable[None]]
# TODO: remove this when breaking API
PercentProgress = RateProgress
RawProgress = Callable[[int], Awaitable[None]]

DictStrAny = dict[str, Any]
IntStr = int | str
AbstractSetIntStr = AbstractSet[IntStr]
MappingIntStrAny = Mapping[IntStr, Any]


class DependencyTeardown(Protocol):
    def __call__(
        self,
        exc_type: type[Exception] | None,
        exc_value: Exception | None,
        traceback: TracebackType | None,
    ) -> None: ...


class DependencyAsyncTeardown(Protocol):
    async def __call__(
        self,
        exc_type: type[Exception] | None,
        exc_value: Exception | None,
        traceback: TracebackType | None,
    ) -> None: ...


Dependency = tuple[
    DependencyLabel,
    DependencySetup | DependencyAsyncSetup,
    DependencyTeardown | DependencyAsyncTeardown | None,
]
