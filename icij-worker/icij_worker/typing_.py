from types import TracebackType
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    Mapping,
    Optional,
    Protocol,
    Tuple,
    Type,
    Union,
)

from typing_extensions import AbstractSet

DependencyLabel = Optional[str]
DependencySetup = Callable[..., None]
DependencyAsyncSetup = Callable[..., Coroutine[None, None, None]]

PercentProgress = Callable[[float], Awaitable[None]]
RawProgress = Callable[[int], Awaitable[None]]

DictStrAny = Dict[str, Any]
IntStr = Union[int, str]
AbstractSetIntStr = AbstractSet[IntStr]
MappingIntStrAny = Mapping[IntStr, Any]


class DependencyTeardown(Protocol):
    def __call__(
        self,
        exc_type: Optional[Type[Exception]],
        exc_value: Optional[Exception],
        traceback: Optional[TracebackType],
    ) -> None: ...


class DependencyAsyncTeardown(Protocol):
    async def __call__(
        self,
        exc_type: Optional[Type[Exception]],
        exc_value: Optional[Exception],
        traceback: Optional[TracebackType],
    ) -> None: ...


Dependency = Tuple[
    DependencyLabel,
    Union[DependencySetup, DependencyAsyncSetup],
    Optional[Union[DependencyTeardown, DependencyAsyncTeardown]],
]
