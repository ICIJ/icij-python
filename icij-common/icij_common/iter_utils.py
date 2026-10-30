import asyncio
import inspect
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Generator,
    Iterable,
)
from itertools import islice
from typing import TypeVar

T = TypeVar("T")

Predicate = Callable[[T], bool] | Callable[[T], Awaitable[bool]]


async def async_batches[I](
    iterable: AsyncIterable[I], batch_size: int
) -> AsyncIterator[tuple[I]]:
    it = aiter(iterable)
    if batch_size < 1:
        raise ValueError("n must be at least one")
    while True:
        batch = []
        while len(batch) < batch_size:
            try:
                batch.append(await anext(it))
            except StopAsyncIteration:
                if batch:
                    yield tuple(batch)
                return
        yield tuple(batch)


def batches[I](
    iterable: Iterable[I], batch_size: int
) -> Generator[tuple[I, ...], None, None]:
    if batch_size < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(islice(it, batch_size)):
        yield batch


async def _maybe_await[I](maybe_awaitable: Awaitable[I] | I) -> I:
    if inspect.isawaitable(maybe_awaitable):
        return await maybe_awaitable
    return maybe_awaitable


async def once[I](item: I) -> AsyncIterator[I]:
    yield item


def before_and_after[I](
    iterable: AsyncIterable[I], predicate: Predicate[I]
) -> tuple[AsyncIterable[I], AsyncIterable[I]]:
    transition = asyncio.get_event_loop().create_future()

    async def true_iterator() -> AsyncIterator[T]:
        async for elem in iterable:
            if await _maybe_await(predicate(elem)):
                yield elem
            else:
                transition.set_result(elem)
                return
        transition.set_exception(StopAsyncIteration)

    async def remainder_iterator() -> AsyncIterator[T]:
        try:
            yield await transition
        except StopAsyncIteration:
            return
        async for elm in iterable:
            yield elm

    return true_iterator(), remainder_iterator()
