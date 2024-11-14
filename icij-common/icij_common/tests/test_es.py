import abc
import contextlib
import sys
from typing import Any, AsyncGenerator, Collection, Dict, List, Optional, Type
from unittest.mock import patch

import pytest
from elasticsearch import AsyncElasticsearch, TransportError
from tenacity import RetryCallState, Retrying

from icij_common.es import ESClient, HITS, PointInTime, SORT, retry_if_error_code
from icij_common.test_utils import fail_if_exception


class MockedESClient(ESClient):
    def __init__(
        self,
        n_hits: int,
        failure: Optional[Exception],
        fail_at: List[int] = None,
        **kwargs,
    ):
        super().__init__(pagination=1, **kwargs)
        self._n_calls: int = 0
        self._n_returned: int = 0
        if fail_at is None:
            fail_at = []
        self._fail_at = set(fail_at)
        self._failure = failure
        self._n_hits = n_hits

    async def search(self, **kwargs) -> Dict[str, Any]:
        # pylint: disable=arguments-differ
        return await self._mocked_search(**kwargs)

    async def scroll(self, **kwargs) -> Any:
        # pylint: disable=arguments-differ
        return await self._mocked_search(**kwargs)

    async def clear_scroll(self, **kwargs) -> Any:
        # pylint: disable=arguments-differ
        pass

    @contextlib.asynccontextmanager
    async def try_open_pit(
        self,
        *,
        index: str,
        keep_alive: str,
        **kwargs,
    ) -> AsyncGenerator[Optional[PointInTime], None]:
        # pylint: disable=unused-argument
        yield dict()

    @abc.abstractmethod
    async def _mocked_search(self, **kwargs):
        pass

    def _make_hits(self) -> List[Dict[str, Any]]:
        # pylint: disable=unused-argument
        if isinstance(self._failure, Exception) and self._n_calls in self._fail_at:
            self._n_calls += 1
            raise self._failure
        hits = [{SORT: None}] if self._n_returned < self._n_hits else []
        self._n_calls += 1
        self._n_returned += 1
        return hits


async def _mocked_search(
    *, body: Optional[Dict], index: Optional[str], size: int, **kwargs
):
    # pylint: disable=unused-argument
    return {}


async def test_es_client_should_search_with_pagination_size():
    # Given
    pagination = 666
    es_client = ESClient(pagination=pagination)
    index = "test-datashare-project"

    # When
    with patch.object(AsyncElasticsearch, "search") as mocked_search:
        mocked_search.side_effect = _mocked_search
        await es_client.search(body=None, index=index)
        # Then
        mocked_search.assert_called_once_with(
            headers={}, body=None, index=index, size=pagination
        )


class _MockFailingClient(MockedESClient, metaclass=abc.ABCMeta):
    def __init__(
        self,
        n_hits: int,
        failure: Optional[Exception],
        fail_at: List[int] = None,
        **kwargs,
    ):
        super().__init__(n_hits=n_hits, failure=failure, **kwargs)
        self._n_calls: int = 0
        self._n_returned: int = 0
        if fail_at is None:
            fail_at = []
        self._fail_at = set(fail_at)
        self._failure = failure
        self._n_hits = n_hits

    @property
    async def supports_pit(self) -> bool:
        return True

    async def _mocked_search(self, **kwargs) -> Dict[str, Any]:
        # pylint: disable=unused-argument
        hits = self._make_hits()
        return {HITS: {HITS: hits}}

    def _make_hits(self) -> List[Dict[str, Any]]:
        # pylint: disable=unused-argument
        if isinstance(self._failure, Exception) and self._n_calls in self._fail_at:
            self._n_calls += 1
            raise self._failure
        hits = [{SORT: None}] if self._n_returned < self._n_hits else []
        self._n_calls += 1
        self._n_returned += 1
        return hits


def _make_transport_error(error_code: int) -> TransportError:
    return TransportError(error_code, "", "")


@pytest.mark.parametrize(
    "fail_at,max_retries,failure,raised",
    [
        # No failure
        (None, 10, None, None),
        # 0 retries
        ([0], 0, _make_transport_error(429), TransportError),
        # Failure at the first call (which is different when polling/scrolling),
        # recovery
        ([0], 2, _make_transport_error(429), None),
        # Failure after the first call, recovery
        ([1], 2, _make_transport_error(429), None),
        # Recurring failure after the first call, exceeds retries
        (list(range(1, 100)), 2, _make_transport_error(429), TransportError),
        # Recurring failure after the first call, recovery
        ([1, 2], 10, _make_transport_error(429), None),
        # A non recoverable transport error
        ([1, 2], 10, _make_transport_error(400), TransportError),
    ],
)
async def test_poll_search_pages_should_retry(
    fail_at: Optional[int],
    max_retries: int,
    failure: Optional[Exception],
    raised: Optional[Type[Exception]],
):
    # Given
    n_hits = 3
    client = _MockFailingClient(
        max_retries=max_retries,
        max_retry_wait_s=int(1e-6),
        n_hits=n_hits,
        fail_at=fail_at,
        failure=failure,
    )

    # When/Then
    pages = client.poll_search_pages(index="", body={})

    if raised is not None:
        with pytest.raises(raised):
            _ = [h async for h in pages]
    else:
        with fail_if_exception(msg="Failed to retrieve search hits"):
            hits = [h async for h in pages]
        assert len(hits) == n_hits


@pytest.mark.parametrize(
    "error_codes,raised,expected_should_retry",
    [
        ([], TransportError(429), False),
        ([], ValueError(), False),
        ([429], TransportError(429), True),
        ([400, 429], TransportError(400), True),
        ([400, 429], TransportError(401), False),
    ],
)
def test_retry_if_error_code(
    error_codes: Collection[int],
    raised: Optional[Exception],
    expected_should_retry: bool,
):
    # Given
    retry = retry_if_error_code(error_codes)
    retry_state = RetryCallState(Retrying(), None, None, None)
    if raised:
        try:
            raise raised
        except:  # pylint: disable=bare-except
            retry_state.set_exception(sys.exc_info())

    # When
    should_retry = retry(retry_state)

    # Then
    assert should_retry == expected_should_retry
