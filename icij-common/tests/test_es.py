import abc
import contextlib
import sys
from collections.abc import AsyncGenerator, Collection
from typing import Any
from unittest.mock import patch

import pytest
from aiohttp.web_exceptions import (
    HTTPBadRequest,
    HTTPRequestTimeout,
    HTTPServiceUnavailable,
    HTTPTooManyRequests,
    HTTPUnauthorized,
)
from elasticsearch import AsyncElasticsearch, NotFoundError, TransportError
from elasticsearch.helpers import async_streaming_bulk
from tenacity import RetryCallState, Retrying
from typing_extensions import Generator

from icij_common.es import (
    DOC_CONTENT,
    DOC_ROOT_ID,
    HITS,
    ID,
    ID_,
    QUERY,
    SORT,
    ESClient,
    PointInTime,
    match_all,
    retry_if_error_code,
)
from icij_common.test_utils import fail_if_exception

_INDEX_BODY = {
    "mappings": {
        "properties": {
            "id": {"type": "keyword"},
            "type": {"type": "keyword"},
            "language": {"type": "keyword"},
            "documentId": {"type": "keyword"},
            "rootDocument": {"type": "keyword"},
            "join": {"type": "join", "relations": {"Document": "NamedEntity"}},
            "contentType": {"type": "keyword"},
            "content": {"type": "text"},
        }
    }
}

TEST_PROJECT = "test-project"


@pytest.fixture(scope="session")
async def test_es_client_session() -> ESClient:
    es = ESClient(hosts=["http://localhost:9200"], pagination=2)
    with contextlib.suppress(NotFoundError):
        await es.indices.delete(index=TEST_PROJECT)
    await es.indices.create(index=TEST_PROJECT, body=_INDEX_BODY)
    return es


@pytest.fixture
async def test_es_client(test_es_client_session: ESClient) -> ESClient:
    es = test_es_client_session
    with contextlib.suppress(NotFoundError):
        await es.indices.delete(index=TEST_PROJECT)
    await es.indices.create(index=TEST_PROJECT, body=_INDEX_BODY)
    return es


def index_docs_ops(docs: list[dict], index_name: str) -> Generator[dict, None, None]:
    for doc in docs:
        op = {"_op_type": "index", "_index": index_name}
        op.update(doc)
        if "path" in op:
            op["path"] = str(op["path"])
        op["_id"] = doc[ID]
        op["routing"] = doc[DOC_ROOT_ID]
        op["type"] = "Document"
        yield op


async def index_docs(
    client: ESClient, *, docs: list[dict], index_name: str = TEST_PROJECT
) -> AsyncGenerator[dict, None]:
    ops = index_docs_ops(docs, index_name)
    # Let's wait to make this operation visible to the search
    refresh = "wait_for"
    async for res in async_streaming_bulk(client, actions=ops, refresh=refresh):
        yield res


@pytest.fixture(scope="session")
async def indexed_docs() -> list[dict]:
    docs = [
        {ID: f"doc-{i}", DOC_ROOT_ID: f"root-{i}", DOC_CONTENT: f"content-{i}"}
        for i in range(4)
    ]
    return docs


@pytest.fixture
async def populate_es(test_es_client: ESClient, indexed_docs: list[dict]) -> list[dict]:
    async for _ in index_docs(
        test_es_client, docs=indexed_docs, index_name=TEST_PROJECT
    ):
        pass
    return indexed_docs


class MockedESClient(ESClient):
    def __init__(
        self,
        n_hits: int,
        failure: Exception | None,
        fail_at: list[int] | None = None,
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

    async def search(self, **kwargs) -> dict[str, Any]:
        return await self._mocked_search(**kwargs)

    async def scroll(self, **kwargs) -> Any:
        return await self._mocked_search(**kwargs)

    async def clear_scroll(self, **kwargs) -> Any: ...

    @contextlib.asynccontextmanager
    async def try_open_pit(
        self,
        *,
        index: str,  # noqa: ARG002
        keep_alive: str,  # noqa: ARG002
        **kwargs,  # noqa: ARG002
    ) -> AsyncGenerator[PointInTime | None, None]:
        yield dict()

    @abc.abstractmethod
    async def _mocked_search(self, **kwargs) -> dict[str, Any]: ...

    def _make_hits(self) -> list[dict[str, Any]]:
        if isinstance(self._failure, Exception) and self._n_calls in self._fail_at:
            self._n_calls += 1
            raise self._failure
        hits = [{SORT: None}] if self._n_returned < self._n_hits else []
        self._n_calls += 1
        self._n_returned += 1
        return hits


async def _mocked_search(
    body: dict | None,  # noqa: ARG001
    index: str | None,  # noqa: ARG001
    size: int,  # noqa: ARG001
    **kwargs,  # noqa: ARG001
) -> dict[str, Any]:
    return {}


async def test_es_client_should_search_with_pagination_size() -> None:
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
        failure: Exception | None,
        fail_at: list[int] | None = None,
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

    async def _mocked_search(self, **kwargs) -> dict[str, Any]:  # noqa: ARG002
        hits = self._make_hits()
        return {HITS: {HITS: hits}}

    def _make_hits(self) -> list[dict[str, Any]]:
        if isinstance(self._failure, Exception) and self._n_calls in self._fail_at:
            self._n_calls += 1
            raise self._failure
        hits = [{SORT: None}] if self._n_returned < self._n_hits else []
        self._n_calls += 1
        self._n_returned += 1
        return hits


def _make_transport_error(error_code: int) -> TransportError:
    try:
        return TransportError(error_code, "", "")
    except TypeError:
        return TransportError(error_code, (HTTPRequestTimeout(),))


@pytest.mark.parametrize(
    ("fail_at", "max_retries", "failure", "raised"),
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
    fail_at: int | None,
    max_retries: int,
    failure: Exception | None,
    raised: type[Exception] | None,
) -> None:
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
    ("error_codes", "raised", "expected_should_retry"),
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
    raised: Exception | None,
    *,
    expected_should_retry: bool,
) -> None:
    # Given
    retry = retry_if_error_code(error_codes)
    retry_state = RetryCallState(Retrying(), None, None, None)
    if raised:
        try:
            raise raised
        except:  # noqa: E722
            retry_state.set_exception(sys.exc_info())

    # When
    should_retry = retry(retry_state)

    # Then
    assert should_retry == expected_should_retry


@pytest.mark.es8
@pytest.mark.parametrize(
    ("error_codes", "raised", "expected_should_retry"),
    [
        ([], TransportError("service unavailable", (HTTPServiceUnavailable(),)), False),
        ([], ValueError(), False),
        ([429], TransportError("too many request", (HTTPTooManyRequests(),)), True),
        (
            [400, 429],
            TransportError("many errors", (HTTPBadRequest(), HTTPTooManyRequests())),
            True,
        ),
        ([400, 429], TransportError("unauthorize", (HTTPUnauthorized(),)), False),
    ],
)
def test_retry_if_error_code_es8(
    error_codes: Collection[int],
    raised: Exception | None,
    *,
    expected_should_retry: bool,
) -> None:
    # Given
    retry = retry_if_error_code(error_codes)
    retry_state = RetryCallState(Retrying(), None, None, None)
    if raised:
        try:
            raise raised
        except:  # noqa: E722
            retry_state.set_exception(sys.exc_info())

    # When
    should_retry = retry(retry_state)

    # Then
    assert should_retry == expected_should_retry


async def _test_search(
    client: ESClient, query: dict, expected_ids: set[str], **kwargs
) -> None:
    query = {QUERY: query}
    retrieved = set()
    async for page in client.poll_search_pages(query, index=TEST_PROJECT, **kwargs):
        for doc in page[HITS][HITS]:
            retrieved.add(doc[ID_])
    assert retrieved == expected_ids


async def _test_poll_with_pit(client: ESClient) -> None:
    # Given
    query = {QUERY: match_all()}
    count = 0
    # When
    async with client.pit(index=TEST_PROJECT) as pit:
        async for page in client.poll_search_pages(query, pit=pit):
            count += len(page[HITS][HITS])
    # Then
    assert count == 4


@pytest.mark.usefixtures("populate_es")
async def test_poll_with_pit(test_es_client: ESClient) -> None:
    await _test_poll_with_pit(test_es_client)


@pytest.mark.es8
@pytest.mark.usefixtures("populate_es")
async def test_poll_with_pit_es8(test_es_client: ESClient) -> None:
    await _test_poll_with_pit(test_es_client)


@pytest.mark.es9
@pytest.mark.usefixtures("populate_es")
async def test_poll_with_pit_es9(test_es_client: ESClient) -> None:
    await _test_poll_with_pit(test_es_client)


@pytest.mark.parametrize(
    ("query", "expected_ids", "kwargs"),
    [
        # Match all
        (match_all(), {"doc-0", "doc-1", "doc-2", "doc-3"}, dict()),
        # Match all with sort and size
        (match_all(), {"doc-0", "doc-1"}, {"sort": "_id", "limit": 2}),
        # By ROOT ID
        ({"term": {DOC_ROOT_ID: "root-3"}}, {"doc-3"}, dict()),
    ],
)
@pytest.mark.usefixtures("populate_es")
async def test_search_es7(
    test_es_client: ESClient, query: dict, expected_ids: set[str], kwargs: dict
) -> None:
    await _test_search(test_es_client, query, expected_ids, **kwargs)


@pytest.mark.es8
@pytest.mark.parametrize(
    ("query", "expected_ids", "kwargs"),
    [
        # Match all
        (match_all(), {"doc-0", "doc-1", "doc-2", "doc-3"}, {"sort": ID}),
        # Match all with sort and size
        (match_all(), {"doc-0", "doc-1"}, {"sort": ID, "limit": 2}),
        # By ROOT ID
        ({"term": {DOC_ROOT_ID: "root-3"}}, {"doc-3"}, {"sort": ID}),
    ],
)
@pytest.mark.usefixtures("populate_es")
async def test_search_es8(
    test_es_client: ESClient, query: dict, expected_ids: set[str], kwargs: dict
) -> None:
    await _test_search(test_es_client, query, expected_ids, **kwargs)


@pytest.mark.es9
@pytest.mark.parametrize(
    ("query", "expected_ids", "kwargs"),
    [
        # Match all
        (match_all(), {"doc-0", "doc-1", "doc-2", "doc-3"}, {"sort": ID}),
        # Match all with sort and size
        (match_all(), {"doc-0", "doc-1"}, {"sort": ID, "limit": 2}),
        # By ROOT ID
        ({"term": {DOC_ROOT_ID: "root-3"}}, {"doc-3"}, {"sort": ID}),
    ],
)
@pytest.mark.usefixtures("populate_es")
async def test_search_es9(
    test_es_client: ESClient, query: dict, expected_ids: set[str], kwargs: dict
) -> None:
    await _test_search(test_es_client, query, expected_ids, **kwargs)
