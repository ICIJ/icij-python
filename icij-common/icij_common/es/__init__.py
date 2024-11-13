import hashlib
import json
import logging
from contextlib import asynccontextmanager
from copy import deepcopy
from functools import cached_property
from typing import (
    Any,
    AsyncGenerator,
    Collection,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from elasticsearch import AsyncElasticsearch, TransportError
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    before_sleep_log,
    retry_base,
    stop_after_attempt,
    wait_random_exponential,
)

logger = logging.getLogger(__name__)
_ES_CLIENTS: Dict[str, AsyncElasticsearch] = dict()

PointInTime = Dict[str, Any]

ASC = "asc"
BOOL = "bool"
COUNT = "count"
DESC = "desc"
DOC_TYPE = "_doc"
DOCS = "docs"
FUNCTION_SCORE = "function_score"
HITS = "hits"
ID = "id"
ID_ = "_id"
INDEX_ = "_index"
IDS = "ids"
KEEP_ALIVE = "keep_alive"
LIMIT = "limit"
LTE = "lte"
MATCH_ALL = "match_all"
MUST = "must"
PIT = "pit"
QUERY = "query"
RANGE = "range"
SOURCE = "_source"
SCORE_ = "_score"
SCROLL = "scroll"
SCROLL_ID = "scroll_id"
SCROLL_ID_ = "_scroll_id"
SEARCH_AFTER = "search_after"
SHARD_DOC_ = "_shard_doc"
SIZE = "size"
SORT = "sort"
SHOULD = "should"
TERM = "term"
TOTAL = "total"
VALUE = "value"
VALUES = "values"

ES_DOCUMENT_TYPE = "Document"
ES_NAMED_ENTITY_TYPE = "NamedEntity"

DOC_CONTENT = "content"
DOC_CONTENT_LENGTH = "contentLength"
DOC_CONTENT_TYPE = "contentType"
DOC_CREATED_AT = "createdAt"
DOC_DIRNAME = "dirname"
DOC_EXTRACTION_DATE = "extractionDate"
DOC_EXTRACTION_LEVEL = "extractionLevel"
DOC_LANGUAGE = "language"
DOC_METADATA = "metadata"
DOC_MODIFIED_AT = "modifiedAt"
DOC_PATH = "path"
DOC_ROOT_ID = "rootDocument"
DOC_URL = "url"

DOC_SOURCES = [
    DOC_CONTENT,
    DOC_CONTENT_LENGTH,
    DOC_CONTENT_TYPE,
    DOC_CREATED_AT,
    DOC_DIRNAME,
    DOC_EXTRACTION_DATE,
    DOC_EXTRACTION_LEVEL,
    DOC_METADATA,
    DOC_MODIFIED_AT,
    DOC_PATH,
]


class ESClient(AsyncElasticsearch):
    _recoverable_error_codes: Set[int] = {429}

    def __init__(
        self,
        *,
        pagination: int,
        keep_alive: str = "1m",
        max_concurrency: int = 5,
        max_retries: int = 0,
        max_retry_wait_s: Union[int, float] = 60,
        api_key: Optional[str] = None,
        **kwargs,
    ):
        AsyncElasticsearch.__init__(self, **kwargs)
        self._api_key = api_key
        if pagination > 10000:
            raise ValueError("Elasticsearch doesn't support pages of size > 10000")
        self._pagination_size = pagination
        self._keep_alive = keep_alive
        self._max_concurrency = max_concurrency
        self._max_retries = max_retries
        self._max_retry_wait_s = max_retry_wait_s

    @cached_property
    def max_concurrency(self) -> int:
        return self._max_concurrency

    @cached_property
    def pagination_size(self) -> int:
        return self._pagination_size

    @cached_property
    def keep_alive(self) -> str:
        return self._keep_alive

    def _async_retrying(self) -> AsyncRetrying:
        return AsyncRetrying(
            wait=wait_random_exponential(max=self._max_retry_wait_s),
            stop=stop_after_attempt(self._max_retries),
            retry=retry_if_error_code(self._recoverable_error_codes),
            before_sleep=before_sleep_log(logger, logging.ERROR),
            reraise=True,
        )

    async def poll_search_pages(
        self, body: Dict, sort: Union[List[str], Tuple[str, ...], None] = None, **kwargs
    ) -> AsyncGenerator[Dict[str, Any], None]:
        retrying = self._async_retrying()
        if sort is None:
            sort = self.default_sort()
        res = await retrying(self.search, sort=sort, body=body, **kwargs)
        kwargs = deepcopy(kwargs)
        yield res
        page_hits = res[HITS][HITS]
        while page_hits:
            search_after = page_hits[-1][SORT]
            body[SEARCH_AFTER] = search_after
            res = await retrying(self.search, sort=sort, body=body, **kwargs)
            page_hits = res[HITS][HITS]
            if page_hits:
                yield res

    @asynccontextmanager
    async def pit(
        self, *, index: str, keep_alive: Optional[str] = None, **kwargs
    ) -> AsyncGenerator[Dict[str, Any], None]:
        if keep_alive is None:
            keep_alive = self._keep_alive
        pit_id = None
        try:
            pit = await self.open_point_in_time(
                index=index, keep_alive=keep_alive, **kwargs
            )
            yield pit
        finally:
            if pit_id is not None:
                await self._close_pit(pit_id)

    def _with_headers(self, headers: Dict) -> Dict:
        if self._api_key is not None:
            headers["Authorization"] = f"bearer {self._api_key}"
        return headers

    async def count(
        self, body: Optional[Dict] = None, index: Optional[str] = None, **kwargs
    ):
        headers = kwargs.pop("headers", dict())
        headers = self._with_headers(headers)
        return await super().count(body=body, index=index, headers=headers, **kwargs)

    async def search(self, body: Optional[Dict], **kwargs):
        # pylint: disable=unexpected-keyword-arg
        headers = kwargs.pop("headers", dict())
        headers = self._with_headers(headers)
        size = kwargs.get("size", self.pagination_size)
        return await super().search(
            headers=headers,
            body=body,
            size=size,
            **kwargs,
        )

    def default_sort(self) -> str:
        return f"{SHARD_DOC_}:{ASC}"

    async def _close_pit(self, pit_id: str):
        await self.close_point_in_time(body={ID: pit_id})


def _hash_params(params: Dict) -> str:
    # TODO not very robust by should be enough
    hashed = json.dumps(params, sort_keys=True)
    return hashlib.md5(hashed).hexdigest()


class retry_if_error_code(retry_base):  # pylint: disable=invalid-name
    def __init__(self, codes: Collection[int]):
        self._recoverable = set(codes)

    def __call__(self, retry_state: RetryCallState) -> bool:
        if retry_state.outcome.failed:
            exc = retry_state.outcome.exception()
            return (
                isinstance(exc, TransportError) and exc.status_code in self._recoverable
            )
        return False


def and_query(*queries: Dict) -> Dict:
    return {QUERY: {BOOL: {MUST: list(queries)}}}


def with_sort(*, query: Dict, sort: Dict) -> Dict:
    if SORT not in query:
        query[SORT] = []
    return query[SORT].append(sort)


def with_source(query: Dict, sources: List[str]) -> Dict:
    query[SOURCE] = sources
    return query


def with_pit(query: Dict, pit: PointInTime) -> Dict:
    query[PIT] = pit
    return query


def with_limit(query: Dict, limit: int) -> Dict:
    query[SIZE] = limit
    return query


_RANDOM_SCORE = {
    FUNCTION_SCORE: {
        "functions": [
            {"random_score": {"seed": 666, "field": "_seq_no"}},
        ]
    }
}


def with_random_score(query: Dict) -> Dict:
    if FUNCTION_SCORE in query[QUERY]:
        raise ValueError(f"query already has an existing {FUNCTION_SCORE}")
    new_query = deepcopy(_RANDOM_SCORE)
    new_query[FUNCTION_SCORE][QUERY] = query.pop(QUERY)
    query[QUERY] = new_query
    return query


def has_type(*, type_field: str, type_value: str) -> Dict:
    return {TERM: {type_field: type_value}}


def has_id(ids: List[str]) -> Dict:
    return {IDS: {VALUES: ids}}


def function_score(*, query: Dict, **kwargs) -> Dict:
    query = {FUNCTION_SCORE: {QUERY: query}}
    if kwargs:
        query[FUNCTION_SCORE].update(kwargs)
    return query


def match_all_query() -> Dict:
    return {QUERY: match_all()}


def make_document_query(
    es_query: Optional[Dict],
    sources: Optional[List[str]] = None,
    es_doc_type_field: str = "type",
) -> Dict:
    document_type_query = has_type(
        type_field=es_doc_type_field, type_value=ES_DOCUMENT_TYPE
    )
    if es_query is not None and es_query:
        es_query = and_query(document_type_query, es_query)
    else:
        es_query = {QUERY: document_type_query}
    if sources is not None:
        es_query = with_source(es_query, sources)
    return es_query


def match_all() -> Dict:
    return {MATCH_ALL: {}}


def ids_query(ids: List[str]) -> Dict:
    return {IDS: {VALUES: ids}}


def get_scroll_id(res: Dict) -> str:
    scroll_id = res.get(SCROLL_ID_)
    if scroll_id is None:
        msg = "Missing scroll ID, this response is probably not from a scroll search"
        raise ValueError(msg)
    return scroll_id


def get_total_hits(res: Dict) -> int:
    return res[HITS][TOTAL][VALUE]


def add_url_to_document(doc: Dict[str, Any], base_url: str) -> Dict[str, Any]:
    sources = doc[SOURCE]
    doc_index = doc[INDEX_]
    doc_id = doc[ID_]
    url = f"{base_url}#/ds/{doc_index}/{doc_id}/{doc.get(DOC_ROOT_ID, doc_id)}"
    sources[DOC_URL] = url
    return doc


def mget_doc_body(
    index_and_ids: List[Tuple[str, str]] = None,
) -> Dict:
    body = {
        DOCS: [
            {ID_: doc_id, INDEX_: doc_index} for (doc_id, doc_index) in index_and_ids
        ]
    }
    return body
