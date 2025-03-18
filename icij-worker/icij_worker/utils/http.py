import logging
from contextlib import asynccontextmanager
from copy import deepcopy
from typing import Any, AsyncContextManager

from aiohttp import BasicAuth, ClientResponse, ClientResponseError, ClientSession
from aiohttp.typedefs import StrOrURL
from typing_extensions import Unpack

logger = logging.getLogger(__name__)


class AiohttpClient(AsyncContextManager):
    def __init__(
        self,
        base_url: str,
        auth: BasicAuth | None = None,
        headers: dict | None = None,
    ):
        self._base_url = base_url
        self._auth = auth
        self._session: ClientSession | None = None
        if headers is None:
            headers = dict()
        self._headers = headers

    async def __aenter__(self):
        self._session = ClientSession(self._base_url, auth=self._auth)
        await self._session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self._session.__aexit__(exc_type, exc_value, traceback)

    @asynccontextmanager
    async def _put(self, url: StrOrURL, *, data: Any = None, **kwargs: Any):
        headers = deepcopy(self._headers)
        headers.update(kwargs.pop("headers", dict()))
        async with self._session.put(url, data=data, headers=headers, **kwargs) as res:
            _raise_for_status(res)
            yield res

    @asynccontextmanager
    async def _get(self, url: StrOrURL, *, allow_redirects: bool = True, **kwargs: Any):
        headers = deepcopy(self._headers)
        headers.update(kwargs.pop("headers", dict()))
        async with self._session.get(
            url, headers=headers, allow_redirects=allow_redirects, **kwargs
        ) as res:
            _raise_for_status(res)
            yield res

    @asynccontextmanager
    async def _delete(self, url: StrOrURL, **kwargs: Unpack["_RequestOptions"]):
        headers = deepcopy(self._headers)
        headers.update(kwargs.pop("headers", dict()))
        async with self._session.delete(url, headers=headers, **kwargs) as res:
            _raise_for_status(res)
            yield res


def _raise_for_status(res: ClientResponse):
    try:
        res.raise_for_status()
    except ClientResponseError as e:
        msg = "request to %s, failed with reason %s"
        logger.exception(msg, res.request_info, res.reason)
        raise e
