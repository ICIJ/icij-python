import logging
import traceback
from typing import Dict

from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.utils import is_body_allowed_for_status_code
from pydantic.error_wrappers import display_errors
from starlette import status
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

INTERNAL_SERVER_ERROR = "Internal Server Error"
_REQUEST_VALIDATION_ERROR = "Request Validation Error"

logger = logging.getLogger(__name__)


def json_error(*, title, detail, **kwargs) -> Dict:
    error = {"title": title, "detail": detail}
    error.update(kwargs)
    return error


async def request_validation_error_handler(
    request: Request, exc: RequestValidationError
):
    title = _REQUEST_VALIDATION_ERROR
    detail = display_errors(list(exc.errors()))
    error = json_error(title=title, detail=detail)
    logger.error("%s\nURL: %s\nDetail: %s", title, request.url, detail)
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST, content=jsonable_encoder(error)
    )


async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    headers = getattr(exc, "headers", None)
    if not is_body_allowed_for_status_code(exc.status_code):
        return Response(status_code=exc.status_code, headers=headers)
    title = detail = exc.detail
    error = json_error(title=title, detail=detail)
    logger.error("%s\nURL: %s", title, request.url)
    return JSONResponse(
        jsonable_encoder(error), status_code=exc.status_code, headers=headers
    )


async def internal_exception_handler(request: Request, exc: Exception):
    # pylint: disable=unused-argument
    title = INTERNAL_SERVER_ERROR
    detail = f"{type(exc).__name__}: {exc}"
    trace = "".join(traceback.format_exc())
    error = json_error(title=title, detail=detail, trace=trace)
    logger.exception(
        "%s\nURL: %s\nDetail: %s",
        title,
        request.url,
        detail,
    )
    return JSONResponse(jsonable_encoder(error), status_code=500)
