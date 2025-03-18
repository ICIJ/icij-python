import logging
import traceback

from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.utils import is_body_allowed_for_status_code
from pydantic_core import ErrorDetails
from starlette import status
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

INTERNAL_SERVER_ERROR = "Internal Server Error"
_REQUEST_VALIDATION_ERROR = "Request Validation Error"

logger = logging.getLogger(__name__)


def json_error(*, title, detail, **kwargs) -> dict:
    error = {"title": title, "detail": detail}
    error.update(kwargs)
    return error


def display_errors(errors: list[ErrorDetails]) -> str:
    return "\n".join(
        f'{_display_error_loc(e)}\n  {e["msg"]} ({_display_error_type_and_ctx(e)})'
        for e in errors
    )


def _display_error_loc(error: ErrorDetails) -> str:
    return " -> ".join(str(e) for e in error["loc"])


def _display_error_type_and_ctx(error: ErrorDetails) -> str:
    t = "type=" + error["type"]
    ctx = error.get("ctx")
    if ctx:
        return t + "".join(f"; {k}={v}" for k, v in ctx.items())
    return t


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
