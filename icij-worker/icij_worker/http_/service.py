import logging
from collections.abc import Iterable
from functools import partial

from fastapi import FastAPI, routing
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from icij_common.fastapi_utils import (
    http_exception_handler,
    internal_exception_handler,
    request_validation_error_handler,
)
from icij_worker.http_ import OTHER_TAG, TASKS_TAG
from icij_worker.http_.config import HttpServiceConfig
from icij_worker.http_.dependencies import BASE_DEPENDENCIES, run_http_service_deps
from icij_worker.http_.main import main_router
from icij_worker.http_.tasks import tasks_router
from icij_worker.typing_ import Dependency

RouterTag = str

INTERNAL_SERVER_ERROR = "Internal Server Error"
_REQUEST_VALIDATION_ERROR = "Request Validation Error"

logger = logging.getLogger(__name__)


def _make_open_api_tags(tags: Iterable[str]) -> list[dict]:
    return [{"name": t} for t in tags]


def create_service(
    config: HttpServiceConfig,
    routers: list[tuple[RouterTag, routing.APIRouter]] = None,
    dependencies: list[Dependency] | None = None,
) -> FastAPI:
    if routers is None:
        routers, tags = [], []
    else:
        tags, routers = zip(*routers)
        tags = list(tags)
        routers = list(routers)
    routers.append(main_router())
    routers.append(tasks_router())
    tags.extend([TASKS_TAG, OTHER_TAG])
    deps = BASE_DEPENDENCIES
    if dependencies is not None:
        deps += dependencies
    lifespan = partial(run_http_service_deps, config=config, dependencies=deps)
    app = FastAPI(
        title=config.app_title,
        openapi_tags=_make_open_api_tags(tags),
        lifespan=lifespan,
    )
    app.add_exception_handler(RequestValidationError, request_validation_error_handler)
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(Exception, internal_exception_handler)
    for router in routers:
        app.include_router(router)
    return app
