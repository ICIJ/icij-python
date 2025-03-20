from fastapi import APIRouter
from starlette.responses import Response
from starlette.status import HTTP_200_OK, HTTP_503_SERVICE_UNAVAILABLE

from icij_worker.http_ import OTHER_TAG
from icij_worker.http_.dependencies import lifespan_task_manager


def main_router(app_version: str | None = None) -> APIRouter:
    router = APIRouter(tags=[OTHER_TAG])
    if app_version is not None:

        @router.get("/version")
        def version() -> Response:
            return Response(content=app_version, media_type="text/plain")

    @router.get("/health")
    async def health(response: Response) -> dict[str, bool]:
        task_manager = lifespan_task_manager()
        health = await task_manager.get_health()
        is_healthy = all(health.values())
        response.status_code = (
            HTTP_200_OK if is_healthy else HTTP_503_SERVICE_UNAVAILABLE
        )
        return health

    return router
