import uuid
from typing import Any, Dict, Optional

from icij_common.pydantic_utils import jsonable_encoder
from icij_worker import Task, TaskState
from icij_worker.utils.http import AiohttpClient

# TODO: maxRetries is not supported by java, it's automatically set to 3
_TASK_UNSUPPORTED = {"max_retries"}


class DatashareTaskClient(AiohttpClient):
    def __init__(self, datashare_url: str):
        super().__init__(datashare_url)

    async def create_task(
        self,
        name: str,
        args: Dict[str, Any],
        *,
        id_: Optional[str] = None,
        group: Optional[str] = None,
    ) -> Task:
        if id_ is None:
            id_ = _generate_task_id(name)
        task = Task.create(task_id=id_, task_name=name, args=args)
        task = jsonable_encoder(task, exclude=_TASK_UNSUPPORTED, exclude_unset=True)
        task.pop("createdAt")
        url = f"/api/task/{id_}"
        data = {"task": task, "group": group}
        async with self._put(url, json=data) as res:
            task = await res.json()
        task = Task(**task)
        return task

    async def get_task(self, id_: str) -> Task:
        url = f"/api/task/{id_}"
        async with self._get(url) as res:
            task = await res.json()
        # TODO: align Java on Python here... it's not a good idea to store results
        #  inside tasks since result can be quite large and we may want to get the task
        #  metadata without having to deal with the large task results...
        task.pop("result", None)
        task = Task(**task)
        return task

    async def get_task_state(self, id_: str) -> TaskState:
        return (await self.get_task(id_)).state

    async def get_task_result(self, id_: str) -> object:
        # TODO: we probably want to use /api/task/:id/results instead but it's
        #  restricted, we might need an API key or some auth
        url = f"/api/task/{id_}"
        async with self._get(url) as res:
            task = await res.json()
        return task.get("result")


def _generate_task_id(task_name: str) -> str:
    return f"{task_name}-{uuid.uuid4()}"


TaskClient = DatashareTaskClient
