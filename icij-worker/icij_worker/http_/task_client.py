import uuid
from typing import Any

from aiohttp import BasicAuth

from icij_worker import Task, TaskError, TaskState
from icij_worker.exceptions import UnknownTask
from icij_worker.utils.http import AiohttpClient


class TaskClient(AiohttpClient):
    def __init__(
        self,
        base_url: str,
        auth: BasicAuth | None = None,
        headers: dict | None = None,
        api_prefix: str = "/api",
    ):
        super().__init__(base_url, auth, headers)
        self._api_prefix = api_prefix

    async def create_task(
        self,
        name: str,
        args: dict[str, Any],
        *,
        id_: str | None = None,
        group: str | None = None,
    ) -> str:
        if id_ is None:
            id_ = _generate_task_id(name)
        task = Task.create(task_id=id_, task_name=name, args=args)
        task = task.model_dump(exclude_unset=True)
        task.pop("created_at")
        url = f"{self._api_prefix}/tasks/{id_}"
        if group is not None:
            if not isinstance(group, str):
                raise TypeError(f"expected group to be a string found {group}")
            url += f"?group={group}"
        async with self._put(url, json=task) as res:
            task_res = await res.json()
        return task_res["id"]

    async def get_task(self, id_: str) -> Task:
        url = f"{self._api_prefix}/tasks/{id_}"
        async with self._get(url) as res:
            task = await res.json()
        if task is None:
            raise UnknownTask(id_)
        task = Task.model_validate(task)
        return task

    async def get_tasks(self) -> list[Task]:
        url = f"{self._api_prefix}/tasks"
        async with self._get(url) as res:
            tasks = await res.json()
        tasks = [Task.model_validate(task) for task in tasks]
        return tasks

    async def get_task_state(self, id_: str) -> TaskState:
        return (await self.get_task(id_)).state

    async def get_task_result(self, id_: str) -> Any:
        url = f"{self._api_prefix}/tasks/{id_}/results"
        async with self._get(url) as res:
            task_res = await res.json()
        return task_res

    async def get_task_error(self, id_: str) -> list[TaskError]:
        url = f"{self._api_prefix}/tasks/{id_}/errors"
        async with self._get(url) as res:
            errors = await res.json()
        return [TaskError.model_validate(e) for e in errors]

    async def delete(self, id_: str):
        url = f"{self._api_prefix}/tasks/{id_}"
        async with self._delete(url):
            pass

    async def delete_all_tasks(self):
        for t in await self.get_tasks():
            await self.delete(t.id)


def _generate_task_id(task_name: str) -> str:
    return f"{task_name}-{uuid.uuid4()}"
