import uuid
from typing import Any, Dict, Optional

from icij_worker import Task
from icij_worker.utils.http import AiohttpClient


class TaskClient(AiohttpClient):
    def __init__(self, datashare_url: str):
        super().__init__(datashare_url)

    async def create_task(
        self,
        name: str,
        args: Dict[str, Any],
        *,
        id_: Optional[str] = None,
        group: Optional[str] = None,
    ):
        if id_ is None:
            id_ = _generate_task_id(name)
        task = Task.create(task_id=id_, task_name=name, args=args)
        url = f"/api/task/{id_}"
        data = {"task": task, "group": group}
        async with self._put(url, json=data):
            pass


def _generate_task_id(task_name: str) -> str:
    return f"{task_name}-{uuid.uuid4()}"
