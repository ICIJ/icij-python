from typing import Any

from pydantic import BaseModel

from icij_common.pydantic_utils import icij_config
from icij_worker import TaskState


class TaskSearch(BaseModel):
    model_config = icij_config()

    name: str | None = None
    status: list[TaskState] | TaskState | None = None


class TaskCreationQuery(BaseModel):
    model_config = icij_config()

    name: str
    args: dict[str, Any]
