import asyncio
import sys
from datetime import datetime
from functools import reduce
from json import dumps, loads
from os.path import basename
from typing import Any, Callable
from urllib.parse import unquote, urlparse

import redis.asyncio as redis

from batch_download_utils.hmap_cleaner import DS_TASK_MANAGER
from batch_download_utils.migrate_tasks import get_value_from_json_path, change_value


async def main(args: dict) -> None:
    pool = redis.ConnectionPool.from_url(args.get("redis_url"))
    client = redis.Redis.from_pool(pool)
    tasks = await client.hgetall(DS_TASK_MANAGER)
    for v in tasks.values():
        task = loads(v)
        if "BatchSearch" in get_value_from_json_path(task,"task.name"):
            value_to_set = {
                "value": {
                    "@type": "BatchSearchRunnerResult",
                    "nbResults": get_value_from_json_path(task,"task.result.value"),
                    "nbQueriesWithoutResults": -1
                }
            }
            new_task = change_value(task, "task.result", lambda p: value_to_set)
            task_id = get_value_from_json_path(task, "task.id")
            await client.hset(DS_TASK_MANAGER, task_id, dumps(new_task))
            print(f"Task id: {task_id} updated")


def parse_args(argv) -> dict:
    if len(argv) != 2:
        print(f"usage: {argv[0]} <redis_url (ex: redis://localhost:6379)>")
        exit(1)
    return {"redis_url": argv[1]}


def main_async():
    asyncio.get_event_loop().run_until_complete(main(parse_args(sys.argv)))


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main(parse_args(sys.argv)))


