import asyncio
import sys
from datetime import datetime
from functools import reduce
from json import loads, dumps
from os.path import basename
from typing import Callable
from urllib.parse import urlparse, unquote

import redis

from batch_download_utils.hmap_cleaner import DS_TASK_MANAGER


def rename_field(task: dict, old_name: str, new_name: str) -> dict:
    new_task = dict()
    for k, v in task.items():
        if k == old_name:
            new_task[new_name] = v
        elif isinstance(v, dict):
            new_task[k] = rename_field(v, old_name, new_name)
        else:
            new_task[k] = v
    return new_task


def rename_value(task: dict, old_value: str, new_value: str) -> dict:
    new_task = dict()
    for k, v in task.items():
        if v == old_value:
            new_task[k] = new_value
        else:
            new_task[k] = v
    return new_task


def add_field(task: dict, key, value):
    new_task = task.copy()
    new_task[key] = value
    return new_task


def move_field(task, source_json_path, dest_json_path) -> dict:
    source_fields = source_json_path.split(".")
    dest_fields = dest_json_path.split(".")
    # get the "leaf" value = final value of the source_json_path
    value = reduce(lambda d, k: d[k], source_fields, task)
    # build one way dest_fields dictionary with the original value {c:value} if we have "a.b.c" path
    dest_path_result = reduce(lambda d, k: {k: d}, reversed(dest_fields[0:-1]), {dest_fields[-1]: value})
    # then merge it with the original task dict copy
    result = {**task.copy(), **dest_path_result}
    # finally remove original path
    del result[source_fields[0]]
    return result


def get_date_from_task(task: dict) -> datetime:
    if task.get("createdAt"):
        return datetime.fromtimestamp(task.get("createdAt"))
    elif task.get("args") and task.get("args").get("batchDownload"):
        batch_download = task.get("args").get("batchDownload")
        filename_with_path = batch_download.get("filename")[1]
        parse_result = urlparse(unquote(filename_with_path))
        filename = basename(parse_result.path)
        date_index = filename.find('_', filename.find('_') + 1)
        date_and_zip = filename[date_index+1:]
        date_str = date_and_zip.replace(".zip", "")
        date_str = date_str.replace("Z[GMT]", "")
        date_str = date_str.replace("_", ":")
        return datetime.fromisoformat(date_str)
    else:
        return datetime.now()


def change_value(task: dict, dict_path: str, change_func: Callable) -> dict:
    fields = dict_path.split(".")
    old_value = reduce(lambda d, k: d[k], fields, task)
    dest_path_result = reduce(lambda d, k: {k: d}, reversed(fields[0:-1]), {fields[-1]: change_func(old_value)})
    return {**task.copy(), **dest_path_result}


async def main(args: dict) -> None:
    pool = redis.ConnectionPool.from_url(args.get("redis_url"))
    client = redis.Redis.from_pool(pool)
    tasks = await client.hgetall(DS_TASK_MANAGER)
    for k, v in tasks.items():
        task = loads(v)
        new_task = rename_field(task, "@class", "@type")
        new_task = rename_field(new_task, "properties", "args")
        new_task = rename_value(new_task, "org.icij.datashare.asynctasks.TaskView", "Task")
        new_task = add_field(new_task, "retriesLeft", 3)
        new_task = add_field(new_task, "createdAt", get_date_from_task(task).timestamp())
        new_task = move_field(new_task, "user", "args.user")
        new_task = change_value(new_task, "args.batchDownload.filename", lambda v: v[1])
        await client.hset(DS_TASK_MANAGER, task.get("id"), dumps(new_task))


def parse_args(argv) -> dict:
    if len(argv) != 3:
        print(f"usage: {argv[0]} <redis_url (ex: redis://localhost:6379)>")
        exit(1)
    return {"redis_url": argv[1]}


def main_async():
    asyncio.get_event_loop().run_until_complete(main(parse_args(sys.argv)))


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main(parse_args(sys.argv)))