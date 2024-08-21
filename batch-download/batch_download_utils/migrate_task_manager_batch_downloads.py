import asyncio
import sys
from datetime import datetime
from functools import reduce
from json import loads, dumps
from os.path import basename
from typing import Callable, Any
from urllib.parse import urlparse, unquote

import redis.asyncio as redis

from batch_download_utils.hmap_cleaner import DS_TASK_MANAGER


def rename_field(task: dict, old_name: str, new_name: str) -> dict:
    if isinstance(task, dict):
        new_task = dict()
        for k, v in task.items():
            if k == old_name:
                new_task[new_name] = v
            elif isinstance(v, list):
                new_task[k] = list(map(lambda item: rename_field(item, old_name, new_name), v))
            elif isinstance(v, dict):
                new_task[k] = rename_field(v, old_name, new_name)
            else:
                new_task[k] = v
        return new_task
    else:
        return task


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


def merge_dicts(d1, d2):
    return {**d1, **{k: merge_dicts(d1[k], d2[k]) if isinstance(d1.get(k), dict) and isinstance(v, dict) else v for k, v in d2.items()}}


def move_field(task, source_json_path, dest_json_path) -> dict:
    source_fields = source_json_path.split(".")
    dest_fields = dest_json_path.split(".")
    # get the "leaf" value = final value of the source_json_path
    value = get_value_from_json_path(task, source_json_path)
    # build one way dest_fields dictionary with the original value {c:value} if we have "a.b.c" path
    dest_path_result = reduce(lambda d, k: {k: d}, reversed(dest_fields[0:-1]), {dest_fields[-1]: value})
    # then merge it with the original task dict copy
    result = merge_dicts(task, dest_path_result)
    # finally remove original path
    del result[source_fields[0]]
    return result


def get_date_from_task(task: dict) -> datetime:
    if task.get("createdAt"):
        return datetime.fromtimestamp(task.get("createdAt")/1000)
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
    result = task.copy()
    value = result
    for field_index, field in enumerate(fields):
        if field.endswith('[]'):
            list_field = field.replace('[]', '')
            list_value = value[list_field]
            for list_index, item in enumerate(list_value):
                list_value[list_index] = change_value(item, ".".join(fields[field_index+1:]), change_func)
            break
        elif field_index == len(fields) - 1:
            value[field] = change_func(value[field])
        else:
            value = value.get(field)
    return result


def get_value_from_json_path(task: dict, path: str) -> Any:
    try:
        return reduce(lambda d, k: d[k], path.split("."), task)
    except KeyError:
        return None


async def main(args: dict) -> None:
    pool = redis.ConnectionPool.from_url(args.get("redis_url"))
    client = redis.Redis.from_pool(pool)
    tasks = await client.hgetall(DS_TASK_MANAGER)
    await client.copy(DS_TASK_MANAGER, DS_TASK_MANAGER + ":backup")
    for k, v in tasks.items():
        task = loads(v)
        new_task = rename_field(task, "properties", "args")
        if get_value_from_json_path(new_task, "args.batchDownload.projects") is not None:
            new_task = change_value(new_task, "args.batchDownload.projects", lambda v: v[1])
            new_task = change_value(new_task, "args.batchDownload.projects[].sourcePath", lambda v: v[1])
        if get_value_from_json_path(new_task, "args.batchDownload.filename") is not None:
            new_task = change_value(new_task, "args.batchDownload.filename", lambda v: v[1])

        new_task = rename_field(new_task, "@class", "@type")
        new_task = rename_value(new_task, "org.icij.datashare.asynctasks.TaskView", "Task")
        new_task = add_field(new_task, "retriesLeft", 3)
        new_task = add_field(new_task, "createdAt", int(get_date_from_task(task).timestamp()*1000))
        new_task = move_field(new_task, "user", "args.user")

        await client.hset(DS_TASK_MANAGER, task.get("id"), dumps(new_task))


def parse_args(argv) -> dict:
    if len(argv) != 2:
        print(f"usage: {argv[0]} <redis_url (ex: redis://localhost:6379)>")
        exit(1)
    return {"redis_url": argv[1]}


def main_async():
    asyncio.get_event_loop().run_until_complete(main(parse_args(sys.argv)))


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main(parse_args(sys.argv)))