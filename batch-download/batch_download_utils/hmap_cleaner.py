import asyncio
import sys
from json import loads
from os.path import exists, basename, join
from urllib.parse import urlparse, unquote

import redis.asyncio as redis

DS_TASK_MANAGER = "ds:task:manager"


async def main(args: dict) -> None:
    pool = redis.ConnectionPool.from_url(args.get("redis_url"))
    base_dir = args.get("base_dir")
    client = redis.Redis.from_pool(pool)
    tasks = await client.hgetall(DS_TASK_MANAGER)
    for k, v in tasks.items():
        task = loads(v)

        if task.get("args") and task.get("args").get("batchDownload"):
            batch_download = task.get("args").get("batchDownload")
            filename = batch_download.get("filename")
            parse_result = urlparse(unquote(filename))
            file_path = join(base_dir, basename(parse_result.path))
            if not exists(file_path):
                print(
                    f"removing batch download {task.get('id')} from redis hmap (no file {file_path} found)"
                )
                await client.hdel(DS_TASK_MANAGER, task.get("id"))


def parse_args(argv) -> dict:
    if len(argv) != 3:
        print(
            f"usage: {argv[0]} <redis_url (ex: redis://localhost:6379/0)> <base_dir (ex: /tmp)>"
        )
        exit(1)
    return {"redis_url": argv[1], "base_dir": argv[2]}


def main_async():
    asyncio.get_event_loop().run_until_complete(main(parse_args(sys.argv)))


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main(parse_args(sys.argv)))
