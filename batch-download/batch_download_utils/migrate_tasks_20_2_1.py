import asyncio
import sys
from json import dumps, loads

import redis.asyncio as redis

from batch_download_utils.hmap_cleaner import DS_TASK_MANAGER
from batch_download_utils.migrate_tasks import get_value_from_json_path, change_value


async def main(args: dict) -> None:
    pool = redis.ConnectionPool.from_url(args.get("redis_url"))
    client = redis.Redis.from_pool(pool)
    tasks = await client.hgetall(DS_TASK_MANAGER)
    for v in tasks.values():
        task = loads(v)
        new_task = task
        if "@type" not in get_value_from_json_path(new_task, "task.args"):
            args_to_set = get_value_from_json_path(new_task, "task.args")
            args_to_set["@type"] = "java.util.Collections$UnmodifiableMap"
            change_value(new_task, "task.args", lambda p: args_to_set)
        if type(get_value_from_json_path(task, "task.createdAt")) is int:
            created_at_to_set = ["java.util.Date",get_value_from_json_path(task, "task.createdAt")]
            new_task = change_value(new_task, "task.createdAt", lambda p: created_at_to_set)
        if  get_value_from_json_path(task, "task.completedAt") is not None:
            if type(get_value_from_json_path(task, "task.completedAt")) is int:
                completed_at_to_set = ["java.util.Date",get_value_from_json_path(task, "task.completedAt")]
                new_task = change_value(new_task, "task.completedAt", lambda p: completed_at_to_set)
        if "BatchSearch" in get_value_from_json_path(task,"task.name"):
            value_to_set = {
                "value": {
                    "@type": "BatchSearchRunnerResult",
                    "nbResults": get_value_from_json_path(task,"task.result.value"),
                    "nbQueriesWithoutResults": -1
                }
            }
            new_task = change_value(new_task, "task.result", lambda p: value_to_set)
            if not get_value_from_json_path(new_task, "task.args.batchRecord.projects")[0] == "java.util.ArrayList":
                projects_to_set = ["java.util.ArrayList", get_value_from_json_path(new_task, "task.args.batchRecord.projects")]
                change_value(new_task, "task.args.batchRecord.projects", lambda p: projects_to_set)
            if type(get_value_from_json_path(new_task, "task.args.batchRecord.date")) is int:
                date_to_set = ["java.util.Date", get_value_from_json_path(task, "task.args.batchRecord.date")]
                new_task = change_value(new_task, "task.args.batchRecord.date", lambda p: date_to_set)
            if "@type" not in get_value_from_json_path(new_task, "task.args.batchRecord.user"):
                user_to_set = get_value_from_json_path(new_task, "task.args.batchRecord.user")
                user_to_set["@type"] = "org.icij.datashare.session.DatashareUser"
                change_value(new_task, "task.args.batchRecord.user", lambda p: user_to_set)
        if "BatchDownload" in get_value_from_json_path(new_task, "task.name"):
            if not get_value_from_json_path(new_task, "task.args.batchDownload.projects")[0] == "java.util.Collections$UnmodifiableRandomAccessList":
                projects_tab = get_value_from_json_path(new_task, "task.args.batchDownload.projects")
                for project in projects_tab:
                    if "@type" not in get_value_from_json_path(new_task, "task.args.batchDownload.projects"):
                        project["@type"] = "org.icij.datashare.text.Project"
                    if type(project["sourcePath"]) is str:
                        project["sourcePath"] = ["java.nio.file.Path", project["sourcePath"]]
                projects_to_set = ["java.util.Collections$UnmodifiableRandomAccessList", projects_tab]
                change_value(new_task, "task.args.batchDownload.projects", lambda p: projects_to_set)
            if type(get_value_from_json_path(new_task, "task.args.batchDownload.filename")) is str:
                filename_to_set = ["java.nio.file.Path", get_value_from_json_path(task, "task.args.batchDownload.filename")]
                new_task = change_value(new_task, "task.args.batchDownload.filename", lambda p: filename_to_set)
            if get_value_from_json_path(new_task, "task.args.batchDownload.query.@type") is None:
                query_to_set = {
                    "@type": "org.icij.datashare.text.indexing.SearchQuery",
                    "query": get_value_from_json_path(new_task, "task.args.batchDownload.query.query")
                }
                new_task = change_value(new_task, "task.args.batchDownload.query", lambda p: query_to_set)
            if get_value_from_json_path(new_task, "task.args.batchDownload.user.@type") is None:
                user_to_set = get_value_from_json_path(new_task, "task.args.batchDownload.user")
                user_to_set["@type"] = "org.icij.datashare.session.DatashareUser"
                change_value(new_task, "task.args.batchDownload.user", lambda p: user_to_set)
        task_id = get_value_from_json_path(task, "task.id")
        await client.hset(DS_TASK_MANAGER, task_id, dumps(new_task, separators=(',',':')))
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


