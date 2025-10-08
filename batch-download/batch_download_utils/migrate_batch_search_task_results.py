import asyncio, json, sys
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text

def db_handling(db_url):
    if db_url.startswith("jdbc:"):
        db_url = db_url[5:]

    if db_url.startswith("postgresql"):
        return db_url.replace("postgresql://", "postgresql+asyncpg://")
    if db_url.startswith("sqlite"):
        return db_url.replace("sqlite:file:/", "sqlite+aiosqlite:////")
    if db_url.startswith("mysql"):
        return db_url.replace("mysql://", "mysql+aiomysql://")

    raise ValueError("unknown database url")

async def main(args: dict) -> None:
    engine = create_async_engine(db_handling(args.get("db_url")))
    async with engine.begin() as conn:
        res_tasks_ids = await conn.execute(text("SELECT id FROM task WHERE name LIKE '%BatchSearchRunner%'"))
        tasks_ids = res_tasks_ids.fetchall()
        for id_rows in tasks_ids:
            task_id = id_rows[0]
            res = await conn.execute(text(f"SELECT batch_results,nb_queries_without_results FROM batch_search WHERE uuid = '{task_id}'"))
            (bs_res, nb_quer_without_res) = res.fetchone()
            if nb_quer_without_res is None:
                nb_quer_without_res = -1
            value_to_set = {
                "value": {
                    "@type": "BatchSearchRunnerResult",
                    "nbResults": bs_res,
                    "nbQueriesWithoutResults": nb_quer_without_res
                }
            }

            await conn.execute(text("UPDATE task SET result = :result WHERE id = :id"),
                               {"result": json.dumps(value_to_set, separators=(',',':')), "id": task_id})
            print(f"Task id: {task_id} updated")

def parse_args(argv) -> dict:
    if len(argv) != 2:
        print(f"usage: {argv[0]} <db_url (ex: jdbc:postgresql://postgres/foo?user=bar&password=baz)>")
        exit(1)
    return {"db_url": argv[1]}


def main_async():
    asyncio.get_event_loop().run_until_complete(main(parse_args(sys.argv)))


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main(parse_args(sys.argv)))


