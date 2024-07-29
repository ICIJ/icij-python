from icij_worker import AsyncApp

my_app = AsyncApp(name="my_app")


@my_app.task
def long_running_task(greeted: str) -> str:
    greeting = f"Hello {greeted} !"
    return greeting
