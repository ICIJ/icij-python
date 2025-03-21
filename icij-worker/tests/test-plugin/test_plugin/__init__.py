from icij_worker import AsyncApp

app = AsyncApp(name="plugged")


@app.task()
def plugged_hello_world() -> str:
    return "Hello Plugged World!"
