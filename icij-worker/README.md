# ICIJ's async worker library [![Test for icij-worker](https://github.com/ICIJ/icij-python/actions/workflows/tests-worker.yml/badge.svg)](https://github.com/ICIJ/icij-python/actions/workflows/tests-worker.yml)

## Installation

With AMQP:
```bash
pip install icij-worker["amqp"]
```

With neo4j:
```bash
pip install icij-worker["neo4j"]
```

## Usage

### Create an async app and register tasks

Create asynchronous task tailored for long running Python functions:

Given the following pure Python function inside the `app.py` module:

```python
def long_running_task(greeted: str) -> str:
    greeting = f"Hello {greeted} !"
    return greeting
```

decorate your function with `ICIJApp` class and register a new task:

```python
from icij_worker import AsyncApp

my_app = AsyncApp(name="my_app")

@my_app.task
def long_running_task(greeted: str) -> str:
    greeting = f"Hello {greeted} !"
    return greeting
```

this will register the `long_running_task` function under the `long_running_task` task name.

Optionally add progress handlers for a better task monitoring:

```python
@my_app.task
async def long_running_task(
    greeted: str,
    progress: Optional[Callable[[float], Awaitable]] = None
) -> str:
    if progress is not None:
        await progress(0.0)
    greeting = f"Hello {greeted} !"
    if progress is not None:
        await progress(100.0)
    return greeting
```

### Launch a async worker pool

Start a worker pool using:

```bash
icij-worker workers start "app.my_app"
```

provide worker pool options using:

```bash
icij-worker workers start -c worker_config.json -n 2 --backend multiprocessing "app.my_app"
```

**depending on the worker configuration additional setup might be required**.

## Async worker implementations

- [Neo4j](https://neo4j.com/docs/api/python-driver/current/)
- [AMQP](https://www.amqp.org/) (`v0.9.1` based on [RabbitMQ](https://www.rabbitmq.com/))

## Worker asynchronous backends

### Implemented

- [multiprocessing](https://docs.python.org/3/library/multiprocessing.html) for CPU bound tasks

### To be implemented

- [asyncio](https://docs.python.org/3/library/asyncio.html) for I/O bound tasks
