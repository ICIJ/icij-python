# ICIJ's Style Guide for Python Applications

This guide largely follows [Google's Python Style Guide](https://google.github.io/styleguide/pyguide.html) with certain adjustments
enforced by our ruff configuration.

---

## Tooling

### Ruff

Formatting and linting should be handled by [ruff](https://docs.astral.sh/ruff/) and configuration values should
be kept in `qa/ruff.toml`.

- **Target:** Python >=3.12
- **Line length:** 88 characters (ruff default)
- **Import sorting:** enforced via the `I` ruleset

Ruff should be run on commit with `pre-commit`. Make sure to install it before
committing (`uv run pre-commit install`). If `pre-commit` isn't included in a project's
dev dependencies, add it:

```console
uv add pre-commit
uv sync
uv run pre-commit install
```

### Key rules enabled

| Rule group | What it enforces |
|---|---|
| `ANN` | Type annotations on public APIs |
| `D` | Docstrings (D1xx missing-docstring rules are ignored; format rules apply) |
| `I` | Import ordering |
| `G` | Logging format (no f-strings in log calls) |
| `BLE` | No bare `except Exception` without re-raising |
| `FBT` | No boolean positional arguments |
| `ASYNC` | Async best practices |
| `UP` | Use modern Python syntax (pyupgrade) |
| `SIM` | Simplifiable code patterns |

### Key rules ignored

| Rule | Reason |
|---|---|
| `D1xx` (missing docstrings) | Not every internal helper requires a docstring |
| `D205`, `D400`, `D415` | Docstring formatting leniency (no trailing period required) |
| `PLR09xx` (complexity limits) | We trust authors to keep functions readable |
| `ANN002`, `ANN003` | `*args`/`**kwargs` type annotations not required |
| `ANN401` | `Any` is allowed when unavoidable |
| `N818` | Exception classes don't need an `Error` suffix |

---

## Naming Conventions

Follow [Google's naming rules](https://google.github.io/styleguide/pyguide.html#316-naming):

| Kind | Style | Example                            |
|---|---|------------------------------------|
| Packages / modules | `snake_case` | `task_client`, `objects_`          |
| Classes | `PascalCase` | `WorkerConfig`, `AppActivities`    |
| Functions / methods | `snake_case` | `create_batches`                   |
| Variables | `snake_case` | `batch_size`, `run_id`             |
| Constants (module-level) | `UPPER_SNAKE_CASE` | `WORKFLOW_NAME`         |
| Private constants | `_UPPER_SNAKE_CASE` | `_ONE_MINUTE`, `_TEN_MINUTES`      |
| Private attributes / methods | leading `_` | `self._client`, `_raise_for_status` |

**Conventions to follow:**

- Use full words; avoid non-standard abbreviations (`translation_config`, not `tc`).
- Single-letter names are acceptable only for type variables (`T`, `P`) and trivial loop
indices.
- Type aliases (and `TypeVar`) live at module level, conventionally near the top after
imports.

```python
P = ParamSpec("P")
T = TypeVar("T")
DependencyLabel = str | None
```

---

## Imports

Import ordering (enforced by ruff `I`):

1. Standard library
2. Third-party packages
3. Local / intra-package imports

Each group is separated by a blank line. Within a group, imports are sorted
lexicographically.

```python
# Standard library
import asyncio
import logging
from collections.abc import AsyncGenerator, Callable, Iterable
from dataclasses import dataclass
from functools import wraps

# Third-party
from pydantic import Field
from temporalio import activity, workflow
from temporalio.client import Client

# Local
from .constants import TRANSLATION_WORKFLOW_NAME
from .objects import TranslationRequest, TranslationResponse
```

**Rules:**

- Relative imports are allowed within the same package, but **not above the parent
level** (enforced by `ban-relative-imports = "parents"`).
- Prefer `from collections.abc import Callable` over `from typing import Callable`
(enforced by `UP`).
- Group multiple names from the same module on one `from … import (…)` line.
Use parentheses when the line would exceed 88 characters.
- Never use wildcard imports (`from module import *`).

### Temporal workflow imports

Modules that import non-deterministic or third-party code inside a workflow file must
use the `workflow.unsafe.imports_passed_through()` guard:

```python
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from datashare_python.utils import WorkflowWithProgress
    from .activities import TranslateDocuments
    from .objects import TranslationRequest
```

---

## Type Annotations

Annotate all public functions and methods. Use PEP 604 union syntax (`X | Y`) instead of
`Optional[X]` or `Union[X, Y]`.

```python
# Yes
def find_worker(worker_id: str | None = None) -> Worker | None: ...

# No
from typing import Optional, Union

def find_worker(worker_id: Optional[str] = None) -> Optional[Worker]: ...
```

**Rules:**

- Do not annotate `self` or `cls`.
- Do not annotate `__init__` return types (covered by `ANN204` ignore).
- `*args` and `**kwargs` annotations are optional (covered by `ANN002`/`ANN003`
ignores).
- Use `Any` only when the type genuinely cannot be expressed. Do not use it to suppress
annotation errors.
- Prefer abstract types from `collections.abc` over concrete ones: `Callable` not
`function`, `Iterable` not `list` in signatures.
- Use `Self` (from `typing`) for class methods that return the instance.

```python
from typing import Self

class Document(DatashareModel):
    @classmethod
    def from_es(cls, es_doc: dict) -> Self:
        ...
```

### Type aliases

Define complex or reusable type aliases at module level, close to the top of the file:

```python
DependencyLabel = str | None
DependencySetup = Callable[..., None]
FactoryReturnType = AbstractContextManager | AbstractAsyncContextManager | None
```

---

## Docstrings

Use **reStructuredText (reST) / Sphinx-style** docstrings. The summary line should fit
on one line. Use the extended body for additional detail when needed.

```python
def find_device(device_name: str = CPU) -> str:
    """Check if a device is available; if not, return cpu.

    :param device_name: The name of the device to check (e.g. "mps", "cuda").
    :return: The requested device name if available, otherwise "cpu".
    """
```

Fields to include when applicable:

- **`:param name:`** — One entry per parameter (except `self`/`cls`). Describe the
parameter, not its type. The type is already in the annotation.
- **`:return:`** — What the function returns; omit when returning `None`.
- **`:raises ExcType:`** — Exceptions the caller should handle.

**Rules:**

- Short class docstrings can be a single line: `"""Contains activity definitions and
model references"""`
- A docstring is not required for every private helper, but any function with
non-obvious behavior should have one.
- Don't end single-line summary docstrings with a period (`"""Do this""""` not
`"""This.""""`)
- Do not repeat information already in type annotations.

---

## Classes

### Pydantic models

Use Pydantic `BaseModel` (or project-specific subclasses) for data objects,
configuration, and API payloads. Use the project base classes rather than inheriting
from raw Pydantic models where applicable:

```python
from datashare_python.objects import BaseModel, BasePayload, DatashareModel

class WorkerResponse(BasePayload):
    status: str
    error: str | None = None
    results: int = 0
```

Use `Field()` when you need defaults, aliases, or frozen fields:

```python
class Task(DatashareModel):
    type: str = Field(frozen=True, alias="@type", default="Task")
    state: TaskState = TaskState.CREATED
    created_at: datetime = Field(default_factory=_datetime_now)
```

### Protocol classes

Define structural interfaces with `Protocol` rather than abstract base classes when you
only care about the callable signature:

```python
from collections.abc import Callable
from typing import Protocol

class ProgressRateHandler(Protocol):
    async def __call__(self, progress_rate: float) -> None: ...
```

### Enums

Use `StrEnum` (Python 3.11+) with `@unique` for string-valued enums:

```python
from enum import StrEnum, unique

@unique
class TaskState(StrEnum):
    CREATED = "CREATED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    ERROR = "ERROR"
    DONE = "DONE"
    CANCELLED = "CANCELLED"
```

### Abstract base classes

Use `ABC` for base classes that must not be instantiated directly. Prefer `...`
(Ellipsis) over `pass` for empty abstract class bodies:

```python
from abc import ABC

class DatashareError(Exception, ABC): ...
```

---

## Functions and Methods

- Keep functions focused on a single responsibility.
- Boolean parameters must be keyword-only (enforced by `FBT`):

```python
# Yes
def activity_defn(name: str, *, no_thread_cancel_exception: bool = False) -> ...: ...

# No — positional boolean is ambiguous at the call site
def activity_defn(name: str, no_thread_cancel_exception: bool = False) -> ...: ...
```

- Use `*` to separate positional from keyword-only arguments when a function has many
parameters:

```python
async def progress_handler(
    progress: float,
    handle: WorkflowHandler,
    *,
    activity_id: str,
    run_id: str,
    weight: float = 1.0,
) -> None:
```

- Avoid mutable default arguments. Use `None` and initialize inside the function:

```python
# Yes
def with_retryables(retryables: set[type[Exception]] | None = None) -> ...:
    if retryables is None:
        retryables = set()

# No
def with_retryables(retryables: set[type[Exception]] = set()) -> ...: ...
```

### Decorators

Stack decorators from inner- to outermost. Use `@functools.wraps` when writing
wrapper functions to preserve the wrapped function's metadata:

```python
from functools import wraps

def with_retryables(...):
    def decorator(activity_fn):
        @wraps(activity_fn)
        async def wrapper(*args, **kwargs):
            ...
        return wrapper
    return decorator
```

---

## Exception Handling

### Custom exceptions

Inherit from a project base error class. Use multiple inheritance to subclass a
built-in exception type so callers can catch either:

```python
from abc import ABC

class DatashareError(Exception, ABC): ...

class UnknownTask(DatashareError, ValueError):
    def __init__(self, task_id: str, worker_id: str | None = None):
        msg = f'Unknown task "{task_id}"'
        if worker_id is not None:
            msg += f" for {worker_id}"
        super().__init__(msg)
```

Build the message string separately and pass it to `super().__init__()`.
This keeps `__init__` readable and avoids inline string construction inside `super()`.

### Catching exceptions

- Always catch the most specific exception type possible.
- Use `raise ... from e` to preserve the exception chain:

```python
try:
    return await activity_fn(*args, **kwargs)
except retryables:
    raise
except Exception as e:
    raise fatal_error_from_exception(e) from e
```

- Avoid using bare `except:` (catches `SystemExit`, `KeyboardInterrupt`, etc.).
- Catching `Exception` broadly is only acceptable when at an isolation boundary (e.g., a
top-level activity wrapper that converts unknown exceptions to non-retryable Temporal
errors). Mark such catch-alls with `# noqa: BLE001` and a comment explaining the intent.
- Minimize the code inside `try` blocks to avoid masking unrelated errors.

### Raising errors

Raise errors on invalid input at the top of a function, before doing any work:

```python
def to_raw_progress(progress: ProgressRateHandler, max_progress: int) -> RawProgressHandler:
    if not max_progress > 0:
        raise ValueError("max_progress must be > 0")
    ...
```

---

## Logging

Declare a module-level logger using `__name__`:

```python
import logging

logger = logging.getLogger(__name__)
```

**Use `%`-style format strings** in log calls, not f-strings. This defers string
interpolation until the log record is actually emitted, and is enforced by the `G`
ruleset:

```python
# Yes
logger.info("Processing %d documents in project %s", count, project)
logger.warning("Retrying after error: %s", exc)

# No — eagerly formats the string even if the log level is suppressed
logger.info(f"Processing {count} documents in project {project}")
```

Inside Temporal workflows, use `workflow.logger`:

```python
workflow.logger.info("Preprocessing complete")
workflow.logger.debug("recording progress signal %s", signal)
```

---

## Async / Await

Datashare and adjacent applications are async-first. Follow these patterns:

### Async context managers

Use `@asynccontextmanager` for resource management:

```python
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator

@asynccontextmanager
async def _put(self, url: str, *, data: Any = None) -> AsyncGenerator[ClientResponse, None]:
    async with self._session.put(url, data=data) as res:
        _raise_for_status(res)
        yield res
```

### Concurrent execution

Use `asyncio.gather` for independent concurrent tasks:

```python
results = await asyncio.gather(
    *[
        workflow.execute_activity_method(
            ASRActivities.preprocess,
            args=[batch],
            task_queue=TaskQueues.CPU,
            start_to_close_timeout=timedelta(minutes=10),
        )
        for batch in batches
    ]
)
```

### Async generators

Annotate async generators with `AsyncGenerator[YieldType, SendType]` or `AsyncIterator[YieldType]`:

```python
from collections.abc import AsyncIterator

async def async_batches(
    iterable: AsyncIterable[T], batch_size: int
) -> AsyncIterator[tuple[T, ...]]:
    ...
    yield tuple(batch)
```

### Temporal activity executors

Synchronous activities (run in a `ThreadPoolExecutor`) that need to call async code
should use `asyncio.run_coroutine_threadsafe` with the worker's event loop, not
`asyncio.run()`, to prevent double-loading context:

```python
asyncio.run_coroutine_threadsafe(handler(0.0), self._event_loop).result()
```

---

## Constants

Constants belong in a `constants.py` file at the package root. Group related constants
together with a blank line between groups. Prefer module-level constants over class
attributes for values that aren't logically tied to a class:

```python
# translation_worker/constants.py

TRANSLATION_TASK_NAME = "translation"
TRANSLATION_WORKER_NAME = "translation-worker"

TRANSLATION_CPU_TASK_QUEUE = "translation-cpu-tasks"
TRANSLATION_GPU_TASK_QUEUE = "translation-gpu-tasks"
TRANSLATION_WORKFLOW_NAME = "translation"
```

Computed or derived constants are fine:

```python
_ONE_MINUTE = 60
_TEN_MINUTES = _ONE_MINUTE * 10
```

---

## Comments

- Write comments to explain *why*, not *what*. Assume the reader knows Python.
- Use complete sentences. Capitalize the first word.
- Inline comments need at least two spaces of separation from the code.
- Use `# TODO:` for deferred work. Include a brief explanation of what needs doing:

```python
# TODO: Need to break this out into a separate module
```

- Use `# noqa: RULE_CODE` to suppress a specific ruff rule locally. Always include a
short comment explaining the suppression on the same line or just above it:

```python
except Exception as e:  # noqa: BLE001 — top-level boundary; converts to
# non-retryable error
```

---

## Strings

- Use double quotes consistently (ruff format enforces this).
- Use f-strings for string interpolation in general code:

```python
msg = f'Unknown task "{task_id}" for {worker_id}'
```

- **Don't use f-strings in logging calls** (see [Logging](#logging)).
- Build long strings by concatenating inside parentheses rather than with `\`
continuation or `+` in a loop.

---

## Testing

### Structure

- Tests live in a `tests/` directory at the package root.
- Shared fixtures go in `conftest.py`.
- Integration tests (those that require external services) are marked
`@pytest.mark.integration`.
- End-to-end tests that require a running worker are marked `@pytest.mark.e2e`.

### Fixtures

Use `scope="session"` for expensive resources (Temporal clients, running workers) and
`scope="function"` (the default) for cheap, isolated state:

```python
@pytest.fixture(scope="session")
async def cpu_worker(
    test_temporal_client_session: TemporalClient,
    event_loop: asyncio.AbstractEventLoop,
) -> AsyncGenerator[None, None]:
    worker = Worker(test_temporal_client_session, ...)
    async with worker:
        t = asyncio.create_task(worker.run())
        try:
            yield
        finally:
            t.cancel()
```

### Async tests

All async tests run automatically under `asyncio_mode = "auto"` (configured in
`pyproject.toml`). No explicit `@pytest.mark.asyncio` decorator is needed.

