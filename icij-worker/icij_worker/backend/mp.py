import functools
import logging
import multiprocessing
import os
import signal
from concurrent.futures import (
    CancelledError,
    Future,
    ProcessPoolExecutor,
    as_completed,
)
from contextlib import contextmanager
from typing import Callable, Dict, List, Optional, Set, Tuple

import sys

import icij_worker
from icij_common.logging_utils import setup_loggers
from icij_worker import AsyncApp, Worker, WorkerConfig

logger = logging.getLogger(__name__)

_HANDLED_SIGNALS = [signal.SIGTERM, signal.SIGINT]
if sys.platform == "win32":
    _HANDLED_SIGNALS += [signal.CTRL_C_EVENT, signal.CTRL_BREAK_EVENT]


def _mp_work_forever(
    app: str,
    config: WorkerConfig,
    worker_id: str,
    *,
    worker_extras: Optional[Dict] = None,
    app_deps_extras: Optional[Dict] = None,
):
    setup_loggers(["__main__", icij_worker.__name__])
    try:
        if worker_extras is None:
            worker_extras = dict()
        if app_deps_extras is None:
            app_deps_extras = dict()
        # For multiprocessing, lifespan dependencies need to be run once per process
        app = AsyncApp.load(app)
        deps_cm = app.lifetime_dependencies(
            worker_id=worker_id, worker_config=config, **app_deps_extras
        )
    except BaseException as e:
        msg = "Error occurred during app loading or dependency injection: %s"
        logger.error(msg, e, exc_info=True)
        raise e
    # From now on, the deps_cm should have setup loggers, we can let it log errors
    worker = Worker.from_config(config, app=app, worker_id=worker_id, **worker_extras)
    # This is ugly, but we have to work around the fact that we can't use asyncio code
    # here
    worker.loop.run_until_complete(
        deps_cm.__aenter__()  # pylint: disable=unnecessary-dunder-call
    )
    try:
        worker.work_forever()
    finally:
        worker.info("worker stopped working, tearing down %s dependencies", app.name)
        worker.loop.run_until_complete(deps_cm.__aexit__(*sys.exc_info()))
        worker.info("dependencies closed for %s !", app.name)


def signal_handler(sig_num, *_):
    logger.error(
        "received %s, triggering process executor shutdown !",
        signal.Signals(sig_num).name,
    )


def setup_main_process_signal_handlers():
    for s in _HANDLED_SIGNALS:
        signal.signal(s, signal_handler)


def _get_mp_async_runner(
    app: str,
    config: WorkerConfig,
    n_workers: int,
    *,
    worker_extras: Optional[Dict] = None,
    app_deps_extras: Optional[Dict] = None,
) -> Tuple[ProcessPoolExecutor, List[Tuple[str, Callable]]]:
    # This function is here to avoid code duplication, it will be removed

    # Here we set maxtasksperchild to 1. Each worker has a single never ending task
    # which consists in working forever. Additionally, in some cases using
    # maxtasksperchild=1 seems to help to terminate the worker pull
    # (cpython bug: https://github.com/python/cpython/pull/8009)
    mp_ctx = multiprocessing.get_context("spawn")
    executor = ProcessPoolExecutor(max_workers=n_workers, mp_context=mp_ctx)
    main_process_id = os.getpid()
    # TODO: make this a bit more informative be for instance adding the child process ID
    worker_ids = [f"worker-{main_process_id}-{i}" for i in range(n_workers)]
    kwds = {
        "app": app,
        "config": config,
        "worker_extras": worker_extras,
        "app_deps_extras": app_deps_extras,
    }
    futures = []
    for w_id in worker_ids:
        kwds.update({"worker_id": w_id})
        futures.append(
            (w_id, functools.partial(executor.submit, _mp_work_forever, **kwds))
        )
    return executor, futures


@contextmanager
def _handle_executor_termination(
    executor: ProcessPoolExecutor, futures: Set[Future], handle_signals: bool
):
    try:
        yield
    except KeyboardInterrupt as e:
        if not handle_signals:
            logger.info(
                "received %s, triggering process executor worker shutdown !",
                KeyboardInterrupt.__name__,
            )
        else:
            msg = (
                f"Received {KeyboardInterrupt.__name__} while SIGINT was expected to"
                f" be handled"
            )
            raise RuntimeError(msg) from e
    finally:
        msg = "Worker terminated by the executor"
        exc = CancelledError(msg)
        for f in futures:
            if not f.done():
                f.set_exception(exc)
        for f in futures:
            futures.discard(f)
        logger.info("Sending termination signal to workers (SIGTERM)...")
        executor.shutdown(wait=False, cancel_futures=True)
        logger.info("Terminated worker executor !")


@contextmanager
def run_workers_with_multiprocessing_cm(
    app: str,
    n_workers: int,
    config: WorkerConfig,
    *,
    worker_extras: Optional[Dict] = None,
    app_deps_extras: Optional[Dict] = None,
):
    if n_workers < 1:
        raise ValueError("n_workers must be >=1")
    logger.info("Creating multiprocessing executor with %s workers", n_workers)
    executor, worker_runners = _get_mp_async_runner(
        app,
        config,
        n_workers,
        worker_extras=worker_extras,
        app_deps_extras=app_deps_extras,
    )
    futures = set()
    for w_id, process_runner in worker_runners:
        logger.info("starting worker %s", w_id)
        future = process_runner()
        futures.add(future)
    with _handle_executor_termination(executor, futures, True):
        for _ in as_completed(futures):
            pass


def run_workers_with_multiprocessing(
    app: str,
    n_workers: int,
    config: WorkerConfig,
    *,
    worker_extras: Optional[Dict] = None,
    app_deps_extras: Optional[Dict] = None,
):
    if n_workers < 1:
        raise ValueError("n_workers must be >=1")
    logger.info("Creating multiprocessing executor with %s workers", n_workers)
    executor, worker_runners = _get_mp_async_runner(
        app,
        config,
        n_workers,
        worker_extras=worker_extras,
        app_deps_extras=app_deps_extras,
    )
    setup_main_process_signal_handlers()
    futures = set()
    for w_id, process_runner in worker_runners:
        logger.info("starting worker %s", w_id)
        future = process_runner()
        futures.add(future)
        future.add_done_callback(futures.discard)
    with _handle_executor_termination(executor, futures, True):
        for _ in as_completed(futures):
            pass
