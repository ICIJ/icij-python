import asyncio
import os
import socket
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from temporalio import workflow

from icij_worker.worker.temporal.asr.constants import ASR_TASK_QUEUE, ASR_WORKER_NAME
from icij_worker.worker.temporal.asr.workflows import ASRWorkflow
from icij_worker.worker.temporal.constants import DEFAULT_TARGET_HOST

with workflow.unsafe.imports_passed_through():
    from icij_worker.worker.temporal.asr.activities import ASRActivities

interrupt_event = asyncio.Event()


async def asr_worker() -> None:
    """ASR worker task"""
    target_host = DEFAULT_TARGET_HOST
    client = await Client.connect(target_host, namespace="default")
    identity = f"{ASR_WORKER_NAME}:{os.getpid()}@{socket.gethostname()}"
    activities = ASRActivities()
    worker = Worker(
        client=client,
        task_queue=ASR_TASK_QUEUE,
        workflows=[ASRWorkflow],
        activities=[activities.preprocess, activities.infer, activities.postprocess],
        activity_executor=ThreadPoolExecutor(),
        identity=identity,
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(asr_worker())