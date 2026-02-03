import asyncio
import traceback
from dataclasses import asdict

from temporalio.client import Client, WorkflowFailureError

from icij_worker.worker.temporal.asr.constants import ASR_TASK_QUEUE
from icij_worker.worker.temporal.asr.helpers import ASRInputs, PreprocessingConfig, \
    ASRPipelineConfig
from icij_worker.worker.temporal.asr.workflows import ASRWorkflow


async def main() -> None:
    # Create client connected to server at the given address
    client: Client = await Client.connect("localhost:7233")

    inputs: ASRInputs = ASRInputs(
        file_paths=["tests/resources/asr_test.wav"],
        pipeline=ASRPipelineConfig()
    )

    try:
        result = await client.execute_workflow(
            ASRWorkflow.run,
            inputs,
            id="test-001",
            task_queue=ASR_TASK_QUEUE,
        )

        print(f"Result: {result}")

    except WorkflowFailureError:
        print("Got expected exception: ", traceback.format_exc())


if __name__ == "__main__":
    asyncio.run(main())