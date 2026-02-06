import logging
from asyncio import gather
from dataclasses import asdict
from datetime import timedelta

from more_itertools import flatten
from temporalio import workflow
from temporalio.common import RetryPolicy

from icij_worker.worker.temporal.asr.helpers import ASRResponse, ASRInputs
from icij_worker.worker.temporal.asr.constants import _TEN_MINUTES
from icij_worker.worker.temporal.constants import RESPONSE_SUCCESS, RESPONSE_ERROR

with workflow.unsafe.imports_passed_through():
    from icij_worker.worker.temporal.asr.activities import ASRActivities

logging.basicConfig(level=logging.INFO)

LOGGER = logging.getLogger(__name__)


# TODO: Figure out which modules are violating sandbox restrictions
#  and grant a limited passthrough
@workflow.defn(sandboxed=False)
class ASRWorkflow:
    """ASR workflow definition"""

    @workflow.run
    async def run(self, inputs: ASRInputs) -> ASRResponse:
        """Run ASR workflow

        :param inputs: ASRInputs
        :return: ASRResponse
        """
        retry_policy = RetryPolicy(
            maximum_attempts=3,
            maximum_interval=timedelta(seconds=2),
            non_retryable_error_types=["TypeError"],
        )

        try:
            # Preprocessing
            # TODO: Let caul.preprocessing.ParakeetProcessor.process accept
            #  a batch_size argument to limit the number of items in an
            #  individual batch
            # TODO: Replace naive batching with buffering

            preprocessed_batches = await gather(
                *[
                    workflow.execute_activity_method(
                        ASRActivities.preprocess,
                        inputs.file_paths[
                            offset : offset + inputs.pipeline.preprocessing.batch_size
                        ],
                        start_to_close_timeout=timedelta(seconds=_TEN_MINUTES),
                        # heartbeat_timeout=timedelta(seconds=_ONE_MINUTE),
                        retry_policy=retry_policy,
                    )
                    for offset in range(
                        0,
                        len(inputs.file_paths),
                        inputs.pipeline.preprocessing.batch_size,
                    )
                ]
            )

            LOGGER.info("Preprocessing complete")

            # Inference
            inference_results = [
                await gather(
                    *[
                        workflow.execute_activity_method(
                            ASRActivities.infer,
                            inner_batch,
                            start_to_close_timeout=timedelta(seconds=_TEN_MINUTES),
                            # heartbeat_timeout=timedelta(seconds=_ONE_MINUTE),
                            retry_policy=retry_policy,
                        )
                        for inner_batch in outer_batch
                    ]
                )
                for outer_batch in preprocessed_batches
            ]

            LOGGER.info("Inference complete")

            # Postprocessing
            transcriptions = await gather(
                *[
                    workflow.execute_activity_method(
                        ASRActivities.postprocess,
                        flatten(inference_result_batch),
                        start_to_close_timeout=timedelta(seconds=_TEN_MINUTES),
                        # heartbeat_timeout=timedelta(seconds=_ONE_MINUTE),
                        retry_policy=retry_policy,
                    )
                    for inference_result_batch in inference_results
                ]
            )

            serialized_transcriptions = []

            # drop unnecessary fields, serialize
            for transcription in flatten(transcriptions):
                transcription = asdict(transcription)

                del transcription["input_ordering"]

                serialized_transcriptions.append(transcription)
                LOGGER.info(transcription)

            LOGGER.info("Postprocessing complete")

            # TODO: Output formatting; do we want to keep PreprocessedInput metadata and remap
            #  results to it?
            return ASRResponse(
                status=RESPONSE_SUCCESS, transcriptions=serialized_transcriptions
            )
        except ValueError as e:
            LOGGER.error(e)
            return ASRResponse(status=RESPONSE_ERROR, error=str(e))
