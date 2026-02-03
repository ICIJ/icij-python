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
            preprocessed_batches = await workflow.execute_activity_method(
                ASRActivities.preprocess,
                inputs.file_paths,
                start_to_close_timeout=timedelta(seconds=_TEN_MINUTES),
                #heartbeat_timeout=timedelta(seconds=_ONE_MINUTE),
                retry_policy=retry_policy,
            )

            LOGGER.info("Preprocessing complete")

            # Inference
            transcriptions = await gather(
                *[
                    workflow.execute_activity_method(
                        ASRActivities.infer,
                        batch,
                        start_to_close_timeout=timedelta(seconds=_TEN_MINUTES),
                        #heartbeat_timeout=timedelta(seconds=_ONE_MINUTE),
                        retry_policy=retry_policy,
                    )
                    for batch in preprocessed_batches
                ]
            )

            transcriptions = flatten(transcriptions)

            LOGGER.info("Inference complete")

            # Postprocessing
            results = await workflow.execute_activity_method(
                ASRActivities.postprocess,
                transcriptions,
                start_to_close_timeout=timedelta(seconds=_TEN_MINUTES),
                #heartbeat_timeout=timedelta(seconds=_ONE_MINUTE),
                retry_policy=retry_policy,
            )

            LOGGER.info("Postprocessing complete")

            # TODO: Output formatting; do we want to keep PreprocessedInput metadata and remap
            #  results to it?
            return ASRResponse(status=RESPONSE_SUCCESS, transcriptions=[asdict(r) for r in results])
        except Exception as e:
            LOGGER.error(e)
            return ASRResponse(status=RESPONSE_ERROR, error=str(e))
