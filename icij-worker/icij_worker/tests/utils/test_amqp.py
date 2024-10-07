import asyncio

from aio_pika.abc import AbstractRobustQueue

from icij_worker.task_storage.postgres.postgres import logger
from icij_worker.utils.amqp import AMQPManagementClient, AMQPPolicy, ApplyTo


async def test_amqp_management_client_set_policy(
    management_client: AMQPManagementClient,
    rabbit_mq,  # pylint: disable=unused-argument
):
    # Given
    client = management_client
    policy = AMQPPolicy(
        name="test-policy",
        priority=1,
        definition={"max-length": 6},
        pattern="test-pattern-*",
        apply_to=ApplyTo.QUEUES,
    )

    # When
    async with client:
        existing_policies = await client.list_policies()
        assert not existing_policies
        await client.set_policy(policy=policy)
        # Then
        existing_policies = await client.list_policies()
        assert len(existing_policies) == 1
        policy = existing_policies[0]
        expected_policy = {
            "apply-to": "queues",
            "definition": {"max-length": 6},
            "name": "test-policy",
            "pattern": "test-pattern-*",
            "priority": 1,
            "vhost": "/",
        }
        assert policy == expected_policy


async def _drain_queue(queue: AbstractRobustQueue):
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            logger.info("received message: %s", message.body.decode())
            while True:
                await asyncio.sleep(0.1)
