import asyncio
from functools import partial

import pytest
from aio_pika import Message, connect_robust
from aio_pika.abc import AbstractRobustQueue

from icij_worker.task_storage.postgres.postgres import logger
from icij_worker.utils.amqp import (
    AMQPManagementClient,
    AMQPPolicy,
    ApplyTo,
    parse_consumer_timeout,
    RobustConnection,
)


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


def _timeout_cb(_: any, exc: BaseException, running_task: asyncio.Task):
    timeout_exc = parse_consumer_timeout(exc)
    if timeout_exc is not None:
        running_task.cancel()


async def test_robust_channel_should_handle_consumer_timeout(rabbit_mq_session: str):
    # Given
    conn = await connect_robust(rabbit_mq_session, connection_class=RobustConnection)
    async with conn:
        channel = await conn.channel()
        arguments = {"x-consumer-timeout": 1 * 1000}
        # When
        queue_name = "short-queue"
        queue = await channel.declare_queue(
            queue_name, arguments=arguments, auto_delete=True
        )
        await channel.default_exchange.publish(
            Message(body="wait forever please".encode()), routing_key=queue_name
        )
        task = asyncio.create_task(_drain_queue(queue))
        timeout_cb = partial(_timeout_cb, running_task=task)
        channel.close_callbacks.add(timeout_cb)
        with pytest.raises(asyncio.CancelledError):
            await task
        assert channel.is_closed
