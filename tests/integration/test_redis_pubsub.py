"""Integration test: Redis Pub/Sub publish and subscribe."""

import asyncio
import json
import uuid

import pytest
import redis.asyncio as aioredis

from chris_streaming.common.schemas import JobType, LogEvent
from chris_streaming.log_consumer.redis_publisher import LogRedisPublisher

pytestmark = pytest.mark.integration


class TestRedisPublisher:
    async def test_publish_and_subscribe(self, redis_url):
        job_id = f"integ-{uuid.uuid4().hex[:8]}"
        channel = f"job:{job_id}:logs"

        # Set up subscriber first
        r = aioredis.from_url(redis_url, decode_responses=True)
        pubsub = r.pubsub()
        await pubsub.subscribe(channel)

        # Publish via LogRedisPublisher
        pub = LogRedisPublisher(redis_url)
        await pub.connect()
        try:
            events = [
                LogEvent(job_id=job_id, job_type=JobType.plugin, line="hello from integration"),
            ]
            await pub.publish_batch(events)
        finally:
            await pub.close()

        # Read the message
        received = []
        for _ in range(20):
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
            if msg and msg["type"] == "message":
                received.append(json.loads(msg["data"]))
                break
            await asyncio.sleep(0.05)

        await pubsub.unsubscribe(channel)
        await pubsub.close()
        await r.close()

        assert len(received) == 1
        assert received[0]["job_id"] == job_id
        assert received[0]["line"] == "hello from integration"


class TestRedisKeyOperations:
    async def test_set_and_get_logs_flushed(self, redis_url):
        r = aioredis.from_url(redis_url, decode_responses=True)
        job_id = f"integ-{uuid.uuid4().hex[:8]}"
        key = f"job:{job_id}:plugin:logs_flushed"

        try:
            await r.set(key, "1", ex=60)
            val = await r.get(key)
            assert val == "1"
        finally:
            await r.delete(key)
            await r.close()
