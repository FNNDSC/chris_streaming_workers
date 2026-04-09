"""
Redis Pub/Sub publisher for real-time log fan-out.

Publishes each log line to a per-job channel so SSE subscribers
only receive logs for the jobs they're watching.

Channel pattern: job:{job_id}:logs
"""

from __future__ import annotations

import logging

import redis.asyncio as aioredis

from chris_streaming.common.schemas import LogEvent

logger = logging.getLogger(__name__)


class LogRedisPublisher:
    """Publishes log events to Redis Pub/Sub channels."""

    def __init__(self, redis_url: str):
        self._redis: aioredis.Redis | None = None
        self._redis_url = redis_url

    async def connect(self) -> None:
        self._redis = aioredis.from_url(self._redis_url, decode_responses=True)
        await self._redis.ping()
        logger.info("Redis connected for log publishing")

    async def publish_batch(self, events: list[LogEvent]) -> None:
        """Publish a batch of log events to their respective channels."""
        pipe = self._redis.pipeline()
        for event in events:
            channel = f"job:{event.job_id}:logs"
            pipe.publish(channel, event.model_dump_json())
        await pipe.execute()

    async def close(self) -> None:
        if self._redis:
            await self._redis.close()
