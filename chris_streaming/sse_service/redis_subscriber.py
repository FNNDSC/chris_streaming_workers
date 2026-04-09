"""
Async Redis Pub/Sub subscriber for SSE connections.

Each SSE client gets its own subscriber that listens to the relevant
Redis channels for a specific job_id.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Literal

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


async def subscribe_to_job(
    redis_url: str,
    job_id: str,
    event_type: Literal["status", "logs", "all"] = "all",
) -> AsyncIterator[dict]:
    """
    Async generator that yields events from Redis Pub/Sub for a job.

    Channels:
      - job:{job_id}:status  (status events)
      - job:{job_id}:logs    (log events)
      - all: both channels

    Yields parsed JSON dicts. Cleans up the subscription on generator close.
    """
    r = aioredis.from_url(redis_url, decode_responses=True)
    pubsub = r.pubsub()

    channels = []
    if event_type in ("status", "all"):
        channels.append(f"job:{job_id}:status")
    if event_type in ("logs", "all"):
        channels.append(f"job:{job_id}:logs")

    await pubsub.subscribe(*channels)
    logger.debug("Subscribed to %s for job %s", channels, job_id)

    try:
        while True:
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if msg is not None and msg["type"] == "message":
                try:
                    data = json.loads(msg["data"])
                    # Tag with the channel type for the SSE event name
                    channel = msg["channel"]
                    if channel.endswith(":status"):
                        data["_event_type"] = "status"
                    elif channel.endswith(":logs"):
                        data["_event_type"] = "logs"
                    yield data
                except json.JSONDecodeError:
                    logger.warning("Non-JSON message on channel %s", msg["channel"])
            else:
                # Yield control to avoid busy-waiting
                await asyncio.sleep(0.01)
    finally:
        await pubsub.unsubscribe(*channels)
        await pubsub.close()
        await r.close()
