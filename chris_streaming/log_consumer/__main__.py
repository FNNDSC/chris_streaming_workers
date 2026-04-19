"""
Entry point for the Log Consumer service.

    python -m chris_streaming.log_consumer

Reads log events from the sharded ``stream:job-logs:{shard}`` Redis Streams,
batches them, writes to OpenSearch, and publishes to Redis Pub/Sub for
real-time streaming.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket

import structlog

from chris_streaming.common.redis_stream import create_redis_client
from chris_streaming.common.settings import LogConsumerSettings
from .consumer import LogEventConsumer
from .opensearch_writer import OpenSearchWriter
from .redis_publisher import LogRedisPublisher

logger = structlog.get_logger()


def _consumer_name() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


async def main() -> None:
    settings = LogConsumerSettings()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info(
        "Starting Log Consumer",
        group=settings.log_consumer_group,
        stream_base=settings.stream_logs_base,
        num_shards=settings.stream_num_shards,
    )

    # OpenSearch writer
    opensearch = OpenSearchWriter(
        settings.opensearch_url, settings.opensearch_index_prefix,
    )
    await opensearch.connect()

    # Redis Pub/Sub publisher (real-time fan-out)
    redis_pub = LogRedisPublisher(settings.redis_url)
    await redis_pub.connect()

    # Redis client for Streams (decode_responses=False for binary payloads)
    redis = await create_redis_client(settings.redis_url)

    consumer = LogEventConsumer(
        redis=redis,
        base_stream=settings.stream_logs_base,
        num_shards=settings.stream_num_shards,
        group_name=settings.log_consumer_group,
        consumer_name=_consumer_name(),
        opensearch=opensearch,
        redis_pub=redis_pub,
        batch_max_size=settings.batch_max_size,
        batch_max_wait_seconds=settings.batch_max_wait_seconds,
    )

    # Graceful shutdown
    shutdown_event = asyncio.Event()

    def _shutdown(sig):
        logger.info("Received signal %s, shutting down", sig)
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _shutdown, sig)

    consume_task = asyncio.create_task(consumer.run())
    await shutdown_event.wait()
    await consumer.stop()
    consume_task.cancel()

    try:
        await consume_task
    except asyncio.CancelledError:
        pass

    await redis_pub.close()
    await opensearch.close()
    await redis.close()
    logger.info("Log Consumer stopped")


if __name__ == "__main__":
    asyncio.run(main())
