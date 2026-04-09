"""
Entry point for the Log Consumer service.

    python -m chris_streaming.log_consumer

Reads job-logs from Kafka in batches, writes to OpenSearch, and publishes
to Redis Pub/Sub for real-time streaming.
"""

from __future__ import annotations

import asyncio
import logging
import signal

import structlog

from chris_streaming.common.kafka import create_consumer
from chris_streaming.common.settings import LogConsumerSettings
from .consumer import LogEventConsumer
from .opensearch_writer import OpenSearchWriter
from .redis_publisher import LogRedisPublisher

logger = structlog.get_logger()


async def main() -> None:
    settings = LogConsumerSettings()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info(
        "Starting Log Consumer",
        group=settings.kafka_consumer_group,
        topic=settings.kafka_topic_logs,
    )

    # Initialize components
    opensearch = OpenSearchWriter(settings.opensearch_url, settings.opensearch_index_prefix)
    await opensearch.connect()

    redis_pub = LogRedisPublisher(settings.redis_url)
    await redis_pub.connect()

    kafka_consumer = await create_consumer(
        settings,
        topic=settings.kafka_topic_logs,
        group_id=settings.kafka_consumer_group,
        max_poll_records=settings.batch_max_size,
    )

    consumer = LogEventConsumer(
        consumer=kafka_consumer,
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
    consume_task.cancel()

    try:
        await consume_task
    except asyncio.CancelledError:
        pass

    await consumer.close()
    await redis_pub.close()
    await opensearch.close()
    logger.info("Log Consumer stopped")


if __name__ == "__main__":
    asyncio.run(main())
