"""
Entry point for the Status Consumer service.

    python -m chris_streaming.status_consumer

Reads job-status-events from Kafka, upserts to PostgreSQL, publishes to
Redis Pub/Sub, and schedules Celery tasks for terminal statuses.
"""

from __future__ import annotations

import asyncio
import logging
import signal

import structlog

from chris_streaming.common.kafka import create_consumer, create_producer
from chris_streaming.common.settings import StatusConsumerSettings
from .consumer import StatusEventConsumer
from .db import StatusDB
from .notifier import StatusNotifier

logger = structlog.get_logger()


async def main() -> None:
    settings = StatusConsumerSettings()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info(
        "Starting Status Consumer",
        group=settings.kafka_consumer_group,
        topic=settings.kafka_topic_status,
    )

    # Initialize components
    db = StatusDB(settings.db_dsn)
    await db.connect()

    notifier = StatusNotifier(settings.redis_url, settings.celery_broker_url)
    await notifier.connect()

    kafka_consumer = await create_consumer(
        settings,
        topic=settings.kafka_topic_status,
        group_id=settings.kafka_consumer_group,
    )

    dlq_producer = await create_producer(settings)

    consumer = StatusEventConsumer(
        consumer=kafka_consumer,
        db=db,
        notifier=notifier,
        dlq_producer=dlq_producer,
        dlq_topic=settings.kafka_topic_status_dlq,
        max_retries=settings.max_retries,
    )

    # Graceful shutdown
    shutdown_event = asyncio.Event()

    def _shutdown(sig):
        logger.info("Received signal %s, shutting down", sig)
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _shutdown, sig)

    # Run consumer until shutdown
    consume_task = asyncio.create_task(consumer.run())

    await shutdown_event.wait()
    consume_task.cancel()

    try:
        await consume_task
    except asyncio.CancelledError:
        pass

    await consumer.close()
    await notifier.close()
    await db.close()
    logger.info("Status Consumer stopped")


if __name__ == "__main__":
    asyncio.run(main())
