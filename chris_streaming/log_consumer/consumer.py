"""
Kafka consumer for job-logs.

Reads log events in batches, writes to OpenSearch (bulk API), and
publishes to Redis Pub/Sub for real-time streaming.
"""

from __future__ import annotations

import asyncio
import logging
import time

from aiokafka import AIOKafkaConsumer

from chris_streaming.common.schemas import LogEvent
from .opensearch_writer import OpenSearchWriter
from .redis_publisher import LogRedisPublisher

logger = logging.getLogger(__name__)


class LogEventConsumer:
    """Batched consumer for log events."""

    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        opensearch: OpenSearchWriter,
        redis_pub: LogRedisPublisher,
        batch_max_size: int = 200,
        batch_max_wait_seconds: float = 2.0,
    ):
        self._consumer = consumer
        self._opensearch = opensearch
        self._redis_pub = redis_pub
        self._batch_max_size = batch_max_size
        self._batch_max_wait = batch_max_wait_seconds

    async def run(self) -> None:
        """
        Main consume loop with batching.

        Accumulates messages until batch_max_size or batch_max_wait_seconds,
        then flushes to OpenSearch and Redis. Commits offsets after flush.
        """
        batch: list[LogEvent] = []
        last_flush = time.monotonic()

        while True:
            # Poll with short timeout for batching
            records = await self._consumer.getmany(
                timeout_ms=int(self._batch_max_wait * 1000),
                max_records=self._batch_max_size,
            )

            for tp, messages in records.items():
                for msg in messages:
                    try:
                        event = LogEvent.deserialize_from_kafka(msg.value)
                        batch.append(event)
                    except Exception as e:
                        logger.error("Failed to deserialize log at offset %d: %s", msg.offset, e)

            now = time.monotonic()
            should_flush = (
                len(batch) >= self._batch_max_size
                or (batch and (now - last_flush) >= self._batch_max_wait)
            )

            if should_flush and batch:
                await self._flush(batch)
                await self._consumer.commit()
                batch = []
                last_flush = time.monotonic()

    async def _flush(self, batch: list[LogEvent]) -> None:
        """Write batch to OpenSearch and publish to Redis."""
        # Write to OpenSearch (durable storage)
        try:
            await self._opensearch.write_batch(batch)
        except Exception as e:
            logger.error("OpenSearch batch write failed (%d events): %s", len(batch), e)

        # Publish to Redis Pub/Sub (real-time fan-out, best-effort)
        try:
            await self._redis_pub.publish_batch(batch)
        except Exception as e:
            logger.warning("Redis publish failed (%d events): %s", len(batch), e)

        logger.info("Flushed batch of %d log events", len(batch))

    async def close(self) -> None:
        await self._consumer.stop()
