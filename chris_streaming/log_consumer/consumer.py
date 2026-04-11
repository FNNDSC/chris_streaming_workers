"""
Kafka consumer for job-logs.

Reads log events in batches, writes to OpenSearch (bulk API), and
publishes to Redis Pub/Sub for real-time streaming.

When an EOS (End-of-Stream) marker is received, flushes the current
batch immediately and sets a Redis key to signal that all logs for
that container have been written to OpenSearch.
"""

from __future__ import annotations

import logging
import time

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer

from chris_streaming.common.schemas import LogEvent
from .opensearch_writer import OpenSearchWriter
from .redis_publisher import LogRedisPublisher

logger = logging.getLogger(__name__)

# TTL for logs_flushed Redis keys (1 hour)
_LOGS_FLUSHED_TTL = 3600


class LogEventConsumer:
    """Batched consumer for log events with EOS marker support."""

    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        opensearch: OpenSearchWriter,
        redis_pub: LogRedisPublisher,
        redis_url: str,
        batch_max_size: int = 200,
        batch_max_wait_seconds: float = 2.0,
    ):
        self._consumer = consumer
        self._opensearch = opensearch
        self._redis_pub = redis_pub
        self._redis_url = redis_url
        self._redis: aioredis.Redis | None = None
        self._batch_max_size = batch_max_size
        self._batch_max_wait = batch_max_wait_seconds

    async def connect_redis(self) -> None:
        self._redis = aioredis.from_url(self._redis_url, decode_responses=True)

    async def run(self) -> None:
        """
        Main consume loop with batching.

        Accumulates messages until batch_max_size or batch_max_wait_seconds,
        then flushes to OpenSearch and Redis. Commits offsets only after a
        successful OpenSearch write. EOS markers trigger an immediate flush
        and set a Redis key to signal log completion.
        """
        batch: list[LogEvent] = []
        eos_signals: list[tuple[str, str]] = []
        last_flush = time.monotonic()

        while True:
            # Poll with short timeout for batching
            records = await self._consumer.getmany(
                timeout_ms=int(self._batch_max_wait * 1000),
                max_records=self._batch_max_size,
            )

            force_flush = False
            for tp, messages in records.items():
                for msg in messages:
                    try:
                        event = LogEvent.deserialize_from_kafka(msg.value)
                    except Exception as e:
                        logger.error("Failed to deserialize log at offset %d: %s", msg.offset, e)
                        continue

                    if event.eos:
                        # EOS marker: trigger immediate flush, don't add to batch
                        eos_signals.append((event.job_id, event.job_type.value))
                        force_flush = True
                        logger.info(
                            "Received EOS marker: job=%s type=%s",
                            event.job_id, event.job_type.value,
                        )
                    else:
                        batch.append(event)

            now = time.monotonic()
            should_flush = (
                force_flush
                or len(batch) >= self._batch_max_size
                or (batch and (now - last_flush) >= self._batch_max_wait)
            )

            if should_flush:
                if batch:
                    # Sort by timestamp so OpenSearch insertion order and the
                    # live Redis fan-out are both chronological. Fluent Bit
                    # tails multiple container files concurrently and does
                    # not guarantee temporal ordering across them, so logs
                    # from e.g. the copy and plugin containers can arrive
                    # interleaved within one Kafka batch.
                    batch.sort(key=lambda e: e.timestamp)
                    try:
                        await self._flush(batch)
                    except Exception as e:
                        logger.error(
                            "Flush failed (%d events), will retry: %s",
                            len(batch), e,
                        )
                        # Don't commit — messages will be re-delivered
                        eos_signals.clear()
                        continue

                # Signal log completion for any EOS markers in this batch
                for job_id, job_type in eos_signals:
                    await self._set_logs_flushed(job_id, job_type)
                eos_signals.clear()

                await self._consumer.commit()
                batch = []
                last_flush = time.monotonic()

    async def _flush(self, batch: list[LogEvent]) -> None:
        """Write batch to OpenSearch and publish to Redis.

        Raises on OpenSearch failure so the caller can skip the offset commit.
        """
        # Write to OpenSearch (durable storage) — must succeed
        await self._opensearch.write_batch(batch)

        # Publish to Redis Pub/Sub (real-time fan-out, best-effort)
        try:
            await self._redis_pub.publish_batch(batch)
        except Exception as e:
            logger.warning("Redis publish failed (%d events): %s", len(batch), e)

        logger.info("Flushed batch of %d log events", len(batch))

    async def _set_logs_flushed(self, job_id: str, job_type: str) -> None:
        """Set Redis key to signal that all logs for a container are in OpenSearch."""
        key = f"job:{job_id}:{job_type}:logs_flushed"
        try:
            await self._redis.set(key, "1", ex=_LOGS_FLUSHED_TTL)
            logger.info("Set logs_flushed: %s", key)
        except Exception as e:
            logger.error("Failed to set logs_flushed key %s: %s", key, e)

    async def close(self) -> None:
        await self._consumer.stop()
        if self._redis:
            await self._redis.close()
