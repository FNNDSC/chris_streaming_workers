"""
Kafka consumer for job-status-events.

Reads events, publishes to Redis Pub/Sub, and schedules Celery tasks for
DB persistence and terminal status confirmation.
Failed messages are sent to the dead-letter topic after max retries.
"""

from __future__ import annotations

import logging

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from chris_streaming.common.schemas import StatusEvent
from .notifier import StatusNotifier

logger = logging.getLogger(__name__)


class StatusEventConsumer:
    """Consumes status events from Kafka and processes them."""

    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        notifier: StatusNotifier,
        dlq_producer: AIOKafkaProducer,
        dlq_topic: str,
        max_retries: int = 3,
    ):
        self._consumer = consumer
        self._notifier = notifier
        self._dlq_producer = dlq_producer
        self._dlq_topic = dlq_topic
        self._max_retries = max_retries

    async def run(self) -> None:
        """
        Main consume loop. Processes messages one at a time with manual
        offset commit after successful processing.
        """
        async for msg in self._consumer:
            try:
                event = StatusEvent.deserialize_from_kafka(msg.value)
            except Exception as e:
                logger.error("Failed to deserialize message at offset %d: %s", msg.offset, e)
                await self._send_to_dlq(msg.value, str(e))
                await self._consumer.commit()
                continue

            success = await self._process_with_retry(event, msg.value)
            if success:
                await self._consumer.commit()
            else:
                await self._send_to_dlq(msg.value, "max retries exceeded")
                await self._consumer.commit()

    async def _process_with_retry(self, event: StatusEvent, raw_value: bytes) -> bool:
        """Process a single event with retry logic."""
        for attempt in range(1, self._max_retries + 1):
            try:
                await self._notifier.notify(event)
                logger.info(
                    "Processed event: job=%s type=%s status=%s",
                    event.job_id, event.job_type.value, event.status.value,
                )
                return True
            except Exception as e:
                logger.warning(
                    "Processing attempt %d/%d failed for job=%s: %s",
                    attempt, self._max_retries, event.job_id, e,
                )
        return False

    async def _send_to_dlq(self, raw_value: bytes, reason: str) -> None:
        """Send a failed message to the dead-letter topic."""
        try:
            await self._dlq_producer.send_and_wait(
                self._dlq_topic,
                value=raw_value,
                headers=[("dlq_reason", reason.encode("utf-8"))],
            )
            logger.warning("Sent message to DLQ: %s", reason)
        except Exception as e:
            logger.error("Failed to send to DLQ: %s", e)

    async def close(self) -> None:
        await self._consumer.stop()
        await self._dlq_producer.stop()
