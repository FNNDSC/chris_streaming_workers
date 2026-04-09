"""
Kafka producer for status events with deduplication.

Wraps the shared Kafka producer factory with event-specific logic:
  - Deterministic event_id for consumer-side dedup
  - Kafka key = job_id for partition ordering
  - Idempotent producer (network-level exactly-once)
"""

from __future__ import annotations

import logging
from collections import OrderedDict

from aiokafka import AIOKafkaProducer

from chris_streaming.common.schemas import StatusEvent, kafka_key_for_job

logger = logging.getLogger(__name__)

# LRU cache size for recent event IDs (dedup window)
_DEDUP_CACHE_SIZE = 10_000


class StatusEventProducer:
    """Produces StatusEvent messages to the job-status-events Kafka topic."""

    def __init__(self, producer: AIOKafkaProducer, topic: str):
        self._producer = producer
        self._topic = topic
        self._seen: OrderedDict[str, None] = OrderedDict()

    async def send(self, event: StatusEvent) -> bool:
        """
        Send a status event to Kafka. Returns False if deduplicated (already sent).

        Deduplication is best-effort (in-memory LRU). The consumer should also
        handle duplicates idempotently via upsert semantics.
        """
        if event.event_id in self._seen:
            logger.debug("Dedup: skipping event %s for job %s", event.event_id, event.job_id)
            return False

        key = kafka_key_for_job(event.job_id)
        value = event.serialize_for_kafka()

        await self._producer.send_and_wait(self._topic, value=value, key=key)
        logger.info(
            "Produced status event: job=%s type=%s status=%s",
            event.job_id, event.job_type.value, event.status.value,
        )

        # Track in LRU
        self._seen[event.event_id] = None
        if len(self._seen) > _DEDUP_CACHE_SIZE:
            self._seen.popitem(last=False)

        return True

    async def close(self) -> None:
        await self._producer.stop()
