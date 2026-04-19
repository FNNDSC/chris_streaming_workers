"""
Redis Streams producer for status events with deduplication.

Wraps the shared RedisStreamProducer with event-specific logic:
  - Deterministic event_id for consumer-side dedup
  - Shard routing by job_id (producer side of per-job ordering)
  - In-memory LRU cache as a best-effort producer-side dedup
"""

from __future__ import annotations

import logging
from collections import OrderedDict

from chris_streaming.common.redis_stream import RedisStreamProducer
from chris_streaming.common.schemas import StatusEvent

logger = logging.getLogger(__name__)

# LRU cache size for recent event IDs (dedup window)
_DEDUP_CACHE_SIZE = 10_000


class StatusEventProducer:
    """Produces StatusEvent messages to the job-status Redis Stream."""

    def __init__(self, producer: RedisStreamProducer):
        self._producer = producer
        self._seen: OrderedDict[str, None] = OrderedDict()

    async def send(self, event: StatusEvent) -> bool:
        """
        Send a status event to the Redis Stream. Returns False if
        deduplicated (already sent in this process' lifetime).

        Dedup is best-effort (in-memory LRU). The consumer handler path
        handles duplicates idempotently via the Postgres upsert's
        ``WHERE updated_at < EXCLUDED.updated_at`` guard.
        """
        if event.event_id in self._seen:
            logger.debug("Dedup: skipping event %s for job %s",
                         event.event_id, event.job_id)
            return False

        value = event.serialize()
        entry_id = await self._producer.xadd(event.job_id, value)
        logger.info(
            "Produced status event: job=%s type=%s status=%s entry=%s",
            event.job_id, event.job_type.value, event.status.value, entry_id,
        )

        self._seen[event.event_id] = None
        if len(self._seen) > _DEDUP_CACHE_SIZE:
            self._seen.popitem(last=False)

        return True

    async def close(self) -> None:
        # Producer is owned by the caller (event_forwarder/__main__).
        pass
