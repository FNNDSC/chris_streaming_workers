"""
Handler-side glue for the Status Consumer.

The underlying XREADGROUP loop and retry/DLQ mechanics live in
``chris_streaming.common.redis_stream``. This module exposes a single
``handle_message`` entry point used as the ShardedConsumer handler and the
PendingReclaimer redelivery callback.

Semantics
---------
- Deserialize the raw bytes into a StatusEvent.
- Call the notifier (schedules a Celery task).
- On success: the caller will XACK.
- On unrecoverable deserialization failure: raise, so the ShardedConsumer
  retries `handler_retries` times and then leaves it in the PEL.
  PendingReclaimer will eventually route it to the DLQ after
  `max_deliveries` delivery attempts.
"""

from __future__ import annotations

import logging

from chris_streaming.common.schemas import StatusEvent
from .notifier import StatusNotifier

logger = logging.getLogger(__name__)


class StatusMessageHandler:
    """Decode a stream entry payload and schedule a Celery task."""

    def __init__(self, notifier: StatusNotifier) -> None:
        self._notifier = notifier

    async def __call__(self, raw: bytes) -> None:
        event = StatusEvent.deserialize(raw)
        await self._notifier.notify(event)
        logger.info(
            "Processed event: job=%s type=%s status=%s",
            event.job_id, event.job_type.value, event.status.value,
        )

    async def reclaim(self, raw: bytes) -> bool:
        """Redelivery callback for PendingReclaimer. Returns True on success."""
        try:
            await self.__call__(raw)
            return True
        except Exception as e:
            logger.warning("Reclaim handler failed: %s", e)
            return False
