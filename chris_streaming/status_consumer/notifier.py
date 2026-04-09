"""
Notification layer: pushes status events to Redis Pub/Sub and schedules
Celery tasks for terminal statuses.

Redis Pub/Sub channels:
  - job:{job_id}:status  -- all status events for a job

Celery tasks:
  - confirm_job_status   -- scheduled when status is terminal
                           (finishedSuccessfully, finishedWithError, undefined)
"""

from __future__ import annotations

import logging

import redis.asyncio as aioredis
from celery import Celery

from chris_streaming.common.schemas import StatusEvent, TERMINAL_STATUSES

logger = logging.getLogger(__name__)


class StatusNotifier:
    """Publishes status events to Redis and schedules Celery confirmation tasks."""

    def __init__(self, redis_url: str, celery_broker_url: str):
        self._redis: aioredis.Redis | None = None
        self._redis_url = redis_url
        self._celery = Celery("chris_streaming", broker=celery_broker_url)
        self._celery.conf.update(
            task_serializer="json",
            accept_content=["json"],
            result_serializer="json",
        )

    async def connect(self) -> None:
        self._redis = aioredis.from_url(self._redis_url, decode_responses=True)
        await self._redis.ping()
        logger.info("Redis connected for status notifications")

    async def notify(self, event: StatusEvent) -> None:
        """
        Publish event to Redis Pub/Sub. If the status is terminal,
        also schedule a Celery confirmation task.
        """
        channel = f"job:{event.job_id}:status"
        payload = event.model_dump_json()

        await self._redis.publish(channel, payload)
        logger.debug("Published to %s: %s", channel, event.status.value)

        if event.status in TERMINAL_STATUSES:
            self._schedule_confirmation(event)

    def _schedule_confirmation(self, event: StatusEvent) -> None:
        """Schedule the Celery confirm_job_status task."""
        self._celery.send_task(
            "chris_streaming.sse_service.tasks.confirm_job_status",
            kwargs={"event_data": event.model_dump(mode="json")},
            queue="confirmation",
        )
        logger.info(
            "Scheduled confirmation task for job=%s status=%s",
            event.job_id, event.status.value,
        )

    async def close(self) -> None:
        if self._redis:
            await self._redis.close()
