"""
Celery scheduling layer: dispatches every status event to the Celery
process_job_status task for DB persistence, Redis Pub/Sub publishing,
and terminal status confirmation.
"""

from __future__ import annotations

import logging

from celery import Celery

from chris_streaming.common.schemas import StatusEvent

logger = logging.getLogger(__name__)


class StatusNotifier:
    """Schedules Celery processing tasks for status events."""

    def __init__(self, celery_broker_url: str):
        self._celery = Celery("chris_streaming", broker=celery_broker_url)
        self._celery.conf.update(
            task_serializer="json",
            accept_content=["json"],
            result_serializer="json",
        )

    async def notify(self, event: StatusEvent) -> None:
        """Schedule a Celery task for DB persistence and Redis publishing."""
        self._celery.send_task(
            "chris_streaming.sse_service.tasks.process_job_status",
            kwargs={"event_data": event.model_dump(mode="json")},
            queue="status-processing",
        )
        logger.info(
            "Scheduled processing task for job=%s status=%s",
            event.job_id, event.status.value,
        )
