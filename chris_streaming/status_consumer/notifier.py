"""
Celery scheduling layer: dispatches every status event to the Celery
process_job_status task for DB persistence and terminal-status
``confirmed_*`` emission to the status stream.
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
        """Schedule a Celery task for DB persistence and confirmed_* emission.

        ``confirmed_*`` events are re-emitted to the status stream by the
        Celery worker itself (so SSE clients and CUBE see them). Re-processing
        those would overwrite the real terminal status in ``job_status`` and
        cause an infinite loop, so we drop them here.
        """
        if event.status.value.startswith("confirmed_"):
            logger.debug(
                "Skipping confirmed_* re-entry: job=%s status=%s",
                event.job_id, event.status.value,
            )
            return
        self._celery.send_task(
            "chris_streaming.sse_service.tasks.process_job_status",
            kwargs={"event_data": event.model_dump(mode="json")},
            queue="status-processing",
        )
        logger.info(
            "Scheduled processing task for job=%s status=%s",
            event.job_id, event.status.value,
        )
