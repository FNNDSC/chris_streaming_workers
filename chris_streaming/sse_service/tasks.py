"""
Celery tasks for the SSE service.

The confirm_job_status task is scheduled by the Status Consumer when a
terminal status is received. It publishes a confirmed_* status back to
Redis Pub/Sub so the SSE service can notify connected clients.
"""

from __future__ import annotations

import logging

import redis as sync_redis
from celery import Celery

from chris_streaming.common.schemas import StatusEvent, JobStatus
from chris_streaming.common.settings import SSEServiceSettings

logger = logging.getLogger(__name__)

settings = SSEServiceSettings()

celery_app = Celery(
    "chris_streaming",
    broker=settings.celery_broker_url,
)
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    task_routes={
        "chris_streaming.sse_service.tasks.confirm_job_status": {"queue": "confirmation"},
    },
)

# Map terminal statuses to their confirmed counterparts
_CONFIRMED_MAP = {
    "finishedSuccessfully": "confirmed_finishedSuccessfully",
    "finishedWithError": "confirmed_finishedWithError",
    "undefined": "confirmed_undefined",
}


@celery_app.task(name="chris_streaming.sse_service.tasks.confirm_job_status")
def confirm_job_status(event_data: dict) -> dict:
    """
    Celery task that confirms a terminal job status.

    Takes the original StatusEvent data, creates a new event with the
    confirmed_* status, and publishes it to Redis Pub/Sub.
    """
    original_status = event_data.get("status", "")
    confirmed_status = _CONFIRMED_MAP.get(original_status)

    if confirmed_status is None:
        logger.warning("No confirmed mapping for status: %s", original_status)
        return {"status": "skipped", "reason": f"no mapping for {original_status}"}

    job_id = event_data["job_id"]

    confirmed_event = StatusEvent(
        job_id=job_id,
        job_type=event_data["job_type"],
        status=JobStatus(confirmed_status),
        previous_status=JobStatus(original_status),
        image=event_data.get("image", ""),
        cmd=event_data.get("cmd", ""),
        message=f"confirmed by celery worker",
        exit_code=event_data.get("exit_code"),
        source=event_data.get("source", "docker"),
    )

    # Publish to Redis Pub/Sub
    r = sync_redis.from_url(settings.redis_url, decode_responses=True)
    try:
        channel = f"job:{job_id}:status"
        r.publish(channel, confirmed_event.model_dump_json())
        logger.info("Published confirmed status: job=%s status=%s", job_id, confirmed_status)
    finally:
        r.close()

    return {"status": "confirmed", "job_id": job_id, "confirmed_status": confirmed_status}
