"""
FastAPI routes for the SSE service.

Endpoints:
  GET /events/{job_id}/status   - SSE stream of status changes
  GET /events/{job_id}/logs     - SSE stream of log lines
  GET /events/{job_id}/all      - SSE stream of both interleaved
  GET /logs/{job_id}/history    - JSON historical logs from OpenSearch
  GET /health                   - Health check
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from opensearchpy import AsyncOpenSearch
from sse_starlette.sse import EventSourceResponse

from chris_streaming.common.settings import SSEServiceSettings
from .redis_subscriber import subscribe_to_job

logger = logging.getLogger(__name__)

router = APIRouter()


def _get_settings() -> SSEServiceSettings:
    return SSEServiceSettings()


async def _event_generator(request: Request, job_id: str, event_type: str):
    """Async generator that yields SSE events from Redis Pub/Sub."""
    settings = _get_settings()
    async for data in subscribe_to_job(settings.redis_url, job_id, event_type):
        if await request.is_disconnected():
            break
        sse_event = data.pop("_event_type", event_type)
        yield {"event": sse_event, "data": json.dumps(data)}


@router.get("/events/{job_id}/status")
async def stream_status(request: Request, job_id: str):
    """SSE stream of status events for a job."""
    return EventSourceResponse(_event_generator(request, job_id, "status"))


@router.get("/events/{job_id}/logs")
async def stream_logs(request: Request, job_id: str):
    """SSE stream of log lines for a job."""
    return EventSourceResponse(_event_generator(request, job_id, "logs"))


@router.get("/events/{job_id}/all")
async def stream_all(request: Request, job_id: str):
    """SSE stream of both status and log events for a job."""
    return EventSourceResponse(_event_generator(request, job_id, "all"))


@router.get("/logs/{job_id}/history")
async def get_log_history(job_id: str, limit: int = 1000, offset: int = 0):
    """
    Retrieve historical logs for a job from OpenSearch.

    Returns log lines sorted by timestamp, paginated.
    """
    settings = _get_settings()
    client = AsyncOpenSearch(hosts=[settings.opensearch_url], use_ssl=False, verify_certs=False)

    try:
        index_pattern = f"{settings.opensearch_index_prefix}-*"
        body = {
            "query": {"term": {"job_id": job_id}},
            "sort": [{"timestamp": {"order": "asc"}}],
            "from": offset,
            "size": limit,
        }
        resp = await client.search(index=index_pattern, body=body)
        hits = resp.get("hits", {})
        total = hits.get("total", {}).get("value", 0)
        lines = [hit["_source"] for hit in hits.get("hits", [])]

        return {"job_id": job_id, "total": total, "offset": offset, "lines": lines}
    finally:
        await client.close()


@router.get("/health")
async def health():
    return {"status": "ok"}
