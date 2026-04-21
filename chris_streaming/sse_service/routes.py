"""
FastAPI routes for the SSE service.

Endpoints:
  GET  /events/{job_id}/status   - SSE stream of status changes (with historical replay)
  GET  /events/{job_id}/logs     - SSE stream of log lines (with historical replay)
  GET  /events/{job_id}/workflow - SSE stream of workflow-state events (with historical replay)
  GET  /events/{job_id}/all      - SSE stream of status + logs + workflow interleaved
  GET  /logs/{job_id}/history    - JSON historical logs from Quickwit
  POST /api/jobs/{job_id}/run    - Submit a workflow (async via Celery)
  GET  /api/jobs/{job_id}/workflow - Workflow status
  GET  /api/jobs/{job_id}/status/history - Historical status from PostgreSQL
  GET  /health                   - Health check
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional

import psycopg2
from fastapi import APIRouter, Request
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from chris_streaming.common.quickwit import QuickwitClient
from chris_streaming.common.settings import SSEServiceSettings
from chris_streaming.common.redis_stream import create_redis_client
from chris_streaming.common.stream_metrics import collect_stream_metrics
from .redis_subscriber import subscribe_to_job
from .tasks import celery_app

logger = logging.getLogger(__name__)

router = APIRouter()


def _get_settings() -> SSEServiceSettings:
    return SSEServiceSettings()


# ── Pydantic models for API ────────────────────────────────────────────────

class WorkflowRequest(BaseModel):
    """Request body for POST /api/jobs/{job_id}/run."""
    image: str
    entrypoint: list[str]
    type: str = "ds"
    input_dirs: str = ""
    output_dir: str = ""
    args: list[str] = Field(default_factory=list)
    cpu_limit: int = 1000
    memory_limit: int = 300
    gpu_limit: int = 0
    number_of_workers: int = 1
    env: list[str] = Field(default_factory=list)
    auid: str = "cube"


# ── SSE streaming endpoints ────────────────────────────────────────────────

async def _event_generator(request: Request, job_id: str, event_type: str):
    """Async generator that yields SSE events: historical replay first, then live."""
    settings = _get_settings()
    dispatcher = request.app.state.dispatcher
    async for data in subscribe_to_job(
        dispatcher=dispatcher,
        job_id=job_id,
        event_type=event_type,
        db_dsn=settings.db_dsn,
        quickwit_url=settings.quickwit_url,
        quickwit_index=settings.quickwit_index,
    ):
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


@router.get("/events/{job_id}/workflow")
async def stream_workflow(request: Request, job_id: str):
    """SSE stream of workflow-state events for a job."""
    return EventSourceResponse(_event_generator(request, job_id, "workflow"))


@router.get("/events/{job_id}/all")
async def stream_all(request: Request, job_id: str):
    """SSE stream of status + logs + workflow events for a job."""
    return EventSourceResponse(_event_generator(request, job_id, "all"))


# ── Historical data endpoints ──────────────────────────────────────────────

@router.get("/logs/{job_id}/history")
async def get_log_history(job_id: str, limit: int = 1000, offset: int = 0):
    """Retrieve historical logs for a job from Quickwit."""
    settings = _get_settings()
    client = QuickwitClient(settings.quickwit_url, index_id=settings.quickwit_index)
    await client.connect()
    try:
        result = await client.search_by_job(job_id, limit=limit, offset=offset)
        return {
            "job_id": job_id,
            "total": result["total"],
            "offset": offset,
            "lines": result["lines"],
        }
    finally:
        await client.close()


@router.get("/api/jobs/{job_id}/status/history")
async def get_status_history(job_id: str):
    """Retrieve historical status records from PostgreSQL."""
    settings = _get_settings()
    rows = await asyncio.to_thread(_query_status_history, settings.db_dsn, job_id)
    return {"job_id": job_id, "statuses": rows}


def _query_status_history(db_dsn: str, job_id: str) -> list[dict]:
    """Query job_status table for all records matching job_id."""
    try:
        conn = psycopg2.connect(db_dsn)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT job_id, job_type, status, image, cmd, message, "
                    "exit_code, source, event_id, updated_at "
                    "FROM job_status WHERE job_id = %s ORDER BY updated_at",
                    (job_id,),
                )
                columns = [desc[0] for desc in cur.description]
                return [
                    {col: (val.isoformat() if hasattr(val, "isoformat") else val)
                     for col, val in zip(columns, row)}
                    for row in cur.fetchall()
                ]
        finally:
            conn.close()
    except Exception as e:
        logger.error("Failed to query status history: %s", e)
        return []


# ── Workflow API endpoints ──────────────────────────────────────────────────

@router.post("/api/jobs/{job_id}/run", status_code=202)
async def run_workflow(job_id: str, req: WorkflowRequest):
    """Submit a full workflow (copy → plugin → upload → delete) via Celery."""
    params = req.model_dump()
    celery_app.send_task(
        "chris_streaming.sse_service.tasks.start_workflow",
        kwargs={"job_id": job_id, "params": params},
        queue="status-processing",
    )
    logger.info("Workflow submitted: job=%s image=%s", job_id, req.image)
    return {"job_id": job_id, "status": "submitted"}


@router.get("/api/jobs/{job_id}/workflow")
async def get_workflow_status(job_id: str):
    """Query the workflow status from PostgreSQL."""
    settings = _get_settings()
    row = await asyncio.to_thread(_query_workflow, settings.db_dsn, job_id)
    if row is None:
        return {"job_id": job_id, "status": "not_found"}
    return row


def _query_workflow(db_dsn: str, job_id: str) -> Optional[dict]:
    """Query job_workflow table for a specific job_id."""
    try:
        conn = psycopg2.connect(db_dsn)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT job_id, current_step, status, created_at, updated_at "
                    "FROM job_workflow WHERE job_id = %s",
                    (job_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                columns = [desc[0] for desc in cur.description]
                return {
                    col: (val.isoformat() if hasattr(val, "isoformat") else val)
                    for col, val in zip(columns, row)
                }
        finally:
            conn.close()
    except Exception as e:
        logger.error("Failed to query workflow: %s", e)
        return None


# ── Health check ────────────────────────────────────────────────────────────

@router.get("/health")
async def health():
    return {"status": "ok"}


@router.get("/metrics")
async def metrics():
    """Stream depth / PEL / DLQ snapshot for both pipelines."""
    settings = _get_settings()
    redis = await create_redis_client(settings.redis_url)
    try:
        return await collect_stream_metrics(
            redis,
            status_base=settings.stream_status_base,
            status_dlq=settings.stream_status_dlq,
            status_group=settings.status_consumer_group,
            logs_base=settings.stream_logs_base,
            logs_dlq=settings.stream_logs_dlq,
            logs_group=settings.log_consumer_group,
            num_shards=settings.stream_num_shards,
        )
    finally:
        await redis.close()
