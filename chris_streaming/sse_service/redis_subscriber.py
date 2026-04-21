"""
Historical replay + live stream fan-out for SSE connections.

When a client connects, historical data is replayed from PostgreSQL (status,
workflow events) and Quickwit (logs) before switching to live events via the
process-global ``StreamDispatcher`` (which reads the sharded Redis Streams).
Events emitted during the replay window are buffered on the dispatcher queue
and deduplicated against replayed rows via ``event_id``.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Literal

import psycopg2

from chris_streaming.common.quickwit import QuickwitClient, QuickwitSearchError
from .dispatcher import StreamDispatcher

logger = logging.getLogger(__name__)


async def subscribe_to_job(
    dispatcher: StreamDispatcher,
    job_id: str,
    event_type: Literal["status", "logs", "workflow", "all"] = "all",
    db_dsn: str = "",
    quickwit_url: str = "",
    quickwit_index: str = "job-logs",
) -> AsyncIterator[dict]:
    """Async generator: historical replay, then live events for a job.

    The dispatcher queue starts buffering live events as soon as we subscribe,
    so nothing produced during the replay phase is lost. Duplicates between
    replay and live are suppressed via ``event_id``.
    """
    if event_type == "all":
        event_types = {"status", "logs", "workflow"}
    else:
        event_types = {event_type}

    async with dispatcher.subscribe(job_id, event_types) as queue:
        seen_event_ids: set[str] = set()

        # Phase 1: historical replay (durable sources).
        if "status" in event_types and db_dsn:
            async for data in _replay_status_history(db_dsn, job_id):
                eid = data.get("event_id", "")
                if eid:
                    seen_event_ids.add(eid)
                yield data

        if "workflow" in event_types and db_dsn:
            async for data in _replay_workflow_history(db_dsn, job_id):
                eid = data.get("event_id", "")
                if eid:
                    seen_event_ids.add(eid)
                yield data

        if "logs" in event_types and quickwit_url:
            async for data in _replay_log_history(
                quickwit_url, quickwit_index, job_id
            ):
                eid = data.get("event_id", "")
                if eid:
                    seen_event_ids.add(eid)
                yield data

        # Phase 2: live. Queue has everything received since subscribe().
        # Dedup any overlap with replay by event_id.
        while True:
            data = await queue.get()
            eid = data.get("event_id", "")
            if eid and eid in seen_event_ids:
                continue
            if eid:
                seen_event_ids.add(eid)
            yield data


async def _replay_status_history(db_dsn: str, job_id: str) -> AsyncIterator[dict]:
    """Query PostgreSQL for historical status records and yield them."""
    rows = await asyncio.to_thread(_fetch_status_rows, db_dsn, job_id)
    for row in rows:
        row["_event_type"] = "status"
        yield row


def _fetch_status_rows(db_dsn: str, job_id: str) -> list[dict]:
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
        logger.warning("Failed to fetch status history for replay: %s", e)
        return []


async def _replay_workflow_history(db_dsn: str, job_id: str) -> AsyncIterator[dict]:
    """Query PostgreSQL for historical workflow events and yield them."""
    rows = await asyncio.to_thread(_fetch_workflow_rows, db_dsn, job_id)
    for row in rows:
        row["_event_type"] = "workflow"
        yield row


def _fetch_workflow_rows(db_dsn: str, job_id: str) -> list[dict]:
    try:
        conn = psycopg2.connect(db_dsn)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT job_id, current_step, current_step_status, "
                    "workflow_status, error, event_id, created_at "
                    "FROM job_workflow_events WHERE job_id = %s ORDER BY id",
                    (job_id,),
                )
                columns = [desc[0] for desc in cur.description]
                out: list[dict] = []
                for row in cur.fetchall():
                    rec = {
                        col: (val.isoformat() if hasattr(val, "isoformat") else val)
                        for col, val in zip(columns, row)
                    }
                    # Normalize timestamp name to match WorkflowEvent's field.
                    ts = rec.pop("created_at", None)
                    if ts is not None:
                        rec["timestamp"] = ts
                    out.append(rec)
                return out
        finally:
            conn.close()
    except Exception as e:
        logger.warning("Failed to fetch workflow history for replay: %s", e)
        return []


async def _replay_log_history(
    quickwit_url: str, index_id: str, job_id: str
) -> AsyncIterator[dict]:
    """Query Quickwit for historical log records and yield them."""
    client = QuickwitClient(quickwit_url, index_id=index_id)
    try:
        await client.connect()
        result = await client.search_by_job(job_id, limit=5000)
        for doc in result.get("lines", []):
            doc["_event_type"] = "logs"
            yield doc
    except QuickwitSearchError as e:
        logger.warning("Failed to fetch log history for replay: %s", e)
    except Exception as e:
        logger.warning("Failed to fetch log history for replay: %s", e)
    finally:
        await client.close()
