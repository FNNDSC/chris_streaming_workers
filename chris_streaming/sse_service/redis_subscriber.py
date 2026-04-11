"""
Async Redis Pub/Sub subscriber for SSE connections with historical replay.

When a client connects, historical data is replayed from PostgreSQL (status)
and OpenSearch (logs) before switching to live Redis Pub/Sub events. This
ensures late-connecting clients receive the full event history.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Literal

import psycopg2
import redis.asyncio as aioredis
from opensearchpy import AsyncOpenSearch

logger = logging.getLogger(__name__)


async def subscribe_to_job(
    redis_url: str,
    job_id: str,
    event_type: Literal["status", "logs", "all"] = "all",
    db_dsn: str = "",
    opensearch_url: str = "",
    opensearch_index_prefix: str = "job-logs",
) -> AsyncIterator[dict]:
    """
    Async generator that yields events for a job.

    1. Subscribe to Redis Pub/Sub first (buffer live messages)
    2. Replay historical data from PostgreSQL (status) and OpenSearch (logs)
    3. Yield historical events, then switch to live events
    4. Deduplicate overlapping events by event_id

    Channels:
      - job:{job_id}:status  (status events)
      - job:{job_id}:logs    (log events)
      - all: both channels
    """
    r = aioredis.from_url(redis_url, decode_responses=True)
    pubsub = r.pubsub()

    channels = []
    if event_type in ("status", "all"):
        channels.append(f"job:{job_id}:status")
    if event_type in ("logs", "all"):
        channels.append(f"job:{job_id}:logs")

    await pubsub.subscribe(*channels)
    logger.debug("Subscribed to %s for job %s", channels, job_id)

    # Buffer to collect live messages that arrive during replay
    live_buffer: list[dict] = []
    seen_event_ids: set[str] = set()

    try:
        # Phase 1: Start buffering live messages in the background
        buffer_task = asyncio.create_task(
            _buffer_live_messages(pubsub, live_buffer, timeout=0.1)
        )

        # Phase 2: Replay historical data
        if event_type in ("status", "all") and db_dsn:
            async for data in _replay_status_history(db_dsn, job_id):
                event_id = data.get("event_id", "")
                if event_id:
                    seen_event_ids.add(event_id)
                yield data

        if event_type in ("logs", "all") and opensearch_url:
            async for data in _replay_log_history(
                opensearch_url, opensearch_index_prefix, job_id
            ):
                event_id = data.get("event_id", "")
                if event_id:
                    seen_event_ids.add(event_id)
                yield data

        # Phase 3: Drain buffered live messages
        buffer_task.cancel()
        try:
            await buffer_task
        except asyncio.CancelledError:
            pass

        for data in live_buffer:
            event_id = data.get("event_id", "")
            if event_id and event_id in seen_event_ids:
                continue  # Deduplicate
            yield data

        # Phase 4: Continue with live messages
        while True:
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if msg is not None and msg["type"] == "message":
                try:
                    data = json.loads(msg["data"])
                    channel = msg["channel"]
                    if channel.endswith(":status"):
                        data["_event_type"] = "status"
                    elif channel.endswith(":logs"):
                        data["_event_type"] = "logs"
                    yield data
                except json.JSONDecodeError:
                    logger.warning("Non-JSON message on channel %s", msg["channel"])
            else:
                await asyncio.sleep(0.01)
    finally:
        await pubsub.unsubscribe(*channels)
        await pubsub.close()
        await r.close()


async def _buffer_live_messages(
    pubsub: aioredis.client.PubSub,
    buffer: list[dict],
    timeout: float = 0.1,
) -> None:
    """Buffer live messages while historical replay is in progress."""
    try:
        while True:
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=timeout)
            if msg is not None and msg["type"] == "message":
                try:
                    data = json.loads(msg["data"])
                    channel = msg["channel"]
                    if channel.endswith(":status"):
                        data["_event_type"] = "status"
                    elif channel.endswith(":logs"):
                        data["_event_type"] = "logs"
                    buffer.append(data)
                except json.JSONDecodeError:
                    pass
            else:
                await asyncio.sleep(0.01)
    except asyncio.CancelledError:
        raise


async def _replay_status_history(db_dsn: str, job_id: str) -> AsyncIterator[dict]:
    """Query PostgreSQL for historical status records and yield them."""
    rows = await asyncio.to_thread(_fetch_status_rows, db_dsn, job_id)
    for row in rows:
        row["_event_type"] = "status"
        yield row


def _fetch_status_rows(db_dsn: str, job_id: str) -> list[dict]:
    """Synchronous PostgreSQL query for status history."""
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


async def _replay_log_history(
    opensearch_url: str, index_prefix: str, job_id: str
) -> AsyncIterator[dict]:
    """Query OpenSearch for historical log records and yield them."""
    client = AsyncOpenSearch(hosts=[opensearch_url], use_ssl=False, verify_certs=False)
    try:
        index_pattern = f"{index_prefix}-*"
        body = {
            "query": {"term": {"job_id": job_id}},
            "sort": [{"timestamp": {"order": "asc"}}],
            "size": 5000,
        }
        resp = await client.search(index=index_pattern, body=body)
        hits = resp.get("hits", {}).get("hits", [])
        for hit in hits:
            data = hit["_source"]
            data["_event_type"] = "logs"
            yield data
    except Exception as e:
        logger.warning("Failed to fetch log history for replay: %s", e)
    finally:
        await client.close()
