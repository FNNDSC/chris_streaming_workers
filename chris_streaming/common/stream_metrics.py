"""
Stream observability helpers.

Exposes a single coroutine, :func:`collect_stream_metrics`, that returns a
dict of per-stream depth + pending/DLQ stats for the two pipelines. Used by
the SSE service's ``/metrics`` endpoint and any smoke tests.

Metrics shape::

    {
      "status": {
        "shards": [
          {"stream": "stream:job-status:0", "xlen": 12, "pending": 0},
          ...
        ],
        "dlq_xlen": 0,
      },
      "logs": { ... same shape ... },
    }
"""

from __future__ import annotations

from typing import Any

import redis.asyncio as aioredis

from .redis_stream import ShardRouter


async def _safe_xlen(redis: aioredis.Redis, key: str) -> int:
    try:
        return int(await redis.xlen(key))
    except Exception:
        return 0


async def _safe_pending(
    redis: aioredis.Redis, stream: str, group: str
) -> int:
    try:
        summary = await redis.xpending(stream, group)
    except Exception:
        return 0
    if isinstance(summary, dict):
        return int(summary.get("pending") or summary.get(b"pending") or 0)
    if isinstance(summary, (list, tuple)) and summary:
        return int(summary[0] or 0)
    return 0


async def _pipeline_metrics(
    redis: aioredis.Redis,
    base: str,
    dlq: str,
    num_shards: int,
    group: str,
) -> dict[str, Any]:
    router = ShardRouter(num_shards)
    shards = []
    for i in range(num_shards):
        stream = router.stream_name(base, i)
        xlen = await _safe_xlen(redis, stream)
        pending = await _safe_pending(redis, stream, group)
        shards.append({"stream": stream, "xlen": xlen, "pending": pending})
    dlq_xlen = await _safe_xlen(redis, dlq)
    return {"shards": shards, "dlq_xlen": dlq_xlen}


async def collect_stream_metrics(
    redis: aioredis.Redis,
    *,
    status_base: str,
    status_dlq: str,
    status_group: str,
    logs_base: str,
    logs_dlq: str,
    logs_group: str,
    num_shards: int,
) -> dict[str, Any]:
    """Return stream depth + pending/DLQ stats for both pipelines."""
    return {
        "status": await _pipeline_metrics(
            redis, status_base, status_dlq, num_shards, status_group,
        ),
        "logs": await _pipeline_metrics(
            redis, logs_base, logs_dlq, num_shards, logs_group,
        ),
    }
