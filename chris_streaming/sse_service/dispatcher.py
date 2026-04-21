"""
Process-global stream dispatcher for SSE fan-out.

One reader task per base stream (status, logs, workflow) runs an ungrouped
``XREAD`` loop across all shards, starting from ``$``. Every entry is
dispatched by ``job_id`` to any subscriber registered for that job — all
SSE replicas read every event independently (no consumer group), giving
the broadcast semantics SSE fan-out needs.

Durability on client reconnect comes from historical replay (Postgres for
status/workflow, Quickwit for logs). The streams themselves are MAXLEN-
trimmed and are not the source of truth for replay.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass

import redis.asyncio as aioredis

from chris_streaming.common.schemas import LogEvent, StatusEvent, WorkflowEvent

logger = logging.getLogger(__name__)


@dataclass
class _Subscriber:
    job_id: str
    event_types: frozenset[str]
    queue: asyncio.Queue


class StreamDispatcher:
    """Fans out sharded Redis Stream entries to per-job SSE subscribers."""

    _DECODERS = {
        "status": StatusEvent.deserialize,
        "logs": LogEvent.deserialize,
        "workflow": WorkflowEvent.deserialize,
    }

    def __init__(
        self,
        redis: aioredis.Redis,
        *,
        num_shards: int,
        status_base: str,
        logs_base: str,
        workflow_base: str,
        block_ms: int = 500,
        count: int = 200,
    ) -> None:
        self._redis = redis
        self._num_shards = num_shards
        self._bases: dict[str, str] = {
            "status": status_base,
            "logs": logs_base,
            "workflow": workflow_base,
        }
        self._block_ms = block_ms
        self._count = count
        self._subs: dict[str, list[_Subscriber]] = {}
        self._tasks: list[asyncio.Task] = []
        self._running = False

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        for event_type, base in self._bases.items():
            self._tasks.append(asyncio.create_task(
                self._reader_loop(event_type, base),
                name=f"stream-dispatch-{event_type}",
            ))
        logger.info(
            "StreamDispatcher started shards=%d bases=%s",
            self._num_shards, list(self._bases.values()),
        )

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.warning("Dispatcher reader shutdown error: %s", e)
        self._tasks.clear()
        logger.info("StreamDispatcher stopped")

    @asynccontextmanager
    async def subscribe(
        self, job_id: str, event_types: Iterable[str]
    ) -> AsyncIterator[asyncio.Queue]:
        """Yield a queue that receives every matching event for ``job_id``.

        Events are put on the queue as ``dict`` with an injected
        ``_event_type`` key (``"status"`` / ``"logs"`` / ``"workflow"``).
        """
        sub = _Subscriber(
            job_id=job_id,
            event_types=frozenset(event_types),
            queue=asyncio.Queue(),
        )
        self._subs.setdefault(job_id, []).append(sub)
        try:
            yield sub.queue
        finally:
            try:
                self._subs[job_id].remove(sub)
                if not self._subs[job_id]:
                    self._subs.pop(job_id, None)
            except (KeyError, ValueError):
                pass

    async def _reader_loop(self, event_type: str, base: str) -> None:
        """XREAD across every shard of ``base`` and dispatch by job_id."""
        # Keyed by stream name; advance each shard's last-id independently.
        streams: dict[str, bytes] = {
            f"{base}:{i}".encode(): b"$" for i in range(self._num_shards)
        }
        try:
            while self._running:
                try:
                    resp = await self._redis.xread(
                        streams, count=self._count, block=self._block_ms,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning("XREAD %s failed: %s", base, e)
                    await asyncio.sleep(0.5)
                    continue
                if not resp:
                    continue
                for stream_key, entries in resp:
                    name = (
                        stream_key.decode()
                        if isinstance(stream_key, bytes) else stream_key
                    )
                    key = stream_key if isinstance(stream_key, bytes) else name.encode()
                    for entry_id, fields in entries:
                        raw = fields.get(b"data") or fields.get("data")
                        if raw is None:
                            continue
                        try:
                            event_dict = self._decode(event_type, raw)
                        except Exception as e:
                            logger.warning(
                                "Failed to parse %s entry: %s", event_type, e,
                            )
                            streams[key] = (
                                entry_id if isinstance(entry_id, bytes)
                                else str(entry_id).encode()
                            )
                            continue
                        self._dispatch(event_type, event_dict)
                        streams[key] = (
                            entry_id if isinstance(entry_id, bytes)
                            else str(entry_id).encode()
                        )
        except asyncio.CancelledError:
            logger.debug("Dispatcher reader %s cancelled", event_type)

    def _decode(self, event_type: str, raw) -> dict:
        decoder = self._DECODERS[event_type]
        if isinstance(raw, str):
            raw = raw.encode()
        evt = decoder(raw)
        return evt.model_dump(mode="json")

    def _dispatch(self, event_type: str, event_dict: dict) -> None:
        job_id = event_dict.get("job_id", "")
        if not job_id:
            return
        payload = {**event_dict, "_event_type": event_type}
        for sub in self._subs.get(job_id, ()):
            if event_type in sub.event_types:
                sub.queue.put_nowait(payload)
