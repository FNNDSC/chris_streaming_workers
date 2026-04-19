"""
Redis Streams consumer for the job-logs streams.

Reads log events in batches from all N shards, writes to OpenSearch via the
_bulk API, and publishes to Redis Pub/Sub for real-time streaming.

Unlike the status pipeline, logs tolerate loss of per-job ordering across
reorderings, so we use the standard consumer-group pattern (no shard
leases): every replica runs M tasks (one per shard) in the same consumer
group, and Redis load-balances entries across replicas.

Batching: messages accumulate up to batch_max_size or batch_max_wait_seconds
before being flushed to OpenSearch. Entries are XACK'd after a successful
OpenSearch write (at-least-once semantics).
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from chris_streaming.common.redis_stream import ShardRouter
from chris_streaming.common.schemas import LogEvent
from .opensearch_writer import OpenSearchWriter
from .redis_publisher import LogRedisPublisher

logger = logging.getLogger(__name__)


@dataclass
class _PendingEntry:
    stream: str
    entry_id: str
    event: LogEvent


class LogEventConsumer:
    """Batched Redis Streams consumer for log events."""

    def __init__(
        self,
        redis: aioredis.Redis,
        base_stream: str,
        num_shards: int,
        group_name: str,
        consumer_name: str,
        opensearch: OpenSearchWriter,
        redis_pub: LogRedisPublisher,
        batch_max_size: int = 200,
        batch_max_wait_seconds: float = 2.0,
        block_ms: int = 1_000,
    ):
        self._redis = redis
        self._base_stream = base_stream
        self._router = ShardRouter(num_shards)
        self._group = group_name
        self._consumer_name = consumer_name
        self._opensearch = opensearch
        self._redis_pub = redis_pub
        self._batch_max_size = batch_max_size
        self._batch_max_wait = batch_max_wait_seconds
        self._block_ms = block_ms
        self._running = False
        self._ensured_groups: set[str] = set()

    async def _ensure_group(self, stream: str) -> None:
        if stream in self._ensured_groups:
            return
        try:
            await self._redis.xgroup_create(
                stream, self._group, id="0", mkstream=True,
            )
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
        self._ensured_groups.add(stream)

    async def run(self) -> None:
        """
        Main consume loop with batching across all shards.

        Each iteration:
          1. XREADGROUP across all shards (BLOCK = batch_max_wait_seconds).
          2. Deserialize log events and EOS markers.
          3. When batch_max_size or batch_max_wait_seconds is reached, flush
             to OpenSearch + publish to Redis Pub/Sub + XACK consumed IDs.
        """
        self._running = True
        streams = {
            self._router.stream_name(self._base_stream, s): ">"
            for s in range(self._router.num_shards)
        }
        for stream in streams:
            await self._ensure_group(stream)

        pending: list[_PendingEntry] = []
        last_flush = time.monotonic()

        while self._running:
            try:
                resp = await self._redis.xreadgroup(
                    groupname=self._group,
                    consumername=self._consumer_name,
                    streams=streams,
                    count=self._batch_max_size,
                    block=int(self._batch_max_wait * 1000),
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("XREADGROUP failed: %s", e)
                await asyncio.sleep(1.0)
                continue

            for entry in (resp or []):
                stream_raw, entries = entry
                stream = stream_raw.decode() if isinstance(stream_raw, bytes) else stream_raw
                for entry_id, fields in entries:
                    eid = entry_id.decode() if isinstance(entry_id, bytes) else entry_id
                    raw = None
                    if isinstance(fields, dict):
                        raw = fields.get(b"data") or fields.get("data")
                    if raw is None:
                        logger.error("Log entry missing 'data' field id=%s", eid)
                        # Ack and drop
                        await self._redis.xack(stream, self._group, eid)
                        continue
                    raw_bytes = raw if isinstance(raw, (bytes, bytearray)) else raw.encode("utf-8")
                    try:
                        event = LogEvent.deserialize(raw_bytes)
                    except Exception as e:
                        logger.error("Failed to deserialize log id=%s: %s", eid, e)
                        await self._redis.xack(stream, self._group, eid)
                        continue
                    pending.append(_PendingEntry(stream=stream, entry_id=eid, event=event))

            now = time.monotonic()
            should_flush = (
                len(pending) >= self._batch_max_size
                or (pending and (now - last_flush) >= self._batch_max_wait)
            )
            if should_flush:
                await self._flush_pending(pending)
                pending = []
                last_flush = time.monotonic()

    async def _flush_pending(self, pending: list[_PendingEntry]) -> None:
        """Flush accumulated entries to OpenSearch + Pub/Sub, then XACK.

        EOS markers are filtered out of the OpenSearch write (they are not
        log lines). After a successful flush, for each EOS marker we SET
        ``job:{job_id}:{job_type}:logs_flushed`` in Redis (TTL 1h) so
        ``cleanup_containers`` knows the container's logs are durable.

        EOS signalling happens *after* the OpenSearch write so the key
        cannot race ahead of the data it attests to.
        """
        real = [p for p in pending if not p.event.eos]
        eos = [p for p in pending if p.event.eos]

        if real:
            real.sort(key=lambda p: p.event.timestamp)
            events = [p.event for p in real]
            try:
                await self._flush(events)
            except Exception as e:
                logger.error(
                    "Flush failed (%d events), will retry: %s",
                    len(events), e,
                )
                # Don't XACK — entries stay in PEL for reclaim / retry.
                # EOS markers in the same batch are also held back so the
                # logs_flushed signal doesn't fire ahead of the data.
                return

        for p in eos:
            key = f"job:{p.event.job_id}:{p.event.job_type.value}:logs_flushed"
            try:
                await self._redis.set(key, "1", ex=3600)
            except Exception as e:
                logger.warning("Failed to SET %s: %s", key, e)

        to_ack: dict[str, list[str]] = {}
        for p in pending:
            to_ack.setdefault(p.stream, []).append(p.entry_id)
        for stream, ids in to_ack.items():
            try:
                await self._redis.xack(stream, self._group, *ids)
            except Exception as e:
                logger.warning("XACK batch failed stream=%s: %s", stream, e)

        if eos:
            logger.info("Signalled logs_flushed for %d EOS markers", len(eos))

    async def _flush(self, batch: list[LogEvent]) -> None:
        """Write batch to OpenSearch and publish to Redis. Raises on OS failure."""
        await self._opensearch.write_batch(batch)
        try:
            await self._redis_pub.publish_batch(batch)
        except Exception as e:
            logger.warning("Redis publish failed (%d events): %s", len(batch), e)
        logger.info("Flushed batch of %d log events", len(batch))

    async def stop(self) -> None:
        self._running = False

    async def close(self) -> None:
        await self.stop()
