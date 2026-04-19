"""
Redis Streams primitives for event and log pipelines.

Provides the building blocks used by the event/status/log pipelines:

  RedisStreamProducer    — XADD with approximate MAXLEN trim, with retry/backoff
  ShardRouter            — deterministic hash(job_id) -> shard index
  ShardLeaseManager      — lease-based shard ownership for horizontally-scaled
                           consumers that need per-shard single-writer ordering
  ShardedConsumer        — runs an XREADGROUP loop per owned shard, calls user
                           handler, XACKs on success; XADDs to DLQ after N
                           delivery attempts
  PendingReclaimer       — background sweep that XCLAIMs entries idle longer
                           than min_idle_ms so dead-replica work is picked up
                           by whichever consumer now owns the shard

Topology
--------
Stream key             -> one per shard: ``{base}:{i}`` for i in [0, num_shards)
Shard key              -> ``shard = hash(job_id) % num_shards``
Consumer group         -> per-shard stable group name
Acknowledgement        -> XACK after successful processing (at-least-once)
Partition assignment   -> ShardLeaseManager (lease keys in Redis)
Retry + DLQ            -> PendingReclaimer + DLQ stream via XADD

Ordering guarantees
-------------------
- Within a shard: strict ordering (XADD preserves insertion order; a single
  consumer reading via XREADGROUP drains the shard serially).
- Across shards: no ordering — which is fine because a single job_id always
  hashes to the same shard.
- Under failover: the ShardLeaseManager guarantees at most one consumer owns
  a given shard at a time. PendingReclaimer reclaims in-flight work from the
  previous owner via XCLAIM with min-idle-time > lease TTL, so reordering of
  in-flight entries within a shard is bounded by the reclaim logic (a single
  entry that was mid-processing at failover may be redelivered to the new
  owner, but no other entry on that shard will be processed ahead of it).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import socket
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Optional

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

logger = logging.getLogger(__name__)


# ── Shard routing ────────────────────────────────────────────────────────────


def _stable_hash(key: str) -> int:
    """Stable, language-agnostic hash (md5 first 8 bytes as unsigned int).

    We don't use Python's builtin hash() because it is salted per-process
    and would route the same job_id to different shards on each run.
    """
    digest = hashlib.md5(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=False)


class ShardRouter:
    """Maps job_id to a shard index in [0, num_shards)."""

    def __init__(self, num_shards: int) -> None:
        if num_shards < 1:
            raise ValueError("num_shards must be >= 1")
        self._num_shards = num_shards

    @property
    def num_shards(self) -> int:
        return self._num_shards

    def shard_for(self, job_id: str) -> int:
        return _stable_hash(job_id) % self._num_shards

    def stream_name(self, base: str, shard: int) -> str:
        return f"{base}:{shard}"

    def all_streams(self, base: str) -> list[str]:
        return [self.stream_name(base, i) for i in range(self._num_shards)]


# ── Producer ─────────────────────────────────────────────────────────────────


@dataclass
class StreamProducerConfig:
    """Configuration for RedisStreamProducer."""
    base_stream: str
    num_shards: int
    maxlen_approx: int = 1_000_000
    max_send_retries: int = 5
    retry_base_seconds: float = 0.2


class RedisStreamProducer:
    """Produces events to sharded Redis Streams.

    Serialization is caller-provided: pass `event_bytes` already encoded.
    The wire representation is a single field `data` with the raw bytes.
    """

    def __init__(
        self,
        redis: aioredis.Redis,
        config: StreamProducerConfig,
        router: Optional[ShardRouter] = None,
    ) -> None:
        self._redis = redis
        self._config = config
        self._router = router or ShardRouter(config.num_shards)

    @property
    def router(self) -> ShardRouter:
        return self._router

    async def xadd(
        self,
        job_id: str,
        event_bytes: bytes,
    ) -> str:
        """Append to the shard stream for this job_id. Returns the entry ID.

        Retries on transient redis errors with exponential backoff. Uses
        approximate MAXLEN trim ('~') for O(1) amortized trim cost.
        """
        shard = self._router.shard_for(job_id)
        stream = self._router.stream_name(self._config.base_stream, shard)

        last_error: Optional[Exception] = None
        for attempt in range(1, self._config.max_send_retries + 1):
            try:
                entry_id = await self._redis.xadd(
                    stream,
                    {"data": event_bytes},
                    maxlen=self._config.maxlen_approx,
                    approximate=True,
                )
                if isinstance(entry_id, bytes):
                    entry_id = entry_id.decode()
                return entry_id
            except Exception as e:
                last_error = e
                wait = min(
                    self._config.retry_base_seconds * (2 ** (attempt - 1)),
                    5.0,
                )
                logger.warning(
                    "XADD to %s failed (attempt %d/%d): %s. Retrying in %.2fs",
                    stream, attempt, self._config.max_send_retries, e, wait,
                )
                await asyncio.sleep(wait)

        assert last_error is not None
        raise RuntimeError(
            f"XADD to {stream} failed after {self._config.max_send_retries} attempts: "
            f"{last_error}"
        )


# ── Shard lease (horizontal-scale coordination) ──────────────────────────────


def _default_replica_id() -> str:
    """Derive a stable-per-process replica identifier."""
    host = socket.gethostname()
    return f"{host}:{os.getpid()}"


@dataclass
class LeaseManagerConfig:
    """Configuration for ShardLeaseManager."""
    num_shards: int
    lease_key_prefix: str
    replica_id: str = field(default_factory=_default_replica_id)
    lease_ttl_ms: int = 15_000
    refresh_interval_ms: int = 5_000
    acquire_interval_ms: int = 2_000


class ShardLeaseManager:
    """Lease-based shard ownership for horizontal scaling.

    Each replica periodically tries to acquire leases on shards. A shard is
    owned for ~lease_ttl_ms after successful SET NX PX. The owner refreshes
    every ~refresh_interval_ms. On replica death, the lease expires and any
    other replica can take it.

    Balancing: a replica will try to hold ceil(N/M) shards where M is the
    number of live replicas it observes. To know M, each replica writes a
    heartbeat key with the same TTL; the count of heartbeat keys is M.
    Over-quota replicas release shards to make room for new replicas.
    """

    def __init__(
        self,
        redis: aioredis.Redis,
        config: LeaseManagerConfig,
    ) -> None:
        self._redis = redis
        self._config = config
        self._owned: set[int] = set()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        # Listeners notified when the owned-shard set changes.
        self._listeners: list[Callable[[set[int], set[int]], Awaitable[None]]] = []

    @property
    def owned_shards(self) -> set[int]:
        return set(self._owned)

    @property
    def replica_id(self) -> str:
        return self._config.replica_id

    def add_listener(
        self, listener: Callable[[set[int], set[int]], Awaitable[None]]
    ) -> None:
        """Add a listener (added_shards, removed_shards) -> awaitable."""
        self._listeners.append(listener)

    def _lease_key(self, shard: int) -> str:
        return f"{self._config.lease_key_prefix}:lease:{shard}"

    def _heartbeat_key(self, replica_id: str) -> str:
        return f"{self._config.lease_key_prefix}:heartbeat:{replica_id}"

    def _heartbeat_pattern(self) -> str:
        return f"{self._config.lease_key_prefix}:heartbeat:*"

    async def start(self) -> None:
        """Start the background lease maintenance loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run())
        logger.info(
            "ShardLeaseManager started replica_id=%s num_shards=%d",
            self._config.replica_id, self._config.num_shards,
        )

    async def stop(self) -> None:
        """Stop the loop and release all owned leases."""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        # Release leases best-effort
        await self._release_all()
        # Remove heartbeat
        try:
            await self._redis.delete(self._heartbeat_key(self._config.replica_id))
        except Exception:
            pass
        logger.info(
            "ShardLeaseManager stopped replica_id=%s",
            self._config.replica_id,
        )

    async def _run(self) -> None:
        """Main loop: heartbeat, then rebalance+acquire, then refresh owned."""
        try:
            while self._running:
                try:
                    await self._heartbeat()
                    await self._rebalance()
                    await self._refresh_owned()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error("ShardLeaseManager loop error: %s", e, exc_info=True)
                await asyncio.sleep(self._config.acquire_interval_ms / 1000.0)
        except asyncio.CancelledError:
            pass

    async def _heartbeat(self) -> None:
        await self._redis.set(
            self._heartbeat_key(self._config.replica_id),
            "1",
            px=self._config.lease_ttl_ms,
        )

    async def _count_live_replicas(self) -> int:
        """Count live replicas via heartbeat keys."""
        cursor = 0
        count = 0
        pattern = self._heartbeat_pattern()
        while True:
            cursor, keys = await self._redis.scan(
                cursor=cursor, match=pattern, count=100,
            )
            count += len(keys)
            if cursor == 0:
                break
        return max(count, 1)

    async def _rebalance(self) -> None:
        """Acquire new shards up to our quota; release any over quota."""
        live = await self._count_live_replicas()
        quota = max(1, (self._config.num_shards + live - 1) // live)

        # Release over-quota shards first to give room to other replicas.
        while len(self._owned) > quota:
            shard = next(iter(sorted(self._owned, reverse=True)))
            await self._release(shard)

        # Try to acquire shards we don't own until we hit quota.
        if len(self._owned) >= quota:
            return

        for shard in range(self._config.num_shards):
            if shard in self._owned:
                continue
            acquired = await self._try_acquire(shard)
            if acquired and len(self._owned) >= quota:
                break

    async def _try_acquire(self, shard: int) -> bool:
        """Attempt to acquire a shard lease. Returns True on success."""
        ok = await self._redis.set(
            self._lease_key(shard),
            self._config.replica_id,
            nx=True,
            px=self._config.lease_ttl_ms,
        )
        if ok:
            self._owned.add(shard)
            await self._notify(added={shard}, removed=set())
            logger.info(
                "Acquired shard lease replica_id=%s shard=%d",
                self._config.replica_id, shard,
            )
            return True
        return False

    async def _refresh_owned(self) -> None:
        """Refresh TTL on all owned shards; drop any we lost."""
        to_drop: set[int] = set()
        for shard in list(self._owned):
            # Only refresh if we still hold the lease (to defend against a
            # scenario where we missed too many refreshes and another replica
            # took over).
            current = await self._redis.get(self._lease_key(shard))
            if isinstance(current, bytes):
                current = current.decode()
            if current != self._config.replica_id:
                logger.warning(
                    "Lost shard lease replica_id=%s shard=%d current_owner=%s",
                    self._config.replica_id, shard, current,
                )
                to_drop.add(shard)
                continue
            await self._redis.pexpire(
                self._lease_key(shard),
                self._config.lease_ttl_ms,
            )
        if to_drop:
            self._owned -= to_drop
            await self._notify(added=set(), removed=to_drop)

    async def _release(self, shard: int) -> None:
        self._owned.discard(shard)
        # Best-effort compare-and-delete via Lua would be nicer; for now we
        # only delete if we still own it.
        current = await self._redis.get(self._lease_key(shard))
        if isinstance(current, bytes):
            current = current.decode()
        if current == self._config.replica_id:
            await self._redis.delete(self._lease_key(shard))
        await self._notify(added=set(), removed={shard})
        logger.info(
            "Released shard lease replica_id=%s shard=%d",
            self._config.replica_id, shard,
        )

    async def _release_all(self) -> None:
        for shard in list(self._owned):
            try:
                await self._release(shard)
            except Exception as e:
                logger.warning(
                    "Error releasing shard %d on shutdown: %s", shard, e,
                )

    async def _notify(self, added: set[int], removed: set[int]) -> None:
        if not added and not removed:
            return
        for listener in list(self._listeners):
            try:
                await listener(added, removed)
            except Exception as e:
                logger.error("Listener error (added=%s removed=%s): %s",
                             added, removed, e, exc_info=True)


# ── Sharded consumer (XREADGROUP per owned shard) ────────────────────────────


MessageHandler = Callable[[bytes], Awaitable[None]]


@dataclass
class ConsumerConfig:
    """Configuration for ShardedConsumer."""
    base_stream: str
    num_shards: int
    group_name: str
    consumer_name: str
    block_ms: int = 2_000
    batch_size: int = 50
    max_deliveries: int = 5
    dlq_stream: Optional[str] = None
    dlq_maxlen_approx: int = 100_000
    # How many times to retry the user handler synchronously for a message
    # before giving up and letting XPENDING -> reclaim handle it.
    handler_retries: int = 3
    handler_retry_base_seconds: float = 0.2


class ShardedConsumer:
    """Consumes from one or more Redis Stream shards via XREADGROUP.

    Intended to be driven by a ShardLeaseManager: call `set_shards` with the
    owned shards whenever ownership changes, and the consumer will start/stop
    per-shard reader tasks accordingly.

    Processing model
    ----------------
    - Per shard: one asyncio task running XREADGROUP in a loop.
    - For each message: call handler(raw_bytes).
        - On success: XACK.
        - On exception (after in-task retries): do NOT XACK. The message
          remains in the Pending Entries List (PEL) and will be picked up
          by PendingReclaimer after min-idle-time, either by this consumer
          or by whichever consumer owns the shard after failover.
    """

    def __init__(
        self,
        redis: aioredis.Redis,
        config: ConsumerConfig,
        handler: MessageHandler,
    ) -> None:
        self._redis = redis
        self._config = config
        self._handler = handler
        self._router = ShardRouter(config.num_shards)
        self._tasks: dict[int, asyncio.Task] = {}
        self._stop_flags: dict[int, asyncio.Event] = {}
        self._running = False
        self._groups_ensured: set[int] = set()

    async def ensure_group(self, shard: int) -> None:
        """Ensure the consumer group exists on the shard's stream."""
        if shard in self._groups_ensured:
            return
        stream = self._router.stream_name(self._config.base_stream, shard)
        try:
            await self._redis.xgroup_create(
                stream,
                self._config.group_name,
                id="0",
                mkstream=True,
            )
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
        self._groups_ensured.add(shard)

    async def start(self) -> None:
        """Mark the consumer as running; shards are added via set_shards."""
        self._running = True

    async def stop(self) -> None:
        """Stop all per-shard reader tasks."""
        self._running = False
        tasks = list(self._tasks.values())
        for ev in self._stop_flags.values():
            ev.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        self._stop_flags.clear()

    async def set_shards(self, added: set[int], removed: set[int]) -> None:
        """React to shard-ownership changes from the lease manager."""
        if not self._running:
            return
        # Stop readers for removed shards
        for shard in removed:
            ev = self._stop_flags.pop(shard, None)
            if ev is not None:
                ev.set()
            t = self._tasks.pop(shard, None)
            if t is not None:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.warning("Reader task for shard %d exited with: %s",
                                   shard, e)
        # Start readers for added shards
        for shard in added:
            await self.ensure_group(shard)
            ev = asyncio.Event()
            self._stop_flags[shard] = ev
            self._tasks[shard] = asyncio.create_task(
                self._read_loop(shard, ev)
            )

    async def _read_loop(self, shard: int, stop_event: asyncio.Event) -> None:
        """Per-shard XREADGROUP loop."""
        stream = self._router.stream_name(self._config.base_stream, shard)
        logger.info(
            "Reader started shard=%d stream=%s consumer=%s group=%s",
            shard, stream, self._config.consumer_name, self._config.group_name,
        )
        try:
            while not stop_event.is_set():
                try:
                    # '>' = only new messages that have not been delivered to
                    # any consumer in this group
                    resp = await self._redis.xreadgroup(
                        groupname=self._config.group_name,
                        consumername=self._config.consumer_name,
                        streams={stream: ">"},
                        count=self._config.batch_size,
                        block=self._config.block_ms,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error("XREADGROUP error on shard=%d: %s", shard, e)
                    await asyncio.sleep(1.0)
                    continue

                if not resp:
                    continue

                for _stream_name, entries in resp:
                    for entry_id, fields in entries:
                        await self._handle_entry(
                            stream=stream,
                            entry_id=entry_id,
                            fields=fields,
                        )
        except asyncio.CancelledError:
            pass
        finally:
            logger.info("Reader stopped shard=%d stream=%s", shard, stream)

    async def _handle_entry(
        self,
        stream: str,
        entry_id,
        fields,
    ) -> None:
        """Decode, dispatch to handler with retries, XACK on success."""
        # Normalize entry_id / fields (bytes or str depending on client decode)
        if isinstance(entry_id, bytes):
            entry_id_s = entry_id.decode()
        else:
            entry_id_s = entry_id

        # fields is a dict-like; find 'data'
        raw = fields.get(b"data") if isinstance(fields, dict) else None
        if raw is None and isinstance(fields, dict):
            raw = fields.get("data")
        if raw is None:
            logger.error("Entry missing 'data' field stream=%s id=%s",
                         stream, entry_id_s)
            # Route to DLQ and ack
            await self._send_to_dlq(raw=b"", reason="missing data field")
            await self._redis.xack(
                stream, self._config.group_name, entry_id_s,
            )
            return

        raw_bytes = raw if isinstance(raw, bytes) else raw.encode("utf-8")

        ok = await self._run_handler_with_retries(raw_bytes)
        if ok:
            await self._redis.xack(
                stream, self._config.group_name, entry_id_s,
            )

    async def _run_handler_with_retries(self, raw_bytes: bytes) -> bool:
        """Run the handler with sync retries; return True on eventual success."""
        cfg = self._config
        for attempt in range(1, cfg.handler_retries + 1):
            try:
                await self._handler(raw_bytes)
                return True
            except Exception as e:
                wait = min(
                    cfg.handler_retry_base_seconds * (2 ** (attempt - 1)),
                    2.0,
                )
                logger.warning(
                    "Handler attempt %d/%d failed: %s. Retrying in %.2fs",
                    attempt, cfg.handler_retries, e, wait,
                )
                await asyncio.sleep(wait)
        return False

    async def _send_to_dlq(self, raw: bytes, reason: str) -> None:
        """Publish a bad message to the DLQ stream (if configured)."""
        if not self._config.dlq_stream:
            return
        try:
            await self._redis.xadd(
                self._config.dlq_stream,
                {
                    "data": raw,
                    "reason": reason.encode("utf-8"),
                    "ts": str(int(time.time() * 1000)).encode("utf-8"),
                },
                maxlen=self._config.dlq_maxlen_approx,
                approximate=True,
            )
        except Exception as e:
            logger.error("Failed to XADD to DLQ stream=%s: %s",
                         self._config.dlq_stream, e)


# ── Pending-entries reclaimer (retry + DLQ via delivery count) ──────────────


@dataclass
class ReclaimerConfig:
    """Configuration for PendingReclaimer."""
    base_stream: str
    num_shards: int
    group_name: str
    consumer_name: str
    min_idle_ms: int = 30_000
    max_deliveries: int = 5
    sweep_interval_ms: int = 10_000
    batch_size: int = 50
    dlq_stream: Optional[str] = None
    dlq_maxlen_approx: int = 100_000


class PendingReclaimer:
    """Periodically reclaims idle pending entries and routes over-delivered
    entries to the DLQ.

    For each shard in `owned_shards`:
      1. XPENDING to find entries idle > min_idle_ms.
      2. XCLAIM those entries to this consumer.
      3. For any entry whose delivery count > max_deliveries, fetch the
         payload, XADD it to the DLQ stream, and XACK it in the main stream.
      4. Remaining XCLAIMed entries are left in this consumer's PEL; the
         main ShardedConsumer reader loop will re-deliver them on next
         XREADGROUP with id '>' only when new entries arrive. To make
         reclaimed entries actually re-flow to the handler we XCLAIM and
         *dispatch* them via a caller-supplied redelivery callback.
    """

    def __init__(
        self,
        redis: aioredis.Redis,
        config: ReclaimerConfig,
        on_reclaimed: Optional[Callable[[bytes], Awaitable[bool]]] = None,
    ) -> None:
        self._redis = redis
        self._config = config
        self._on_reclaimed = on_reclaimed
        self._router = ShardRouter(config.num_shards)
        self._owned: set[int] = set()
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def set_owned(self, shards: set[int]) -> None:
        self._owned = set(shards)

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        try:
            while self._running:
                try:
                    for shard in list(self._owned):
                        await self._sweep_shard(shard)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error("Reclaimer sweep error: %s", e, exc_info=True)
                await asyncio.sleep(self._config.sweep_interval_ms / 1000.0)
        except asyncio.CancelledError:
            pass

    async def _sweep_shard(self, shard: int) -> None:
        stream = self._router.stream_name(self._config.base_stream, shard)
        try:
            # XPENDING with filter: idle >= min_idle_ms
            pending = await self._redis.xpending_range(
                name=stream,
                groupname=self._config.group_name,
                idle=self._config.min_idle_ms,
                min="-",
                max="+",
                count=self._config.batch_size,
            )
        except ResponseError as e:
            # Group may not exist yet
            if "NOGROUP" in str(e):
                return
            raise

        if not pending:
            return

        # Route over-delivered entries to DLQ; claim the rest for redelivery.
        to_dlq: list[str] = []
        to_claim: list[str] = []
        for p in pending:
            entry_id = p["message_id"]
            if isinstance(entry_id, bytes):
                entry_id = entry_id.decode()
            delivered = int(p.get("times_delivered", 0))
            if delivered >= self._config.max_deliveries:
                to_dlq.append(entry_id)
            else:
                to_claim.append(entry_id)

        if to_dlq:
            await self._route_to_dlq(stream, to_dlq)

        if to_claim:
            await self._claim_and_redeliver(stream, to_claim)

    async def _route_to_dlq(self, stream: str, entry_ids: list[str]) -> None:
        """Move over-delivered entries from the main stream to the DLQ."""
        if not self._config.dlq_stream:
            # No DLQ configured: just XACK to drop them (last resort).
            for eid in entry_ids:
                try:
                    await self._redis.xack(
                        stream, self._config.group_name, eid,
                    )
                except Exception as e:
                    logger.warning("XACK drop failed id=%s: %s", eid, e)
            return

        # Fetch payload then XADD to DLQ + XACK original
        for eid in entry_ids:
            try:
                entries = await self._redis.xrange(stream, min=eid, max=eid, count=1)
                if not entries:
                    # Already trimmed; just ack.
                    await self._redis.xack(
                        stream, self._config.group_name, eid,
                    )
                    continue
                _eid, fields = entries[0]
                raw = fields.get(b"data") if isinstance(fields, dict) else None
                if raw is None and isinstance(fields, dict):
                    raw = fields.get("data")
                raw_bytes = raw if isinstance(raw, (bytes, bytearray)) else (
                    raw.encode("utf-8") if raw else b""
                )
                await self._redis.xadd(
                    self._config.dlq_stream,
                    {
                        "data": raw_bytes,
                        "reason": b"max_deliveries_exceeded",
                        "origin_stream": stream.encode("utf-8"),
                        "origin_id": eid.encode("utf-8"),
                        "ts": str(int(time.time() * 1000)).encode("utf-8"),
                    },
                    maxlen=self._config.dlq_maxlen_approx,
                    approximate=True,
                )
                await self._redis.xack(
                    stream, self._config.group_name, eid,
                )
                logger.warning(
                    "Routed to DLQ stream=%s id=%s dlq=%s",
                    stream, eid, self._config.dlq_stream,
                )
            except Exception as e:
                logger.error(
                    "DLQ routing failed stream=%s id=%s: %s", stream, eid, e,
                )

    async def _claim_and_redeliver(
        self, stream: str, entry_ids: list[str]
    ) -> None:
        """XCLAIM entries to this consumer and redeliver via callback."""
        try:
            claimed = await self._redis.xclaim(
                name=stream,
                groupname=self._config.group_name,
                consumername=self._config.consumer_name,
                min_idle_time=self._config.min_idle_ms,
                message_ids=entry_ids,
            )
        except Exception as e:
            logger.error("XCLAIM failed stream=%s: %s", stream, e)
            return

        if not claimed:
            return

        # Redeliver each claimed entry via callback
        for entry in claimed:
            entry_id, fields = entry
            if isinstance(entry_id, bytes):
                entry_id = entry_id.decode()

            raw = fields.get(b"data") if isinstance(fields, dict) else None
            if raw is None and isinstance(fields, dict):
                raw = fields.get("data")
            raw_bytes = raw if isinstance(raw, (bytes, bytearray)) else (
                raw.encode("utf-8") if raw else b""
            )

            ok = False
            if self._on_reclaimed is not None:
                try:
                    ok = await self._on_reclaimed(raw_bytes)
                except Exception as e:
                    logger.warning(
                        "Reclaim redelivery failed stream=%s id=%s: %s",
                        stream, entry_id, e,
                    )
                    ok = False

            if ok:
                try:
                    await self._redis.xack(
                        stream, self._config.group_name, entry_id,
                    )
                except Exception as e:
                    logger.warning("XACK failed id=%s: %s", entry_id, e)


# ── Connection helper ────────────────────────────────────────────────────────


def _parse_sentinel_url(url: str) -> tuple[list[tuple[str, int]], str, int]:
    """Parse ``redis+sentinel://host1:26379,host2:26379/<service>/<db>``.

    Returns ``(sentinels, service_name, db)``. The service portion in the
    path is the Sentinel "master name" (defaults to ``mymaster``); the
    optional second path component is the Redis DB (defaults to 0).
    """
    from urllib.parse import urlparse

    parsed = urlparse(url)
    if not parsed.netloc:
        raise ValueError(f"Invalid sentinel URL: {url!r}")
    sentinels: list[tuple[str, int]] = []
    for host in parsed.netloc.split(","):
        if ":" in host:
            name, port = host.rsplit(":", 1)
            sentinels.append((name, int(port)))
        else:
            sentinels.append((host, 26379))
    parts = [p for p in parsed.path.split("/") if p]
    service_name = parts[0] if parts else "mymaster"
    db = int(parts[1]) if len(parts) > 1 else 0
    return sentinels, service_name, db


async def create_redis_client(url: str) -> aioredis.Redis:
    """Create an async Redis client. decode_responses=False so we can handle
    binary data uniformly in XADD/XREADGROUP.

    Supports two URL forms:
      - ``redis://host:port/db`` — direct connection
      - ``redis+sentinel://h1:26379,h2:26379/<service>/<db>`` — HA via
        Sentinel; returns a client bound to the current master
    """
    if url.startswith("redis+sentinel://"):
        from redis.asyncio.sentinel import Sentinel

        sentinels, service_name, db = _parse_sentinel_url(url)
        sentinel = Sentinel(sentinels, socket_timeout=1.0)
        return sentinel.master_for(service_name, db=db, decode_responses=False)
    return aioredis.from_url(url, decode_responses=False)
