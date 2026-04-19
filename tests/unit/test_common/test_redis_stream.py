"""Tests for chris_streaming.common.redis_stream.

Covers:
  - ShardRouter stable hashing + shard assignment
  - RedisStreamProducer.xadd (happy path, retry, max-retries)
  - ShardLeaseManager (acquire / release / single-writer guarantee)
  - PendingReclaimer (DLQ routing, reclaim + redeliver)

Uses fakeredis for an in-memory async Redis so tests do not depend on
external services. Areas where fakeredis does not yet fully implement a
Redis command (e.g. XPENDING idle filtering) are covered with small
AsyncMock harnesses instead.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import fakeredis.aioredis
import pytest

from chris_streaming.common.redis_stream import (
    ConsumerConfig,
    LeaseManagerConfig,
    PendingReclaimer,
    ReclaimerConfig,
    RedisStreamProducer,
    ShardLeaseManager,
    ShardRouter,
    _parse_sentinel_url,
    ShardedConsumer,
    StreamProducerConfig,
    _stable_hash,
)


# ── Shard routing ───────────────────────────────────────────────────────────


class TestStableHash:
    def test_deterministic(self):
        assert _stable_hash("job-1") == _stable_hash("job-1")

    def test_differs_for_different_inputs(self):
        assert _stable_hash("job-1") != _stable_hash("job-2")


class TestShardRouter:
    def test_num_shards_must_be_positive(self):
        with pytest.raises(ValueError):
            ShardRouter(0)

    def test_shard_for_in_range(self):
        router = ShardRouter(8)
        for i in range(100):
            shard = router.shard_for(f"job-{i}")
            assert 0 <= shard < 8

    def test_stable_across_calls(self):
        router = ShardRouter(8)
        assert router.shard_for("job-1") == router.shard_for("job-1")

    def test_stream_name(self):
        router = ShardRouter(4)
        assert router.stream_name("stream:job-status", 2) == "stream:job-status:2"

    def test_all_streams(self):
        router = ShardRouter(3)
        assert router.all_streams("x") == ["x:0", "x:1", "x:2"]


# ── Producer ────────────────────────────────────────────────────────────────


class TestRedisStreamProducer:
    async def test_xadd_writes_to_correct_shard(self):
        redis = fakeredis.aioredis.FakeRedis(decode_responses=False)
        cfg = StreamProducerConfig(base_stream="stream:x", num_shards=4)
        producer = RedisStreamProducer(redis, cfg)

        entry_id = await producer.xadd("job-abc", b"payload")

        assert isinstance(entry_id, str)
        shard = producer.router.shard_for("job-abc")
        stream = f"stream:x:{shard}"
        # Verify the entry landed in the right shard stream with the raw bytes
        entries = await redis.xrange(stream)
        assert len(entries) == 1
        _eid, fields = entries[0]
        assert fields[b"data"] == b"payload"
        await redis.aclose()

    async def test_xadd_retries_then_succeeds(self):
        """Transient failures on XADD are retried."""
        redis = AsyncMock()
        redis.xadd = AsyncMock(
            side_effect=[Exception("transient"), b"1-0"],
        )
        cfg = StreamProducerConfig(
            base_stream="s",
            num_shards=1,
            max_send_retries=3,
            retry_base_seconds=0.001,
        )
        producer = RedisStreamProducer(redis, cfg)

        entry_id = await producer.xadd("job-1", b"payload")
        assert entry_id == "1-0"
        assert redis.xadd.await_count == 2

    async def test_xadd_raises_after_max_retries(self):
        redis = AsyncMock()
        redis.xadd = AsyncMock(side_effect=Exception("down"))
        cfg = StreamProducerConfig(
            base_stream="s",
            num_shards=1,
            max_send_retries=2,
            retry_base_seconds=0.001,
        )
        producer = RedisStreamProducer(redis, cfg)

        with pytest.raises(RuntimeError, match="XADD to"):
            await producer.xadd("job-1", b"payload")
        assert redis.xadd.await_count == 2


# ── Shard lease ─────────────────────────────────────────────────────────────


class TestShardLeaseManager:
    async def test_single_replica_acquires_all_shards(self):
        redis = fakeredis.aioredis.FakeRedis(decode_responses=False)
        cfg = LeaseManagerConfig(
            num_shards=4,
            lease_key_prefix="test",
            replica_id="replica-a",
            lease_ttl_ms=5000,
            acquire_interval_ms=50,
        )
        manager = ShardLeaseManager(redis, cfg)
        await manager.start()
        try:
            # Let the loop run at least one rebalance
            for _ in range(20):
                if len(manager.owned_shards) == 4:
                    break
                await asyncio.sleep(0.05)
            assert manager.owned_shards == {0, 1, 2, 3}
        finally:
            await manager.stop()
            await redis.aclose()

    async def test_two_replicas_share_shards(self):
        redis = fakeredis.aioredis.FakeRedis(decode_responses=False)
        cfg_a = LeaseManagerConfig(
            num_shards=4, lease_key_prefix="test",
            replica_id="replica-a",
            lease_ttl_ms=5000, acquire_interval_ms=50,
        )
        cfg_b = LeaseManagerConfig(
            num_shards=4, lease_key_prefix="test",
            replica_id="replica-b",
            lease_ttl_ms=5000, acquire_interval_ms=50,
        )
        a = ShardLeaseManager(redis, cfg_a)
        b = ShardLeaseManager(redis, cfg_b)
        await a.start()
        await b.start()
        try:
            # Wait for both to stabilize — together they should own all shards
            # and there should be no overlap (single-writer guarantee).
            for _ in range(40):
                union = a.owned_shards | b.owned_shards
                intersect = a.owned_shards & b.owned_shards
                if union == {0, 1, 2, 3} and not intersect:
                    break
                await asyncio.sleep(0.1)
            assert (a.owned_shards | b.owned_shards) == {0, 1, 2, 3}
            assert not (a.owned_shards & b.owned_shards), \
                "two replicas must never hold the same shard"
            # Each replica should hold roughly half the shards
            assert len(a.owned_shards) in (1, 2, 3)
            assert len(b.owned_shards) in (1, 2, 3)
        finally:
            await a.stop()
            await b.stop()
            await redis.aclose()

    async def test_listener_called_on_acquire_and_release(self):
        redis = fakeredis.aioredis.FakeRedis(decode_responses=False)
        cfg = LeaseManagerConfig(
            num_shards=2, lease_key_prefix="test",
            replica_id="replica-a",
            lease_ttl_ms=5000, acquire_interval_ms=50,
        )
        manager = ShardLeaseManager(redis, cfg)
        events: list[tuple[set[int], set[int]]] = []

        async def listener(added, removed):
            events.append((set(added), set(removed)))

        manager.add_listener(listener)
        await manager.start()
        try:
            for _ in range(20):
                if len(manager.owned_shards) == 2:
                    break
                await asyncio.sleep(0.05)
        finally:
            await manager.stop()
            await redis.aclose()

        # We should have seen at least two "added" events for the two shards,
        # and at least one "removed" event during stop()
        added_all = set().union(*[a for a, _ in events])
        removed_all = set().union(*[r for _, r in events])
        assert {0, 1}.issubset(added_all)
        assert {0, 1}.issubset(removed_all)

    async def test_stop_releases_leases(self):
        redis = fakeredis.aioredis.FakeRedis(decode_responses=False)
        cfg = LeaseManagerConfig(
            num_shards=2, lease_key_prefix="test",
            replica_id="replica-a",
            lease_ttl_ms=5000, acquire_interval_ms=50,
        )
        manager = ShardLeaseManager(redis, cfg)
        await manager.start()
        try:
            for _ in range(20):
                if len(manager.owned_shards) == 2:
                    break
                await asyncio.sleep(0.05)
        finally:
            await manager.stop()

        # After stop, both lease keys should be gone
        key_0 = await redis.get("test:lease:0")
        key_1 = await redis.get("test:lease:1")
        assert key_0 is None
        assert key_1 is None
        await redis.aclose()


# ── Pending reclaimer ───────────────────────────────────────────────────────


class TestPendingReclaimer:
    def _make(self, redis, on_reclaimed=None, max_deliveries=5, dlq=None):
        cfg = ReclaimerConfig(
            base_stream="stream:x",
            num_shards=2,
            group_name="g",
            consumer_name="c",
            min_idle_ms=1000,
            max_deliveries=max_deliveries,
            sweep_interval_ms=100,
            dlq_stream=dlq,
        )
        return PendingReclaimer(redis, cfg, on_reclaimed=on_reclaimed)

    async def test_over_delivered_entries_routed_to_dlq_and_acked(self):
        redis = AsyncMock()
        # XPENDING returns one message, delivered 6 times (> max 5)
        redis.xpending_range = AsyncMock(return_value=[
            {"message_id": b"1-0", "times_delivered": 6},
        ])
        # XRANGE fetch payload
        redis.xrange = AsyncMock(return_value=[
            (b"1-0", {b"data": b"bad-payload"}),
        ])
        redis.xadd = AsyncMock()
        redis.xack = AsyncMock()

        reclaimer = self._make(redis, dlq="stream:dlq", max_deliveries=5)
        reclaimer.set_owned({0})
        await reclaimer._sweep_shard(0)

        redis.xadd.assert_awaited_once()
        # First positional arg is the DLQ stream
        assert redis.xadd.call_args[0][0] == "stream:dlq"
        # ACK on the origin stream
        redis.xack.assert_awaited_once_with("stream:x:0", "g", "1-0")

    async def test_over_delivered_without_dlq_still_acked(self):
        redis = AsyncMock()
        redis.xpending_range = AsyncMock(return_value=[
            {"message_id": b"1-0", "times_delivered": 99},
        ])
        redis.xack = AsyncMock()

        reclaimer = self._make(redis, dlq=None, max_deliveries=5)
        reclaimer.set_owned({0})
        await reclaimer._sweep_shard(0)

        redis.xack.assert_awaited_once_with("stream:x:0", "g", "1-0")

    async def test_under_delivered_entries_claimed_and_redelivered(self):
        redis = AsyncMock()
        redis.xpending_range = AsyncMock(return_value=[
            {"message_id": b"1-0", "times_delivered": 1},
        ])
        redis.xclaim = AsyncMock(return_value=[
            (b"1-0", {b"data": b"payload"}),
        ])
        redis.xack = AsyncMock()

        on_reclaimed = AsyncMock(return_value=True)
        reclaimer = self._make(redis, on_reclaimed=on_reclaimed)
        reclaimer.set_owned({0})
        await reclaimer._sweep_shard(0)

        on_reclaimed.assert_awaited_once_with(b"payload")
        # ACK only after the redelivery callback returned True
        redis.xack.assert_awaited_once_with("stream:x:0", "g", "1-0")

    async def test_redelivery_failure_leaves_in_pel(self):
        redis = AsyncMock()
        redis.xpending_range = AsyncMock(return_value=[
            {"message_id": b"1-0", "times_delivered": 1},
        ])
        redis.xclaim = AsyncMock(return_value=[
            (b"1-0", {b"data": b"payload"}),
        ])
        redis.xack = AsyncMock()

        on_reclaimed = AsyncMock(return_value=False)
        reclaimer = self._make(redis, on_reclaimed=on_reclaimed)
        reclaimer.set_owned({0})
        await reclaimer._sweep_shard(0)

        # No ACK — the entry must stay in the PEL for the next sweep
        redis.xack.assert_not_awaited()

    async def test_empty_pending_is_noop(self):
        redis = AsyncMock()
        redis.xpending_range = AsyncMock(return_value=[])
        redis.xclaim = AsyncMock()
        redis.xack = AsyncMock()

        reclaimer = self._make(redis)
        reclaimer.set_owned({0})
        await reclaimer._sweep_shard(0)

        redis.xclaim.assert_not_awaited()
        redis.xack.assert_not_awaited()


# ── Sharded consumer ────────────────────────────────────────────────────────


class TestShardedConsumer:
    async def test_handle_entry_acks_on_success(self):
        redis = AsyncMock()
        redis.xack = AsyncMock()
        handler = AsyncMock()
        cfg = ConsumerConfig(
            base_stream="stream:x",
            num_shards=2,
            group_name="g",
            consumer_name="c",
            handler_retries=1,
        )
        consumer = ShardedConsumer(redis, cfg, handler)

        await consumer._handle_entry(
            stream="stream:x:0",
            entry_id=b"1-0",
            fields={b"data": b"payload"},
        )

        handler.assert_awaited_once_with(b"payload")
        redis.xack.assert_awaited_once_with("stream:x:0", "g", "1-0")

    async def test_handle_entry_no_ack_on_handler_failure(self):
        redis = AsyncMock()
        redis.xack = AsyncMock()
        handler = AsyncMock(side_effect=Exception("boom"))
        cfg = ConsumerConfig(
            base_stream="stream:x",
            num_shards=2,
            group_name="g",
            consumer_name="c",
            handler_retries=2,
            handler_retry_base_seconds=0.001,
        )
        consumer = ShardedConsumer(redis, cfg, handler)

        await consumer._handle_entry(
            stream="stream:x:0",
            entry_id=b"1-0",
            fields={b"data": b"payload"},
        )

        # All retries exhausted, still no ack (left in PEL for reclaim)
        assert handler.await_count == 2
        redis.xack.assert_not_awaited()

    async def test_handle_entry_missing_data_goes_to_dlq(self):
        redis = AsyncMock()
        redis.xack = AsyncMock()
        redis.xadd = AsyncMock()
        handler = AsyncMock()
        cfg = ConsumerConfig(
            base_stream="stream:x",
            num_shards=2,
            group_name="g",
            consumer_name="c",
            dlq_stream="stream:x-dlq",
        )
        consumer = ShardedConsumer(redis, cfg, handler)

        await consumer._handle_entry(
            stream="stream:x:0",
            entry_id=b"1-0",
            fields={},
        )

        handler.assert_not_awaited()
        redis.xadd.assert_awaited_once()
        assert redis.xadd.call_args[0][0] == "stream:x-dlq"
        redis.xack.assert_awaited_once_with("stream:x:0", "g", "1-0")

    async def test_ensure_group_tolerates_busygroup(self):
        from redis.exceptions import ResponseError

        redis = AsyncMock()
        redis.xgroup_create = AsyncMock(
            side_effect=ResponseError("BUSYGROUP Consumer Group name already exists"),
        )
        handler = AsyncMock()
        cfg = ConsumerConfig(
            base_stream="stream:x",
            num_shards=1,
            group_name="g",
            consumer_name="c",
        )
        consumer = ShardedConsumer(redis, cfg, handler)

        # Should not raise
        await consumer.ensure_group(0)
        assert 0 in consumer._groups_ensured

    async def test_ensure_group_reraises_other_errors(self):
        from redis.exceptions import ResponseError

        redis = AsyncMock()
        redis.xgroup_create = AsyncMock(
            side_effect=ResponseError("some other error"),
        )
        handler = AsyncMock()
        cfg = ConsumerConfig(
            base_stream="stream:x",
            num_shards=1,
            group_name="g",
            consumer_name="c",
        )
        consumer = ShardedConsumer(redis, cfg, handler)

        with pytest.raises(ResponseError):
            await consumer.ensure_group(0)


class TestParseSentinelUrl:
    """Parsing of ``redis+sentinel://`` URLs for HA connections."""

    def test_single_host_defaults(self):
        sentinels, service, db = _parse_sentinel_url(
            "redis+sentinel://sentinel-0:26379/mymaster/0"
        )
        assert sentinels == [("sentinel-0", 26379)]
        assert service == "mymaster"
        assert db == 0

    def test_multiple_hosts(self):
        sentinels, service, db = _parse_sentinel_url(
            "redis+sentinel://a:26379,b:26379,c:26379/mymaster/0"
        )
        assert sentinels == [("a", 26379), ("b", 26379), ("c", 26379)]
        assert service == "mymaster"

    def test_default_port_when_missing(self):
        sentinels, _, _ = _parse_sentinel_url(
            "redis+sentinel://a,b/mymaster"
        )
        assert sentinels == [("a", 26379), ("b", 26379)]

    def test_default_service_when_path_empty(self):
        sentinels, service, db = _parse_sentinel_url(
            "redis+sentinel://a:26379"
        )
        assert sentinels == [("a", 26379)]
        assert service == "mymaster"
        assert db == 0

    def test_custom_service_and_db(self):
        _, service, db = _parse_sentinel_url(
            "redis+sentinel://a:26379/chris-master/3"
        )
        assert service == "chris-master"
        assert db == 3

    def test_invalid_url_raises(self):
        with pytest.raises(ValueError):
            _parse_sentinel_url("redis+sentinel:///mymaster/0")
