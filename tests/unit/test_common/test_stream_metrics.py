"""Tests for chris_streaming.common.stream_metrics."""

from __future__ import annotations

import fakeredis.aioredis
import pytest

from chris_streaming.common.stream_metrics import collect_stream_metrics


@pytest.fixture
async def redis():
    client = fakeredis.aioredis.FakeRedis(decode_responses=False)
    yield client
    await client.flushall()
    await client.aclose()


class TestCollectStreamMetrics:
    async def test_empty_streams_report_zero_xlen(self, redis):
        metrics = await collect_stream_metrics(
            redis,
            status_base="stream:status",
            status_dlq="stream:status-dlq",
            status_group="status-group",
            logs_base="stream:logs",
            logs_dlq="stream:logs-dlq",
            logs_group="logs-group",
            num_shards=2,
        )
        assert {s["stream"] for s in metrics["status"]["shards"]} == {
            "stream:status:0", "stream:status:1",
        }
        for shard in metrics["status"]["shards"]:
            assert shard["xlen"] == 0
            assert shard["pending"] == 0
        assert metrics["status"]["dlq_xlen"] == 0
        assert metrics["logs"]["dlq_xlen"] == 0

    async def test_xlen_reflects_xadd(self, redis):
        for _ in range(3):
            await redis.xadd("stream:status:0", {"data": b"x"})
        for _ in range(2):
            await redis.xadd("stream:status-dlq", {"data": b"d"})

        metrics = await collect_stream_metrics(
            redis,
            status_base="stream:status",
            status_dlq="stream:status-dlq",
            status_group="status-group",
            logs_base="stream:logs",
            logs_dlq="stream:logs-dlq",
            logs_group="logs-group",
            num_shards=2,
        )
        shard_0 = next(
            s for s in metrics["status"]["shards"] if s["stream"] == "stream:status:0"
        )
        assert shard_0["xlen"] == 3
        assert metrics["status"]["dlq_xlen"] == 2

    async def test_pending_reports_unacked_entries(self, redis):
        stream = "stream:status:0"
        group = "status-group"
        await redis.xadd(stream, {"data": b"a"})
        await redis.xgroup_create(stream, group, id="0", mkstream=True)
        # Deliver + leave unacked -> PEL should have 1 entry
        await redis.xreadgroup(group, "c1", {stream: ">"}, count=10)

        metrics = await collect_stream_metrics(
            redis,
            status_base="stream:status",
            status_dlq="stream:status-dlq",
            status_group=group,
            logs_base="stream:logs",
            logs_dlq="stream:logs-dlq",
            logs_group="logs-group",
            num_shards=1,
        )
        assert metrics["status"]["shards"][0]["pending"] >= 1

    async def test_missing_group_does_not_raise(self, redis):
        # No group registered — _safe_pending should swallow the error.
        await redis.xadd("stream:status:0", {"data": b"x"})
        metrics = await collect_stream_metrics(
            redis,
            status_base="stream:status",
            status_dlq="stream:status-dlq",
            status_group="nonexistent-group",
            logs_base="stream:logs",
            logs_dlq="stream:logs-dlq",
            logs_group="logs-group",
            num_shards=1,
        )
        assert metrics["status"]["shards"][0]["pending"] == 0
