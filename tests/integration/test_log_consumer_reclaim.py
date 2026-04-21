"""Integration tests for the Log Consumer PEL reclaim + DLQ wiring.

Spin up a real ``LogEventConsumer`` + ``PendingReclaimer`` against live
Redis + Quickwit. The test simulates a crashed replica by XADDing an
entry, XREADGROUP-ing it (so it lands in the PEL) but never XACKing —
then waits for the reclaimer to either (a) redeliver via the callback
and land the doc in Quickwit, or (b) exile the entry to the DLQ after
N deliveries.
"""

from __future__ import annotations

import asyncio
import os
import socket
import uuid
from datetime import datetime, timezone

import pytest

from chris_streaming.common.quickwit import QuickwitClient
from chris_streaming.common.redis_stream import (
    PendingReclaimer,
    ReclaimerConfig,
    create_redis_client,
)
from chris_streaming.common.schemas import JobType, LogEvent
from chris_streaming.log_consumer.consumer import LogEventConsumer
from chris_streaming.log_consumer.quickwit_writer import (
    QuickwitWriter,
    _load_index_config,
)


pytestmark = pytest.mark.integration


BASE_LOGS = "test:log-reclaim"
DLQ_LOGS = "test:log-reclaim-dlq"
GROUP = "log-reclaim-group"


@pytest.fixture
async def redis(redis_url):
    r = await create_redis_client(redis_url)
    try:
        yield r
    finally:
        await r.close()


async def _cleanup_streams(redis, base: str, num_shards: int, dlq: str) -> None:
    for i in range(num_shards):
        await redis.delete(f"{base}:{i}")
    await redis.delete(dlq)


def _make_event(job_id: str, line: str) -> LogEvent:
    return LogEvent(
        job_id=job_id,
        job_type=JobType.plugin,
        container_name=job_id,
        stream="stdout",
        line=line,
        timestamp=datetime.now(tz=timezone.utc),
    )


@pytest.fixture
async def quickwit_writer(quickwit_url, quickwit_index):
    w = QuickwitWriter(quickwit_url, index_id=quickwit_index)
    await w.connect()
    try:
        yield w
    finally:
        await w.close()



async def _ensure_group(redis, stream: str) -> None:
    try:
        await redis.xgroup_create(stream, GROUP, id="0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            raise


class TestLogConsumerReclaim:
    @pytest.mark.asyncio
    async def test_reclaim_redelivers_and_lands_in_quickwit(
        self, redis, redis_url, quickwit_writer,
        quickwit_url, quickwit_index,
    ):
        """A PEL entry that's reclaimed must end up committed to Quickwit."""
        num_shards = 2
        unique = uuid.uuid4().hex[:8]
        base = f"{BASE_LOGS}:{unique}"
        dlq = f"{DLQ_LOGS}:{unique}"
        job_id = f"reclaim-ok-{unique}"

        await _cleanup_streams(redis, base, num_shards, dlq)

        # Emulate a crashed replica: XADD a log event, then XREADGROUP it to
        # a fake consumer "crashed-replica" so it lands in the PEL without
        # being ACKed.
        stream_name = f"{base}:0"
        await _ensure_group(redis, stream_name)
        event = _make_event(job_id, "reclaim me")
        await redis.xadd(stream_name, {"data": event.serialize()})
        # Read into the fake consumer's PEL.
        await redis.xreadgroup(
            groupname=GROUP,
            consumername="crashed-replica",
            streams={stream_name: ">"},
            count=1,
        )

        # Now set up the real consumer + reclaimer.
        consumer_name = f"{socket.gethostname()}:{os.getpid()}:active"
        consumer = LogEventConsumer(
            redis=redis,
            base_stream=base,
            num_shards=num_shards,
            group_name=GROUP,
            consumer_name=consumer_name,
            quickwit=quickwit_writer,
            batch_max_size=10,
            batch_max_wait_seconds=0.5,
        )

        cfg = ReclaimerConfig(
            base_stream=base,
            num_shards=num_shards,
            group_name=GROUP,
            consumer_name=consumer_name,
            min_idle_ms=100,
            max_deliveries=5,
            sweep_interval_ms=100,
            dlq_stream=dlq,
        )
        reclaimer = PendingReclaimer(redis, cfg, on_reclaimed=consumer.reclaim)
        reclaimer.set_owned(set(range(num_shards)))

        await reclaimer.start()
        try:
            # Wait for Quickwit to see the document.
            qw = QuickwitClient(quickwit_url, index_id=quickwit_index)
            await qw.connect(index_config=_load_index_config())
            try:
                for _ in range(40):  # up to ~4s
                    result = await qw.search_by_job(job_id, limit=10)
                    if any(d.get("line") == "reclaim me" for d in result["lines"]):
                        break
                    await asyncio.sleep(0.1)
                else:
                    pytest.fail(
                        "reclaimed log event never arrived in Quickwit",
                    )
            finally:
                await qw.close()

            # And it must have been XACK'd so XLEN-of-PEL is 0.
            pending = await redis.xpending(stream_name, GROUP)
            # xpending returns a dict {'pending': N, ...} on this redis-py.
            n = pending["pending"] if isinstance(pending, dict) else pending[0]
            assert n == 0, f"expected empty PEL, got {pending}"
        finally:
            await reclaimer.stop()
            await _cleanup_streams(redis, base, num_shards, dlq)

    @pytest.mark.asyncio
    async def test_over_delivered_entry_lands_in_dlq(
        self, redis, quickwit_writer,
    ):
        """After max_deliveries the reclaimer must route the entry to DLQ."""
        num_shards = 2
        unique = uuid.uuid4().hex[:8]
        base = f"{BASE_LOGS}:{unique}"
        dlq = f"{DLQ_LOGS}:{unique}"
        job_id = f"reclaim-dlq-{unique}"

        await _cleanup_streams(redis, base, num_shards, dlq)

        stream_name = f"{base}:0"
        await _ensure_group(redis, stream_name)
        event = _make_event(job_id, "should go to dlq")
        await redis.xadd(stream_name, {"data": event.serialize()})

        # Bump delivery count past max_deliveries by XREADGROUPing N times
        # from separate fake consumers without ACKing. Each call increments
        # times_delivered (if the entry is already claimed, XCLAIMing to a
        # different consumer also increments the count).
        for i in range(6):
            # XCLAIM to a new consumer with min_idle_time=0 so we can
            # force re-delivery immediately.
            await asyncio.sleep(0.01)
            try:
                await redis.xclaim(
                    name=stream_name,
                    groupname=GROUP,
                    consumername=f"fake-{i}",
                    min_idle_time=0,
                    message_ids=[],
                )
            except Exception:
                pass
            # First read claims the entry for this consumer.
            await redis.xreadgroup(
                groupname=GROUP,
                consumername=f"fake-{i}",
                streams={stream_name: ">" if i == 0 else "0"},
                count=10,
            )

        # Verify delivery count is >= 6 before starting reclaimer.
        pending = await redis.xpending_range(
            name=stream_name, groupname=GROUP,
            min="-", max="+", count=10,
        )
        delivered = max(int(p.get("times_delivered", 0)) for p in pending) if pending else 0
        # Sanity: if still 1, bump via XCLAIM a few more times.
        for i in range(max(0, 6 - delivered)):
            await redis.xclaim(
                name=stream_name,
                groupname=GROUP,
                consumername=f"bump-{i}",
                min_idle_time=0,
                message_ids=[p["message_id"] for p in pending],
            )

        consumer_name = f"{socket.gethostname()}:{os.getpid()}:dlq"
        consumer = LogEventConsumer(
            redis=redis,
            base_stream=base,
            num_shards=num_shards,
            group_name=GROUP,
            consumer_name=consumer_name,
            quickwit=quickwit_writer,
            batch_max_size=10,
            batch_max_wait_seconds=0.5,
        )
        cfg = ReclaimerConfig(
            base_stream=base,
            num_shards=num_shards,
            group_name=GROUP,
            consumer_name=consumer_name,
            min_idle_ms=0,
            max_deliveries=5,
            sweep_interval_ms=100,
            dlq_stream=dlq,
        )
        reclaimer = PendingReclaimer(redis, cfg, on_reclaimed=consumer.reclaim)
        reclaimer.set_owned(set(range(num_shards)))

        await reclaimer.start()
        try:
            for _ in range(40):  # up to ~4s
                dlq_len = await redis.xlen(dlq)
                if dlq_len >= 1:
                    break
                await asyncio.sleep(0.1)
            else:
                pytest.fail("over-delivered entry never reached DLQ")

            # DLQ entry must carry the original job payload.
            entries = await redis.xrange(dlq, count=10)
            assert entries, "DLQ unexpectedly empty"
            _eid, fields = entries[0]
            data_key = b"data" if b"data" in fields else "data"
            payload = fields[data_key]
            raw_bytes = payload if isinstance(payload, (bytes, bytearray)) else payload.encode()
            parsed = LogEvent.deserialize(raw_bytes)
            assert parsed.job_id == job_id
            assert parsed.line == "should go to dlq"

            # Original stream PEL is drained.
            pending = await redis.xpending(stream_name, GROUP)
            n = pending["pending"] if isinstance(pending, dict) else pending[0]
            assert n == 0
        finally:
            await reclaimer.stop()
            await _cleanup_streams(redis, base, num_shards, dlq)
