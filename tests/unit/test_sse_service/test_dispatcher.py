"""Tests for chris_streaming.sse_service.dispatcher.StreamDispatcher.

The dispatcher runs one ungrouped XREAD loop per base stream across every
shard and fans entries out by ``job_id`` to per-subscriber asyncio queues.
These tests fake ``redis.xread`` so we can assert dispatch semantics without
a live Redis.
"""

import asyncio

import pytest

from chris_streaming.common.schemas import (
    JobStatus,
    JobType,
    LogEvent,
    StatusEvent,
    WorkflowEvent,
)
from chris_streaming.sse_service.dispatcher import StreamDispatcher


def _entry(event_bytes: bytes, entry_id: bytes = b"1-0"):
    return (entry_id, {b"data": event_bytes})


class _ProgrammableRedis:
    """Minimal aioredis.Redis stub.

    ``per_base`` maps a base-stream name (``"stream:job-status"``) to a queue of
    xread replies. The reader loop for each base dispatches to its own queue so
    responses never cross-pollinate between the three concurrent reader tasks.
    """

    def __init__(self, per_base: dict[str, list]):
        self._per_base = {k: list(v) for k, v in per_base.items()}
        self.calls: list[dict] = []

    async def xread(self, streams, count=None, block=None):
        self.calls.append(dict(streams))
        # Infer which base the caller is reading from by the first stream key.
        any_key = next(iter(streams))
        name = any_key.decode() if isinstance(any_key, bytes) else any_key
        base = name.rsplit(":", 1)[0]
        queue = self._per_base.get(base, [])
        if queue:
            return queue.pop(0)
        # simulate block timeout
        await asyncio.sleep(0.01)
        return None


@pytest.mark.asyncio
async def test_dispatch_routes_by_job_id():
    """An XREAD entry for job A must be delivered only to subscribers of job A."""
    evt_a = StatusEvent(
        job_id="job-a", job_type=JobType.plugin, status=JobStatus.started,
    ).serialize()
    evt_b = StatusEvent(
        job_id="job-b", job_type=JobType.plugin, status=JobStatus.started,
    ).serialize()

    redis = _ProgrammableRedis({
        "stream:job-status": [
            [(b"stream:job-status:0", [_entry(evt_a, b"1-0"),
                                       _entry(evt_b, b"2-0")])],
        ],
    })

    dispatcher = StreamDispatcher(
        redis,
        num_shards=1,
        status_base="stream:job-status",
        logs_base="stream:job-logs",
        workflow_base="stream:job-workflow",
        block_ms=10,
    )
    await dispatcher.start()
    try:
        async with dispatcher.subscribe("job-a", {"status"}) as qa, \
                dispatcher.subscribe("job-b", {"status"}) as qb:
            got_a = await asyncio.wait_for(qa.get(), timeout=1.0)
            got_b = await asyncio.wait_for(qb.get(), timeout=1.0)
            assert got_a["job_id"] == "job-a"
            assert got_a["_event_type"] == "status"
            assert got_b["job_id"] == "job-b"
            # qa should not receive the job-b event
            assert qa.empty()
    finally:
        await dispatcher.stop()


@pytest.mark.asyncio
async def test_event_type_filter():
    """A subscriber for {status} must not receive logs events."""
    log_evt = LogEvent(
        job_id="j1", job_type=JobType.plugin, line="hello",
    ).serialize()

    redis = _ProgrammableRedis({
        "stream:job-logs": [
            [(b"stream:job-logs:0", [_entry(log_evt)])],
        ],
    })

    dispatcher = StreamDispatcher(
        redis,
        num_shards=1,
        status_base="stream:job-status",
        logs_base="stream:job-logs",
        workflow_base="stream:job-workflow",
        block_ms=10,
    )
    await dispatcher.start()
    try:
        async with dispatcher.subscribe("j1", {"status"}) as q_status:
            # Dispatcher should not enqueue the log event on a status-only sub.
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(q_status.get(), timeout=0.2)
    finally:
        await dispatcher.stop()


@pytest.mark.asyncio
async def test_workflow_events_dispatched():
    wf_evt = WorkflowEvent(
        job_id="j1",
        current_step="plugin",
        current_step_status="started",
        workflow_status="running",
    ).serialize()

    redis = _ProgrammableRedis({
        "stream:job-workflow": [
            [(b"stream:job-workflow:0", [_entry(wf_evt)])],
        ],
    })

    dispatcher = StreamDispatcher(
        redis,
        num_shards=1,
        status_base="stream:job-status",
        logs_base="stream:job-logs",
        workflow_base="stream:job-workflow",
        block_ms=10,
    )
    await dispatcher.start()
    try:
        async with dispatcher.subscribe("j1", {"workflow"}) as q:
            got = await asyncio.wait_for(q.get(), timeout=1.0)
            assert got["_event_type"] == "workflow"
            assert got["current_step"] == "plugin"
            assert got["workflow_status"] == "running"
    finally:
        await dispatcher.stop()


@pytest.mark.asyncio
async def test_unsubscribe_removes_from_registry():
    """Exiting the subscribe context must clean up the subscriber list."""
    redis = _ProgrammableRedis({})
    dispatcher = StreamDispatcher(
        redis,
        num_shards=1,
        status_base="stream:job-status",
        logs_base="stream:job-logs",
        workflow_base="stream:job-workflow",
        block_ms=10,
    )
    await dispatcher.start()
    try:
        async with dispatcher.subscribe("j1", {"status"}):
            assert "j1" in dispatcher._subs
            assert len(dispatcher._subs["j1"]) == 1
        # After context exit: registry must be empty for j1.
        assert "j1" not in dispatcher._subs
    finally:
        await dispatcher.stop()


@pytest.mark.asyncio
async def test_stop_is_idempotent():
    redis = _ProgrammableRedis({})
    dispatcher = StreamDispatcher(
        redis,
        num_shards=1,
        status_base="stream:job-status",
        logs_base="stream:job-logs",
        workflow_base="stream:job-workflow",
        block_ms=10,
    )
    await dispatcher.start()
    await dispatcher.stop()
    await dispatcher.stop()  # should not raise
