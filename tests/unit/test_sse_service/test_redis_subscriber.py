"""Tests for chris_streaming.sse_service.redis_subscriber.

Focus: historical replay + live-buffer dedup against the StreamDispatcher.
A log event that arrives both via Quickwit replay and via the dispatcher's
live queue must be yielded only once.
"""

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class _FakeDispatcher:
    """Minimal dispatcher stub with a per-subscribe asyncio.Queue."""

    def __init__(self):
        self.queue = asyncio.Queue()
        self.subscribed_with: list[tuple[str, frozenset[str]]] = []

    @asynccontextmanager
    async def subscribe(self, job_id, event_types):
        self.subscribed_with.append((job_id, frozenset(event_types)))
        try:
            yield self.queue
        finally:
            pass


async def _drain_with_timeout(agen, duration: float = 0.3) -> list[dict]:
    results: list[dict] = []

    async def _collect():
        async for item in agen:
            results.append(item)

    try:
        await asyncio.wait_for(_collect(), timeout=duration)
    except asyncio.TimeoutError:
        pass
    finally:
        await agen.aclose()
    return results


class TestLogReplayDedup:
    @pytest.mark.asyncio
    async def test_duplicate_log_deduped_between_replay_and_buffer(self):
        from chris_streaming.sse_service import redis_subscriber

        duplicate = {
            "event_id": "log-evt-1",
            "job_id": "j1",
            "job_type": "plugin",
            "line": "hello",
            "stream": "stdout",
            "timestamp": "2026-01-01T00:00:00+00:00",
        }

        dispatcher = _FakeDispatcher()
        # Seed the dispatcher queue with the duplicate before subscribing —
        # simulates live stream entry arriving during replay.
        await dispatcher.queue.put({**duplicate, "_event_type": "logs"})

        async def fake_replay_logs(url, index_id, job_id):
            yield dict(duplicate)

        with patch.object(
            redis_subscriber, "_replay_log_history", fake_replay_logs,
        ):
            agen = redis_subscriber.subscribe_to_job(
                dispatcher=dispatcher,
                job_id="j1",
                event_type="logs",
                quickwit_url="http://fake",
            )
            results = await _drain_with_timeout(agen, duration=0.3)

        log_events = [r for r in results if r.get("event_id") == "log-evt-1"]
        assert len(log_events) == 1, (
            f"expected 1 log event after dedup, got {len(log_events)}: {results}"
        )

    @pytest.mark.asyncio
    async def test_distinct_logs_both_yielded(self):
        from chris_streaming.sse_service import redis_subscriber

        dispatcher = _FakeDispatcher()
        await dispatcher.queue.put({
            "event_id": "log-b",
            "job_id": "j1",
            "job_type": "plugin",
            "line": "b",
            "_event_type": "logs",
        })

        async def fake_replay_logs(url, index_id, job_id):
            yield {
                "event_id": "log-a",
                "job_id": "j1",
                "job_type": "plugin",
                "line": "a",
                "timestamp": "2026-01-01T00:00:00+00:00",
            }

        with patch.object(
            redis_subscriber, "_replay_log_history", fake_replay_logs,
        ):
            agen = redis_subscriber.subscribe_to_job(
                dispatcher=dispatcher,
                job_id="j1",
                event_type="logs",
                quickwit_url="http://fake",
            )
            results = await _drain_with_timeout(agen, duration=0.3)

        ids = {r.get("event_id") for r in results}
        assert "log-a" in ids
        assert "log-b" in ids


class TestWorkflowReplay:
    @pytest.mark.asyncio
    async def test_workflow_history_replayed(self):
        """Historical workflow events from Postgres are yielded before live ones."""
        from chris_streaming.sse_service import redis_subscriber

        dispatcher = _FakeDispatcher()
        # Live event arrives during replay.
        await dispatcher.queue.put({
            "event_id": "wf-live",
            "job_id": "j1",
            "current_step": "plugin",
            "current_step_status": "finishedSuccessfully",
            "workflow_status": "running",
            "_event_type": "workflow",
        })

        historical = [
            {
                "job_id": "j1",
                "current_step": "copy",
                "current_step_status": "started",
                "workflow_status": "running",
                "event_id": "wf-hist-1",
                "timestamp": "2026-01-01T00:00:00+00:00",
            },
        ]

        with patch.object(
            redis_subscriber, "_fetch_workflow_rows",
            MagicMock(return_value=historical),
        ):
            agen = redis_subscriber.subscribe_to_job(
                dispatcher=dispatcher,
                job_id="j1",
                event_type="workflow",
                db_dsn="dsn://fake",
            )
            results = await _drain_with_timeout(agen, duration=0.3)

        ids = [r.get("event_id") for r in results]
        # Historical row first, then live.
        assert ids[0] == "wf-hist-1"
        assert "wf-live" in ids


class TestSubscribePassesCorrectEventTypes:
    @pytest.mark.asyncio
    async def test_all_subscribes_to_three_types(self):
        from chris_streaming.sse_service import redis_subscriber

        dispatcher = _FakeDispatcher()
        agen = redis_subscriber.subscribe_to_job(
            dispatcher=dispatcher,
            job_id="j1",
            event_type="all",
        )
        try:
            await asyncio.wait_for(agen.__anext__(), timeout=0.1)
        except (asyncio.TimeoutError, StopAsyncIteration):
            pass
        finally:
            await agen.aclose()

        assert dispatcher.subscribed_with == [("j1", frozenset({"status", "logs", "workflow"}))]
