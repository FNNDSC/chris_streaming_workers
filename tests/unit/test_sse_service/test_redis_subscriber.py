"""Tests for chris_streaming.sse_service.redis_subscriber.

Focus: log-replay deduplication. A log event that arrives both via
OpenSearch replay and via the live Redis Pub/Sub buffer must be
yielded only once.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


async def _drain_with_timeout(agen, duration: float = 0.5) -> list[dict]:
    """Collect everything the generator yields within ``duration`` seconds."""
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


class _FakePubSub:
    """Async pubsub stub that always reports no messages and never hangs."""

    def __init__(self):
        self.subscribed = []
        self.unsubscribed = []
        self.closed = False

    async def subscribe(self, *channels):
        self.subscribed.extend(channels)

    async def unsubscribe(self, *channels):
        self.unsubscribed.extend(channels)

    async def get_message(self, ignore_subscribe_messages=True, timeout=1.0):
        # Return None to indicate no live message — the generator's live
        # phase will loop until cancelled by the test.
        return None

    async def close(self):
        self.closed = True


class TestLogReplayDedup:
    """Log replay + live buffer must not yield the same event twice."""

    @pytest.mark.asyncio
    async def test_duplicate_log_deduped_between_replay_and_buffer(self):
        from chris_streaming.sse_service import redis_subscriber

        fake_pubsub = _FakePubSub()
        fake_redis = MagicMock()
        fake_redis.pubsub = MagicMock(return_value=fake_pubsub)
        fake_redis.close = AsyncMock()

        duplicate = {
            "event_id": "log-evt-1",
            "job_id": "j1",
            "job_type": "plugin",
            "line": "hello",
            "stream": "stdout",
            "timestamp": "2026-01-01T00:00:00+00:00",
        }

        buffer_ready = asyncio.Event()

        async def fake_replay_logs(url, prefix, job_id):
            # Wait until fake_buffer has populated the buffer before the
            # replay phase yields — ensures the dedup path is exercised.
            await asyncio.wait_for(buffer_ready.wait(), timeout=1.0)
            yield dict(duplicate)

        async def fake_buffer(pubsub, buffer, timeout=0.1):
            # Simulate the same event arriving via live Pub/Sub during replay.
            buffer.append({**duplicate, "_event_type": "logs"})
            buffer_ready.set()
            try:
                while True:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                raise

        with patch.object(
            redis_subscriber, "aioredis",
        ) as mock_aioredis, patch.object(
            redis_subscriber, "_replay_log_history", fake_replay_logs,
        ), patch.object(
            redis_subscriber, "_buffer_live_messages", fake_buffer,
        ):
            mock_aioredis.from_url.return_value = fake_redis

            agen = redis_subscriber.subscribe_to_job(
                redis_url="redis://fake",
                job_id="j1",
                event_type="logs",
                opensearch_url="http://fake",
            )

            # The generator's Phase 4 live loop never terminates on its own
            # with the fake pubsub; use a bounded timeout to collect what was
            # yielded during replay + buffer drain.
            results = await _drain_with_timeout(agen, duration=0.3)

        # Exactly one log event should have been yielded — the replay copy.
        # The live-buffer duplicate must be suppressed via event_id dedup.
        log_events = [
            r for r in results
            if r.get("event_id") == "log-evt-1"
        ]
        assert len(log_events) == 1, (
            f"expected 1 log event after dedup, got {len(log_events)}: {results}"
        )

    @pytest.mark.asyncio
    async def test_distinct_logs_both_yielded(self):
        """Sanity: two logs with different event_ids must both be yielded."""
        from chris_streaming.sse_service import redis_subscriber

        fake_pubsub = _FakePubSub()
        fake_redis = MagicMock()
        fake_redis.pubsub = MagicMock(return_value=fake_pubsub)
        fake_redis.close = AsyncMock()

        buffer_ready = asyncio.Event()

        async def fake_replay_logs(url, prefix, job_id):
            # Wait until fake_buffer has populated the buffer so the
            # buffer-drain phase of subscribe_to_job actually has something
            # to yield.
            await asyncio.wait_for(buffer_ready.wait(), timeout=1.0)
            yield {
                "event_id": "log-a",
                "job_id": "j1",
                "job_type": "plugin",
                "line": "a",
                "timestamp": "2026-01-01T00:00:00+00:00",
            }

        async def fake_buffer(pubsub, buffer, timeout=0.1):
            buffer.append({
                "event_id": "log-b",
                "job_id": "j1",
                "job_type": "plugin",
                "line": "b",
                "_event_type": "logs",
            })
            buffer_ready.set()
            try:
                while True:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                raise

        with patch.object(
            redis_subscriber, "aioredis",
        ) as mock_aioredis, patch.object(
            redis_subscriber, "_replay_log_history", fake_replay_logs,
        ), patch.object(
            redis_subscriber, "_buffer_live_messages", fake_buffer,
        ):
            mock_aioredis.from_url.return_value = fake_redis

            agen = redis_subscriber.subscribe_to_job(
                redis_url="redis://fake",
                job_id="j1",
                event_type="logs",
                opensearch_url="http://fake",
            )

            results = await _drain_with_timeout(agen, duration=0.3)

        ids = {r.get("event_id") for r in results}
        assert "log-a" in ids
        assert "log-b" in ids
