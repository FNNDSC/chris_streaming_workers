"""Tests for chris_streaming.event_forwarder.producer."""

from unittest.mock import AsyncMock

from chris_streaming.common.schemas import JobStatus, JobType, StatusEvent
from chris_streaming.event_forwarder.producer import StatusEventProducer


class _FakeStreamProducer:
    """Minimal stand-in for RedisStreamProducer, capturing xadd calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, bytes]] = []
        self.xadd = AsyncMock(side_effect=self._record)

    async def _record(self, job_id: str, event_bytes: bytes) -> str:
        self.calls.append((job_id, event_bytes))
        return f"{len(self.calls)}-0"


class TestStatusEventProducer:
    async def test_send_produces_to_stream(self, sample_status_event):
        fake = _FakeStreamProducer()
        producer = StatusEventProducer(fake)

        result = await producer.send(sample_status_event)

        assert result is True
        fake.xadd.assert_awaited_once()
        assert fake.calls[0][0] == sample_status_event.job_id

    async def test_dedup_skips_duplicate(self, sample_status_event):
        fake = _FakeStreamProducer()
        producer = StatusEventProducer(fake)

        await producer.send(sample_status_event)
        result = await producer.send(sample_status_event)

        assert result is False
        assert fake.xadd.await_count == 1

    async def test_different_events_not_deduped(self):
        fake = _FakeStreamProducer()
        producer = StatusEventProducer(fake)

        e1 = StatusEvent(job_id="j1", job_type=JobType.plugin, status=JobStatus.started)
        e2 = StatusEvent(
            job_id="j1", job_type=JobType.plugin, status=JobStatus.finishedSuccessfully,
        )

        r1 = await producer.send(e1)
        r2 = await producer.send(e2)

        assert r1 is True
        assert r2 is True
        assert fake.xadd.await_count == 2

    async def test_lru_eviction(self):
        fake = _FakeStreamProducer()
        producer = StatusEventProducer(fake)

        from chris_streaming.event_forwarder.producer import _DEDUP_CACHE_SIZE
        for i in range(_DEDUP_CACHE_SIZE + 5):
            event = StatusEvent(
                job_id=f"job-{i}",
                job_type=JobType.plugin,
                status=JobStatus.started,
            )
            await producer.send(event)

        assert len(producer._seen) == _DEDUP_CACHE_SIZE

    async def test_close_is_safe(self):
        fake = _FakeStreamProducer()
        producer = StatusEventProducer(fake)
        # close is a no-op (connection owned by caller), must not raise
        await producer.close()
