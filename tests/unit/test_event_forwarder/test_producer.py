"""Tests for chris_streaming.event_forwarder.producer."""

from unittest.mock import AsyncMock

from chris_streaming.common.schemas import JobStatus, JobType, StatusEvent
from chris_streaming.event_forwarder.producer import StatusEventProducer


class TestStatusEventProducer:
    async def test_send_produces_to_kafka(self, sample_status_event):
        mock_producer = AsyncMock()
        producer = StatusEventProducer(mock_producer, "test-topic")

        result = await producer.send(sample_status_event)

        assert result is True
        mock_producer.send_and_wait.assert_awaited_once()
        call_args = mock_producer.send_and_wait.call_args
        assert call_args[0][0] == "test-topic"

    async def test_dedup_skips_duplicate(self, sample_status_event):
        mock_producer = AsyncMock()
        producer = StatusEventProducer(mock_producer, "test-topic")

        await producer.send(sample_status_event)
        result = await producer.send(sample_status_event)

        assert result is False
        assert mock_producer.send_and_wait.await_count == 1

    async def test_different_events_not_deduped(self):
        mock_producer = AsyncMock()
        producer = StatusEventProducer(mock_producer, "test-topic")

        e1 = StatusEvent(job_id="j1", job_type=JobType.plugin, status=JobStatus.started)
        e2 = StatusEvent(job_id="j1", job_type=JobType.plugin, status=JobStatus.finishedSuccessfully)

        r1 = await producer.send(e1)
        r2 = await producer.send(e2)

        assert r1 is True
        assert r2 is True
        assert mock_producer.send_and_wait.await_count == 2

    async def test_lru_eviction(self):
        mock_producer = AsyncMock()
        producer = StatusEventProducer(mock_producer, "test-topic")

        # Fill beyond cache size by sending unique events
        from chris_streaming.event_forwarder.producer import _DEDUP_CACHE_SIZE
        for i in range(_DEDUP_CACHE_SIZE + 5):
            event = StatusEvent(
                job_id=f"job-{i}",
                job_type=JobType.plugin,
                status=JobStatus.started,
            )
            await producer.send(event)

        # The oldest entries should have been evicted
        assert len(producer._seen) == _DEDUP_CACHE_SIZE

    async def test_close(self):
        mock_producer = AsyncMock()
        producer = StatusEventProducer(mock_producer, "test-topic")
        await producer.close()
        mock_producer.stop.assert_awaited_once()
