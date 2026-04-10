"""Tests for chris_streaming.event_forwarder.eos_producer."""

import asyncio
from unittest.mock import AsyncMock

from chris_streaming.common.schemas import JobType
from chris_streaming.event_forwarder.eos_producer import EOSProducer


class TestEOSProducer:
    async def test_schedule_and_send(self):
        mock_producer = AsyncMock()
        eos = EOSProducer(mock_producer, "job-logs", delay_seconds=0.01)

        eos.schedule_eos("j1", JobType.plugin)
        # Wait for the delayed send to complete
        await asyncio.sleep(0.05)

        mock_producer.send_and_wait.assert_awaited_once()
        call_args = mock_producer.send_and_wait.call_args
        assert call_args[0][0] == "job-logs"

    async def test_no_duplicate_schedule(self):
        mock_producer = AsyncMock()
        eos = EOSProducer(mock_producer, "job-logs", delay_seconds=0.01)

        eos.schedule_eos("j1", JobType.plugin)
        eos.schedule_eos("j1", JobType.plugin)  # duplicate

        await asyncio.sleep(0.05)
        assert mock_producer.send_and_wait.await_count == 1

    async def test_different_job_types_scheduled_separately(self):
        mock_producer = AsyncMock()
        eos = EOSProducer(mock_producer, "job-logs", delay_seconds=0.01)

        eos.schedule_eos("j1", JobType.plugin)
        eos.schedule_eos("j1", JobType.copy)

        await asyncio.sleep(0.05)
        assert mock_producer.send_and_wait.await_count == 2

    async def test_cancel_all(self):
        mock_producer = AsyncMock()
        eos = EOSProducer(mock_producer, "job-logs", delay_seconds=10.0)

        eos.schedule_eos("j1", JobType.plugin)
        eos.schedule_eos("j2", JobType.copy)

        await eos.cancel_all()

        assert len(eos._pending) == 0
        # The producer should not have been called (cancelled before delay elapsed)
        mock_producer.send_and_wait.assert_not_awaited()
