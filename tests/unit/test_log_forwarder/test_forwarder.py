"""Tests for chris_streaming.log_forwarder.forwarder."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock

from chris_streaming.common.schemas import JobType, LogEvent
from chris_streaming.log_forwarder.forwarder import LogForwarder
from chris_streaming.log_forwarder.tailer import LogLine


class _FakeProducer:
    """Stand-in for RedisStreamProducer that records xadd calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, bytes]] = []
        self.xadd = AsyncMock(side_effect=self._record)

    async def _record(self, job_id: str, value: bytes) -> str:
        self.calls.append((job_id, value))
        return f"{len(self.calls)}-0"


class TestLogForwarder:
    async def test_forward_produces_log_event(self):
        producer = _FakeProducer()
        forwarder = LogForwarder(producer)  # type: ignore[arg-type]

        line = LogLine(
            job_id="job-1",
            job_type=JobType.plugin,
            container_name="job-1",
            stream="stdout",
            line="hello",
            timestamp=datetime(2026, 1, 15, 12, 1, 2, tzinfo=timezone.utc),
        )

        await forwarder.forward(line)

        assert len(producer.calls) == 1
        sent_job_id, payload = producer.calls[0]
        assert sent_job_id == "job-1"
        event = LogEvent.deserialize(payload)
        assert event.job_id == "job-1"
        assert event.job_type == JobType.plugin
        assert event.line == "hello"
        assert event.stream == "stdout"
        assert event.container_name == "job-1"
        assert event.eos is False

    async def test_forward_uses_job_id_as_shard_key(self):
        producer = _FakeProducer()
        forwarder = LogForwarder(producer)  # type: ignore[arg-type]
        line = LogLine(
            job_id="job-abc",
            job_type=JobType.plugin,
            container_name="job-abc",
            stream="stderr",
            line="err",
            timestamp=datetime(2026, 1, 15, 12, 1, 2, tzinfo=timezone.utc),
        )
        await forwarder.forward(line)
        assert producer.xadd.await_args.args[0] == "job-abc"

    async def test_forward_propagates_eos(self):
        producer = _FakeProducer()
        forwarder = LogForwarder(producer)  # type: ignore[arg-type]
        line = LogLine(
            job_id="job-eos",
            job_type=JobType.plugin,
            container_name="job-eos",
            stream="",
            line="",
            timestamp=datetime(2026, 1, 15, 12, 1, 2, tzinfo=timezone.utc),
            eos=True,
        )
        await forwarder.forward(line)
        _, payload = producer.calls[0]
        event = LogEvent.deserialize(payload)
        assert event.eos is True
        assert event.job_id == "job-eos"
