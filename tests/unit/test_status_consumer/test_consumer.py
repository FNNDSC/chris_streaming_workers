"""Tests for chris_streaming.status_consumer.consumer."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

from chris_streaming.common.schemas import StatusEvent
from chris_streaming.status_consumer.consumer import StatusEventConsumer


def _make_kafka_message(event: StatusEvent, offset: int = 0):
    return SimpleNamespace(
        value=event.serialize_for_kafka(),
        offset=offset,
        partition=0,
        topic="test-topic",
    )


class TestStatusEventConsumer:
    def _make_consumer(
        self, messages=None, notifier_side_effect=None, max_retries=3
    ) -> tuple[StatusEventConsumer, AsyncMock, AsyncMock]:
        mock_kafka = AsyncMock()
        mock_notifier = AsyncMock()
        mock_dlq = AsyncMock()

        if notifier_side_effect:
            mock_notifier.notify = AsyncMock(side_effect=notifier_side_effect)

        consumer = StatusEventConsumer(
            consumer=mock_kafka,
            notifier=mock_notifier,
            dlq_producer=mock_dlq,
            dlq_topic="test-dlq",
            max_retries=max_retries,
        )
        return consumer, mock_kafka, mock_dlq

    async def test_process_with_retry_success(self, sample_status_event):
        consumer, _, _ = self._make_consumer()
        raw = sample_status_event.serialize_for_kafka()
        result = await consumer._process_with_retry(sample_status_event, raw)
        assert result is True

    async def test_process_with_retry_fails_then_succeeds(self, sample_status_event):
        consumer, _, _ = self._make_consumer(
            notifier_side_effect=[Exception("fail"), None]
        )
        raw = sample_status_event.serialize_for_kafka()
        result = await consumer._process_with_retry(sample_status_event, raw)
        assert result is True

    async def test_process_with_retry_exhausted(self, sample_status_event):
        consumer, _, _ = self._make_consumer(
            notifier_side_effect=Exception("always fails"),
            max_retries=2,
        )
        raw = sample_status_event.serialize_for_kafka()
        result = await consumer._process_with_retry(sample_status_event, raw)
        assert result is False

    async def test_send_to_dlq(self, sample_status_event):
        consumer, _, mock_dlq = self._make_consumer()
        raw = sample_status_event.serialize_for_kafka()
        await consumer._send_to_dlq(raw, "test reason")
        mock_dlq.send_and_wait.assert_awaited_once()
        call_args = mock_dlq.send_and_wait.call_args
        assert call_args[0][0] == "test-dlq"
        assert call_args[1]["value"] == raw

    async def test_send_to_dlq_failure_logged(self, sample_status_event):
        consumer, _, mock_dlq = self._make_consumer()
        mock_dlq.send_and_wait = AsyncMock(side_effect=Exception("DLQ down"))
        raw = sample_status_event.serialize_for_kafka()
        # Should not raise
        await consumer._send_to_dlq(raw, "test reason")

    async def test_close(self):
        consumer, mock_kafka, mock_dlq = self._make_consumer()
        await consumer.close()
        mock_kafka.stop.assert_awaited_once()
        mock_dlq.stop.assert_awaited_once()
