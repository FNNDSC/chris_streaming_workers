"""Tests for chris_streaming.log_consumer.consumer."""

from unittest.mock import AsyncMock

import pytest

from chris_streaming.log_consumer.consumer import LogEventConsumer, _PendingEntry


class TestLogEventConsumer:
    def _make_consumer(self):
        mock_redis = AsyncMock()
        mock_qw = AsyncMock()
        mock_redis_pub = AsyncMock()
        consumer = LogEventConsumer(
            redis=mock_redis,
            base_stream="stream:job-logs",
            num_shards=2,
            group_name="log-consumer-group",
            consumer_name="test-consumer",
            quickwit=mock_qw,
            redis_pub=mock_redis_pub,
            batch_max_size=5,
            batch_max_wait_seconds=0.1,
        )
        return consumer, mock_redis, mock_qw, mock_redis_pub

    async def test_flush_writes_to_quickwit_and_redis(self, sample_log_event):
        consumer, _, mock_qw, mock_redis_pub = self._make_consumer()
        batch = [sample_log_event]

        await consumer._flush(batch)

        mock_qw.write_batch.assert_awaited_once_with(batch)
        mock_redis_pub.publish_batch.assert_awaited_once_with(batch)

    async def test_flush_redis_failure_does_not_raise(self, sample_log_event):
        consumer, _, mock_qw, mock_redis_pub = self._make_consumer()
        mock_redis_pub.publish_batch = AsyncMock(side_effect=Exception("Redis down"))

        await consumer._flush([sample_log_event])
        mock_qw.write_batch.assert_awaited_once()

    async def test_flush_quickwit_failure_raises(self, sample_log_event):
        consumer, _, mock_qw, _ = self._make_consumer()
        mock_qw.write_batch = AsyncMock(side_effect=Exception("QW down"))

        with pytest.raises(Exception, match="QW down"):
            await consumer._flush([sample_log_event])

    async def test_flush_pending_acks_after_success(self, sample_log_event):
        consumer, mock_redis, mock_qw, mock_redis_pub = self._make_consumer()
        pending = [
            _PendingEntry(stream="stream:job-logs:0", entry_id="1-0", event=sample_log_event),
            _PendingEntry(stream="stream:job-logs:0", entry_id="2-0", event=sample_log_event),
        ]

        await consumer._flush_pending(pending)

        mock_qw.write_batch.assert_awaited_once()
        mock_redis_pub.publish_batch.assert_awaited_once()
        mock_redis.xack.assert_awaited_once_with(
            "stream:job-logs:0", "log-consumer-group", "1-0", "2-0",
        )

    async def test_flush_pending_skips_ack_on_quickwit_failure(
        self, sample_log_event
    ):
        consumer, mock_redis, mock_qw, _ = self._make_consumer()
        mock_qw.write_batch = AsyncMock(side_effect=Exception("QW down"))
        pending = [
            _PendingEntry(stream="stream:job-logs:0", entry_id="1-0", event=sample_log_event),
        ]

        await consumer._flush_pending(pending)
        # No XACK because the flush failed
        mock_redis.xack.assert_not_awaited()

    async def test_flush_pending_sets_logs_flushed_on_eos_only_batch(
        self, eos_log_event
    ):
        consumer, mock_redis, mock_qw, _ = self._make_consumer()
        pending = [
            _PendingEntry(stream="stream:job-logs:0", entry_id="1-0", event=eos_log_event),
        ]

        await consumer._flush_pending(pending)

        # No Quickwit write (no real log lines in batch)
        mock_qw.write_batch.assert_not_awaited()
        # EOS signals the logs_flushed key for this job/type
        mock_redis.set.assert_awaited_once_with(
            f"job:{eos_log_event.job_id}:{eos_log_event.job_type.value}:logs_flushed",
            "1",
            ex=3600,
        )
        # And is ACK'd so it does not stall the stream
        mock_redis.xack.assert_awaited_once_with(
            "stream:job-logs:0", "log-consumer-group", "1-0",
        )

    async def test_flush_pending_sets_logs_flushed_after_mixed_batch_flush(
        self, sample_log_event, eos_log_event
    ):
        consumer, mock_redis, mock_qw, _ = self._make_consumer()
        pending = [
            _PendingEntry(stream="stream:job-logs:0", entry_id="1-0", event=sample_log_event),
            _PendingEntry(stream="stream:job-logs:0", entry_id="2-0", event=eos_log_event),
        ]

        await consumer._flush_pending(pending)

        mock_qw.write_batch.assert_awaited_once()
        mock_redis.set.assert_awaited_once_with(
            f"job:{eos_log_event.job_id}:{eos_log_event.job_type.value}:logs_flushed",
            "1",
            ex=3600,
        )
        mock_redis.xack.assert_awaited_once_with(
            "stream:job-logs:0", "log-consumer-group", "1-0", "2-0",
        )

    async def test_flush_pending_holds_eos_on_quickwit_failure(
        self, sample_log_event, eos_log_event
    ):
        """If Quickwit write fails, EOS must NOT fire logs_flushed yet."""
        consumer, mock_redis, mock_qw, _ = self._make_consumer()
        mock_qw.write_batch = AsyncMock(side_effect=Exception("QW down"))
        pending = [
            _PendingEntry(stream="stream:job-logs:0", entry_id="1-0", event=sample_log_event),
            _PendingEntry(stream="stream:job-logs:0", entry_id="2-0", event=eos_log_event),
        ]

        await consumer._flush_pending(pending)

        mock_redis.set.assert_not_awaited()
        mock_redis.xack.assert_not_awaited()

    async def test_close_stops_loop(self):
        consumer, _, _, _ = self._make_consumer()
        consumer._running = True
        await consumer.close()
        assert consumer._running is False
