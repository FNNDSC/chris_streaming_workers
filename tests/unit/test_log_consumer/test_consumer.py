"""Tests for chris_streaming.log_consumer.consumer."""

from unittest.mock import AsyncMock

from chris_streaming.log_consumer.consumer import LogEventConsumer


class TestLogEventConsumer:
    def _make_consumer(self):
        mock_kafka = AsyncMock()
        mock_os = AsyncMock()
        mock_redis_pub = AsyncMock()
        consumer = LogEventConsumer(
            consumer=mock_kafka,
            opensearch=mock_os,
            redis_pub=mock_redis_pub,
            redis_url="redis://localhost:6379/0",
            batch_max_size=5,
            batch_max_wait_seconds=0.1,
        )
        return consumer, mock_kafka, mock_os, mock_redis_pub

    async def test_flush_writes_to_opensearch_and_redis(self, sample_log_event):
        consumer, _, mock_os, mock_redis_pub = self._make_consumer()
        batch = [sample_log_event]

        await consumer._flush(batch)

        mock_os.write_batch.assert_awaited_once_with(batch)
        mock_redis_pub.publish_batch.assert_awaited_once_with(batch)

    async def test_flush_redis_failure_does_not_raise(self, sample_log_event):
        consumer, _, mock_os, mock_redis_pub = self._make_consumer()
        mock_redis_pub.publish_batch = AsyncMock(side_effect=Exception("Redis down"))

        # Should not raise — Redis publish is best-effort
        await consumer._flush([sample_log_event])
        mock_os.write_batch.assert_awaited_once()

    async def test_flush_opensearch_failure_raises(self, sample_log_event):
        consumer, _, mock_os, mock_redis_pub = self._make_consumer()
        mock_os.write_batch = AsyncMock(side_effect=Exception("OS down"))

        import pytest
        with pytest.raises(Exception, match="OS down"):
            await consumer._flush([sample_log_event])

    async def test_set_logs_flushed(self):
        consumer, _, _, _ = self._make_consumer()
        mock_redis = AsyncMock()
        consumer._redis = mock_redis

        await consumer._set_logs_flushed("j1", "plugin")

        mock_redis.set.assert_awaited_once()
        call_args = mock_redis.set.call_args
        assert call_args[0][0] == "job:j1:plugin:logs_flushed"
        assert call_args[0][1] == "1"
        assert call_args[1]["ex"] == 3600

    async def test_set_logs_flushed_error_swallowed(self):
        consumer, _, _, _ = self._make_consumer()
        mock_redis = AsyncMock()
        mock_redis.set = AsyncMock(side_effect=Exception("fail"))
        consumer._redis = mock_redis

        # Should not raise
        await consumer._set_logs_flushed("j1", "plugin")

    async def test_close(self):
        consumer, mock_kafka, _, _ = self._make_consumer()
        mock_redis = AsyncMock()
        consumer._redis = mock_redis

        await consumer.close()
        mock_kafka.stop.assert_awaited_once()
        mock_redis.close.assert_awaited_once()
