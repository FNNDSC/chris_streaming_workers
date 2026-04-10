"""Tests for chris_streaming.log_consumer.redis_publisher."""

from unittest.mock import AsyncMock, patch

from chris_streaming.common.schemas import JobType, LogEvent
from chris_streaming.log_consumer.redis_publisher import LogRedisPublisher


class TestLogRedisPublisher:
    async def test_connect(self):
        with patch("chris_streaming.log_consumer.redis_publisher.aioredis") as mock_redis:
            mock_client = AsyncMock()
            mock_redis.from_url.return_value = mock_client

            pub = LogRedisPublisher("redis://localhost:6379/0")
            await pub.connect()

            mock_redis.from_url.assert_called_once()
            mock_client.ping.assert_awaited_once()

    async def test_publish_batch(self):
        from unittest.mock import MagicMock

        pub = LogRedisPublisher("redis://localhost:6379/0")
        # Use MagicMock for the redis client so pipeline() is a sync call
        mock_redis = MagicMock()
        mock_pipe = MagicMock()
        mock_pipe.execute = AsyncMock(return_value=[])
        mock_redis.pipeline.return_value = mock_pipe
        pub._redis = mock_redis

        events = [
            LogEvent(job_id="j1", job_type=JobType.plugin, line="line 1"),
            LogEvent(job_id="j1", job_type=JobType.plugin, line="line 2"),
            LogEvent(job_id="j2", job_type=JobType.copy, line="line 3"),
        ]

        await pub.publish_batch(events)

        assert mock_pipe.publish.call_count == 3
        channels = [call.args[0] for call in mock_pipe.publish.call_args_list]
        assert channels[0] == "job:j1:logs"
        assert channels[1] == "job:j1:logs"
        assert channels[2] == "job:j2:logs"
        mock_pipe.execute.assert_awaited_once()

    async def test_close(self):
        pub = LogRedisPublisher("redis://localhost:6379/0")
        mock_redis = AsyncMock()
        pub._redis = mock_redis
        await pub.close()
        mock_redis.close.assert_awaited_once()

    async def test_close_when_not_connected(self):
        pub = LogRedisPublisher("redis://localhost:6379/0")
        await pub.close()  # Should not raise
