"""Tests for chris_streaming.common.settings."""

from chris_streaming.common.settings import (
    EventForwarderSettings,
    LogConsumerSettings,
    RedisSettings,
    RedisStreamsSettings,
    SSEServiceSettings,
    StatusConsumerSettings,
)


class TestRedisSettings:
    def test_defaults(self):
        s = RedisSettings()
        assert s.redis_url == "redis://redis:6379/0"


class TestRedisStreamsSettings:
    def test_defaults(self):
        s = RedisStreamsSettings()
        assert s.stream_status_base == "stream:job-status"
        assert s.stream_logs_base == "stream:job-logs"
        assert s.stream_status_dlq == "stream:job-status-dlq"
        assert s.stream_logs_dlq == "stream:job-logs-dlq"
        assert s.stream_num_shards == 8
        assert s.reclaim_max_deliveries == 5
        assert s.lease_ttl_ms == 15_000

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("STREAM_NUM_SHARDS", "16")
        s = RedisStreamsSettings()
        assert s.stream_num_shards == 16


class TestEventForwarderSettings:
    def test_inherits_streams(self):
        s = EventForwarderSettings()
        assert s.redis_url == "redis://redis:6379/0"
        assert s.stream_status_base == "stream:job-status"
        assert s.compute_env == "docker"
        assert s.docker_label_filter == "org.chrisproject.miniChRIS"


class TestStatusConsumerSettings:
    def test_defaults(self):
        s = StatusConsumerSettings()
        assert s.status_consumer_group == "status-consumer-group"
        assert s.handler_retries == 3
        assert s.celery_broker_url == "redis://redis:6379/0"
        assert s.stream_num_shards == 8


class TestLogConsumerSettings:
    def test_inherits_streams_and_redis(self):
        s = LogConsumerSettings()
        assert s.redis_url == "redis://redis:6379/0"
        assert s.stream_logs_base == "stream:job-logs"
        assert s.opensearch_url == "http://opensearch:9200"
        assert s.batch_max_size == 200
        assert s.batch_max_wait_seconds == 2.0


class TestSSEServiceSettings:
    def test_defaults(self):
        s = SSEServiceSettings()
        assert s.host == "0.0.0.0"
        assert s.port == 8080
        assert "postgresql" in s.db_dsn
