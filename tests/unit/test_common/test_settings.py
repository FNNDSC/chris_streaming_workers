"""Tests for chris_streaming.common.settings."""

from chris_streaming.common.settings import (
    EventForwarderSettings,
    KafkaSettings,
    LogConsumerSettings,
    RedisSettings,
    SSEServiceSettings,
    StatusConsumerSettings,
)


class TestKafkaSettings:
    def test_defaults(self):
        s = KafkaSettings()
        assert s.kafka_bootstrap_servers == "kafka:9092"
        assert s.kafka_security_protocol == "SASL_PLAINTEXT"
        assert s.kafka_sasl_mechanism == "PLAIN"
        assert s.kafka_producer_compression == "lz4"
        assert s.kafka_topic_status == "job-status-events"
        assert s.kafka_topic_logs == "job-logs"

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9094")
        s = KafkaSettings()
        assert s.kafka_bootstrap_servers == "broker:9094"


class TestRedisSettings:
    def test_defaults(self):
        s = RedisSettings()
        assert s.redis_url == "redis://redis:6379/0"


class TestEventForwarderSettings:
    def test_inherits_kafka(self):
        s = EventForwarderSettings()
        assert s.kafka_bootstrap_servers == "kafka:9092"
        assert s.compute_env == "docker"
        assert s.docker_label_filter == "org.chrisproject.miniChRIS"
        assert s.eos_delay_seconds == 10.0


class TestStatusConsumerSettings:
    def test_defaults(self):
        s = StatusConsumerSettings()
        assert s.kafka_consumer_group == "status-consumer-group"
        assert s.max_retries == 3
        assert s.celery_broker_url == "redis://redis:6379/0"


class TestLogConsumerSettings:
    def test_inherits_kafka_and_redis(self):
        s = LogConsumerSettings()
        assert s.kafka_bootstrap_servers == "kafka:9092"
        assert s.redis_url == "redis://redis:6379/0"
        assert s.opensearch_url == "http://opensearch:9200"
        assert s.batch_max_size == 200
        assert s.batch_max_wait_seconds == 2.0


class TestSSEServiceSettings:
    def test_defaults(self):
        s = SSEServiceSettings()
        assert s.host == "0.0.0.0"
        assert s.port == 8080
        assert "postgresql" in s.db_dsn
