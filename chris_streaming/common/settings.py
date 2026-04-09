"""
Centralized configuration loaded from environment variables.

Each service imports only the settings class it needs. Common Kafka and Redis
settings are shared; service-specific settings extend them.
"""

from pydantic_settings import BaseSettings
from pydantic import Field


class KafkaSettings(BaseSettings):
    """Kafka connection and SASL/SCRAM-SHA-512 credentials."""
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_security_protocol: str = "SASL_PLAINTEXT"
    kafka_sasl_mechanism: str = "PLAIN"
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = ""

    # Producer tuning
    kafka_producer_acks: str = "all"
    kafka_producer_compression: str = "lz4"
    kafka_producer_linger_ms: int = 100
    kafka_producer_batch_size: int = 16384
    kafka_producer_enable_idempotence: bool = False

    # Topics
    kafka_topic_status: str = "job-status-events"
    kafka_topic_logs: str = "job-logs"
    kafka_topic_status_dlq: str = "job-status-events-dlq"
    kafka_topic_logs_dlq: str = "job-logs-dlq"


class RedisSettings(BaseSettings):
    """Redis connection (used for both Pub/Sub and Celery broker)."""
    redis_url: str = "redis://redis:6379/0"


class EventForwarderSettings(KafkaSettings):
    """Event Forwarder specific settings."""
    compute_env: str = Field("docker", description="docker or kubernetes")
    docker_label_filter: str = "org.chrisproject.miniChRIS"
    docker_label_value: str = "plugininstance"
    k8s_namespace: str = "default"
    k8s_label_selector: str = "org.chrisproject.miniChRIS=plugininstance"
    kafka_sasl_username: str = "event-forwarder"
    kafka_sasl_password: str = "event-forwarder-secret"
    # On startup, emit current state of all matching containers
    emit_initial_state: bool = True


class StatusConsumerSettings(KafkaSettings, RedisSettings):
    """Status Consumer specific settings."""
    kafka_sasl_username: str = "status-consumer"
    kafka_sasl_password: str = "status-consumer-secret"
    kafka_consumer_group: str = "status-consumer-group"
    db_dsn: str = "postgresql://chris:chris1234@postgres:5432/chris_streaming"
    max_retries: int = 3
    celery_broker_url: str = "redis://redis:6379/0"


class LogConsumerSettings(KafkaSettings, RedisSettings):
    """Log Consumer specific settings."""
    kafka_sasl_username: str = "log-consumer"
    kafka_sasl_password: str = "log-consumer-secret"
    kafka_consumer_group: str = "log-consumer-group"
    opensearch_url: str = "http://opensearch:9200"
    opensearch_index_prefix: str = "job-logs"
    # Batching: flush after this many messages or this many seconds
    batch_max_size: int = 200
    batch_max_wait_seconds: float = 2.0


class SSEServiceSettings(RedisSettings):
    """SSE Service specific settings."""
    host: str = "0.0.0.0"
    port: int = 8080
    opensearch_url: str = "http://opensearch:9200"
    opensearch_index_prefix: str = "job-logs"
    celery_broker_url: str = "redis://redis:6379/0"
