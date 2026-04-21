"""
Centralized configuration loaded from environment variables.

Each service imports only the settings class it needs. Redis Streams settings
are shared; service-specific settings extend them. The Celery broker reuses
the same Redis connection as the streams.
"""

from pydantic_settings import BaseSettings
from pydantic import Field


class RedisSettings(BaseSettings):
    """Redis connection (Streams and Celery broker share it)."""
    redis_url: str = "redis://redis:6379/0"


class RedisStreamsSettings(RedisSettings):
    """Redis Streams topology and tuning shared by all services."""
    # Stream bases (per-shard streams are keyed as "{base}:{i}")
    stream_status_base: str = "stream:job-status"
    stream_logs_base: str = "stream:job-logs"
    stream_workflow_base: str = "stream:job-workflow"
    stream_status_dlq: str = "stream:job-status-dlq"
    stream_logs_dlq: str = "stream:job-logs-dlq"

    # Sharding
    stream_num_shards: int = 8

    # Retention (approximate MAXLEN trim on XADD)
    stream_status_maxlen: int = 1_000_000
    stream_logs_maxlen: int = 5_000_000
    stream_workflow_maxlen: int = 100_000
    stream_dlq_maxlen: int = 100_000

    # Reclaimer
    reclaim_min_idle_ms: int = 30_000
    reclaim_sweep_interval_ms: int = 10_000
    reclaim_max_deliveries: int = 5

    # Lease (horizontal scaling)
    lease_ttl_ms: int = 15_000
    lease_refresh_interval_ms: int = 5_000
    lease_acquire_interval_ms: int = 2_000


class K8sLeaderElectionSettings(BaseSettings):
    """Leader-election tuning used by the K8s forwarder entry points."""
    # Namespace the coordination.k8s.io/Lease lives in. Typically the same
    # namespace as the workload; overridable so a central namespace can
    # hold leases from many workloads.
    k8s_leader_namespace: str = "chris-streaming"
    k8s_leader_lease_duration_seconds: int = 15
    k8s_leader_renew_deadline_seconds: int = 10
    k8s_leader_retry_period_seconds: int = 2
    # Identity of this replica as seen in ``kubectl describe lease``;
    # falls back to hostname+uuid if unset. In-cluster, set from the
    # Downward API ``metadata.name`` so each Pod gets a unique value.
    k8s_leader_identity: str = ""


class EventForwarderSettings(RedisStreamsSettings, K8sLeaderElectionSettings):
    """Event Forwarder specific settings."""
    compute_env: str = Field("docker", description="docker or kubernetes")
    docker_label_filter: str = "org.chrisproject.miniChRIS"
    docker_label_value: str = "plugininstance"
    k8s_namespace: str = "default"
    k8s_label_selector: str = "chrisproject.org/role=plugininstance"
    # Lease object name for event-forwarder leader election.
    k8s_leader_lease_name: str = "chris-event-forwarder"
    # On startup, emit current state of all matching containers
    emit_initial_state: bool = True
    # Opt-in Docker reconciler: periodically inspect tracked containers
    # and emit a status event if the mapped state disagrees with what
    # we last emitted. Catches containers stuck in degenerate states
    # (e.g. under heavily-overloaded Docker) that never fire die/kill.
    # 0 disables the reconciler (default).
    docker_reconcile_seconds: float = 0.0


class StatusConsumerSettings(RedisStreamsSettings):
    """Status Consumer specific settings."""
    status_consumer_group: str = "status-consumer-group"
    # Handler retry policy (before a message is left in the PEL for reclaim).
    handler_retries: int = 3
    # Celery broker (same Redis instance in our setup)
    celery_broker_url: str = "redis://redis:6379/0"


class LogConsumerSettings(RedisStreamsSettings):
    """Log Consumer specific settings."""
    log_consumer_group: str = "log-consumer-group"
    quickwit_url: str = "http://quickwit:7280"
    quickwit_index: str = "job-logs"
    # Batching: flush after this many messages or this many seconds
    batch_max_size: int = 200
    batch_max_wait_seconds: float = 2.0


class LogForwarderSettings(RedisStreamsSettings, K8sLeaderElectionSettings):
    """Log Forwarder specific settings."""
    compute_env: str = Field("docker", description="docker or kubernetes")
    docker_label_filter: str = "org.chrisproject.miniChRIS"
    docker_label_value: str = "plugininstance"
    k8s_namespace: str = "default"
    k8s_label_selector: str = "chrisproject.org/role=plugininstance"
    # Lease object name for log-forwarder leader election.
    k8s_leader_lease_name: str = "chris-log-forwarder"


class SSEServiceSettings(RedisStreamsSettings):
    """SSE Service and Celery Worker settings."""
    host: str = "0.0.0.0"
    port: int = 8080
    quickwit_url: str = "http://quickwit:7280"
    quickwit_index: str = "job-logs"
    celery_broker_url: str = "redis://redis:6379/0"
    db_dsn: str = "postgresql://chris:chris1234@postgres:5432/chris_streaming"
    # pfcon connection (used by Celery worker for workflow orchestration)
    pfcon_url: str = "http://pfcon:30005"
    pfcon_user: str = "pfcon"
    pfcon_password: str = "pfcon1234"
    # Quiescence window used by cleanup_containers as the EOS backstop:
    # if a terminal per-step status has been stable for this many seconds
    # without a logs_flushed signal, treat the logs as drained and proceed.
    eos_quiescence_seconds: float = 10.0
    # Consumer group names used by the /metrics endpoint (must match the
    # values the status + log consumers register on startup).
    status_consumer_group: str = "status-consumer-group"
    log_consumer_group: str = "log-consumer-group"
