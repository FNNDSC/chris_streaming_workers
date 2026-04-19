"""
Entry point for the Status Consumer service.

    python -m chris_streaming.status_consumer

Consumes the sharded ``stream:job-status:{shard}`` Redis Streams via a
lease-coordinated ShardedConsumer, retries + DLQs via PendingReclaimer,
and schedules Celery tasks for downstream processing.

Horizontal scaling: run N replicas. Each replica acquires leases on a
subset of shards via ShardLeaseManager. On replica death, other replicas
pick up the abandoned shards automatically.
"""

from __future__ import annotations

import asyncio
import logging
import signal

import structlog

from chris_streaming.common.redis_stream import (
    ConsumerConfig,
    LeaseManagerConfig,
    PendingReclaimer,
    ReclaimerConfig,
    ShardLeaseManager,
    ShardedConsumer,
    create_redis_client,
)
from chris_streaming.common.settings import StatusConsumerSettings
from .consumer import StatusMessageHandler
from .notifier import StatusNotifier

logger = structlog.get_logger()


async def main() -> None:
    settings = StatusConsumerSettings()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info(
        "Starting Status Consumer",
        group=settings.status_consumer_group,
        stream_base=settings.stream_status_base,
        num_shards=settings.stream_num_shards,
    )

    notifier = StatusNotifier(settings.celery_broker_url)
    handler = StatusMessageHandler(notifier)

    redis = await create_redis_client(settings.redis_url)

    # Lease manager (shard ownership coordination)
    lease_cfg = LeaseManagerConfig(
        num_shards=settings.stream_num_shards,
        lease_key_prefix=f"chris:lease:{settings.status_consumer_group}",
        lease_ttl_ms=settings.lease_ttl_ms,
        refresh_interval_ms=settings.lease_refresh_interval_ms,
        acquire_interval_ms=settings.lease_acquire_interval_ms,
    )
    leases = ShardLeaseManager(redis, lease_cfg)

    # Sharded consumer (per-shard XREADGROUP tasks)
    consumer_cfg = ConsumerConfig(
        base_stream=settings.stream_status_base,
        num_shards=settings.stream_num_shards,
        group_name=settings.status_consumer_group,
        consumer_name=leases.replica_id,
        dlq_stream=settings.stream_status_dlq,
        dlq_maxlen_approx=settings.stream_dlq_maxlen,
        handler_retries=settings.handler_retries,
    )
    consumer = ShardedConsumer(redis, consumer_cfg, handler=handler)

    # Pending-entries reclaimer (retry + DLQ)
    reclaim_cfg = ReclaimerConfig(
        base_stream=settings.stream_status_base,
        num_shards=settings.stream_num_shards,
        group_name=settings.status_consumer_group,
        consumer_name=leases.replica_id,
        min_idle_ms=settings.reclaim_min_idle_ms,
        max_deliveries=settings.reclaim_max_deliveries,
        sweep_interval_ms=settings.reclaim_sweep_interval_ms,
        dlq_stream=settings.stream_status_dlq,
        dlq_maxlen_approx=settings.stream_dlq_maxlen,
    )
    reclaimer = PendingReclaimer(redis, reclaim_cfg, on_reclaimed=handler.reclaim)

    async def _on_shard_change(added: set[int], removed: set[int]) -> None:
        await consumer.set_shards(added, removed)
        reclaimer.set_owned(leases.owned_shards)

    leases.add_listener(_on_shard_change)

    await consumer.start()
    await leases.start()
    await reclaimer.start()

    # Graceful shutdown
    shutdown_event = asyncio.Event()

    def _shutdown(sig):
        logger.info("Received signal %s, shutting down", sig)
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _shutdown, sig)

    await shutdown_event.wait()

    await reclaimer.stop()
    await leases.stop()
    await consumer.stop()
    await redis.close()
    logger.info("Status Consumer stopped")


if __name__ == "__main__":
    asyncio.run(main())
