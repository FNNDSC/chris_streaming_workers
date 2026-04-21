"""
Entry point for the Log Consumer service.

    python -m chris_streaming.log_consumer

Reads log events from the sharded ``stream:job-logs:{shard}`` Redis Streams,
batches them, and writes to Quickwit. Live fan-out to SSE clients is done
by the SSE service via an ungrouped ``XREAD`` on the same streams — this
consumer no longer needs to publish anywhere.

Horizontal scaling: run N replicas. All replicas join the same consumer
group, and Redis load-balances stream entries across them (no shard lease
needed — logs tolerate cross-shard reordering). Entries left in the PEL by
a crashed replica or a failed flush are recovered by ``PendingReclaimer``,
which sweeps every shard and XCLAIMs idle entries. Over-delivered entries
are routed to the ``STREAM_LOGS_DLQ`` stream.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket

import structlog

from chris_streaming.common.redis_stream import (
    PendingReclaimer,
    ReclaimerConfig,
    create_redis_client,
)
from chris_streaming.common.settings import LogConsumerSettings
from .consumer import LogEventConsumer
from .quickwit_writer import QuickwitWriter

logger = structlog.get_logger()


def _consumer_name() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


async def main() -> None:
    settings = LogConsumerSettings()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info(
        "Starting Log Consumer",
        group=settings.log_consumer_group,
        stream_base=settings.stream_logs_base,
        num_shards=settings.stream_num_shards,
    )

    # Quickwit ingest writer
    quickwit = QuickwitWriter(settings.quickwit_url, settings.quickwit_index)
    await quickwit.connect()

    # Redis client for Streams (decode_responses=False for binary payloads)
    redis = await create_redis_client(settings.redis_url)

    consumer_name = _consumer_name()
    consumer = LogEventConsumer(
        redis=redis,
        base_stream=settings.stream_logs_base,
        num_shards=settings.stream_num_shards,
        group_name=settings.log_consumer_group,
        consumer_name=consumer_name,
        quickwit=quickwit,
        batch_max_size=settings.batch_max_size,
        batch_max_wait_seconds=settings.batch_max_wait_seconds,
    )

    # Pending-entries reclaimer (retry + DLQ).
    #
    # Unlike Status Consumer there is no shard lease: every replica sweeps
    # every shard. XCLAIM is atomic, so only one replica wins any given
    # idle entry. This makes horizontal scaling safe without coordination.
    reclaim_cfg = ReclaimerConfig(
        base_stream=settings.stream_logs_base,
        num_shards=settings.stream_num_shards,
        group_name=settings.log_consumer_group,
        consumer_name=consumer_name,
        min_idle_ms=settings.reclaim_min_idle_ms,
        max_deliveries=settings.reclaim_max_deliveries,
        sweep_interval_ms=settings.reclaim_sweep_interval_ms,
        dlq_stream=settings.stream_logs_dlq,
        dlq_maxlen_approx=settings.stream_dlq_maxlen,
    )
    reclaimer = PendingReclaimer(redis, reclaim_cfg, on_reclaimed=consumer.reclaim)
    reclaimer.set_owned(set(range(settings.stream_num_shards)))

    # Graceful shutdown
    shutdown_event = asyncio.Event()

    def _shutdown(sig):
        logger.info("Received signal %s, shutting down", sig)
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _shutdown, sig)

    consume_task = asyncio.create_task(consumer.run())
    await reclaimer.start()
    await shutdown_event.wait()

    await reclaimer.stop()
    await consumer.stop()
    consume_task.cancel()

    try:
        await consume_task
    except asyncio.CancelledError:
        pass

    await quickwit.close()
    await redis.close()
    logger.info("Log Consumer stopped")


if __name__ == "__main__":
    asyncio.run(main())
