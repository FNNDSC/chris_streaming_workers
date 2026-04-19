"""
Entry point for the Event Forwarder daemon.

    python -m chris_streaming.event_forwarder

Watches Docker or Kubernetes for ChRIS job container events and XADDs them
to the sharded Redis Streams ``stream:job-status:{shard}``.

The ``logs_flushed`` Redis signal is emitted by the Log Consumer after the
container's log stream reaches EOF and its tail has been flushed to
OpenSearch — not by this service.
"""

from __future__ import annotations

import asyncio
import logging
import signal

import structlog

from chris_streaming.common.redis_stream import (
    RedisStreamProducer,
    StreamProducerConfig,
    create_redis_client,
)
from chris_streaming.common.settings import EventForwarderSettings
from .docker_watcher import DockerWatcher
from .k8s_watcher import K8sWatcher
from .producer import StatusEventProducer
from .watcher import Watcher

logger = structlog.get_logger()


def _create_watcher(settings: EventForwarderSettings) -> Watcher:
    match settings.compute_env:
        case "docker" | "podman":
            return DockerWatcher(settings)
        case "kubernetes" | "openshift":
            return K8sWatcher(settings)
        case _:
            raise ValueError(f"Unsupported compute_env: {settings.compute_env}")


async def main() -> None:
    settings = EventForwarderSettings()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info(
        "Starting Event Forwarder",
        compute_env=settings.compute_env,
        redis_url=settings.redis_url,
        stream_base=settings.stream_status_base,
        num_shards=settings.stream_num_shards,
    )

    redis = await create_redis_client(settings.redis_url)

    stream_producer = RedisStreamProducer(
        redis,
        StreamProducerConfig(
            base_stream=settings.stream_status_base,
            num_shards=settings.stream_num_shards,
            maxlen_approx=settings.stream_status_maxlen,
        ),
    )
    producer = StatusEventProducer(stream_producer)

    watcher = _create_watcher(settings)

    shutdown_event = asyncio.Event()

    def _shutdown(sig):
        logger.info("Received signal %s, shutting down", sig)
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _shutdown, sig)

    try:
        async with watcher:
            async for event in watcher.watch():
                if shutdown_event.is_set():
                    break
                await producer.send(event)
    finally:
        logger.info("Shutting down producers")
        await producer.close()
        await redis.close()
        logger.info("Event Forwarder stopped")


if __name__ == "__main__":
    asyncio.run(main())
