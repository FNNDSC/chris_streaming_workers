"""
Entry point for the Log Forwarder service.

    python -m chris_streaming.log_forwarder

Replaces Fluent Bit. Tails container logs for ChRIS jobs and XADDs each
line to the sharded Redis Streams ``stream:job-logs:{shard}``.

Dispatches between Docker and Kubernetes tailers based on COMPUTE_ENV.
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
from chris_streaming.common.settings import LogForwarderSettings
from .forwarder import LogForwarder
from .tailer import DockerLogTailer

logger = structlog.get_logger()


def _create_tailer(settings: LogForwarderSettings):
    match settings.compute_env:
        case "docker" | "podman":
            return DockerLogTailer(
                settings.docker_label_filter,
                settings.docker_label_value,
            )
        case "kubernetes" | "openshift":
            # Imported lazily so the docker-only image doesn't have to
            # install kubernetes-asyncio.
            from .k8s_tailer import K8sLogTailer
            return K8sLogTailer(
                namespace=settings.k8s_namespace,
                label_selector=settings.k8s_label_selector,
            )
        case _:
            raise ValueError(f"Unsupported compute_env: {settings.compute_env}")


async def main() -> None:
    settings = LogForwarderSettings()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info(
        "Starting Log Forwarder",
        compute_env=settings.compute_env,
        redis_url=settings.redis_url,
        stream_base=settings.stream_logs_base,
        num_shards=settings.stream_num_shards,
    )

    redis = await create_redis_client(settings.redis_url)
    producer = RedisStreamProducer(
        redis,
        StreamProducerConfig(
            base_stream=settings.stream_logs_base,
            num_shards=settings.stream_num_shards,
            maxlen_approx=settings.stream_logs_maxlen,
        ),
    )
    forwarder = LogForwarder(producer)
    tailer = _create_tailer(settings)

    shutdown_event = asyncio.Event()

    def _shutdown(sig):
        logger.info("Received signal %s, shutting down", sig)
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _shutdown, sig)

    try:
        async with tailer:
            async for line in tailer.stream():
                if shutdown_event.is_set():
                    break
                try:
                    await forwarder.forward(line)
                except Exception as e:
                    logger.warning("forward failed: %s", e)
    finally:
        await redis.close()
        logger.info("Log Forwarder stopped")


if __name__ == "__main__":
    asyncio.run(main())
