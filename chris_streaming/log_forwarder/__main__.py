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


async def _run_forwarder(
    settings: LogForwarderSettings,
    shutdown_event: asyncio.Event,
    cancel_event: asyncio.Event | None = None,
) -> None:
    """One iteration of the tailer body, used by both Docker and K8s paths."""
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

    def _should_stop() -> bool:
        if shutdown_event.is_set():
            return True
        return cancel_event is not None and cancel_event.is_set()

    try:
        async with tailer:
            async for line in tailer.stream():
                if _should_stop():
                    break
                try:
                    await forwarder.forward(line)
                except Exception as e:
                    logger.warning("forward failed: %s", e)
    finally:
        await redis.close()


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

    shutdown_event = asyncio.Event()

    def _shutdown(sig):
        logger.info("Received signal %s, shutting down", sig)
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _shutdown, sig)

    try:
        if settings.compute_env in ("kubernetes", "openshift"):
            from chris_streaming.common.k8s_leader import LeaderElection

            election = LeaderElection(
                namespace=settings.k8s_leader_namespace,
                name=settings.k8s_leader_lease_name,
                identity=settings.k8s_leader_identity or None,
                lease_duration_seconds=settings.k8s_leader_lease_duration_seconds,
                renew_deadline_seconds=settings.k8s_leader_renew_deadline_seconds,
                retry_period_seconds=settings.k8s_leader_retry_period_seconds,
            )

            async def body(cancel_event: asyncio.Event) -> None:
                await _run_forwarder(settings, shutdown_event, cancel_event)

            election_task = asyncio.create_task(election.run(body))
            shutdown_task = asyncio.create_task(shutdown_event.wait())
            done, pending = await asyncio.wait(
                {election_task, shutdown_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
            for t in pending:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
            if election_task in done:
                election_task.result()
        else:
            await _run_forwarder(settings, shutdown_event)
    finally:
        logger.info("Log Forwarder stopped")


if __name__ == "__main__":
    asyncio.run(main())
