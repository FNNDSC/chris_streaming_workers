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


async def _run_forwarder(
    settings: EventForwarderSettings,
    shutdown_event: asyncio.Event,
    cancel_event: asyncio.Event | None = None,
) -> None:
    """Run one iteration of the forwarder body (watch → produce).

    On K8s, ``cancel_event`` is set by the leader-election layer when this
    replica loses the lease; the body stops cleanly so a new leader can
    take over without cross-replica duplicate emission.
    """
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

    async def _should_stop() -> bool:
        if shutdown_event.is_set():
            return True
        return cancel_event is not None and cancel_event.is_set()

    try:
        async with watcher:
            async for event in watcher.watch():
                if await _should_stop():
                    break
                await producer.send(event)
    finally:
        logger.info("Shutting down producers")
        await producer.close()
        await redis.close()


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

    shutdown_event = asyncio.Event()

    def _shutdown(sig):
        logger.info("Received signal %s, shutting down", sig)
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _shutdown, sig)

    try:
        if settings.compute_env in ("kubernetes", "openshift"):
            # Leader election: only one replica emits at a time.
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

            # Race leader-election against the shutdown signal so SIGTERM
            # exits promptly even when this replica is a follower.
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
                election_task.result()  # re-raise on error
        else:
            await _run_forwarder(settings, shutdown_event)
    finally:
        logger.info("Event Forwarder stopped")


if __name__ == "__main__":
    asyncio.run(main())
