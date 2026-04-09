"""
Entry point for the Event Forwarder daemon.

    python -m chris_streaming.event_forwarder

Watches Docker or Kubernetes for ChRIS job container events and produces
them to the Kafka job-status-events topic.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

import structlog

from chris_streaming.common.kafka import create_producer
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
        bootstrap_servers=settings.kafka_bootstrap_servers,
    )

    # Create Kafka producer
    kafka_producer = await create_producer(settings)
    producer = StatusEventProducer(kafka_producer, settings.kafka_topic_status)

    # Create compute runtime watcher
    watcher = _create_watcher(settings)

    # Graceful shutdown
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
        logger.info("Shutting down producer")
        await producer.close()
        logger.info("Event Forwarder stopped")


if __name__ == "__main__":
    asyncio.run(main())
