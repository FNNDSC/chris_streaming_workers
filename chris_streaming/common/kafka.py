"""
Async Kafka producer and consumer factories.

Shared by all three services. Handles SASL/SCRAM-SHA-512 configuration,
serialization, and reconnection.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

from .settings import KafkaSettings

logger = logging.getLogger(__name__)

# Max reconnection attempts before giving up
MAX_CONNECT_RETRIES = 20
CONNECT_RETRY_BASE_SECONDS = 2.0


def _sasl_kwargs(settings: KafkaSettings) -> dict:
    """Build the SASL keyword arguments for aiokafka."""
    return dict(
        security_protocol=settings.kafka_security_protocol,
        sasl_mechanism=settings.kafka_sasl_mechanism,
        sasl_plain_username=settings.kafka_sasl_username,
        sasl_plain_password=settings.kafka_sasl_password,
    )


async def create_producer(settings: KafkaSettings) -> AIOKafkaProducer:
    """
    Create and start an idempotent Kafka producer with retry logic.

    The producer uses lz4 compression, configurable batching, and
    enable_idempotence=True for exactly-once network-level delivery.
    """
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        acks=settings.kafka_producer_acks,
        compression_type=settings.kafka_producer_compression,
        linger_ms=settings.kafka_producer_linger_ms,
        max_batch_size=settings.kafka_producer_batch_size,
        enable_idempotence=settings.kafka_producer_enable_idempotence,
        **_sasl_kwargs(settings),
    )
    await _start_with_retry(producer, "producer")
    return producer


async def create_consumer(
    settings: KafkaSettings,
    topic: str,
    group_id: str,
    auto_offset_reset: str = "earliest",
    enable_auto_commit: bool = False,
    max_poll_records: Optional[int] = None,
) -> AIOKafkaConsumer:
    """
    Create and start a Kafka consumer with manual offset commit.

    auto_commit is disabled by default so that services can commit
    offsets only after successful processing (at-least-once semantics).
    """
    kwargs = {}
    if max_poll_records is not None:
        kwargs["max_poll_records"] = max_poll_records

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
        **_sasl_kwargs(settings),
        **kwargs,
    )
    await _start_with_retry(consumer, f"consumer({topic})")
    return consumer


async def _start_with_retry(client, label: str) -> None:
    """Start a Kafka client with exponential backoff retry."""
    for attempt in range(1, MAX_CONNECT_RETRIES + 1):
        try:
            await client.start()
            logger.info("Kafka %s connected (attempt %d)", label, attempt)
            return
        except KafkaConnectionError as e:
            wait = min(CONNECT_RETRY_BASE_SECONDS * (2 ** (attempt - 1)), 60)
            logger.warning(
                "Kafka %s connection failed (attempt %d/%d): %s. Retrying in %.1fs",
                label, attempt, MAX_CONNECT_RETRIES, e, wait,
            )
            await asyncio.sleep(wait)
    raise RuntimeError(f"Kafka {label} failed to connect after {MAX_CONNECT_RETRIES} attempts")
