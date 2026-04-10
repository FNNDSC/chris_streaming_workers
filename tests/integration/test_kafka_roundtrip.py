"""Integration test: produce and consume messages through real Kafka."""

import uuid

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from chris_streaming.common.schemas import (
    JobStatus,
    JobType,
    LogEvent,
    StatusEvent,
    kafka_key_for_job,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def unique_topic():
    return f"test-topic-{uuid.uuid4().hex[:8]}"


async def _make_producer(bootstrap, username, password):
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=username,
        sasl_plain_password=password,
    )
    await producer.start()
    return producer


async def _make_consumer(bootstrap, username, password, topic, group):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=username,
        sasl_plain_password=password,
    )
    await consumer.start()
    return consumer


class TestKafkaStatusRoundtrip:
    async def test_produce_consume_status_event(
        self, kafka_bootstrap, kafka_sasl_username, kafka_sasl_password
    ):
        topic = "job-status-events"
        group = f"test-group-{uuid.uuid4().hex[:8]}"

        event = StatusEvent(
            job_id=f"integ-{uuid.uuid4().hex[:8]}",
            job_type=JobType.plugin,
            status=JobStatus.started,
            image="test:latest",
        )

        producer = await _make_producer(kafka_bootstrap, kafka_sasl_username, kafka_sasl_password)
        try:
            await producer.send_and_wait(
                topic,
                value=event.serialize_for_kafka(),
                key=kafka_key_for_job(event.job_id),
            )
        finally:
            await producer.stop()

        consumer = await _make_consumer(
            kafka_bootstrap, kafka_sasl_username, kafka_sasl_password, topic, group
        )
        try:
            records = await consumer.getmany(timeout_ms=10_000, max_records=100)
            all_values = [
                msg.value for msgs in records.values() for msg in msgs
            ]
            found = False
            for raw in all_values:
                restored = StatusEvent.deserialize_from_kafka(raw)
                if restored.job_id == event.job_id:
                    assert restored.status == JobStatus.started
                    assert restored.job_type == JobType.plugin
                    found = True
                    break
            assert found, f"Event for {event.job_id} not found in Kafka"
        finally:
            await consumer.stop()


class TestKafkaLogRoundtrip:
    async def test_produce_consume_log_event(
        self, kafka_bootstrap, kafka_sasl_username, kafka_sasl_password
    ):
        topic = "job-logs"
        group = f"test-group-{uuid.uuid4().hex[:8]}"

        event = LogEvent(
            job_id=f"integ-{uuid.uuid4().hex[:8]}",
            job_type=JobType.plugin,
            line="integration test log line",
        )

        producer = await _make_producer(kafka_bootstrap, kafka_sasl_username, kafka_sasl_password)
        try:
            await producer.send_and_wait(
                topic,
                value=event.serialize_for_kafka(),
                key=kafka_key_for_job(event.job_id),
            )
        finally:
            await producer.stop()

        consumer = await _make_consumer(
            kafka_bootstrap, kafka_sasl_username, kafka_sasl_password, topic, group
        )
        try:
            records = await consumer.getmany(timeout_ms=10_000, max_records=100)
            all_values = [
                msg.value for msgs in records.values() for msg in msgs
            ]
            found = False
            for raw in all_values:
                restored = LogEvent.deserialize_from_kafka(raw)
                if restored.job_id == event.job_id:
                    assert restored.line == "integration test log line"
                    found = True
                    break
            assert found, f"Log event for {event.job_id} not found in Kafka"
        finally:
            await consumer.stop()
