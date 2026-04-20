"""Tests for chris_streaming.log_consumer.consumer.LogEventConsumer.reclaim."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from chris_streaming.log_consumer.consumer import LogEventConsumer


def _make_consumer():
    mock_redis = AsyncMock()
    mock_qw = AsyncMock()
    mock_redis_pub = AsyncMock()
    consumer = LogEventConsumer(
        redis=mock_redis,
        base_stream="stream:job-logs",
        num_shards=2,
        group_name="log-consumer-group",
        consumer_name="test-consumer",
        quickwit=mock_qw,
        redis_pub=mock_redis_pub,
        batch_max_size=5,
        batch_max_wait_seconds=0.1,
    )
    return consumer, mock_redis, mock_qw, mock_redis_pub


class TestReclaim:
    @pytest.mark.asyncio
    async def test_real_event_ingests_and_returns_true(self, sample_log_event):
        consumer, _, mock_qw, mock_redis_pub = _make_consumer()
        raw = sample_log_event.model_dump_json().encode("utf-8")

        ok = await consumer.reclaim(raw)

        assert ok is True
        mock_qw.write_batch.assert_awaited_once()
        mock_redis_pub.publish_batch.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_eos_event_sets_logs_flushed(self, eos_log_event):
        consumer, mock_redis, mock_qw, _ = _make_consumer()
        raw = eos_log_event.model_dump_json().encode("utf-8")

        ok = await consumer.reclaim(raw)

        assert ok is True
        mock_qw.write_batch.assert_not_awaited()
        mock_redis.set.assert_awaited_once_with(
            f"job:{eos_log_event.job_id}:{eos_log_event.job_type.value}:logs_flushed",
            "1",
            ex=3600,
        )

    @pytest.mark.asyncio
    async def test_ingest_failure_returns_false(self, sample_log_event):
        consumer, _, mock_qw, _ = _make_consumer()
        mock_qw.write_batch = AsyncMock(side_effect=Exception("QW down"))
        raw = sample_log_event.model_dump_json().encode("utf-8")

        ok = await consumer.reclaim(raw)

        assert ok is False

    @pytest.mark.asyncio
    async def test_malformed_payload_returns_true(self):
        """Malformed payloads should be dropped (returned True so reclaimer XACKs)."""
        consumer, _, mock_qw, _ = _make_consumer()
        ok = await consumer.reclaim(b"not-json-at-all")
        assert ok is True
        mock_qw.write_batch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_eos_redis_failure_returns_false(self, eos_log_event):
        consumer, mock_redis, _, _ = _make_consumer()
        mock_redis.set = AsyncMock(side_effect=Exception("redis down"))
        raw = eos_log_event.model_dump_json().encode("utf-8")

        ok = await consumer.reclaim(raw)

        assert ok is False
