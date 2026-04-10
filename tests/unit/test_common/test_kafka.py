"""Tests for chris_streaming.common.kafka."""

from unittest.mock import AsyncMock, patch

import pytest
from aiokafka.errors import KafkaConnectionError

from chris_streaming.common.kafka import (
    _sasl_kwargs,
    _start_with_retry,
    create_consumer,
    create_producer,
)
from chris_streaming.common.settings import KafkaSettings


class TestSaslKwargs:
    def test_builds_sasl_dict(self):
        s = KafkaSettings(
            kafka_security_protocol="SASL_PLAINTEXT",
            kafka_sasl_mechanism="PLAIN",
            kafka_sasl_username="user1",
            kafka_sasl_password="pass1",
        )
        result = _sasl_kwargs(s)
        assert result["security_protocol"] == "SASL_PLAINTEXT"
        assert result["sasl_mechanism"] == "PLAIN"
        assert result["sasl_plain_username"] == "user1"
        assert result["sasl_plain_password"] == "pass1"


class TestStartWithRetry:
    async def test_success_first_try(self):
        client = AsyncMock()
        client.start = AsyncMock()
        await _start_with_retry(client, "test")
        client.start.assert_awaited_once()

    async def test_retries_on_failure_then_succeeds(self):
        client = AsyncMock()
        client.start = AsyncMock(
            side_effect=[KafkaConnectionError("fail"), None]
        )
        with patch("chris_streaming.common.kafka.asyncio.sleep", new_callable=AsyncMock):
            await _start_with_retry(client, "test")
        assert client.start.await_count == 2

    async def test_raises_after_max_retries(self):
        client = AsyncMock()
        client.start = AsyncMock(side_effect=KafkaConnectionError("fail"))
        with patch("chris_streaming.common.kafka.asyncio.sleep", new_callable=AsyncMock):
            with patch("chris_streaming.common.kafka.MAX_CONNECT_RETRIES", 2):
                with pytest.raises(RuntimeError, match="failed to connect"):
                    await _start_with_retry(client, "test")


class TestCreateProducer:
    async def test_creates_and_starts(self):
        with patch("chris_streaming.common.kafka.AIOKafkaProducer") as MockProducer:
            mock_instance = AsyncMock()
            MockProducer.return_value = mock_instance
            settings = KafkaSettings()
            result = await create_producer(settings)
            assert result is mock_instance
            mock_instance.start.assert_awaited_once()


class TestCreateConsumer:
    async def test_creates_and_starts(self):
        with patch("chris_streaming.common.kafka.AIOKafkaConsumer") as MockConsumer:
            mock_instance = AsyncMock()
            MockConsumer.return_value = mock_instance
            settings = KafkaSettings()
            result = await create_consumer(settings, "test-topic", "test-group")
            assert result is mock_instance
            mock_instance.start.assert_awaited_once()
