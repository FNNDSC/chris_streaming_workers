"""Tests for chris_streaming.log_consumer.quickwit_writer."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from chris_streaming.log_consumer.quickwit_writer import (
    QuickwitWriter,
    _load_index_config,
)


class TestLoadIndexConfig:
    def test_returns_yaml_content(self):
        yaml = _load_index_config()
        assert "index_id: job-logs" in yaml
        assert "field_mappings" in yaml


class TestQuickwitWriter:
    @pytest.mark.asyncio
    async def test_connect_passes_yaml_to_client(self):
        with patch(
            "chris_streaming.log_consumer.quickwit_writer.QuickwitClient"
        ) as MockClient:
            instance = AsyncMock()
            MockClient.return_value = instance
            writer = QuickwitWriter("http://qw:7280", index_id="job-logs")
            await writer.connect()

            MockClient.assert_called_once_with("http://qw:7280", index_id="job-logs")
            call_kwargs = instance.connect.await_args.kwargs
            assert "index_id: job-logs" in call_kwargs["index_config"]

    @pytest.mark.asyncio
    async def test_write_batch_delegates(self, sample_log_event):
        with patch(
            "chris_streaming.log_consumer.quickwit_writer.QuickwitClient"
        ) as MockClient:
            instance = AsyncMock()
            MockClient.return_value = instance
            writer = QuickwitWriter("http://qw:7280")
            await writer.write_batch([sample_log_event])
            instance.write_batch.assert_awaited_once_with([sample_log_event])

    @pytest.mark.asyncio
    async def test_close_delegates(self):
        with patch(
            "chris_streaming.log_consumer.quickwit_writer.QuickwitClient"
        ) as MockClient:
            instance = AsyncMock()
            MockClient.return_value = instance
            writer = QuickwitWriter("http://qw:7280")
            await writer.close()
            instance.close.assert_awaited_once()
