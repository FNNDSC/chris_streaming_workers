"""Tests for chris_streaming.log_consumer.opensearch_writer."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from chris_streaming.common.schemas import JobType, LogEvent
from chris_streaming.log_consumer.opensearch_writer import (
    OpenSearchBulkError,
    OpenSearchWriter,
)


class TestOpenSearchWriter:
    def test_index_name(self):
        writer = OpenSearchWriter("http://localhost:9200", "job-logs")
        ts = datetime(2026, 3, 15, tzinfo=timezone.utc)
        assert writer._index_name(ts) == "job-logs-2026.03.15"

    def test_index_name_fallback_for_non_datetime(self):
        writer = OpenSearchWriter("http://localhost:9200", "job-logs")
        result = writer._index_name("not-a-datetime")
        assert result.startswith("job-logs-")

    async def test_connect_applies_template(self):
        with patch("chris_streaming.log_consumer.opensearch_writer.AsyncOpenSearch") as MockOS:
            mock_client = AsyncMock()
            MockOS.return_value = mock_client

            writer = OpenSearchWriter("http://localhost:9200", "job-logs")
            await writer.connect()

            mock_client.indices.put_index_template.assert_awaited_once()
            call_kwargs = mock_client.indices.put_index_template.call_args[1]
            assert call_kwargs["name"] == "job-logs-template"

    async def test_write_batch_empty(self):
        writer = OpenSearchWriter("http://localhost:9200", "job-logs")
        writer._client = AsyncMock()
        await writer.write_batch([])
        writer._client.bulk.assert_not_awaited()

    async def test_write_batch_builds_bulk_body(self, sample_log_event):
        writer = OpenSearchWriter("http://localhost:9200", "job-logs")
        mock_client = AsyncMock()
        mock_client.bulk = AsyncMock(return_value={"errors": False, "items": [{}]})
        writer._client = mock_client

        await writer.write_batch([sample_log_event])

        mock_client.bulk.assert_awaited_once()
        body = mock_client.bulk.call_args[1]["body"]
        # Should have 2 entries: index action + document
        assert len(body) == 2
        assert "index" in body[0]
        assert body[1]["job_id"] == "test-job-1"

    async def test_write_batch_multiple_events(self):
        writer = OpenSearchWriter("http://localhost:9200", "job-logs")
        mock_client = AsyncMock()
        mock_client.bulk = AsyncMock(return_value={"errors": False, "items": [{}, {}]})
        writer._client = mock_client

        events = [
            LogEvent(
                job_id=f"job-{i}",
                job_type=JobType.plugin,
                line=f"line {i}",
                timestamp=datetime(2026, 1, 15, 12, 0, i, tzinfo=timezone.utc),
            )
            for i in range(3)
        ]
        await writer.write_batch(events)

        body = mock_client.bulk.call_args[1]["body"]
        assert len(body) == 6  # 3 events * 2 (action + doc)

    async def test_close(self):
        writer = OpenSearchWriter("http://localhost:9200", "job-logs")
        mock_client = AsyncMock()
        writer._client = mock_client
        await writer.close()
        mock_client.close.assert_awaited_once()

    async def test_write_batch_raises_on_partial_failure(self, sample_log_event):
        """A partial _bulk failure must raise so the consumer skips commit."""
        writer = OpenSearchWriter("http://localhost:9200", "job-logs")
        mock_client = AsyncMock()
        mock_client.bulk = AsyncMock(return_value={
            "errors": True,
            "items": [
                {"index": {"status": 201}},
                {"index": {
                    "status": 400,
                    "error": {"type": "mapper_parsing_exception", "reason": "bad type"},
                }},
                {"index": {
                    "status": 503,
                    "error": {"type": "cluster_block_exception", "reason": "read-only"},
                }},
            ],
        })
        writer._client = mock_client

        events = [
            LogEvent(
                job_id=f"job-{i}",
                job_type=JobType.plugin,
                line=f"line {i}",
                timestamp=datetime(2026, 1, 15, 12, 0, i, tzinfo=timezone.utc),
            )
            for i in range(3)
        ]
        with pytest.raises(OpenSearchBulkError) as excinfo:
            await writer.write_batch(events)
        assert excinfo.value.failed == 2
        assert excinfo.value.total == 3
        assert "bad type" in excinfo.value.sample_reasons
        assert "read-only" in excinfo.value.sample_reasons

    async def test_write_batch_raises_even_with_single_item_failure(self):
        writer = OpenSearchWriter("http://localhost:9200", "job-logs")
        mock_client = AsyncMock()
        mock_client.bulk = AsyncMock(return_value={
            "errors": True,
            "items": [{"index": {"error": "boom"}}],
        })
        writer._client = mock_client

        event = LogEvent(
            job_id="j",
            job_type=JobType.plugin,
            line="line",
            timestamp=datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        )
        with pytest.raises(OpenSearchBulkError):
            await writer.write_batch([event])
