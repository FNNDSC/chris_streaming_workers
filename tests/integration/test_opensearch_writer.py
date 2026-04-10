"""Integration test: OpenSearch bulk writes and queries."""

import uuid
from datetime import datetime, timezone

import pytest

from chris_streaming.common.schemas import JobType, LogEvent
from chris_streaming.log_consumer.opensearch_writer import OpenSearchWriter

pytestmark = pytest.mark.integration


class TestOpenSearchWriterIntegration:
    async def test_connect_and_write_batch(self, opensearch_url):
        prefix = f"test-logs-{uuid.uuid4().hex[:8]}"
        writer = OpenSearchWriter(opensearch_url, prefix)

        await writer.connect()
        try:
            job_id = f"integ-{uuid.uuid4().hex[:8]}"
            events = [
                LogEvent(
                    job_id=job_id,
                    job_type=JobType.plugin,
                    container_name=f"{job_id}",
                    line=f"log line {i}",
                    timestamp=datetime(2026, 1, 15, 12, 0, i, tzinfo=timezone.utc),
                )
                for i in range(5)
            ]

            await writer.write_batch(events)

            # Force refresh so documents are searchable
            await writer._client.indices.refresh(index=f"{prefix}-*")

            # Query back
            resp = await writer._client.search(
                index=f"{prefix}-*",
                body={"query": {"term": {"job_id": job_id}}, "size": 10},
            )
            hits = resp["hits"]["hits"]
            assert len(hits) == 5
            lines = sorted(h["_source"]["line"] for h in hits)
            assert lines == [f"log line {i}" for i in range(5)]
        finally:
            # Cleanup: delete the test index
            try:
                await writer._client.indices.delete(index=f"{prefix}-*")
            except Exception:
                pass
            await writer.close()

    async def test_write_empty_batch(self, opensearch_url):
        prefix = f"test-logs-{uuid.uuid4().hex[:8]}"
        writer = OpenSearchWriter(opensearch_url, prefix)
        await writer.connect()
        try:
            await writer.write_batch([])  # Should be a no-op
        finally:
            try:
                await writer._client.indices.delete_index_template(name=f"{prefix}-template")
            except Exception:
                pass
            await writer.close()
