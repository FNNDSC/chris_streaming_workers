"""Integration tests for QuickwitClient against a live Quickwit.

Run with `just run integration-tests`.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from chris_streaming.common.quickwit import QuickwitClient
from chris_streaming.common.schemas import JobType, LogEvent
from chris_streaming.log_consumer.quickwit_writer import _load_index_config


pytestmark = pytest.mark.integration


def _event(job_id: str, line: str, ts: datetime) -> LogEvent:
    return LogEvent(
        job_id=job_id,
        job_type=JobType.plugin,
        container_name=job_id,
        stream="stdout",
        line=line,
        timestamp=ts,
    )


class TestQuickwitClient:
    @pytest.fixture
    async def client(self, quickwit_url, quickwit_index):
        c = QuickwitClient(quickwit_url, index_id=quickwit_index)
        await c.connect(index_config=_load_index_config())
        try:
            yield c
        finally:
            await c.close()

    @pytest.mark.asyncio
    async def test_write_then_search_by_job(self, client):
        job_id = f"it-{uuid.uuid4().hex[:8]}"
        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        events = [
            _event(job_id, "line 1", base),
            _event(job_id, "line 2", base + timedelta(seconds=1)),
            _event(job_id, "line 3", base + timedelta(seconds=2)),
        ]
        await client.write_batch(events)

        result = await client.search_by_job(job_id, limit=10)
        assert result["total"] >= 3
        lines = [d["line"] for d in result["lines"]]
        # Results come back asc after our reverse; first-written first.
        assert lines[:3] == ["line 1", "line 2", "line 3"]
        for d in result["lines"][:3]:
            assert d["job_id"] == job_id

    @pytest.mark.asyncio
    async def test_search_scopes_to_job_id(self, client):
        job_a = f"it-a-{uuid.uuid4().hex[:8]}"
        job_b = f"it-b-{uuid.uuid4().hex[:8]}"
        base = datetime(2026, 2, 1, tzinfo=timezone.utc)
        await client.write_batch([
            _event(job_a, "A1", base),
            _event(job_b, "B1", base),
            _event(job_a, "A2", base + timedelta(seconds=1)),
        ])

        a = await client.search_by_job(job_a, limit=10)
        assert {d["line"] for d in a["lines"]} == {"A1", "A2"}
        assert all(d["job_id"] == job_a for d in a["lines"])

        b = await client.search_by_job(job_b, limit=10)
        assert {d["line"] for d in b["lines"]} == {"B1"}

    @pytest.mark.asyncio
    async def test_empty_batch_noop(self, client):
        # Must not raise and must not contact the server.
        await client.write_batch([])

    @pytest.mark.asyncio
    async def test_ensure_index_is_idempotent(self, quickwit_url, quickwit_index):
        # Second connect() must succeed against an already-existing index.
        c = QuickwitClient(quickwit_url, index_id=quickwit_index)
        await c.connect(index_config=_load_index_config())
        await c.close()
