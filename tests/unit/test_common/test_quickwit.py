"""Tests for chris_streaming.common.quickwit.QuickwitClient."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import httpx
import pytest

from chris_streaming.common.quickwit import (
    QuickwitBulkError,
    QuickwitClient,
    QuickwitSearchError,
    _escape_keyword,
)
from chris_streaming.common.schemas import JobType, LogEvent


def _make_event(line: str = "hello") -> LogEvent:
    return LogEvent(
        event_id="evt-1",
        job_id="job-1",
        job_type=JobType.plugin,
        container_name="job-1",
        stream="stdout",
        line=line,
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


class _Recorder:
    """Wrap a user handler so we can later assert on the requests it saw."""

    def __init__(self, handler):
        self._handler = handler
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self._handler(request)


async def _client_with(handler) -> tuple[QuickwitClient, _Recorder]:
    recorder = _Recorder(handler)
    transport = httpx.MockTransport(recorder)
    client = QuickwitClient("http://quickwit:7280", index_id="job-logs")
    client._client = httpx.AsyncClient(
        base_url="http://quickwit:7280", transport=transport,
    )
    return client, recorder


class TestEnsureIndex:
    @pytest.mark.asyncio
    async def test_existing_index_no_create(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.method == "GET"
            assert request.url.path == "/api/v1/indexes/job-logs"
            return httpx.Response(200, json={"index_id": "job-logs"})

        client, recorder = await _client_with(handler)
        try:
            await client._ensure_index("version: 0.8\n")
        finally:
            await client.close()

        assert [r.method for r in recorder.requests] == ["GET"]

    @pytest.mark.asyncio
    async def test_missing_index_creates(self):
        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "GET":
                return httpx.Response(404, text="not found")
            assert request.method == "POST"
            assert request.url.path == "/api/v1/indexes"
            assert request.headers["content-type"] == "application/yaml"
            assert b"job-logs" in request.content
            return httpx.Response(200, json={"index_id": "job-logs"})

        client, recorder = await _client_with(handler)
        try:
            await client._ensure_index("version: 0.8\nindex_id: job-logs\n")
        finally:
            await client.close()

        assert [r.method for r in recorder.requests] == ["GET", "POST"]

    @pytest.mark.asyncio
    async def test_create_failure_raises(self):
        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "GET":
                return httpx.Response(404)
            return httpx.Response(500, text="boom")

        client, _ = await _client_with(handler)
        try:
            with pytest.raises(QuickwitBulkError) as exc:
                await client._ensure_index("yaml")
        finally:
            await client.close()
        assert exc.value.status_code == 500


class TestWriteBatch:
    @pytest.mark.asyncio
    async def test_empty_batch_noop(self):
        def handler(_request):
            raise AssertionError("no request expected for empty batch")

        client, recorder = await _client_with(handler)
        try:
            await client.write_batch([])
        finally:
            await client.close()
        assert recorder.requests == []

    @pytest.mark.asyncio
    async def test_ingest_with_commit_force(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.method == "POST"
            assert request.url.path == "/api/v1/job-logs/ingest"
            assert request.url.query == b"commit=force"
            assert request.headers["content-type"] == "application/x-ndjson"
            # Two events = two NDJSON lines
            lines = request.content.decode().strip().split("\n")
            assert len(lines) == 2
            for line in lines:
                payload = json.loads(line)
                assert payload["job_id"] == "job-1"
            return httpx.Response(200, json={"num_docs_for_processing": 2})

        client, _ = await _client_with(handler)
        try:
            await client.write_batch([_make_event("a"), _make_event("b")])
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_ingest_error_raises(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, text="nope")

        client, _ = await _client_with(handler)
        try:
            with pytest.raises(QuickwitBulkError) as exc:
                await client.write_batch([_make_event()])
        finally:
            await client.close()
        assert exc.value.status_code == 500

    @pytest.mark.asyncio
    async def test_transport_error_wrapped(self):
        def handler(_request):
            raise httpx.ConnectError("refused")

        client, _ = await _client_with(handler)
        try:
            with pytest.raises(QuickwitBulkError) as exc:
                await client.write_batch([_make_event()])
        finally:
            await client.close()
        assert exc.value.status_code == 0


class TestSearchByJob:
    @pytest.mark.asyncio
    async def test_query_shape_and_reverse_order(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v1/job-logs/search"
            body = json.loads(request.content.decode())
            # Values are quoted as phrase queries for exact keyword match.
            assert body["query"] == 'job_id:"job-1"'
            assert body["max_hits"] == 10
            assert body["start_offset"] == 5
            assert body["sort_by"] == "timestamp"
            return httpx.Response(200, json={
                "num_hits": 2,
                "hits": [
                    {"line": "second", "timestamp": "2026-01-01T00:00:02Z"},
                    {"line": "first", "timestamp": "2026-01-01T00:00:01Z"},
                ],
            })

        client, _ = await _client_with(handler)
        try:
            result = await client.search_by_job("job-1", limit=10, offset=5)
        finally:
            await client.close()
        assert result["total"] == 2
        # Returned asc after reversing Quickwit's desc order.
        assert result["lines"][0]["line"] == "first"
        assert result["lines"][1]["line"] == "second"

    @pytest.mark.asyncio
    async def test_search_error(self):
        def handler(_request):
            return httpx.Response(500, text="boom")

        client, _ = await _client_with(handler)
        try:
            with pytest.raises(QuickwitSearchError):
                await client.search_by_job("job-1")
        finally:
            await client.close()


class TestEscapeKeyword:
    def test_simple_value_quoted(self):
        assert _escape_keyword("abc123") == '"abc123"'

    def test_hyphen_preserved_inside_quotes(self):
        assert _escape_keyword("job-1") == '"job-1"'

    def test_embedded_quotes_escaped(self):
        assert _escape_keyword('has"quote') == '"has\\"quote"'

    def test_backslash_escaped(self):
        assert _escape_keyword("a\\b") == '"a\\\\b"'
