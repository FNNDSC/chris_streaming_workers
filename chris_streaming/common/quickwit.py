"""
Async Quickwit client for log ingest and point-query search.

Quickwit 0.8+ exposes a REST API under ``/api/v1``:

* ``POST /api/v1/{index}/ingest?commit=force`` — NDJSON ingest with durable commit.
  We use ``commit=force`` so the EOS / ``logs_flushed`` contract holds: once
  ``write_batch`` returns, the documents are searchable.
* ``POST /api/v1/{index}/search`` — Tantivy-style query (``job_id:<id>``).
* ``GET  /api/v1/indexes/{index}`` / ``POST /api/v1/indexes`` — create-if-missing
  bootstrap. We post a YAML (content-type ``application/yaml``) describing the
  ``job-logs`` index schema on startup.

Raises ``QuickwitBulkError`` on HTTP-level failures so the consumer leaves
the batch in the PEL for reclaim.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from chris_streaming.common.schemas import LogEvent

logger = logging.getLogger(__name__)


class QuickwitBulkError(Exception):
    """Raised when a Quickwit ingest request fails."""

    def __init__(self, status_code: int, reason: str):
        self.status_code = status_code
        self.reason = reason
        super().__init__(f"Quickwit ingest failed: {status_code} {reason}")


class QuickwitSearchError(Exception):
    """Raised when a Quickwit search request fails."""


class QuickwitClient:
    """Async Quickwit REST client."""

    def __init__(
        self,
        url: str,
        index_id: str = "job-logs",
        *,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._base_url = url.rstrip("/")
        self._index_id = index_id
        self._timeout = httpx.Timeout(timeout_seconds)
        self._client: httpx.AsyncClient | None = None

    async def connect(self, index_config: str | None = None) -> None:
        """Open the HTTP client and ensure the index exists.

        ``index_config`` is the YAML body to POST if the index is missing.
        If None, the client assumes the index is pre-provisioned.
        """
        self._client = httpx.AsyncClient(
            base_url=self._base_url, timeout=self._timeout,
        )
        if index_config is not None:
            await self._ensure_index(index_config)

    async def _ensure_index(self, index_config_yaml: str) -> None:
        assert self._client is not None
        try:
            resp = await self._client.get(f"/api/v1/indexes/{self._index_id}")
        except httpx.HTTPError as e:
            raise QuickwitBulkError(0, f"reach quickwit: {e}") from e
        if resp.status_code == 200:
            logger.info("Quickwit index %s already exists", self._index_id)
            return
        if resp.status_code != 404:
            raise QuickwitBulkError(resp.status_code, resp.text)

        create = await self._client.post(
            "/api/v1/indexes",
            content=index_config_yaml,
            headers={"content-type": "application/yaml"},
        )
        if create.status_code not in (200, 201):
            raise QuickwitBulkError(create.status_code, create.text)
        logger.info("Created Quickwit index %s", self._index_id)

    async def write_batch(self, events: list[LogEvent]) -> None:
        """Ingest a batch of log events as NDJSON with ``commit=force``.

        ``commit=force`` blocks until the docs are committed + searchable.
        Ingesting with ``commit=auto`` would return sooner but defeat the
        EOS / logs_flushed guarantee: a subsequent ``SET logs_flushed`` could
        fire before the data is actually searchable.
        """
        if not events:
            return
        assert self._client is not None

        ndjson = "\n".join(event.model_dump_json() for event in events)
        try:
            resp = await self._client.post(
                f"/api/v1/{self._index_id}/ingest?commit=force",
                content=ndjson,
                headers={"content-type": "application/x-ndjson"},
            )
        except httpx.HTTPError as e:
            raise QuickwitBulkError(0, str(e)) from e

        if resp.status_code >= 400:
            logger.error(
                "Quickwit ingest failed: %d %s (batch=%d)",
                resp.status_code, resp.text[:200], len(events),
            )
            raise QuickwitBulkError(resp.status_code, resp.text[:500])

        logger.debug("Quickwit ingest: %d documents committed", len(events))

    async def search_by_job(
        self,
        job_id: str,
        *,
        limit: int = 1000,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Return ``{total, lines}`` for a job, sorted by timestamp asc.

        ``lines`` are the raw documents (``_source``-equivalent) — the same
        shape the old OpenSearch endpoint returned. ``total`` is the best
        estimate Quickwit reports (``num_hits``).
        """
        assert self._client is not None
        body = {
            "query": f"job_id:{_escape_keyword(job_id)}",
            "max_hits": limit,
            "start_offset": offset,
            "sort_by": "timestamp",
        }
        try:
            resp = await self._client.post(
                f"/api/v1/{self._index_id}/search",
                content=json.dumps(body),
                headers={"content-type": "application/json"},
            )
        except httpx.HTTPError as e:
            raise QuickwitSearchError(str(e)) from e
        if resp.status_code >= 400:
            raise QuickwitSearchError(f"{resp.status_code} {resp.text[:200]}")

        payload = resp.json()
        hits = payload.get("hits", [])
        total = payload.get("num_hits", len(hits))
        # Quickwit returns newest-first for descending sort_by; we asked for
        # "timestamp" which is treated as descending by default, so reverse
        # to match the old OpenSearch "asc" contract.
        hits = list(reversed(hits))
        return {"total": total, "lines": hits}

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None


def _escape_keyword(value: str) -> str:
    """Quote a value for exact keyword matching via Tantivy's query parser.

    The ``raw`` tokenizer stores the field value as a single token, so the
    simplest and safest form of exact match is a phrase query:
    ``field:"verbatim value"``. Double-quoting avoids every Tantivy special
    except ``"`` and ``\\``, which we backslash-escape.
    """
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'
