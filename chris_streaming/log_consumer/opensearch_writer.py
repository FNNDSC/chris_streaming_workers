"""
Bulk writer for OpenSearch with daily index rotation.

Writes log events in batches using the _bulk API for efficiency.
Index pattern: {prefix}-YYYY.MM.DD
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from opensearchpy import AsyncOpenSearch

from chris_streaming.common.schemas import LogEvent

logger = logging.getLogger(__name__)


class OpenSearchBulkError(Exception):
    """Raised when an OpenSearch bulk write reports any per-item failures."""

    def __init__(self, failed: int, total: int, sample_reasons: list[str]):
        self.failed = failed
        self.total = total
        self.sample_reasons = sample_reasons
        msg = (
            f"OpenSearch bulk write failed: {failed}/{total} items rejected; "
            f"sample reasons: {sample_reasons}"
        )
        super().__init__(msg)


class OpenSearchWriter:
    """Async OpenSearch bulk writer for log events."""

    def __init__(self, url: str, index_prefix: str):
        self._client: AsyncOpenSearch | None = None
        self._url = url
        self._index_prefix = index_prefix

    async def connect(self) -> None:
        self._client = AsyncOpenSearch(
            hosts=[self._url],
            use_ssl=False,
            verify_certs=False,
        )
        # Apply index template
        template_body = {
            "index_patterns": [f"{self._index_prefix}-*"],
            "template": {
                "settings": {
                    "number_of_shards": 2,
                    "number_of_replicas": 0,
                    "refresh_interval": "5s",
                },
                "mappings": {
                    "properties": {
                        "event_id": {"type": "keyword"},
                        "job_id": {"type": "keyword"},
                        "job_type": {"type": "keyword"},
                        "container_name": {"type": "keyword"},
                        "line": {"type": "text"},
                        "stream": {"type": "keyword"},
                        "timestamp": {"type": "date"},
                    }
                },
            },
        }
        await self._client.indices.put_index_template(
            name=f"{self._index_prefix}-template",
            body=template_body,
        )
        logger.info("OpenSearch connected, index template applied")

    async def write_batch(self, events: list[LogEvent]) -> None:
        """Write a batch of log events using the bulk API."""
        if not events:
            return

        body = []
        for event in events:
            index_name = self._index_name(event.timestamp)
            body.append({"index": {"_index": index_name}})
            body.append(event.model_dump(mode="json"))

        resp = await self._client.bulk(body=body)
        if resp.get("errors"):
            items = resp.get("items", [])
            failed = sum(1 for item in items if "error" in item.get("index", {}))
            sample_reasons: list[str] = []
            for item in items:
                err = item.get("index", {}).get("error")
                if err:
                    reason = err.get("reason") if isinstance(err, dict) else str(err)
                    if reason and reason not in sample_reasons:
                        sample_reasons.append(reason)
                    if len(sample_reasons) >= 3:
                        break
            logger.error(
                "OpenSearch bulk write: %d/%d failed; reasons=%s",
                failed, len(events), sample_reasons,
            )
            raise OpenSearchBulkError(failed, len(events), sample_reasons)
        logger.debug("OpenSearch bulk write: %d documents indexed", len(events))

    def _index_name(self, timestamp: datetime) -> str:
        if hasattr(timestamp, "strftime"):
            date_str = timestamp.strftime("%Y.%m.%d")
        else:
            date_str = datetime.now(timezone.utc).strftime("%Y.%m.%d")
        return f"{self._index_prefix}-{date_str}"

    async def close(self) -> None:
        if self._client:
            await self._client.close()
