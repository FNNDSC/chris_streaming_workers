"""
Thin wrapper that bootstraps a ``QuickwitClient`` with the ``job-logs`` index
YAML shipped in ``config/quickwit/job-logs-index.yaml``.

Exists so the Log Consumer can pass one object to the consumer loop instead of
wiring the YAML load + client setup itself. The client is re-exported so tests
can mock the HTTP layer.
"""

from __future__ import annotations

import logging
from importlib import resources
from pathlib import Path

from chris_streaming.common.quickwit import (
    QuickwitBulkError,
    QuickwitClient,
    QuickwitSearchError,
)
from chris_streaming.common.schemas import LogEvent

logger = logging.getLogger(__name__)


_INDEX_CONFIG_CANDIDATES = (
    # In-repo path when running from source.
    Path(__file__).resolve().parents[2] / "config" / "quickwit" / "job-logs-index.yaml",
    # Container path (shipped in the image under /app/config/...).
    Path("/app/config/quickwit/job-logs-index.yaml"),
)


def _load_index_config() -> str:
    for candidate in _INDEX_CONFIG_CANDIDATES:
        if candidate.is_file():
            return candidate.read_text()
    # Fallback: try package resources (if the YAML is ever bundled in).
    try:
        return (resources.files("chris_streaming") / "config" / "quickwit" / "job-logs-index.yaml").read_text()  # type: ignore[no-any-return]
    except Exception as e:  # pragma: no cover - defensive
        raise FileNotFoundError(
            f"Quickwit index config not found in {_INDEX_CONFIG_CANDIDATES}",
        ) from e


class QuickwitWriter:
    """Quickwit ingest writer (thin wrapper over ``QuickwitClient``)."""

    def __init__(self, url: str, index_id: str = "job-logs") -> None:
        self._client = QuickwitClient(url, index_id=index_id)

    async def connect(self) -> None:
        await self._client.connect(index_config=_load_index_config())

    async def write_batch(self, events: list[LogEvent]) -> None:
        await self._client.write_batch(events)

    async def close(self) -> None:
        await self._client.close()


__all__ = [
    "QuickwitBulkError",
    "QuickwitClient",
    "QuickwitSearchError",
    "QuickwitWriter",
]
