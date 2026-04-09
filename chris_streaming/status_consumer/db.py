"""
PostgreSQL persistence layer for job status events.

Uses asyncpg for async upserts. The schema is simple: one row per
(job_id, job_type) combination, always updated to the latest status.
"""

from __future__ import annotations

import logging

import asyncpg

from chris_streaming.common.schemas import StatusEvent

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS job_status (
    job_id       TEXT NOT NULL,
    job_type     TEXT NOT NULL,
    status       TEXT NOT NULL,
    image        TEXT NOT NULL DEFAULT '',
    cmd          TEXT NOT NULL DEFAULT '',
    message      TEXT NOT NULL DEFAULT '',
    exit_code    INTEGER,
    source       TEXT NOT NULL DEFAULT 'docker',
    event_id     TEXT NOT NULL DEFAULT '',
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (job_id, job_type)
);

CREATE INDEX IF NOT EXISTS idx_job_status_status ON job_status (status);
CREATE INDEX IF NOT EXISTS idx_job_status_updated ON job_status (updated_at);
"""

UPSERT_SQL = """
INSERT INTO job_status (job_id, job_type, status, image, cmd, message, exit_code,
                        source, event_id, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (job_id, job_type) DO UPDATE SET
    status     = EXCLUDED.status,
    image      = EXCLUDED.image,
    cmd        = EXCLUDED.cmd,
    message    = EXCLUDED.message,
    exit_code  = EXCLUDED.exit_code,
    source     = EXCLUDED.source,
    event_id   = EXCLUDED.event_id,
    updated_at = EXCLUDED.updated_at
"""


class StatusDB:
    """Async PostgreSQL client for job status upserts."""

    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(self._dsn, min_size=2, max_size=10)
        async with self._pool.acquire() as conn:
            await conn.execute(CREATE_TABLE_SQL)
        logger.info("PostgreSQL connected and schema initialized")

    async def upsert(self, event: StatusEvent) -> None:
        """Idempotent upsert of a status event."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                UPSERT_SQL,
                event.job_id,
                event.job_type.value,
                event.status.value,
                event.image,
                event.cmd,
                event.message,
                event.exit_code,
                event.source,
                event.event_id,
                event.timestamp,
            )

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
