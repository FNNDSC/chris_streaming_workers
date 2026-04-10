"""Integration test: PostgreSQL schema creation and upsert logic."""

import uuid

import psycopg2
import pytest

from chris_streaming.sse_service.tasks import CREATE_TABLE_SQL, UPSERT_SQL

pytestmark = pytest.mark.integration


@pytest.fixture
def db_conn(db_dsn):
    conn = psycopg2.connect(db_dsn)
    conn.autocommit = False
    yield conn
    conn.rollback()
    conn.close()


class TestPostgresSchema:
    def test_create_tables(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        db_conn.commit()

        # Verify tables exist
        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name IN ('job_status', 'job_workflow')"
            )
            tables = {row[0] for row in cur.fetchall()}
        assert "job_status" in tables
        assert "job_workflow" in tables


class TestPostgresUpsert:
    def test_insert_and_upsert(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        db_conn.commit()

        job_id = f"integ-{uuid.uuid4().hex[:8]}"
        params = {
            "job_id": job_id,
            "job_type": "plugin",
            "status": "started",
            "image": "test:latest",
            "cmd": "python app.py",
            "message": "start",
            "exit_code": None,
            "source": "docker",
            "event_id": "evt-001",
            "updated_at": "2026-01-15T12:00:00+00:00",
        }

        # Insert
        with db_conn.cursor() as cur:
            cur.execute(UPSERT_SQL, params)
        db_conn.commit()

        with db_conn.cursor() as cur:
            cur.execute("SELECT status FROM job_status WHERE job_id = %s", (job_id,))
            assert cur.fetchone()[0] == "started"

        # Upsert with newer timestamp
        params["status"] = "finishedSuccessfully"
        params["updated_at"] = "2026-01-15T12:05:00+00:00"
        params["event_id"] = "evt-002"
        with db_conn.cursor() as cur:
            cur.execute(UPSERT_SQL, params)
        db_conn.commit()

        with db_conn.cursor() as cur:
            cur.execute("SELECT status FROM job_status WHERE job_id = %s", (job_id,))
            assert cur.fetchone()[0] == "finishedSuccessfully"

    def test_upsert_rejects_older_timestamp(self, db_conn):
        with db_conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        db_conn.commit()

        job_id = f"integ-{uuid.uuid4().hex[:8]}"
        params = {
            "job_id": job_id,
            "job_type": "plugin",
            "status": "finishedSuccessfully",
            "image": "",
            "cmd": "",
            "message": "",
            "exit_code": 0,
            "source": "docker",
            "event_id": "evt-new",
            "updated_at": "2026-01-15T12:05:00+00:00",
        }

        with db_conn.cursor() as cur:
            cur.execute(UPSERT_SQL, params)
        db_conn.commit()

        # Try to upsert with an older timestamp — should be rejected
        params["status"] = "started"
        params["updated_at"] = "2026-01-15T12:00:00+00:00"
        params["event_id"] = "evt-old"
        with db_conn.cursor() as cur:
            cur.execute(UPSERT_SQL, params)
        db_conn.commit()

        with db_conn.cursor() as cur:
            cur.execute("SELECT status FROM job_status WHERE job_id = %s", (job_id,))
            assert cur.fetchone()[0] == "finishedSuccessfully"  # unchanged
