"""
Celery tasks for job status processing and workflow orchestration.

Tasks:
  - process_job_status: Upserts status to PostgreSQL, publishes to Redis,
    and advances the workflow state machine on terminal events.
  - start_workflow: Stores workflow params and schedules the copy job on pfcon.
  - cleanup_containers: Waits for logs_flushed signals, then removes containers.
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager

import psycopg2
import psycopg2.pool
import redis as sync_redis
from celery import Celery, Task
from celery.signals import worker_init, worker_shutdown

from chris_streaming.common.schemas import StatusEvent, JobStatus
from chris_streaming.common.settings import SSEServiceSettings
from .pfcon_client import PfconClient

logger = logging.getLogger(__name__)

settings = SSEServiceSettings()

celery_app = Celery(
    "chris_streaming",
    broker=settings.celery_broker_url,
)
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    task_routes={
        "chris_streaming.sse_service.tasks.process_job_status": {
            "queue": "status-processing",
        },
        "chris_streaming.sse_service.tasks.start_workflow": {
            "queue": "status-processing",
        },
        "chris_streaming.sse_service.tasks.cleanup_containers": {
            "queue": "status-processing",
        },
    },
)

# ── SQL ──────────────────────────────────────────────────────────────────────

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

CREATE TABLE IF NOT EXISTS job_workflow (
    job_id        TEXT PRIMARY KEY,
    current_step  TEXT NOT NULL DEFAULT 'copy',
    status        TEXT NOT NULL DEFAULT 'running',
    params        JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

UPSERT_SQL = """
INSERT INTO job_status (job_id, job_type, status, image, cmd, message, exit_code,
                        source, event_id, updated_at)
VALUES (%(job_id)s, %(job_type)s, %(status)s, %(image)s, %(cmd)s, %(message)s,
        %(exit_code)s, %(source)s, %(event_id)s, %(updated_at)s)
ON CONFLICT (job_id, job_type) DO UPDATE SET
    status     = EXCLUDED.status,
    image      = EXCLUDED.image,
    cmd        = EXCLUDED.cmd,
    message    = EXCLUDED.message,
    exit_code  = EXCLUDED.exit_code,
    source     = EXCLUDED.source,
    event_id   = EXCLUDED.event_id,
    updated_at = EXCLUDED.updated_at
WHERE job_status.updated_at < EXCLUDED.updated_at
"""

# Map terminal statuses to their confirmed counterparts
_CONFIRMED_MAP = {
    "finishedSuccessfully": "confirmed_finishedSuccessfully",
    "finishedWithError": "confirmed_finishedWithError",
    "undefined": "confirmed_undefined",
}

# Workflow step → job_type mapping
_STEP_JOB_TYPE = {
    "copy": "copy",
    "plugin": "plugin",
    "upload": "upload",
    "delete": "delete",
}

# Workflow step progression
_NEXT_STEP = {
    "copy": "plugin",
    "plugin": "upload",
    "upload": "delete",
    "delete": "cleanup",
}

# Steps to skip to on failure (go straight to delete for cleanup)
_FAILURE_STEP = {
    "copy": "delete",
    "plugin": "delete",
    "upload": "delete",
}

# Terminal statuses that indicate completion
_TERMINAL_STATUSES = {"finishedSuccessfully", "finishedWithError", "undefined"}

# ── Connection pools (initialized at worker startup) ────────────────────────

_db_pool: psycopg2.pool.ThreadedConnectionPool | None = None
_redis_pool: sync_redis.ConnectionPool | None = None
_pfcon: PfconClient | None = None


@contextmanager
def _get_db_conn():
    """Get a connection from the pool, return it when done."""
    conn = _db_pool.getconn()
    try:
        yield conn
    finally:
        _db_pool.putconn(conn)


def _get_redis() -> sync_redis.Redis:
    """Get a Redis client using the shared connection pool."""
    return sync_redis.Redis(connection_pool=_redis_pool, decode_responses=True)


@worker_init.connect
def _init_resources(**kwargs):
    """Initialize connection pools and pfcon client when the Celery worker starts."""
    global _db_pool, _redis_pool, _pfcon

    _db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=10,
        dsn=settings.db_dsn,
    )
    logger.info("PostgreSQL connection pool initialized")

    _redis_pool = sync_redis.ConnectionPool.from_url(
        settings.redis_url,
        decode_responses=True,
    )
    logger.info("Redis connection pool initialized")

    _pfcon = PfconClient(
        base_url=settings.pfcon_url,
        username=settings.pfcon_user,
        password=settings.pfcon_password,
    )
    logger.info("pfcon client initialized (url=%s)", settings.pfcon_url)

    # Create DB schema
    with _get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()
    logger.info("PostgreSQL schema initialized")


@worker_shutdown.connect
def _shutdown_resources(**kwargs):
    """Clean up connection pools on worker shutdown."""
    global _db_pool, _redis_pool
    if _db_pool:
        _db_pool.closeall()
        _db_pool = None
    if _redis_pool:
        _redis_pool.disconnect()
        _redis_pool = None


# ── Celery task base ────────────────────────────────────────────────────────

class _StatusTaskBase(Task):
    """Custom base class that logs permanently failed tasks."""

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        event_data = (kwargs or {}).get("event_data", {})
        logger.error(
            "Task %s permanently failed for job=%s status=%s: %s",
            task_id,
            event_data.get("job_id", "?"),
            event_data.get("status", "?"),
            exc,
        )


# ── process_job_status ──────────────────────────────────────────────────────

@celery_app.task(
    base=_StatusTaskBase,
    bind=True,
    name="chris_streaming.sse_service.tasks.process_job_status",
    autoretry_for=(
        psycopg2.OperationalError,
        psycopg2.InterfaceError,
        sync_redis.ConnectionError,
        sync_redis.TimeoutError,
    ),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_kwargs={"max_retries": 5},
    acks_late=True,
    reject_on_worker_lost=True,
)
def process_job_status(self, event_data: dict) -> dict:
    """
    Process a job status event:
      1. Upsert to PostgreSQL (only if newer than existing row).
      2. Publish original event to Redis Pub/Sub for real-time SSE delivery.
      3. For terminal statuses, publish confirmed_* to Redis Pub/Sub.
      4. If an active workflow exists, advance to the next step.
    """
    job_id = event_data["job_id"]
    job_type = event_data.get("job_type", "")
    status = event_data.get("status", "")

    # 1. Upsert to PostgreSQL
    with _get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(UPSERT_SQL, {
                "job_id": job_id,
                "job_type": job_type,
                "status": status,
                "image": event_data.get("image", ""),
                "cmd": event_data.get("cmd", ""),
                "message": event_data.get("message", ""),
                "exit_code": event_data.get("exit_code"),
                "source": event_data.get("source", "docker"),
                "event_id": event_data.get("event_id", ""),
                "updated_at": event_data.get("timestamp"),
            })
        conn.commit()
    logger.info("Upserted status: job=%s type=%s status=%s", job_id, job_type, status)

    # 2. Publish original event to Redis Pub/Sub
    channel = f"job:{job_id}:status"
    original_event = StatusEvent.model_validate(event_data)
    r = _get_redis()
    r.publish(channel, original_event.model_dump_json())
    logger.debug("Published to %s: %s", channel, status)

    # 3. For terminal statuses, also publish confirmed_*
    confirmed_status = _CONFIRMED_MAP.get(status)
    if confirmed_status is not None:
        confirmed_event = StatusEvent(
            job_id=job_id,
            job_type=event_data["job_type"],
            status=JobStatus(confirmed_status),
            previous_status=JobStatus(status),
            image=event_data.get("image", ""),
            cmd=event_data.get("cmd", ""),
            message="confirmed by celery worker",
            exit_code=event_data.get("exit_code"),
            source=event_data.get("source", "docker"),
        )
        r.publish(channel, confirmed_event.model_dump_json())
        logger.info("Published confirmed status: job=%s status=%s", job_id, confirmed_status)

    # 4. Workflow advancement on terminal events
    if status in _TERMINAL_STATUSES:
        _try_advance_workflow(job_id, job_type, status)

    return {
        "status": "processed",
        "job_id": job_id,
        "db_upserted": True,
        "confirmed": confirmed_status is not None,
    }


def _try_advance_workflow(job_id: str, job_type: str, status: str) -> None:
    """Check if there's an active workflow and advance to the next step."""
    with _get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT current_step, status, params FROM job_workflow "
                "WHERE job_id = %s AND status IN ('running', 'failed') FOR UPDATE",
                (job_id,),
            )
            row = cur.fetchone()
            if row is None:
                return  # No active workflow for this job

            current_step, current_wf_status, params = row
            expected_job_type = _STEP_JOB_TYPE.get(current_step)

            # Only advance if this event matches the current step's job_type
            if job_type != expected_job_type:
                return

            succeeded = (status == "finishedSuccessfully")

            if succeeded:
                next_step = _NEXT_STEP.get(current_step)
            else:
                # On failure: skip to delete for cleanup (or cleanup if already at delete)
                next_step = _FAILURE_STEP.get(current_step, "cleanup")

            # Preserve 'failed' status once set; only set 'failed' on new failures
            if current_wf_status == "failed":
                workflow_status = "failed"
            elif not succeeded and current_step != "delete":
                workflow_status = "failed"
            else:
                workflow_status = "running"

            if next_step == "cleanup":
                # Delete step is done — schedule container cleanup
                cur.execute(
                    "UPDATE job_workflow SET current_step = 'cleanup', "
                    "status = CASE WHEN status = 'failed' THEN 'failed' ELSE %s END, "
                    "updated_at = NOW() "
                    "WHERE job_id = %s AND current_step = %s",
                    (workflow_status, job_id, current_step),
                )
            else:
                cur.execute(
                    "UPDATE job_workflow SET current_step = %s, "
                    "status = %s, updated_at = NOW() "
                    "WHERE job_id = %s AND current_step = %s",
                    (next_step, workflow_status, job_id, current_step),
                )

            advanced = cur.rowcount > 0
        conn.commit()

    if not advanced:
        logger.debug("Workflow step already advanced for job=%s", job_id)
        return

    logger.info(
        "Workflow advanced: job=%s %s → %s (succeeded=%s)",
        job_id, current_step, next_step, succeeded,
    )

    # Publish workflow status update to Redis
    r = _get_redis()
    r.publish(f"job:{job_id}:workflow", json.dumps({
        "job_id": job_id,
        "current_step": next_step,
        "status": workflow_status,
    }))

    # Execute the next step
    if next_step == "cleanup":
        cleanup_containers.delay(job_id)
    else:
        _execute_workflow_step(job_id, next_step, params)


def _execute_workflow_step(job_id: str, step: str, params: dict) -> None:
    """Call pfcon to schedule the appropriate job for a workflow step.

    If pfcon returns an immediate terminal status (e.g., uploadSkipped,
    deleteSkipped, copySkipped), advances the workflow without waiting
    for a Docker event — because no container was created.
    """
    resp = None
    try:
        match step:
            case "copy":
                resp = _pfcon.schedule_copy(job_id, params)
                logger.info("Scheduled copy: job=%s resp=%s", job_id, resp)
            case "plugin":
                resp = _pfcon.schedule_plugin(job_id, params)
                logger.info("Scheduled plugin: job=%s resp=%s", job_id, resp)
            case "upload":
                resp = _pfcon.schedule_upload(job_id, params)
                logger.info("Scheduled upload: job=%s resp=%s", job_id, resp)
            case "delete":
                resp = _pfcon.schedule_delete(job_id, params)
                logger.info("Scheduled delete: job=%s resp=%s", job_id, resp)
    except Exception as e:
        logger.error("Failed to execute workflow step %s for job=%s: %s", step, job_id, e)
        # Mark workflow as failed
        with _get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE job_workflow SET status = 'failed', updated_at = NOW() "
                    "WHERE job_id = %s",
                    (job_id,),
                )
            conn.commit()
        r = _get_redis()
        r.publish(f"job:{job_id}:workflow", json.dumps({
            "job_id": job_id,
            "current_step": step,
            "status": "failed",
            "error": str(e),
        }))
        return

    # Check if pfcon returned an immediate terminal status (no container created).
    # This happens for uploadSkipped, deleteSkipped, copySkipped in fslink mode.
    pfcon_status = (resp or {}).get("compute", {}).get("status", "")
    if pfcon_status in _TERMINAL_STATUSES:
        logger.info(
            "Step %s completed immediately (status=%s), advancing workflow",
            step, pfcon_status,
        )
        # No container was created, so no EOS marker will arrive.
        # Set logs_flushed immediately so cleanup_containers won't wait.
        r = _get_redis()
        jt = _STEP_JOB_TYPE[step]
        r.set(f"job:{job_id}:{jt}:logs_flushed", "1", ex=3600)
        logger.info("Set logs_flushed for skipped step: job=%s type=%s", job_id, jt)

        _try_advance_workflow(
            job_id,
            jt,
            pfcon_status,
        )


# ── start_workflow ──────────────────────────────────────────────────────────

@celery_app.task(
    bind=True,
    name="chris_streaming.sse_service.tasks.start_workflow",
    autoretry_for=(psycopg2.OperationalError, psycopg2.InterfaceError),
    retry_backoff=True,
    retry_kwargs={"max_retries": 3},
    acks_late=True,
)
def start_workflow(self, job_id: str, params: dict) -> dict:
    """
    Initialize a workflow: store params in PostgreSQL and schedule
    the copy job on pfcon.
    """
    # Insert workflow record
    with _get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO job_workflow (job_id, current_step, status, params) "
                "VALUES (%s, 'copy', 'running', %s) "
                "ON CONFLICT (job_id) DO UPDATE SET "
                "current_step = 'copy', status = 'running', "
                "params = EXCLUDED.params, updated_at = NOW()",
                (job_id, json.dumps(params)),
            )
        conn.commit()
    logger.info("Workflow started: job=%s", job_id)

    # Publish workflow status
    r = _get_redis()
    r.publish(f"job:{job_id}:workflow", json.dumps({
        "job_id": job_id,
        "current_step": "copy",
        "status": "running",
    }))

    # Schedule copy job on pfcon
    _execute_workflow_step(job_id, "copy", params)

    return {"job_id": job_id, "status": "started"}


# ── cleanup_containers ──────────────────────────────────────────────────────

@celery_app.task(
    bind=True,
    name="chris_streaming.sse_service.tasks.cleanup_containers",
    max_retries=30,
    default_retry_delay=2,
    acks_late=True,
)
def cleanup_containers(self, job_id: str) -> dict:
    """
    Wait for all logs to be flushed, then remove containers from pfcon.

    Checks Redis for logs_flushed keys for each job type. Retries every
    2 seconds (up to 60s) until all are present, then proceeds with
    container removal.
    """
    r = _get_redis()

    # Only wait for job types that actually created containers.
    # Query job_status to find which job types have status records.
    with _get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT job_type FROM job_status WHERE job_id = %s",
                (job_id,),
            )
            job_types = [row[0] for row in cur.fetchall()]

    if not job_types:
        job_types = ["copy", "plugin", "upload", "delete"]
        logger.warning("No job_status records for job=%s, checking all types", job_id)

    # Check which logs are flushed
    missing = []
    for jt in job_types:
        key = f"job:{job_id}:{jt}:logs_flushed"
        if not r.exists(key):
            missing.append(jt)

    if missing and self.request.retries < self.max_retries:
        logger.info(
            "Waiting for logs_flushed: job=%s missing=%s (retry %d/%d)",
            job_id, missing, self.request.retries, self.max_retries,
        )
        raise self.retry()

    if missing:
        logger.warning(
            "Proceeding with cleanup despite missing logs_flushed: job=%s missing=%s",
            job_id, missing,
        )

    # Remove containers for all known job types (including skipped ones, harmless if absent)
    all_job_types = ["copy", "plugin", "upload", "delete"]
    for jt in all_job_types:
        try:
            _pfcon.remove_container(job_id, jt)
        except Exception as e:
            logger.warning("Failed to remove %s container for job=%s: %s", jt, job_id, e)

    # Mark workflow as completed
    with _get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE job_workflow SET current_step = 'completed', "
                "updated_at = NOW(), "
                "status = CASE WHEN status = 'failed' THEN 'failed' ELSE 'completed' END "
                "WHERE job_id = %s",
                (job_id,),
            )
        conn.commit()

    # Publish workflow completion
    r.publish(f"job:{job_id}:workflow", json.dumps({
        "job_id": job_id,
        "current_step": "completed",
        "status": "completed",
    }))

    # Clean up logs_flushed keys
    for jt in all_job_types:
        r.delete(f"job:{job_id}:{jt}:logs_flushed")

    logger.info("Workflow cleanup completed: job=%s", job_id)
    return {"job_id": job_id, "status": "cleaned_up"}
