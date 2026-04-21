"""
Celery tasks for job status processing and workflow orchestration.

Tasks:
  - process_job_status: Upserts status to PostgreSQL, emits confirmed_* to the
    status stream for terminal events, and advances the workflow state machine.
  - start_workflow: Stores workflow params and schedules the copy job on pfcon.
  - cleanup_containers: Waits for logs_flushed signals, then removes containers.
"""

from __future__ import annotations

import hashlib
import json
import logging
from contextlib import contextmanager
from datetime import datetime, timezone

import psycopg2
import psycopg2.pool
import redis as sync_redis
from celery import Celery, Task
from celery.signals import worker_init, worker_shutdown

from chris_streaming.common.redis_stream import ShardRouter
from chris_streaming.common.schemas import JobStatus, StatusEvent, WorkflowEvent
from chris_streaming.common.settings import SSEServiceSettings
from .pfcon_client import PfconClient

logger = logging.getLogger(__name__)

settings = SSEServiceSettings()
_shard_router = ShardRouter(settings.stream_num_shards)

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

CREATE TABLE IF NOT EXISTS job_workflow_events (
    id                  BIGSERIAL PRIMARY KEY,
    job_id              TEXT NOT NULL,
    current_step        TEXT NOT NULL,
    current_step_status TEXT NOT NULL,
    workflow_status     TEXT NOT NULL,
    error               TEXT,
    event_id            TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jwe_job_time ON job_workflow_events (job_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_jwe_event_id ON job_workflow_events (event_id);
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

# Workflow step progression (linear; skip logic is applied on top of this).
_NEXT_STEP = {
    "copy": "plugin",
    "plugin": "upload",
    "upload": "delete",
    "delete": "cleanup",
}

# On failure, jump straight to delete for cleanup (or cleanup if already at delete).
_FAILURE_STEP = {
    "copy": "delete",
    "plugin": "delete",
    "upload": "delete",
}

# Steps that may be skipped based on pfcon server config.
_OPTIONAL_STEPS = {"copy", "upload"}

# Terminal statuses that indicate completion
_TERMINAL_STATUSES = {"finishedSuccessfully", "finishedWithError", "undefined"}


def _is_step_required(step: str, params: dict) -> bool:
    """Return True if the given workflow step must be scheduled."""
    if step not in _OPTIONAL_STEPS:
        return True
    flag = params.get(f"requires_{step}_job")
    # Default to True if the flag is missing — fail-safe (don't silently skip).
    return bool(flag) if flag is not None else True


def _first_active_step(params: dict) -> str:
    """Return the first step that should actually run for this workflow."""
    step = "copy"
    while step in _OPTIONAL_STEPS and not _is_step_required(step, params):
        step = _NEXT_STEP[step]
    return step


def _next_active_step(current_step: str, params: dict) -> str | None:
    """Return the next step to execute, skipping optional steps the server doesn't need."""
    step = _NEXT_STEP.get(current_step)
    while step in _OPTIONAL_STEPS and not _is_step_required(step, params):
        step = _NEXT_STEP.get(step)
    return step

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
      2. For terminal statuses, XADD confirmed_* to the status stream so SSE
         replicas (and downstream consumers like CUBE) see it.
      3. If an active workflow exists, advance to the next step.
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

    # 2. For terminal statuses, emit confirmed_* to the status stream.
    confirmed_status = _CONFIRMED_MAP.get(status)
    if confirmed_status is not None:
        # Reuse the source event's timestamp so the confirmed_* event_id
        # is deterministic across Celery retries — otherwise every retry
        # would synthesize ``datetime.now()`` and produce a fresh event_id,
        # defeating downstream dedup.
        src_ts = event_data.get("timestamp")
        if isinstance(src_ts, str):
            try:
                src_ts = datetime.fromisoformat(src_ts.replace("Z", "+00:00"))
            except ValueError:
                src_ts = None
        confirmed_kwargs = {
            "job_id": job_id,
            "job_type": event_data["job_type"],
            "status": JobStatus(confirmed_status),
            "previous_status": JobStatus(status),
            "image": event_data.get("image", ""),
            "cmd": event_data.get("cmd", ""),
            "message": "confirmed by celery worker",
            "exit_code": event_data.get("exit_code"),
            "source": event_data.get("source", "docker"),
        }
        if src_ts is not None:
            confirmed_kwargs["timestamp"] = src_ts
        confirmed_event = StatusEvent(**confirmed_kwargs)
        shard = _shard_router.shard_for(job_id)
        stream = _shard_router.stream_name(settings.stream_status_base, shard)
        try:
            _get_redis().xadd(
                stream,
                {"data": confirmed_event.serialize()},
                maxlen=settings.stream_status_maxlen,
                approximate=True,
            )
        except Exception as e:
            logger.warning(
                "XADD of confirmed_* to %s failed for job=%s: %s",
                stream, job_id, e,
            )
        logger.info("Emitted confirmed status: job=%s status=%s", job_id, confirmed_status)

    # 3. Workflow advancement on terminal events
    if status in _TERMINAL_STATUSES:
        _try_advance_workflow(job_id, job_type, status)

    return {
        "status": "processed",
        "job_id": job_id,
        "db_upserted": True,
        "confirmed": confirmed_status is not None,
    }


def _publish_workflow_event(
    job_id: str,
    current_step: str,
    current_step_status: str,
    workflow_status: str,
    extra: dict | None = None,
) -> None:
    """Record a workflow event in the history table and publish it live.

    Every workflow event carries three fields:
      - ``current_step``: the step the workflow is now sitting in
      - ``current_step_status``: the status of that current step
      - ``workflow_status``: the overall workflow status (``running`` while
        in motion, or a terminal value at cleanup completion)

    The event_id is deterministic over
    ``(job_id, current_step, current_step_status, workflow_status, error)``
    so task retries naturally dedup via ``ON CONFLICT (event_id) DO NOTHING``.
    """
    error = (extra or {}).get("error") or None
    event = WorkflowEvent(
        job_id=job_id,
        current_step=current_step,
        current_step_status=current_step_status,
        workflow_status=workflow_status,
        error=error,
    )

    # 1. Durable history row (idempotent on retry via deterministic event_id).
    try:
        with _get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO job_workflow_events "
                    "(job_id, current_step, current_step_status, "
                    " workflow_status, error, event_id) "
                    "VALUES (%s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (event_id) DO NOTHING",
                    (job_id, current_step, current_step_status,
                     workflow_status, error, event.event_id),
                )
            conn.commit()
    except Exception as e:
        logger.warning("Failed to record workflow event for job=%s: %s", job_id, e)

    # 2. Live fan-out via the sharded workflow stream.
    shard = _shard_router.shard_for(job_id)
    stream = _shard_router.stream_name(settings.stream_workflow_base, shard)
    try:
        _get_redis().xadd(
            stream,
            {"data": event.serialize()},
            maxlen=settings.stream_workflow_maxlen,
            approximate=True,
        )
    except Exception as e:
        logger.warning("XADD to %s failed for job=%s: %s", stream, job_id, e)


def _try_advance_workflow(job_id: str, job_type: str, status: str) -> None:
    """Check if there's an active workflow and advance to the next step.

    Crash-safety: pfcon is idempotent on ``jid``, so the protocol is
    "schedule-then-advance":

        1. Read the current step under ``FOR UPDATE`` (serialize retries).
        2. Call pfcon for the next step (or schedule cleanup).
        3. UPDATE ``current_step`` with a CAS on the old step, then commit.

    If the worker dies between steps 1-2 or 2-3, Celery re-queues the
    message (``acks_late=True`` + ``reject_on_worker_lost=True``) and the
    retry re-runs the whole protocol: pfcon returns the existing job
    instead of double-scheduling, and the CAS UPDATE converges. An
    earlier implementation committed the UPDATE *before* calling pfcon,
    so a crash in between left the workflow stuck in the new step with
    no container ever scheduled.

    When ``_execute_workflow_step`` reports a synchronous completion
    (pfcon failure or ``*Skipped`` terminal status, both of which create
    no container), the returned continuation drives another advance
    pass — but only *after* the current transaction commits and releases
    the row lock, so the recursive advance can re-acquire it.

    The workflow stays in ``running`` state until cleanup completes, at
    which point ``cleanup_containers`` computes the terminal workflow
    status from the per-step records in ``job_status``.
    """
    while True:
        continuation = _try_advance_workflow_once(job_id, job_type, status)
        if continuation is None:
            return
        job_type, status = continuation


def _try_advance_workflow_once(
    job_id: str, job_type: str, status: str,
) -> tuple[str, str] | None:
    """One atomic advancement pass. Returns a synthetic continuation event
    if the scheduled step completed synchronously (no container created)
    and another advance pass is needed.
    """
    with _get_db_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT current_step, params FROM job_workflow "
                    "WHERE job_id = %s AND status = 'running' FOR UPDATE",
                    (job_id,),
                )
                row = cur.fetchone()
                if row is None:
                    conn.rollback()
                    return None  # No active workflow for this job

                current_step, params = row
                expected_job_type = _STEP_JOB_TYPE.get(current_step)
                if job_type != expected_job_type:
                    conn.rollback()
                    return None

                succeeded = (status == "finishedSuccessfully")
                if succeeded:
                    next_step = _next_active_step(current_step, params)
                else:
                    next_step = _FAILURE_STEP.get(current_step, "cleanup")

            # Side effects BEFORE committing the advance. pfcon is
            # idempotent on jid so retries are safe. Cleanup scheduling
            # goes through Celery .delay() which is also retry-safe.
            continuation: tuple[str, str] | None = None
            if next_step == "cleanup":
                _publish_workflow_event(job_id, "cleanup", "started", "running")
                cleanup_containers.delay(job_id)
            else:
                _publish_workflow_event(job_id, next_step, "started", "running")
                continuation = _execute_workflow_step(job_id, next_step, params)

            # Commit the advance last, after the external side effects
            # have succeeded. CAS ensures concurrent advancers don't
            # double-advance.
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE job_workflow SET current_step = %s, updated_at = NOW() "
                    "WHERE job_id = %s AND current_step = %s",
                    (next_step, job_id, current_step),
                )
                advanced = cur.rowcount > 0
            conn.commit()

            if advanced:
                logger.info(
                    "Workflow advanced: job=%s %s → %s (succeeded=%s)",
                    job_id, current_step, next_step, succeeded,
                )
            else:
                logger.debug(
                    "Workflow step already advanced for job=%s (step=%s)",
                    job_id, current_step,
                )
            return continuation
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise


def _execute_workflow_step(
    job_id: str, step: str, params: dict,
) -> tuple[str, str] | None:
    """Call pfcon to schedule the job for a workflow step.

    Returns a ``(job_type, status)`` continuation if the step completed
    synchronously (no container created): either because pfcon raised
    (→ ``finishedWithError``) or because pfcon returned an immediate
    terminal status such as ``uploadSkipped`` / ``copySkipped``. The
    caller uses the continuation to drive another advance pass *after*
    committing the current advance — doing it inline would deadlock on
    the ``FOR UPDATE`` row lock still held by the caller.

    Returns ``None`` when the step was scheduled normally and the workflow
    is now waiting for a status event from the job container.
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
        # No container will be created, so set logs_flushed for this step.
        jt = _STEP_JOB_TYPE[step]
        _get_redis().set(f"job:{job_id}:{jt}:logs_flushed", "1", ex=3600)
        # Publish a failed step event so SSE clients see the error immediately.
        # The overall workflow_status stays 'running' until cleanup computes it.
        _publish_workflow_event(
            job_id, step, "finishedWithError", "running",
            extra={"error": str(e)},
        )
        return (jt, "finishedWithError")

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
        jt = _STEP_JOB_TYPE[step]
        _get_redis().set(f"job:{job_id}:{jt}:logs_flushed", "1", ex=3600)
        logger.info("Set logs_flushed for skipped step: job=%s type=%s", job_id, jt)
        return (jt, pfcon_status)
    return None


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
    Initialize a workflow: fetch pfcon server config, record which optional
    steps must run, store params in PostgreSQL, and schedule the first
    active step on pfcon.
    """
    # Fetch server config and fold the `requires_*_job` flags into params
    # so step advancement can honour them without re-hitting pfcon.
    try:
        server_info = _pfcon.get_server_info()
    except Exception as e:
        logger.error("Failed to fetch pfcon server info for job=%s: %s", job_id, e)
        # Fail-safe default: assume both optional steps are required.
        server_info = {"requires_copy_job": True, "requires_upload_job": True}

    params = dict(params)  # don't mutate the caller's dict
    params["requires_copy_job"] = bool(server_info.get("requires_copy_job", True))
    params["requires_upload_job"] = bool(server_info.get("requires_upload_job", True))

    initial_step = _first_active_step(params)

    # Idempotent insert: a second start_workflow call for the same job_id
    # (duplicate submission, task retry, etc.) must NOT overwrite an
    # already-running workflow. The previous implementation used
    # ``ON CONFLICT DO UPDATE SET current_step = EXCLUDED.current_step``,
    # which reset an advanced workflow back to its initial step.
    with _get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO job_workflow (job_id, current_step, status, params) "
                "VALUES (%s, %s, 'running', %s) "
                "ON CONFLICT (job_id) DO NOTHING",
                (job_id, initial_step, json.dumps(params)),
            )
            inserted = cur.rowcount > 0
        conn.commit()

    if not inserted:
        logger.info(
            "start_workflow: job=%s already exists — skipping re-start", job_id,
        )
        return {"job_id": job_id, "status": "already_started"}

    logger.info(
        "Workflow started: job=%s initial_step=%s requires_copy=%s requires_upload=%s",
        job_id, initial_step,
        params["requires_copy_job"], params["requires_upload_job"],
    )

    _publish_workflow_event(job_id, initial_step, "started", "running")

    continuation = _execute_workflow_step(job_id, initial_step, params)
    if isinstance(continuation, tuple) and len(continuation) == 2:
        # pfcon completed the initial step synchronously; drive the
        # workflow forward from here via the normal advance loop.
        jt, st = continuation
        _try_advance_workflow(job_id, jt, st)

    return {"job_id": job_id, "status": "started"}


# ── cleanup_containers ──────────────────────────────────────────────────────


def _compute_terminal_workflow_status(
    job_id: str, params: dict, per_step_status: dict[str, str]
) -> str:
    """Compute the terminal workflow_status from the recorded per-step outcomes.

    Rules:
      - Plugin must have run: its status drives finishedSuccessfully vs
        finishedWithError when every required non-plugin step succeeded.
      - Any required step missing or not finishedSuccessfully → ``failed``
        (except plugin, which may be finishedWithError and still be a
        non-failed outcome).
    """
    required_steps = ["plugin", "delete"]
    if _is_step_required("copy", params):
        required_steps.insert(0, "copy")
    if _is_step_required("upload", params):
        required_steps.insert(-1, "upload")

    plugin_status = per_step_status.get("plugin")

    for step in required_steps:
        status = per_step_status.get(step)
        if step == "plugin":
            if status not in ("finishedSuccessfully", "finishedWithError"):
                return "failed"
            continue
        if status != "finishedSuccessfully":
            return "failed"

    # All required non-plugin steps succeeded; plugin dictates success vs error.
    if plugin_status == "finishedSuccessfully":
        return "finishedSuccessfully"
    return "finishedWithError"


@celery_app.task(
    bind=True,
    name="chris_streaming.sse_service.tasks.cleanup_containers",
    max_retries=30,
    default_retry_delay=2,
    acks_late=True,
)
def cleanup_containers(self, job_id: str) -> dict:
    """
    Wait for all logs to be flushed (or for terminal-status quiescence),
    then remove containers from pfcon and publish the terminal workflow
    status.

    The log-flush wait is the fast path: for every job type that has a
    ``job_status`` row we check for a ``logs_flushed`` Redis key. If any
    are missing, we also accept a bounded quiescence condition: the step's
    terminal status has been recorded in ``job_status`` for at least
    ``EOS_QUIESCENCE_SECONDS``. EOS is therefore a hint, not the sole
    correctness signal — the log shipper and event forwarder are
    independent producers, so we must not block cleanup on it alone.
    """
    r = _get_redis()

    # Fetch per-step terminal statuses and their ages. We only care about
    # steps that actually created containers (have a job_status row).
    with _get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT job_type, status, "
                "       EXTRACT(EPOCH FROM (NOW() - updated_at)) "
                "FROM job_status WHERE job_id = %s",
                (job_id,),
            )
            rows = cur.fetchall()
            cur.execute(
                "SELECT params FROM job_workflow WHERE job_id = %s",
                (job_id,),
            )
            wf_row = cur.fetchone()

    per_step_status = {row[0]: row[1] for row in rows}
    per_step_age = {row[0]: float(row[2] or 0.0) for row in rows}
    params = wf_row[0] if wf_row else {}

    quiescence_seconds = float(getattr(settings, "eos_quiescence_seconds", 10.0))

    # Determine which job_types we're waiting on. Log-flush is preferred;
    # quiescence is the backstop.
    waiting_on = []
    for jt, status in per_step_status.items():
        if r.exists(f"job:{job_id}:{jt}:logs_flushed"):
            continue
        if status in _TERMINAL_STATUSES and per_step_age.get(jt, 0.0) >= quiescence_seconds:
            # Terminal status has been stable for long enough — treat as flushed.
            continue
        waiting_on.append(jt)

    if waiting_on and self.request.retries < self.max_retries:
        logger.info(
            "cleanup_containers waiting: job=%s waiting_on=%s "
            "(retry %d/%d, quiescence=%.1fs)",
            job_id, waiting_on, self.request.retries, self.max_retries,
            quiescence_seconds,
        )
        raise self.retry()

    if waiting_on:
        logger.warning(
            "Proceeding with cleanup despite unflushed logs: job=%s waiting_on=%s",
            job_id, waiting_on,
        )

    # Remove containers for all known job types (harmless if absent).
    all_job_types = ["copy", "plugin", "upload", "delete"]
    for jt in all_job_types:
        try:
            _pfcon.remove_container(job_id, jt)
        except Exception as e:
            logger.warning("Failed to remove %s container for job=%s: %s", jt, job_id, e)

    # Re-read per-step status after the delete calls: the delete step itself
    # may have just recorded its own terminal status through the event pipeline.
    with _get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT job_type, status FROM job_status WHERE job_id = %s",
                (job_id,),
            )
            per_step_status = {row[0]: row[1] for row in cur.fetchall()}

    workflow_status = _compute_terminal_workflow_status(
        job_id, params, per_step_status,
    )

    with _get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE job_workflow SET current_step = 'completed', "
                "status = %s, updated_at = NOW() "
                "WHERE job_id = %s",
                (workflow_status, job_id),
            )
        conn.commit()

    logger.info(
        "Workflow cleanup completed: job=%s workflow_status=%s per_step=%s",
        job_id, workflow_status, per_step_status,
    )

    _publish_workflow_event(
        job_id, "completed", "finishedSuccessfully", workflow_status,
    )

    # Clean up logs_flushed keys.
    for jt in all_job_types:
        r.delete(f"job:{job_id}:{jt}:logs_flushed")

    return {
        "job_id": job_id,
        "status": "cleaned_up",
        "workflow_status": workflow_status,
    }
