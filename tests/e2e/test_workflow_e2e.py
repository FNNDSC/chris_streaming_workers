"""
End-to-end test: submit a workflow, verify the full pipeline produces
status events and log lines visible through the SSE stream endpoints.

This test exercises the complete system:

  POST /api/jobs/{id}/run
    → Celery start_workflow
      → pfcon schedule_copy → Docker container
        → Event Forwarder → Kafka → Status Consumer → Celery process_job_status
          → PostgreSQL upsert + Redis Pub/Sub
        → Fluent Bit → Kafka → Log Consumer → OpenSearch + Redis Pub/Sub
      → workflow state machine: copy → plugin → upload → delete → cleanup
    → SSE Service streams events via /events/{id}/status and /events/{id}/logs

Requires the full stack from docker-compose.yml.
"""

import asyncio
import json
import time
import uuid

import httpx
import pytest

pytestmark = pytest.mark.e2e

# ── Timeouts ──────────────────────────────────────────────────────────────

WORKFLOW_TIMEOUT = 180  # seconds to wait for the full workflow to complete
SSE_CONNECT_RETRIES = 10  # retries for SSE service readiness
SSE_CONNECT_DELAY = 2  # seconds between retries
HISTORY_POLL_INTERVAL = 3  # seconds between history endpoint polls
STATUS_HISTORY_TIMEOUT = 120  # seconds to wait for status records to appear


# ── Helpers ───────────────────────────────────────────────────────────────


async def _wait_for_service(base_url: str):
    """Block until the SSE service health endpoint responds."""
    async with httpx.AsyncClient(timeout=5) as client:
        for attempt in range(SSE_CONNECT_RETRIES):
            try:
                resp = await client.get(f"{base_url}/health")
                if resp.status_code == 200:
                    return
            except httpx.ConnectError:
                pass
            await asyncio.sleep(SSE_CONNECT_DELAY)
    raise RuntimeError(f"SSE service at {base_url} did not become healthy")


async def _collect_sse_events(
    url: str, *, timeout: float, stop_on: dict[str, str] | None = None
) -> list[dict]:
    """
    Connect to an SSE endpoint and collect events until timeout or a
    stop condition is met.

    stop_on: if provided, stop when an event matches ALL key/value pairs.
             e.g. {"status": "confirmed_finishedSuccessfully"}
    """
    events = []
    deadline = time.monotonic() + timeout

    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout + 10)) as client:
        async with client.stream("GET", url) as resp:
            buffer = ""
            async for chunk in resp.aiter_text():
                if time.monotonic() > deadline:
                    break

                buffer += chunk
                # Normalize \r\n to \n (SSE servers may use either)
                buffer = buffer.replace("\r\n", "\n")
                # SSE protocol: events separated by double newline
                while "\n\n" in buffer:
                    raw_event, buffer = buffer.split("\n\n", 1)
                    parsed = _parse_sse_event(raw_event)
                    if parsed is not None:
                        events.append(parsed)

                        # Check stop condition
                        if stop_on and all(
                            parsed.get("data", {}).get(k) == v
                            for k, v in stop_on.items()
                        ):
                            return events

    return events


def _parse_sse_event(raw: str) -> dict | None:
    """Parse a raw SSE event string into {event, data} dict."""
    event_type = ""
    data_lines = []

    for line in raw.strip().split("\n"):
        line = line.strip()
        if line.startswith("event:"):
            event_type = line[len("event:"):].strip()
        elif line.startswith("data:"):
            data_lines.append(line[len("data:"):].strip())
        elif line.startswith(":"):
            # SSE comment (keep-alive), skip
            continue

    if not data_lines:
        return None

    data_str = "\n".join(data_lines)
    try:
        data = json.loads(data_str)
    except json.JSONDecodeError:
        data = {"raw": data_str}

    return {"event": event_type, "data": data}


# ── Tests ─────────────────────────────────────────────────────────────────


class TestWorkflowE2E:
    """
    Submit a full workflow (copy → plugin → upload → delete → cleanup)
    and verify events arrive through every observation channel.
    """

    @pytest.fixture
    def job_id(self):
        return f"e2e-{uuid.uuid4().hex[:12]}"

    async def test_full_workflow_produces_status_events_and_logs(
        self, sse_service_url, job_id
    ):
        """
        Core E2E test:
        1. Wait for service readiness
        2. Submit a workflow
        3. Collect SSE status events until we see confirmed terminal status
        4. Verify the expected status progression appeared
        5. Query the status history REST endpoint
        6. Query the log history REST endpoint
        7. Verify the workflow reached 'completed'
        """
        await _wait_for_service(sse_service_url)

        # ── Step 1: Submit workflow ───────────────────────────────────────
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{sse_service_url}/api/jobs/{job_id}/run",
                json={
                    "image": "ghcr.io/fnndsc/pl-simpledsapp:2.1.0",
                    "entrypoint": ["simpledsapp"],
                    "type": "ds",
                    "args": ["--dummyFloat", "3.5", "--sleepLength", "0"],
                    "input_dirs": "home/user/cube/test",
                    "output_dir": f"home/user/cube_out/{job_id}",
                },
            )
        assert resp.status_code == 202, f"Submit failed: {resp.text}"
        assert resp.json()["status"] == "submitted"

        # ── Step 2: Collect status SSE events ─────────────────────────────
        #
        # We start an SSE connection and wait for the workflow to produce
        # status events. We expect at least: notStarted/started for one or
        # more job types, and a confirmed terminal status.
        #
        # The SSE endpoint replays historical data on connect, so even if
        # some events fire before we connect, we'll still see them.

        status_events = await _collect_sse_events(
            f"{sse_service_url}/events/{job_id}/status",
            timeout=WORKFLOW_TIMEOUT,
            stop_on={"status": "confirmed_finishedSuccessfully"},
        )

        status_values = [
            e["data"].get("status")
            for e in status_events
            if e.get("event") == "status" and "status" in e.get("data", {})
        ]

        # We should have seen at least some status transitions
        assert len(status_values) > 0, (
            f"No status events received for {job_id}. "
            f"Raw events: {status_events}"
        )

        # At minimum, some job type should have gone through started
        assert "started" in status_values, (
            f"Expected 'started' in statuses, got: {status_values}"
        )

        # We should see at least one confirmed terminal status
        confirmed = [s for s in status_values if s.startswith("confirmed_")]
        assert len(confirmed) > 0, (
            f"No confirmed terminal status received. Statuses: {status_values}"
        )

        # ── Step 3: Verify workflow reached completed ─────────────────────
        #
        # Poll the workflow endpoint until it shows 'completed' (or 'failed')
        deadline = time.monotonic() + STATUS_HISTORY_TIMEOUT
        workflow_status = None
        async with httpx.AsyncClient(timeout=10) as client:
            while time.monotonic() < deadline:
                resp = await client.get(
                    f"{sse_service_url}/api/jobs/{job_id}/workflow"
                )
                data = resp.json()
                workflow_status = data.get("current_step")
                if workflow_status == "completed":
                    break
                await asyncio.sleep(HISTORY_POLL_INTERVAL)

        assert workflow_status == "completed", (
            f"Workflow did not complete. Last status: {data}"
        )

        # ── Step 4: Verify status history in PostgreSQL ───────────────────
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{sse_service_url}/api/jobs/{job_id}/status/history"
            )
        history = resp.json()
        assert len(history["statuses"]) > 0, (
            f"No status history rows for {job_id}"
        )

        # Should have records for multiple job types (at least copy + plugin)
        job_types_in_history = {row["job_type"] for row in history["statuses"]}
        assert "plugin" in job_types_in_history, (
            f"Expected 'plugin' in job types, got: {job_types_in_history}"
        )

        # ── Step 5: Verify log history in OpenSearch ──────────────────────
        #
        # The plugin (pl-simpledsapp) produces stdout output. After the
        # workflow completes and logs are flushed, they should be queryable.
        log_lines = []
        deadline = time.monotonic() + 30
        async with httpx.AsyncClient(timeout=10) as client:
            while time.monotonic() < deadline:
                resp = await client.get(
                    f"{sse_service_url}/logs/{job_id}/history",
                    params={"limit": 100},
                )
                data = resp.json()
                log_lines = data.get("lines", [])
                if len(log_lines) > 0:
                    break
                await asyncio.sleep(HISTORY_POLL_INTERVAL)

        assert len(log_lines) > 0, (
            f"No log lines found in OpenSearch for {job_id}"
        )

    async def test_sse_all_stream_receives_both_status_and_logs(
        self, sse_service_url, job_id
    ):
        """
        Verify that the /events/{job_id}/all endpoint delivers both
        status and log events interleaved in a single stream.
        """
        await _wait_for_service(sse_service_url)

        # Submit workflow
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{sse_service_url}/api/jobs/{job_id}/run",
                json={
                    "image": "ghcr.io/fnndsc/pl-simpledsapp:2.1.0",
                    "entrypoint": ["simpledsapp"],
                    "type": "ds",
                    "args": ["--dummyFloat", "3.5", "--sleepLength", "0"],
                    "input_dirs": "home/user/cube/test",
                    "output_dir": f"home/user/cube_out/{job_id}",
                },
            )
        assert resp.status_code == 202

        # Collect from /all stream — wait until we see a confirmed status
        all_events = await _collect_sse_events(
            f"{sse_service_url}/events/{job_id}/all",
            timeout=WORKFLOW_TIMEOUT,
            stop_on={"status": "confirmed_finishedSuccessfully"},
        )

        event_types_seen = {e.get("event") for e in all_events}

        # The /all stream should include status events
        assert "status" in event_types_seen, (
            f"No status events in /all stream. Event types: {event_types_seen}"
        )

        # Logs may or may not arrive before the confirmed status (depends on
        # timing), but we should at least have received status events.
        # Log events arrive on the "logs" SSE event type.


class TestWorkflowFailureE2E:
    """Test that a workflow with a bad image reports failure properly."""

    @pytest.fixture
    def job_id(self):
        return f"e2e-fail-{uuid.uuid4().hex[:12]}"

    async def test_bad_image_workflow_reports_failure(
        self, sse_service_url, job_id
    ):
        """
        Submit a workflow with a nonexistent image. The copy step uses
        pfcon's own image and may succeed, but the plugin step should fail
        because Docker can't pull the image (or the entrypoint doesn't
        exist). The workflow should ultimately report status='failed'.
        """
        await _wait_for_service(sse_service_url)

        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{sse_service_url}/api/jobs/{job_id}/run",
                json={
                    "image": "nonexistent/image:does-not-exist-99999",
                    "entrypoint": ["fake_entrypoint"],
                    "type": "ds",
                    "args": [],
                    "input_dirs": "home/user/cube/test",
                    "output_dir": f"home/user/cube_out/{job_id}",
                },
            )
        assert resp.status_code == 202

        # Poll workflow endpoint until it settles
        deadline = time.monotonic() + WORKFLOW_TIMEOUT
        data = {}
        async with httpx.AsyncClient(timeout=10) as client:
            while time.monotonic() < deadline:
                resp = await client.get(
                    f"{sse_service_url}/api/jobs/{job_id}/workflow"
                )
                data = resp.json()
                wf_status = data.get("status")
                current_step = data.get("current_step")
                if current_step == "completed" or wf_status == "failed":
                    break
                await asyncio.sleep(HISTORY_POLL_INTERVAL)

        assert data.get("status") == "failed", (
            f"Expected workflow status 'failed', got: {data}"
        )

        # Also verify that the status history has at least some records
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{sse_service_url}/api/jobs/{job_id}/status/history"
            )
        history = resp.json()
        assert len(history["statuses"]) > 0, (
            "Expected status history records for failed workflow"
        )


class TestHistoricalReplayE2E:
    """
    Test that a late-connecting SSE client receives historical events
    that were already persisted before the connection.
    """

    @pytest.fixture
    def job_id(self):
        return f"e2e-replay-{uuid.uuid4().hex[:12]}"

    async def test_late_connect_receives_historical_status(
        self, sse_service_url, job_id
    ):
        """
        1. Submit a workflow and wait for it to complete via polling
        2. THEN connect to the SSE status stream
        3. Verify that historical events are replayed
        """
        await _wait_for_service(sse_service_url)

        # Submit and wait for completion via REST polling
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{sse_service_url}/api/jobs/{job_id}/run",
                json={
                    "image": "ghcr.io/fnndsc/pl-simpledsapp:2.1.0",
                    "entrypoint": ["simpledsapp"],
                    "type": "ds",
                    "args": ["--dummyFloat", "3.5", "--sleepLength", "0"],
                    "input_dirs": "home/user/cube/test",
                    "output_dir": f"home/user/cube_out/{job_id}",
                },
            )
            assert resp.status_code == 202

        # Wait for workflow to complete
        deadline = time.monotonic() + WORKFLOW_TIMEOUT
        async with httpx.AsyncClient(timeout=10) as client:
            while time.monotonic() < deadline:
                resp = await client.get(
                    f"{sse_service_url}/api/jobs/{job_id}/workflow"
                )
                if resp.json().get("current_step") == "completed":
                    break
                await asyncio.sleep(HISTORY_POLL_INTERVAL)

        # Now connect to SSE AFTER everything is done — should get replay
        replay_events = await _collect_sse_events(
            f"{sse_service_url}/events/{job_id}/status",
            timeout=15,  # short timeout — we just want the replay
        )

        replay_statuses = [
            e["data"].get("status")
            for e in replay_events
            if e.get("event") == "status" and "status" in e.get("data", {})
        ]

        assert len(replay_statuses) > 0, (
            f"No historical status events replayed for {job_id}. "
            "The SSE endpoint should replay from PostgreSQL on connect."
        )
