"""Tests for chris_streaming.sse_service.tasks (unit tests with mocked dependencies)."""

import json
from unittest.mock import MagicMock, patch


def _make_event_data(**overrides):
    defaults = {
        "job_id": "test-job-1",
        "job_type": "plugin",
        "status": "started",
        "image": "img:latest",
        "cmd": "python app.py",
        "message": "start",
        "exit_code": None,
        "source": "docker",
        "event_id": "abc123",
        "timestamp": "2026-01-15T12:00:00+00:00",
    }
    defaults.update(overrides)
    return defaults


def _mock_db(mock_db_ctx):
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    # MagicMock under Python 3.12+ no longer coerces arithmetic comparisons
    # to a truthy MagicMock, so an unset rowcount would blow up on
    # ``rowcount > 0``. Set a concrete default (1 = inserted/updated) so
    # the typical "happy path" test body runs without extra setup.
    mock_cur.rowcount = 1
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_db_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_db_ctx.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cur


class TestProcessJobStatus:
    @patch("chris_streaming.sse_service.tasks._try_advance_workflow")
    @patch("chris_streaming.sse_service.tasks._get_redis")
    @patch("chris_streaming.sse_service.tasks._get_db_conn")
    def test_non_terminal_upserts_only(self, mock_db_ctx, mock_get_redis, mock_advance):
        """For a non-terminal event, we upsert but never XADD confirmed_*."""
        from chris_streaming.sse_service.tasks import process_job_status

        mock_conn, mock_cur = _mock_db(mock_db_ctx)
        mock_redis = MagicMock()
        mock_get_redis.return_value = mock_redis

        result = process_job_status(_make_event_data(status="started"))

        assert result["status"] == "processed"
        assert result["job_id"] == "test-job-1"
        assert result["db_upserted"] is True
        assert result["confirmed"] is False
        mock_cur.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_redis.xadd.assert_not_called()
        mock_redis.publish.assert_not_called()
        mock_advance.assert_not_called()

    @patch("chris_streaming.sse_service.tasks._try_advance_workflow")
    @patch("chris_streaming.sse_service.tasks._get_redis")
    @patch("chris_streaming.sse_service.tasks._get_db_conn")
    def test_terminal_status_xadds_confirmed(self, mock_db_ctx, mock_get_redis, mock_advance):
        """Terminal events XADD confirmed_* onto the sharded status stream."""
        from chris_streaming.sse_service.tasks import process_job_status

        _mock_db(mock_db_ctx)
        mock_redis = MagicMock()
        mock_get_redis.return_value = mock_redis

        result = process_job_status(
            _make_event_data(status="finishedSuccessfully", exit_code=0)
        )

        assert result["confirmed"] is True
        mock_redis.xadd.assert_called_once()
        call = mock_redis.xadd.call_args
        stream_key = call[0][0]
        assert stream_key.startswith("stream:job-status:")
        fields = call[0][1]
        body = json.loads(fields["data"].decode("utf-8"))
        assert body["status"] == "confirmed_finishedSuccessfully"
        assert body["previous_status"] == "finishedSuccessfully"
        assert body["job_id"] == "test-job-1"

    @patch("chris_streaming.sse_service.tasks._try_advance_workflow")
    @patch("chris_streaming.sse_service.tasks._get_redis")
    @patch("chris_streaming.sse_service.tasks._get_db_conn")
    def test_terminal_status_triggers_workflow_advance(
        self, mock_db_ctx, mock_get_redis, mock_advance
    ):
        from chris_streaming.sse_service.tasks import process_job_status

        _mock_db(mock_db_ctx)
        mock_get_redis.return_value = MagicMock()

        process_job_status(_make_event_data(status="finishedSuccessfully"))

        mock_advance.assert_called_once_with(
            "test-job-1", "plugin", "finishedSuccessfully",
        )


class TestPublishWorkflowEvent:
    @patch("chris_streaming.sse_service.tasks._get_redis")
    @patch("chris_streaming.sse_service.tasks._get_db_conn")
    def test_records_history_and_xadds_workflow_stream(
        self, mock_db_ctx, mock_get_redis
    ):
        from chris_streaming.sse_service.tasks import _publish_workflow_event

        mock_conn, mock_cur = _mock_db(mock_db_ctx)
        mock_redis = MagicMock()
        mock_get_redis.return_value = mock_redis

        _publish_workflow_event("job-x", "plugin", "started", "running")

        # Dedup insert into job_workflow_events with ON CONFLICT DO NOTHING.
        insert_sql = mock_cur.execute.call_args[0][0]
        assert "job_workflow_events" in insert_sql
        assert "ON CONFLICT (event_id) DO NOTHING" in insert_sql
        mock_conn.commit.assert_called_once()

        # XADD to the sharded workflow stream, carrying the WorkflowEvent JSON.
        mock_redis.xadd.assert_called_once()
        stream_key = mock_redis.xadd.call_args[0][0]
        assert stream_key.startswith("stream:job-workflow:")
        fields = mock_redis.xadd.call_args[0][1]
        body = json.loads(fields["data"].decode("utf-8"))
        assert body["job_id"] == "job-x"
        assert body["current_step"] == "plugin"
        assert body["current_step_status"] == "started"
        assert body["workflow_status"] == "running"
        assert body["event_id"]

        # No legacy Pub/Sub publish.
        mock_redis.publish.assert_not_called()

    @patch("chris_streaming.sse_service.tasks._get_redis")
    @patch("chris_streaming.sse_service.tasks._get_db_conn")
    def test_error_field_included_in_payload_and_history(
        self, mock_db_ctx, mock_get_redis
    ):
        from chris_streaming.sse_service.tasks import _publish_workflow_event

        _, mock_cur = _mock_db(mock_db_ctx)
        mock_redis = MagicMock()
        mock_get_redis.return_value = mock_redis

        _publish_workflow_event(
            "job-x", "plugin", "finishedWithError", "running",
            extra={"error": "oh no"},
        )

        insert_params = mock_cur.execute.call_args[0][1]
        # (job_id, current_step, current_step_status, workflow_status, error, event_id)
        assert insert_params[4] == "oh no"
        body = json.loads(mock_redis.xadd.call_args[0][1]["data"].decode("utf-8"))
        assert body["error"] == "oh no"

    @patch("chris_streaming.sse_service.tasks._get_redis")
    @patch("chris_streaming.sse_service.tasks._get_db_conn")
    def test_event_id_is_deterministic(self, mock_db_ctx, mock_get_redis):
        """Two calls with identical args produce the same event_id (retry-safe)."""
        from chris_streaming.sse_service.tasks import _publish_workflow_event

        _mock_db(mock_db_ctx)
        mock_redis = MagicMock()
        mock_get_redis.return_value = mock_redis

        _publish_workflow_event("job-x", "plugin", "started", "running")
        _publish_workflow_event("job-x", "plugin", "started", "running")

        first = json.loads(mock_redis.xadd.call_args_list[0][0][1]["data"].decode())
        second = json.loads(mock_redis.xadd.call_args_list[1][0][1]["data"].decode())
        assert first["event_id"] == second["event_id"]


class TestConfirmedMap:
    def test_all_terminal_statuses_have_confirmed(self):
        from chris_streaming.sse_service.tasks import _CONFIRMED_MAP
        assert "finishedSuccessfully" in _CONFIRMED_MAP
        assert "finishedWithError" in _CONFIRMED_MAP
        assert "undefined" in _CONFIRMED_MAP
        assert "started" not in _CONFIRMED_MAP


class TestWorkflowStepMappings:
    def test_next_step_progression(self):
        from chris_streaming.sse_service.tasks import _NEXT_STEP
        assert _NEXT_STEP["copy"] == "plugin"
        assert _NEXT_STEP["plugin"] == "upload"
        assert _NEXT_STEP["upload"] == "delete"
        assert _NEXT_STEP["delete"] == "cleanup"

    def test_failure_step_skips_to_delete(self):
        from chris_streaming.sse_service.tasks import _FAILURE_STEP
        assert _FAILURE_STEP["copy"] == "delete"
        assert _FAILURE_STEP["plugin"] == "delete"
        assert _FAILURE_STEP["upload"] == "delete"
        assert "delete" not in _FAILURE_STEP


class TestStepSkipHelpers:
    """Tests for _is_step_required / _first_active_step / _next_active_step."""

    def test_is_step_required_defaults_true_when_flag_missing(self):
        from chris_streaming.sse_service.tasks import _is_step_required
        assert _is_step_required("copy", {}) is True
        assert _is_step_required("upload", {}) is True

    def test_is_step_required_honours_flag(self):
        from chris_streaming.sse_service.tasks import _is_step_required
        assert _is_step_required("copy", {"requires_copy_job": False}) is False
        assert _is_step_required("upload", {"requires_upload_job": False}) is False
        assert _is_step_required("copy", {"requires_copy_job": True}) is True

    def test_mandatory_steps_always_required(self):
        from chris_streaming.sse_service.tasks import _is_step_required
        # plugin and delete can never be skipped regardless of params.
        assert _is_step_required("plugin", {"requires_copy_job": False}) is True
        assert _is_step_required("delete", {"requires_upload_job": False}) is True

    def test_first_active_step_all_required(self):
        from chris_streaming.sse_service.tasks import _first_active_step
        assert _first_active_step({
            "requires_copy_job": True,
            "requires_upload_job": True,
        }) == "copy"

    def test_first_active_step_skips_copy(self):
        from chris_streaming.sse_service.tasks import _first_active_step
        assert _first_active_step({
            "requires_copy_job": False,
            "requires_upload_job": True,
        }) == "plugin"

    def test_next_active_step_skips_upload(self):
        from chris_streaming.sse_service.tasks import _next_active_step
        # After plugin with upload disabled, next is delete.
        assert _next_active_step("plugin", {
            "requires_copy_job": True,
            "requires_upload_job": False,
        }) == "delete"

    def test_next_active_step_preserves_upload_when_required(self):
        from chris_streaming.sse_service.tasks import _next_active_step
        assert _next_active_step("plugin", {
            "requires_copy_job": True,
            "requires_upload_job": True,
        }) == "upload"

    def test_next_active_step_delete_always_goes_to_cleanup(self):
        from chris_streaming.sse_service.tasks import _next_active_step
        assert _next_active_step("delete", {
            "requires_copy_job": False,
            "requires_upload_job": False,
        }) == "cleanup"


class TestTerminalWorkflowStatus:
    """Tests for _compute_terminal_workflow_status."""

    def _params(self, requires_copy=True, requires_upload=True):
        return {
            "requires_copy_job": requires_copy,
            "requires_upload_job": requires_upload,
        }

    def test_happy_path_finished_successfully(self):
        from chris_streaming.sse_service.tasks import (
            _compute_terminal_workflow_status,
        )
        per_step = {
            "copy": "finishedSuccessfully",
            "plugin": "finishedSuccessfully",
            "upload": "finishedSuccessfully",
            "delete": "finishedSuccessfully",
        }
        assert _compute_terminal_workflow_status(
            "j1", self._params(), per_step,
        ) == "finishedSuccessfully"

    def test_plugin_error_but_clean_pipeline_is_finished_with_error(self):
        from chris_streaming.sse_service.tasks import (
            _compute_terminal_workflow_status,
        )
        per_step = {
            "copy": "finishedSuccessfully",
            "plugin": "finishedWithError",
            "upload": "finishedSuccessfully",
            "delete": "finishedSuccessfully",
        }
        assert _compute_terminal_workflow_status(
            "j1", self._params(), per_step,
        ) == "finishedWithError"

    def test_copy_failure_is_failed(self):
        from chris_streaming.sse_service.tasks import (
            _compute_terminal_workflow_status,
        )
        per_step = {
            "copy": "finishedWithError",
            "plugin": "finishedSuccessfully",
            "upload": "finishedSuccessfully",
            "delete": "finishedSuccessfully",
        }
        assert _compute_terminal_workflow_status(
            "j1", self._params(), per_step,
        ) == "failed"

    def test_upload_failure_is_failed(self):
        from chris_streaming.sse_service.tasks import (
            _compute_terminal_workflow_status,
        )
        per_step = {
            "copy": "finishedSuccessfully",
            "plugin": "finishedSuccessfully",
            "upload": "finishedWithError",
            "delete": "finishedSuccessfully",
        }
        assert _compute_terminal_workflow_status(
            "j1", self._params(), per_step,
        ) == "failed"

    def test_delete_failure_is_failed(self):
        from chris_streaming.sse_service.tasks import (
            _compute_terminal_workflow_status,
        )
        per_step = {
            "copy": "finishedSuccessfully",
            "plugin": "finishedSuccessfully",
            "upload": "finishedSuccessfully",
            "delete": "finishedWithError",
        }
        assert _compute_terminal_workflow_status(
            "j1", self._params(), per_step,
        ) == "failed"

    def test_plugin_undefined_is_failed(self):
        from chris_streaming.sse_service.tasks import (
            _compute_terminal_workflow_status,
        )
        per_step = {
            "copy": "finishedSuccessfully",
            "plugin": "undefined",
            "upload": "finishedSuccessfully",
            "delete": "finishedSuccessfully",
        }
        assert _compute_terminal_workflow_status(
            "j1", self._params(), per_step,
        ) == "failed"

    def test_copy_undefined_is_failed(self):
        """undefined on a required non-plugin step also fails the workflow."""
        from chris_streaming.sse_service.tasks import (
            _compute_terminal_workflow_status,
        )
        per_step = {
            "copy": "undefined",
            "plugin": "finishedSuccessfully",
            "upload": "finishedSuccessfully",
            "delete": "finishedSuccessfully",
        }
        assert _compute_terminal_workflow_status(
            "j1", self._params(), per_step,
        ) == "failed"

    def test_undefined_is_terminal_status(self):
        """Sanity check: orchestrator recognizes undefined as terminal."""
        from chris_streaming.sse_service.tasks import _TERMINAL_STATUSES
        assert "undefined" in _TERMINAL_STATUSES

    def test_missing_plugin_row_is_failed(self):
        from chris_streaming.sse_service.tasks import (
            _compute_terminal_workflow_status,
        )
        per_step = {
            "copy": "finishedSuccessfully",
            "delete": "finishedSuccessfully",
        }
        assert _compute_terminal_workflow_status(
            "j1", self._params(), per_step,
        ) == "failed"

    def test_skipped_upload_does_not_count(self):
        """When requires_upload_job=false, upload must not be required for success."""
        from chris_streaming.sse_service.tasks import (
            _compute_terminal_workflow_status,
        )
        per_step = {
            "copy": "finishedSuccessfully",
            "plugin": "finishedSuccessfully",
            "delete": "finishedSuccessfully",
        }
        assert _compute_terminal_workflow_status(
            "j1", self._params(requires_upload=False), per_step,
        ) == "finishedSuccessfully"

    def test_skipped_copy_does_not_count(self):
        from chris_streaming.sse_service.tasks import (
            _compute_terminal_workflow_status,
        )
        per_step = {
            "plugin": "finishedSuccessfully",
            "upload": "finishedSuccessfully",
            "delete": "finishedSuccessfully",
        }
        assert _compute_terminal_workflow_status(
            "j1", self._params(requires_copy=False), per_step,
        ) == "finishedSuccessfully"

    def test_plugin_error_with_skipped_upload_is_finished_with_error(self):
        from chris_streaming.sse_service.tasks import (
            _compute_terminal_workflow_status,
        )
        per_step = {
            "copy": "finishedSuccessfully",
            "plugin": "finishedWithError",
            "delete": "finishedSuccessfully",
        }
        assert _compute_terminal_workflow_status(
            "j1", self._params(requires_upload=False), per_step,
        ) == "finishedWithError"


class TestStartWorkflowSkipLogic:
    """Tests that start_workflow honours pfcon's requires_*_job flags."""

    @patch("chris_streaming.sse_service.tasks._execute_workflow_step")
    @patch("chris_streaming.sse_service.tasks._pfcon")
    @patch("chris_streaming.sse_service.tasks._get_redis")
    @patch("chris_streaming.sse_service.tasks._get_db_conn")
    def test_skips_copy_when_not_required(
        self, mock_db_ctx, mock_get_redis, mock_pfcon, mock_execute,
    ):
        from chris_streaming.sse_service.tasks import start_workflow

        _, mock_cur = _mock_db(mock_db_ctx)
        mock_get_redis.return_value = MagicMock()
        mock_pfcon.get_server_info.return_value = {
            "requires_copy_job": False,
            "requires_upload_job": True,
        }

        start_workflow("job-x", {"image": "img:1"})

        # The initial insert into job_workflow must land with current_step='plugin'.
        workflow_inserts = [
            call for call in mock_cur.execute.call_args_list
            if "job_workflow " in call[0][0] or "job_workflow(" in call[0][0]
            or call[0][0].strip().startswith("INSERT INTO job_workflow ")
        ]
        assert workflow_inserts, (
            "expected an INSERT into job_workflow (got: "
            f"{[c[0][0] for c in mock_cur.execute.call_args_list]})"
        )
        insert_args = workflow_inserts[0][0][1]
        assert insert_args[1] == "plugin"
        mock_execute.assert_called_once()
        assert mock_execute.call_args[0][1] == "plugin"

    @patch("chris_streaming.sse_service.tasks._execute_workflow_step")
    @patch("chris_streaming.sse_service.tasks._pfcon")
    @patch("chris_streaming.sse_service.tasks._get_redis")
    @patch("chris_streaming.sse_service.tasks._get_db_conn")
    def test_uses_copy_when_required(
        self, mock_db_ctx, mock_get_redis, mock_pfcon, mock_execute,
    ):
        from chris_streaming.sse_service.tasks import start_workflow

        _, mock_cur = _mock_db(mock_db_ctx)
        mock_get_redis.return_value = MagicMock()
        mock_pfcon.get_server_info.return_value = {
            "requires_copy_job": True,
            "requires_upload_job": True,
        }

        start_workflow("job-x", {"image": "img:1"})

        workflow_inserts = [
            c for c in mock_cur.execute.call_args_list
            if c[0][0].strip().startswith("INSERT INTO job_workflow ")
        ]
        insert_args = workflow_inserts[0][0][1]
        assert insert_args[1] == "copy"
        assert mock_execute.call_args[0][1] == "copy"

    @patch("chris_streaming.sse_service.tasks._execute_workflow_step")
    @patch("chris_streaming.sse_service.tasks._pfcon")
    @patch("chris_streaming.sse_service.tasks._get_redis")
    @patch("chris_streaming.sse_service.tasks._get_db_conn")
    def test_persists_flags_in_params(
        self, mock_db_ctx, mock_get_redis, mock_pfcon, mock_execute,
    ):
        from chris_streaming.sse_service.tasks import start_workflow

        _, mock_cur = _mock_db(mock_db_ctx)
        mock_get_redis.return_value = MagicMock()
        mock_pfcon.get_server_info.return_value = {
            "requires_copy_job": True,
            "requires_upload_job": False,
        }

        start_workflow("job-x", {"image": "img:1"})

        workflow_inserts = [
            c for c in mock_cur.execute.call_args_list
            if c[0][0].strip().startswith("INSERT INTO job_workflow ")
        ]
        insert_args = workflow_inserts[0][0][1]
        params = json.loads(insert_args[2])
        assert params["requires_copy_job"] is True
        assert params["requires_upload_job"] is False
        assert params["image"] == "img:1"

    @patch("chris_streaming.sse_service.tasks._execute_workflow_step")
    @patch("chris_streaming.sse_service.tasks._pfcon")
    @patch("chris_streaming.sse_service.tasks._get_redis")
    @patch("chris_streaming.sse_service.tasks._get_db_conn")
    def test_pfcon_unreachable_fails_safe(
        self, mock_db_ctx, mock_get_redis, mock_pfcon, mock_execute,
    ):
        """When pfcon's config fetch fails, default to requiring both optional steps."""
        from chris_streaming.sse_service.tasks import start_workflow

        _, mock_cur = _mock_db(mock_db_ctx)
        mock_get_redis.return_value = MagicMock()
        mock_pfcon.get_server_info.side_effect = RuntimeError("pfcon down")

        start_workflow("job-x", {"image": "img:1"})

        workflow_inserts = [
            c for c in mock_cur.execute.call_args_list
            if c[0][0].strip().startswith("INSERT INTO job_workflow ")
        ]
        insert_args = workflow_inserts[0][0][1]
        assert insert_args[1] == "copy"
        params = json.loads(insert_args[2])
        assert params["requires_copy_job"] is True
        assert params["requires_upload_job"] is True

    @patch("chris_streaming.sse_service.tasks._execute_workflow_step")
    @patch("chris_streaming.sse_service.tasks._pfcon")
    @patch("chris_streaming.sse_service.tasks._get_redis")
    @patch("chris_streaming.sse_service.tasks._get_db_conn")
    def test_duplicate_start_does_not_reset_running_workflow(
        self, mock_db_ctx, mock_get_redis, mock_pfcon, mock_execute,
    ):
        """A second start_workflow for the same job_id must be a no-op.

        The old implementation used ``ON CONFLICT DO UPDATE SET
        current_step = EXCLUDED.current_step`` which reset an already-
        advanced workflow back to its initial step. The fix switches to
        ``ON CONFLICT DO NOTHING`` + an inspection of ``cur.rowcount``.
        """
        from chris_streaming.sse_service.tasks import start_workflow

        _, mock_cur = _mock_db(mock_db_ctx)
        # Simulate ON CONFLICT DO NOTHING: no row inserted.
        mock_cur.rowcount = 0
        mock_get_redis.return_value = MagicMock()
        mock_pfcon.get_server_info.return_value = {
            "requires_copy_job": True,
            "requires_upload_job": True,
        }

        result = start_workflow("job-x", {"image": "img:1"})

        assert result == {"job_id": "job-x", "status": "already_started"}
        # No pfcon schedule call when the workflow already exists.
        mock_execute.assert_not_called()

    @patch("chris_streaming.sse_service.tasks._execute_workflow_step")
    @patch("chris_streaming.sse_service.tasks._pfcon")
    @patch("chris_streaming.sse_service.tasks._get_redis")
    @patch("chris_streaming.sse_service.tasks._get_db_conn")
    def test_insert_uses_on_conflict_do_nothing(
        self, mock_db_ctx, mock_get_redis, mock_pfcon, mock_execute,
    ):
        from chris_streaming.sse_service.tasks import start_workflow

        _, mock_cur = _mock_db(mock_db_ctx)
        mock_cur.rowcount = 1
        mock_execute.return_value = None  # no synchronous continuation
        mock_get_redis.return_value = MagicMock()
        mock_pfcon.get_server_info.return_value = {
            "requires_copy_job": True,
            "requires_upload_job": True,
        }

        start_workflow("job-x", {"image": "img:1"})

        insert_sqls = [
            c[0][0] for c in mock_cur.execute.call_args_list
            if "INSERT INTO job_workflow " in c[0][0]
        ]
        assert insert_sqls, "expected an INSERT into job_workflow"
        assert "ON CONFLICT (job_id) DO NOTHING" in insert_sqls[0]


class TestConfirmedEventDeterministic:
    """Bug B: confirmed_* event_id must be stable across Celery retries."""

    @patch("chris_streaming.sse_service.tasks._try_advance_workflow")
    @patch("chris_streaming.sse_service.tasks._get_redis")
    @patch("chris_streaming.sse_service.tasks._get_db_conn")
    def test_confirmed_event_id_stable_across_calls(
        self, mock_db_ctx, mock_get_redis, mock_advance,
    ):
        from chris_streaming.sse_service.tasks import process_job_status

        _mock_db(mock_db_ctx)
        mock_redis = MagicMock()
        mock_get_redis.return_value = mock_redis

        event_data = _make_event_data(
            status="finishedSuccessfully", exit_code=0,
            timestamp="2026-01-15T12:00:00+00:00",
        )

        # Two back-to-back invocations simulate a Celery retry: the
        # confirmed_* XADD must carry the same event_id both times.
        process_job_status(event_data)
        process_job_status(event_data)

        first = json.loads(
            mock_redis.xadd.call_args_list[0][0][1]["data"].decode("utf-8"),
        )
        second = json.loads(
            mock_redis.xadd.call_args_list[1][0][1]["data"].decode("utf-8"),
        )
        assert first["event_id"] == second["event_id"]
        assert first["timestamp"] == "2026-01-15T12:00:00+00:00"


class TestAdvanceWorkflowOrdering:
    """Bug D: pfcon must be called BEFORE committing the current_step UPDATE."""

    @patch("chris_streaming.sse_service.tasks._pfcon")
    @patch("chris_streaming.sse_service.tasks._get_redis")
    @patch("chris_streaming.sse_service.tasks._get_db_conn")
    def test_pfcon_scheduled_before_update_commit(
        self, mock_db_ctx, mock_get_redis, mock_pfcon,
    ):
        """The UPDATE of current_step must run after pfcon returns.

        Regression guard: a worker crash between commit and pfcon would
        leave the workflow stuck in the new step with no pfcon call ever
        made. The fix reorders the protocol so pfcon is invoked first.
        """
        from chris_streaming.sse_service.tasks import _try_advance_workflow

        mock_conn, mock_cur = _mock_db(mock_db_ctx)
        mock_get_redis.return_value = MagicMock()
        # SELECT FOR UPDATE returns: current_step=copy, params={}.
        mock_cur.fetchone.return_value = (
            "copy",
            {"requires_copy_job": True, "requires_upload_job": True,
             "image": "img:1", "entrypoint": "/run"},
        )
        mock_cur.rowcount = 1

        call_log: list[str] = []

        def _record_pfcon_copy(*a, **kw):
            call_log.append("pfcon.schedule_plugin")
            return {"compute": {"status": "started"}}

        # copy→plugin on a finishedSuccessfully event.
        mock_pfcon.schedule_plugin.side_effect = _record_pfcon_copy

        def _record_execute(sql, *a, **kw):
            s = sql.strip()
            if s.startswith("UPDATE job_workflow"):
                call_log.append("UPDATE")
            return None

        mock_cur.execute.side_effect = _record_execute

        _try_advance_workflow("job-x", "copy", "finishedSuccessfully")

        # Ordering: pfcon must appear in the call log BEFORE the UPDATE
        # that advances current_step.
        assert "pfcon.schedule_plugin" in call_log
        assert "UPDATE" in call_log
        assert call_log.index("pfcon.schedule_plugin") < call_log.index("UPDATE"), (
            f"expected pfcon before UPDATE, got: {call_log}"
        )
