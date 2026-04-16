"""Tests for chris_streaming.sse_service.tasks (unit tests with mocked dependencies)."""

import json
from unittest.mock import MagicMock, patch


# We need to mock the DB and Redis pools before importing the tasks module
# since it initializes them at module level via worker signals.


class TestProcessJobStatus:
    def _make_event_data(self, **overrides):
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

    @patch("chris_streaming.sse_service.tasks._try_advance_workflow")
    @patch("chris_streaming.sse_service.tasks._get_redis")
    @patch("chris_streaming.sse_service.tasks._get_db_conn")
    def test_upserts_and_publishes(self, mock_db_ctx, mock_get_redis, mock_advance):
        from chris_streaming.sse_service.tasks import process_job_status

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_db_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db_ctx.return_value.__exit__ = MagicMock(return_value=False)

        mock_redis = MagicMock()
        mock_get_redis.return_value = mock_redis

        event_data = self._make_event_data()
        result = process_job_status(event_data)

        assert result["status"] == "processed"
        assert result["job_id"] == "test-job-1"
        assert result["db_upserted"] is True
        mock_cur.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_redis.publish.assert_called_once()

    @patch("chris_streaming.sse_service.tasks._try_advance_workflow")
    @patch("chris_streaming.sse_service.tasks._get_redis")
    @patch("chris_streaming.sse_service.tasks._get_db_conn")
    def test_terminal_status_publishes_confirmed(self, mock_db_ctx, mock_get_redis, mock_advance):
        from chris_streaming.sse_service.tasks import process_job_status

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_db_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db_ctx.return_value.__exit__ = MagicMock(return_value=False)

        mock_redis = MagicMock()
        mock_get_redis.return_value = mock_redis

        event_data = self._make_event_data(status="finishedSuccessfully", exit_code=0)
        result = process_job_status(event_data)

        assert result["confirmed"] is True
        # Should have published twice: original + confirmed
        assert mock_redis.publish.call_count == 2
        # Check confirmed event
        confirmed_call = mock_redis.publish.call_args_list[1]
        confirmed_data = json.loads(confirmed_call[0][1])
        assert confirmed_data["status"] == "confirmed_finishedSuccessfully"

    @patch("chris_streaming.sse_service.tasks._try_advance_workflow")
    @patch("chris_streaming.sse_service.tasks._get_redis")
    @patch("chris_streaming.sse_service.tasks._get_db_conn")
    def test_non_terminal_does_not_confirm(self, mock_db_ctx, mock_get_redis, mock_advance):
        from chris_streaming.sse_service.tasks import process_job_status

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_db_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db_ctx.return_value.__exit__ = MagicMock(return_value=False)

        mock_redis = MagicMock()
        mock_get_redis.return_value = mock_redis

        event_data = self._make_event_data(status="started")
        result = process_job_status(event_data)

        assert result["confirmed"] is False
        mock_redis.publish.assert_called_once()
        mock_advance.assert_not_called()

    @patch("chris_streaming.sse_service.tasks._try_advance_workflow")
    @patch("chris_streaming.sse_service.tasks._get_redis")
    @patch("chris_streaming.sse_service.tasks._get_db_conn")
    def test_terminal_status_triggers_workflow_advance(
        self, mock_db_ctx, mock_get_redis, mock_advance
    ):
        from chris_streaming.sse_service.tasks import process_job_status

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_db_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db_ctx.return_value.__exit__ = MagicMock(return_value=False)

        mock_redis = MagicMock()
        mock_get_redis.return_value = mock_redis

        event_data = self._make_event_data(status="finishedSuccessfully")
        process_job_status(event_data)

        mock_advance.assert_called_once_with("test-job-1", "plugin", "finishedSuccessfully")


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

    def _mock_db(self, mock_db_ctx):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_db_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db_ctx.return_value.__exit__ = MagicMock(return_value=False)
        return mock_conn, mock_cur

    @patch("chris_streaming.sse_service.tasks._execute_workflow_step")
    @patch("chris_streaming.sse_service.tasks._pfcon")
    @patch("chris_streaming.sse_service.tasks._get_redis")
    @patch("chris_streaming.sse_service.tasks._get_db_conn")
    def test_skips_copy_when_not_required(
        self, mock_db_ctx, mock_get_redis, mock_pfcon, mock_execute,
    ):
        from chris_streaming.sse_service.tasks import start_workflow

        _, mock_cur = self._mock_db(mock_db_ctx)
        mock_get_redis.return_value = MagicMock()
        mock_pfcon.get_server_info.return_value = {
            "requires_copy_job": False,
            "requires_upload_job": True,
        }

        start_workflow("job-x", {"image": "img:1"})

        # The initial step must be 'plugin', not 'copy'.
        insert_call = mock_cur.execute.call_args_list[0]
        insert_args = insert_call[0][1]
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

        _, mock_cur = self._mock_db(mock_db_ctx)
        mock_get_redis.return_value = MagicMock()
        mock_pfcon.get_server_info.return_value = {
            "requires_copy_job": True,
            "requires_upload_job": True,
        }

        start_workflow("job-x", {"image": "img:1"})

        insert_call = mock_cur.execute.call_args_list[0]
        insert_args = insert_call[0][1]
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

        _, mock_cur = self._mock_db(mock_db_ctx)
        mock_get_redis.return_value = MagicMock()
        mock_pfcon.get_server_info.return_value = {
            "requires_copy_job": True,
            "requires_upload_job": False,
        }

        start_workflow("job-x", {"image": "img:1"})

        insert_args = mock_cur.execute.call_args_list[0][0][1]
        params_json = insert_args[2]
        params = json.loads(params_json)
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

        _, mock_cur = self._mock_db(mock_db_ctx)
        mock_get_redis.return_value = MagicMock()
        mock_pfcon.get_server_info.side_effect = RuntimeError("pfcon down")

        start_workflow("job-x", {"image": "img:1"})

        insert_args = mock_cur.execute.call_args_list[0][0][1]
        assert insert_args[1] == "copy"
        params = json.loads(insert_args[2])
        assert params["requires_copy_job"] is True
        assert params["requires_upload_job"] is True
