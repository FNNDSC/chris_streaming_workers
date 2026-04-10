"""Tests for chris_streaming.sse_service.tasks (unit tests with mocked dependencies)."""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest

from chris_streaming.common.schemas import JobStatus, JobType, StatusEvent


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
