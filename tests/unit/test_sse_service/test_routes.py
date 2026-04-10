"""Tests for chris_streaming.sse_service.routes."""

from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from chris_streaming.sse_service.app import create_app


@pytest.fixture
def app():
    return create_app()


class TestHealthEndpoint:
    async def test_health(self, app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


class TestRunWorkflow:
    async def test_run_workflow_returns_202(self, app):
        with patch("chris_streaming.sse_service.routes.celery_app") as mock_celery:
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.post(
                    "/api/jobs/test-job-1/run",
                    json={
                        "image": "ghcr.io/fnndsc/pl-test:latest",
                        "entrypoint": ["python", "app.py"],
                    },
                )
            assert resp.status_code == 202
            data = resp.json()
            assert data["job_id"] == "test-job-1"
            assert data["status"] == "submitted"
            mock_celery.send_task.assert_called_once()

    async def test_run_workflow_validation_error(self, app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.post(
                "/api/jobs/test-job-1/run",
                json={"invalid": "body"},
            )
        assert resp.status_code == 422


class TestGetWorkflowStatus:
    async def test_workflow_not_found(self, app):
        with patch(
            "chris_streaming.sse_service.routes._query_workflow",
            return_value=None,
        ):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.get("/api/jobs/test-job-1/workflow")
            assert resp.status_code == 200
            assert resp.json()["status"] == "not_found"

    async def test_workflow_found(self, app):
        row = {
            "job_id": "test-job-1",
            "current_step": "plugin",
            "status": "running",
            "created_at": "2026-01-15T12:00:00",
            "updated_at": "2026-01-15T12:05:00",
        }
        with patch(
            "chris_streaming.sse_service.routes._query_workflow",
            return_value=row,
        ):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.get("/api/jobs/test-job-1/workflow")
            assert resp.status_code == 200
            assert resp.json()["current_step"] == "plugin"


class TestGetStatusHistory:
    async def test_returns_status_rows(self, app):
        rows = [
            {"job_id": "j1", "job_type": "plugin", "status": "started",
             "updated_at": "2026-01-15T12:00:00"},
        ]
        with patch(
            "chris_streaming.sse_service.routes._query_status_history",
            return_value=rows,
        ):
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.get("/api/jobs/j1/status/history")
            assert resp.status_code == 200
            data = resp.json()
            assert data["job_id"] == "j1"
            assert len(data["statuses"]) == 1


class TestGetLogHistory:
    async def test_returns_logs(self, app):
        mock_os_resp = {
            "hits": {
                "total": {"value": 1},
                "hits": [
                    {"_source": {"job_id": "j1", "line": "hello", "timestamp": "2026-01-15T12:00:00"}}
                ],
            }
        }
        with patch("chris_streaming.sse_service.routes.AsyncOpenSearch") as MockOS:
            mock_client = AsyncMock()
            mock_client.search = AsyncMock(return_value=mock_os_resp)
            mock_client.close = AsyncMock()
            MockOS.return_value = mock_client

            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                resp = await client.get("/logs/j1/history")
            assert resp.status_code == 200
            data = resp.json()
            assert data["total"] == 1
            assert data["lines"][0]["line"] == "hello"
