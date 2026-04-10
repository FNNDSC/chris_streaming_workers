"""Integration test: SSE service health endpoint through the full FastAPI app."""

import pytest
from httpx import ASGITransport, AsyncClient

from chris_streaming.sse_service.app import create_app

pytestmark = pytest.mark.integration


class TestSSEServiceHealth:
    async def test_health_endpoint(self):
        app = create_app()
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
