"""Tests for chris_streaming.sse_service.app."""

from chris_streaming.sse_service.app import create_app


class TestCreateApp:
    def test_creates_fastapi_instance(self):
        app = create_app()
        assert app.title == "ChRIS Streaming SSE Service"

    def test_cors_middleware_added(self):
        app = create_app()
        middleware_classes = [m.cls.__name__ for m in app.user_middleware]
        assert "CORSMiddleware" in middleware_classes

    def test_routes_registered(self):
        app = create_app()
        route_paths = [r.path for r in app.routes]
        assert "/health" in route_paths
        assert "/events/{job_id}/status" in route_paths
        assert "/events/{job_id}/logs" in route_paths
        assert "/events/{job_id}/all" in route_paths
        assert "/api/jobs/{job_id}/run" in route_paths
