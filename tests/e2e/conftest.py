"""Fixtures for end-to-end tests.

E2E tests require the full stack running via:
  docker compose -f docker-compose.yml -f docker-compose.e2e.yml
"""

import os

import pytest


@pytest.fixture(scope="session")
def sse_service_url():
    return os.environ.get("SSE_SERVICE_URL", "http://localhost:8080")


@pytest.fixture(scope="session")
def redis_url():
    return os.environ.get("REDIS_URL", "redis://localhost:6379/0")


@pytest.fixture(scope="session")
def quickwit_url():
    return os.environ.get("QUICKWIT_URL", "http://localhost:7280")


@pytest.fixture(scope="session")
def quickwit_index():
    return os.environ.get("QUICKWIT_INDEX", "job-logs")


@pytest.fixture(scope="session")
def db_dsn():
    return os.environ.get(
        "DB_DSN", "postgresql://chris:chris1234@localhost:5433/chris_streaming"
    )
