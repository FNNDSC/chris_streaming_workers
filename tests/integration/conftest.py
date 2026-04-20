"""Shared fixtures for integration tests.

All integration tests require Docker Compose infrastructure
(Redis, Quickwit, PostgreSQL) running.
"""

import os

import pytest


def _require_env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


@pytest.fixture(scope="session")
def redis_url():
    return _require_env("REDIS_URL", "redis://localhost:6379/0")


@pytest.fixture(scope="session")
def quickwit_url():
    return _require_env("QUICKWIT_URL", "http://localhost:7280")


@pytest.fixture(scope="session")
def quickwit_index():
    return _require_env("QUICKWIT_INDEX", "job-logs")


@pytest.fixture(scope="session")
def db_dsn():
    return _require_env("DB_DSN", "postgresql://chris:chris1234@localhost:5433/chris_streaming_test")
