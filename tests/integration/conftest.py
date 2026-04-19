"""Shared fixtures for integration tests.

All integration tests require Docker Compose infrastructure
(Redis, OpenSearch, PostgreSQL) running.
"""

import os

import pytest


def _require_env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


@pytest.fixture(scope="session")
def redis_url():
    return _require_env("REDIS_URL", "redis://localhost:6379/0")


@pytest.fixture(scope="session")
def opensearch_url():
    return _require_env("OPENSEARCH_URL", "http://localhost:9200")


@pytest.fixture(scope="session")
def db_dsn():
    return _require_env("DB_DSN", "postgresql://chris:chris1234@localhost:5433/chris_streaming_test")
