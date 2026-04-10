"""Root conftest — shared fixtures for all tests."""

from datetime import datetime, timezone

import pytest

from chris_streaming.common.schemas import (
    JobStatus,
    JobType,
    LogEvent,
    StatusEvent,
)


@pytest.fixture
def sample_status_event() -> StatusEvent:
    return StatusEvent(
        job_id="test-job-1",
        job_type=JobType.plugin,
        status=JobStatus.started,
        image="ghcr.io/fnndsc/pl-test:latest",
        cmd="python app.py --arg1 val1",
        message="start",
        timestamp=datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        source="docker",
    )


@pytest.fixture
def terminal_status_event() -> StatusEvent:
    return StatusEvent(
        job_id="test-job-1",
        job_type=JobType.plugin,
        status=JobStatus.finishedSuccessfully,
        image="ghcr.io/fnndsc/pl-test:latest",
        cmd="python app.py",
        message="die",
        exit_code=0,
        timestamp=datetime(2026, 1, 15, 12, 5, 0, tzinfo=timezone.utc),
        source="docker",
    )


@pytest.fixture
def sample_log_event() -> LogEvent:
    return LogEvent(
        job_id="test-job-1",
        job_type=JobType.plugin,
        container_name="test-job-1",
        line="Processing file 1/10...",
        stream="stdout",
        timestamp=datetime(2026, 1, 15, 12, 1, 0, tzinfo=timezone.utc),
    )


@pytest.fixture
def eos_log_event() -> LogEvent:
    return LogEvent(
        job_id="test-job-1",
        job_type=JobType.plugin,
        eos=True,
        timestamp=datetime(2026, 1, 15, 12, 6, 0, tzinfo=timezone.utc),
    )
