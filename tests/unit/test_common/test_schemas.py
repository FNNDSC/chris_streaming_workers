"""Tests for chris_streaming.common.schemas."""

import json
from datetime import datetime, timezone

from chris_streaming.common.schemas import (
    TERMINAL_STATUSES,
    JobStatus,
    JobType,
    LogEvent,
    StatusEvent,
    kafka_key_for_job,
)


# ── Enums ─────────────────────────────────────────────────────────────────

class TestJobStatus:
    def test_values(self):
        assert JobStatus.notStarted == "notStarted"
        assert JobStatus.started == "started"
        assert JobStatus.finishedSuccessfully == "finishedSuccessfully"
        assert JobStatus.finishedWithError == "finishedWithError"
        assert JobStatus.undefined == "undefined"

    def test_confirmed_variants(self):
        assert JobStatus.confirmed_finishedSuccessfully == "confirmed_finishedSuccessfully"
        assert JobStatus.confirmed_finishedWithError == "confirmed_finishedWithError"
        assert JobStatus.confirmed_undefined == "confirmed_undefined"

    def test_terminal_statuses(self):
        assert TERMINAL_STATUSES == frozenset({
            JobStatus.finishedSuccessfully,
            JobStatus.finishedWithError,
            JobStatus.undefined,
        })
        assert JobStatus.started not in TERMINAL_STATUSES
        assert JobStatus.confirmed_finishedSuccessfully not in TERMINAL_STATUSES


class TestJobType:
    def test_values(self):
        assert JobType.plugin == "plugin"
        assert JobType.copy == "copy"
        assert JobType.upload == "upload"
        assert JobType.delete == "delete"


# ── StatusEvent ───────────────────────────────────────────────────────────

class TestStatusEvent:
    def test_auto_event_id(self):
        event = StatusEvent(
            job_id="j1",
            job_type=JobType.plugin,
            status=JobStatus.started,
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        assert len(event.event_id) == 24
        assert event.event_id != ""

    def test_event_id_deterministic(self):
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        e1 = StatusEvent(job_id="j1", job_type=JobType.plugin, status=JobStatus.started, timestamp=ts)
        e2 = StatusEvent(job_id="j1", job_type=JobType.plugin, status=JobStatus.started, timestamp=ts)
        assert e1.event_id == e2.event_id

    def test_event_id_differs_for_different_inputs(self):
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        e1 = StatusEvent(job_id="j1", job_type=JobType.plugin, status=JobStatus.started, timestamp=ts)
        e2 = StatusEvent(job_id="j2", job_type=JobType.plugin, status=JobStatus.started, timestamp=ts)
        assert e1.event_id != e2.event_id

    def test_custom_event_id_preserved(self):
        event = StatusEvent(
            event_id="custom123",
            job_id="j1",
            job_type=JobType.copy,
            status=JobStatus.notStarted,
        )
        assert event.event_id == "custom123"

    def test_serialize_deserialize_roundtrip(self, sample_status_event):
        data = sample_status_event.serialize_for_kafka()
        assert isinstance(data, bytes)
        restored = StatusEvent.deserialize_from_kafka(data)
        assert restored.job_id == sample_status_event.job_id
        assert restored.job_type == sample_status_event.job_type
        assert restored.status == sample_status_event.status
        assert restored.image == sample_status_event.image
        assert restored.event_id == sample_status_event.event_id

    def test_serialize_produces_valid_json(self, sample_status_event):
        data = sample_status_event.serialize_for_kafka()
        parsed = json.loads(data)
        assert parsed["job_id"] == "test-job-1"
        assert parsed["status"] == "started"
        assert parsed["job_type"] == "plugin"

    def test_timestamp_serialized_as_iso(self, sample_status_event):
        data = json.loads(sample_status_event.serialize_for_kafka())
        assert "2026-01-15" in data["timestamp"]

    def test_default_values(self):
        event = StatusEvent(job_id="j1", job_type=JobType.plugin, status=JobStatus.started)
        assert event.image == ""
        assert event.cmd == ""
        assert event.message == ""
        assert event.exit_code is None
        assert event.source == "docker"
        assert event.previous_status is None

    def test_optional_fields(self):
        event = StatusEvent(
            job_id="j1",
            job_type=JobType.plugin,
            status=JobStatus.finishedWithError,
            previous_status=JobStatus.started,
            exit_code=1,
        )
        assert event.previous_status == JobStatus.started
        assert event.exit_code == 1


# ── LogEvent ──────────────────────────────────────────────────────────────

class TestLogEvent:
    def test_serialize_deserialize_roundtrip(self, sample_log_event):
        data = sample_log_event.serialize_for_kafka()
        assert isinstance(data, bytes)
        restored = LogEvent.deserialize_from_kafka(data)
        assert restored.job_id == sample_log_event.job_id
        assert restored.line == sample_log_event.line
        assert restored.eos is False

    def test_eos_marker(self, eos_log_event):
        assert eos_log_event.eos is True
        data = eos_log_event.serialize_for_kafka()
        restored = LogEvent.deserialize_from_kafka(data)
        assert restored.eos is True
        assert restored.line == ""

    def test_default_values(self):
        event = LogEvent(job_id="j1", job_type=JobType.plugin)
        assert event.container_name == ""
        assert event.line == ""
        assert event.stream == "stdout"
        assert event.eos is False

    def test_auto_event_id(self):
        event = LogEvent(
            job_id="j1",
            job_type=JobType.plugin,
            line="hello",
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        assert len(event.event_id) == 24
        assert event.event_id != ""

    def test_event_id_deterministic(self):
        """Two LogEvents with identical fields must have identical event_id."""
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        e1 = LogEvent(
            job_id="j1", job_type=JobType.plugin,
            container_name="c1", line="hello", stream="stdout", timestamp=ts,
        )
        e2 = LogEvent(
            job_id="j1", job_type=JobType.plugin,
            container_name="c1", line="hello", stream="stdout", timestamp=ts,
        )
        assert e1.event_id == e2.event_id

    def test_event_id_differs_for_different_lines(self):
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        e1 = LogEvent(
            job_id="j1", job_type=JobType.plugin, line="hello", timestamp=ts,
        )
        e2 = LogEvent(
            job_id="j1", job_type=JobType.plugin, line="world", timestamp=ts,
        )
        assert e1.event_id != e2.event_id

    def test_event_id_differs_for_different_streams(self):
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        e1 = LogEvent(
            job_id="j1", job_type=JobType.plugin, line="x",
            stream="stdout", timestamp=ts,
        )
        e2 = LogEvent(
            job_id="j1", job_type=JobType.plugin, line="x",
            stream="stderr", timestamp=ts,
        )
        assert e1.event_id != e2.event_id

    def test_custom_event_id_preserved(self):
        event = LogEvent(
            event_id="custom456",
            job_id="j1",
            job_type=JobType.plugin,
            line="hello",
        )
        assert event.event_id == "custom456"

    def test_event_id_round_trip(self):
        """Deserializing a LogEvent must preserve the event_id."""
        original = LogEvent(
            job_id="j1",
            job_type=JobType.plugin,
            line="hello",
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        restored = LogEvent.deserialize_from_kafka(original.serialize_for_kafka())
        assert restored.event_id == original.event_id


# ── kafka_key_for_job ─────────────────────────────────────────────────────

class TestKafkaKeyForJob:
    def test_returns_bytes(self):
        key = kafka_key_for_job("test-job-1")
        assert isinstance(key, bytes)
        assert key == b"test-job-1"

    def test_different_jobs_different_keys(self):
        assert kafka_key_for_job("a") != kafka_key_for_job("b")
