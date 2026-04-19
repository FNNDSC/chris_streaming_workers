"""Tests for chris_streaming.log_forwarder.tailer.

We test the pure-functional parts (Docker log line parsing, RFC3339 parsing)
directly. The aiodocker-integrated pieces (streaming logs from a live
daemon) are covered by integration/e2e tests, not here.
"""

from __future__ import annotations

from datetime import datetime, timezone

from chris_streaming.log_forwarder.tailer import (
    _parse_docker_log_line,
    _parse_rfc3339,
)


class TestParseRFC3339:
    def test_utc_with_nanoseconds(self):
        ts = _parse_rfc3339("2026-01-15T12:01:02.123456789Z")
        assert ts.year == 2026
        assert ts.hour == 12
        assert ts.tzinfo is not None
        # Nanos truncated to micros
        assert ts.microsecond == 123456

    def test_utc_without_fraction(self):
        ts = _parse_rfc3339("2026-01-15T12:01:02Z")
        assert ts == datetime(2026, 1, 15, 12, 1, 2, tzinfo=timezone.utc)

    def test_with_offset(self):
        ts = _parse_rfc3339("2026-01-15T12:01:02.123456789+00:00")
        assert ts.tzinfo is not None
        assert ts.microsecond == 123456


class TestParseDockerLogLine:
    def test_splits_timestamp_and_message(self):
        ts, stream, msg = _parse_docker_log_line(
            "2026-01-15T12:01:02.000000Z hello world\n"
        )
        assert ts.year == 2026
        assert stream == "stdout"
        assert msg == "hello world"

    def test_strips_trailing_newline(self):
        _ts, _s, msg = _parse_docker_log_line(
            "2026-01-15T12:01:02.000000Z line\n"
        )
        assert msg == "line"
        assert not msg.endswith("\n")

    def test_falls_back_when_timestamp_unparsable(self):
        _ts, _s, msg = _parse_docker_log_line("not-a-timestamp hello\n")
        # When parsing fails, the message is the whole line minus the trailing newline
        assert "hello" in msg
