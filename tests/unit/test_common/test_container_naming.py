"""Tests for chris_streaming.common.container_naming."""

import pytest

from chris_streaming.common.container_naming import (
    job_type_from_label,
    parse_container_name,
    resolve_job_type,
)
from chris_streaming.common.schemas import JobType


class TestParseContainerName:
    def test_plugin_no_suffix(self):
        job_id, job_type = parse_container_name("abc-123")
        assert job_id == "abc-123"
        assert job_type == JobType.plugin

    def test_copy_suffix(self):
        job_id, job_type = parse_container_name("abc-123-copy")
        assert job_id == "abc-123"
        assert job_type == JobType.copy

    def test_upload_suffix(self):
        job_id, job_type = parse_container_name("abc-123-upload")
        assert job_id == "abc-123"
        assert job_type == JobType.upload

    def test_delete_suffix(self):
        job_id, job_type = parse_container_name("abc-123-delete")
        assert job_id == "abc-123"
        assert job_type == JobType.delete

    def test_no_false_positive_on_partial_suffix(self):
        job_id, job_type = parse_container_name("mycopy")
        assert job_id == "mycopy"
        assert job_type == JobType.plugin

    def test_empty_string(self):
        job_id, job_type = parse_container_name("")
        assert job_id == ""
        assert job_type == JobType.plugin


class TestJobTypeFromLabel:
    @pytest.mark.parametrize("value,expected", [
        ("plugin", JobType.plugin),
        ("copy", JobType.copy),
        ("upload", JobType.upload),
        ("delete", JobType.delete),
    ])
    def test_valid_labels(self, value, expected):
        labels = {"org.chrisproject.job_type": value}
        assert job_type_from_label(labels) == expected

    def test_missing_label(self):
        assert job_type_from_label({}) is None

    def test_invalid_value(self):
        labels = {"org.chrisproject.job_type": "unknown"}
        assert job_type_from_label(labels) is None

    def test_empty_value(self):
        labels = {"org.chrisproject.job_type": ""}
        assert job_type_from_label(labels) is None


class TestResolveJobType:
    def test_label_takes_precedence(self):
        labels = {"org.chrisproject.job_type": "copy"}
        job_id, job_type = resolve_job_type("abc-123", labels)
        assert job_id == "abc-123"
        assert job_type == JobType.copy

    def test_falls_back_to_suffix(self):
        job_id, job_type = resolve_job_type("abc-123-upload", {})
        assert job_id == "abc-123"
        assert job_type == JobType.upload

    def test_label_overrides_suffix(self):
        labels = {"org.chrisproject.job_type": "delete"}
        job_id, job_type = resolve_job_type("abc-123-copy", labels)
        assert job_id == "abc-123"
        assert job_type == JobType.delete
