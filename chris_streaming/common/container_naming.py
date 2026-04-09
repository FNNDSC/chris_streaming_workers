"""
Parse pfcon's container naming convention to extract job_id and job_type.

pfcon names containers as:
  - Plugin:  {job_id}
  - Copy:    {job_id}-copy
  - Upload:  {job_id}-upload
  - Delete:  {job_id}-delete

This module provides parsing in both directions and also reads the
org.chrisproject.job_type label when available.
"""

from __future__ import annotations

from .schemas import JobType

# Suffixes in order of longest first to avoid partial matches
_SUFFIXES = [
    ("-upload", JobType.upload),
    ("-delete", JobType.delete),
    ("-copy", JobType.copy),
]


def parse_container_name(name: str) -> tuple[str, JobType]:
    """
    Extract (job_id, job_type) from a pfcon container name.

    Falls back to treating the whole name as job_id with type=plugin
    if no known suffix is found.
    """
    for suffix, job_type in _SUFFIXES:
        if name.endswith(suffix):
            return name[: -len(suffix)], job_type
    return name, JobType.plugin


def job_type_from_label(labels: dict[str, str]) -> JobType | None:
    """
    Read org.chrisproject.job_type label from container labels.
    Returns None if the label is not present or has an unrecognized value.
    """
    raw = labels.get("org.chrisproject.job_type", "")
    try:
        return JobType(raw)
    except ValueError:
        return None


def resolve_job_type(container_name: str, labels: dict[str, str]) -> tuple[str, JobType]:
    """
    Determine job_id and job_type using labels (preferred) or name parsing (fallback).
    """
    label_type = job_type_from_label(labels)
    job_id, name_type = parse_container_name(container_name)
    return job_id, label_type if label_type is not None else name_type
