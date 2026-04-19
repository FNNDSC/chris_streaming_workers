"""
Pydantic models for messages flowing through the streaming pipeline.

StatusEvent mirrors pfcon's JobInfo dataclass (abstractmgr.py).
LogEvent represents a single log line from a container.
Both are serialized as JSON bytes for Redis Streams XADD values.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_serializer


class JobStatus(str, Enum):
    """
    Mirrors pfcon's JobStatus enum from abstractmgr.py.
    Also includes confirmed_ prefixed statuses emitted by the Celery worker.
    """
    notStarted = "notStarted"
    started = "started"
    finishedSuccessfully = "finishedSuccessfully"
    finishedWithError = "finishedWithError"
    undefined = "undefined"
    # Confirmed statuses (emitted by the Celery worker after processing)
    confirmed_finishedSuccessfully = "confirmed_finishedSuccessfully"
    confirmed_finishedWithError = "confirmed_finishedWithError"
    confirmed_undefined = "confirmed_undefined"


TERMINAL_STATUSES = frozenset({
    JobStatus.finishedSuccessfully,
    JobStatus.finishedWithError,
    JobStatus.undefined,
})


class JobType(str, Enum):
    """Container type, matching pfcon's naming convention."""
    plugin = "plugin"
    copy = "copy"
    upload = "upload"
    delete = "delete"


class StatusEvent(BaseModel):
    """
    A job status change event produced by the Event Forwarder.
    Schema matches pfcon's JobInfo plus metadata for deduplication.
    """
    event_id: str = ""
    job_id: str
    job_type: JobType
    status: JobStatus
    previous_status: Optional[JobStatus] = None
    image: str = ""
    cmd: str = ""
    message: str = ""
    exit_code: Optional[int] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str = "docker"

    @field_serializer("timestamp")
    def serialize_timestamp(self, v: datetime, _info) -> str:
        return v.isoformat()

    def model_post_init(self, __context) -> None:
        if not self.event_id:
            raw = f"{self.job_id}:{self.job_type.value}:{self.status.value}:{self.timestamp}"
            self.event_id = hashlib.sha256(raw.encode()).hexdigest()[:24]

    def serialize(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def deserialize(cls, data: bytes) -> "StatusEvent":
        return cls.model_validate_json(data)


class LogEvent(BaseModel):
    """A single log line from a container, produced by the Log Forwarder."""
    event_id: str = ""
    job_id: str
    job_type: JobType
    container_name: str = ""
    line: str = ""
    stream: str = "stdout"
    eos: bool = False
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_serializer("timestamp")
    def serialize_timestamp(self, v: datetime, _info) -> str:
        return v.isoformat()

    def model_post_init(self, __context) -> None:
        if not self.event_id:
            raw = (
                f"{self.job_id}:{self.job_type.value}:{self.container_name}:"
                f"{self.stream}:{self.timestamp}:{self.line}"
            )
            self.event_id = hashlib.sha256(raw.encode()).hexdigest()[:24]

    def serialize(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def deserialize(cls, data: bytes) -> "LogEvent":
        return cls.model_validate_json(data)
