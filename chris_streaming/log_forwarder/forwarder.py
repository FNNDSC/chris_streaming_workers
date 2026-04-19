"""
Log forwarder: enrich + produce log events to Redis Streams.

Wraps a ``DockerLogTailer`` producer → ``RedisStreamProducer`` sink, so the
log pipeline mirrors the event-forwarder pipeline. Each ``LogLine`` is
serialized as a ``LogEvent`` and XADD'd to ``stream:job-logs:{shard}``.
EOS sentinels (``LogLine.eos=True``) are forwarded as ``LogEvent.eos=True``
on the same shard, signalling that a container's log stream has been fully
drained.
"""

from __future__ import annotations

import logging

from chris_streaming.common.redis_stream import RedisStreamProducer
from chris_streaming.common.schemas import LogEvent
from .tailer import LogLine

logger = logging.getLogger(__name__)


class LogForwarder:
    """Serialize each ``LogLine`` as a ``LogEvent`` and XADD to Redis Streams."""

    def __init__(self, producer: RedisStreamProducer) -> None:
        self._producer = producer

    async def forward(self, line: LogLine) -> None:
        event = LogEvent(
            job_id=line.job_id,
            job_type=line.job_type,
            container_name=line.container_name,
            line=line.line,
            stream=line.stream,
            timestamp=line.timestamp,
            eos=line.eos,
        )
        value = event.serialize()
        await self._producer.xadd(event.job_id, value)
