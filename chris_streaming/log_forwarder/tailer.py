"""
Docker log tailer.

Watches Docker events for ChRIS job container start/create events, opens
a follow-style log stream per container, and yields (job_id, job_type,
container_name, stream, timestamp, line) tuples.

Design:
  - One ``asyncio.Task`` per live container log stream.
  - Tasks exit when the container stops (stream closes) or when the
    tailer is shut down.
  - On startup, existing running ChRIS containers are picked up via a
    ``list`` + ``since=0`` log stream (then handed off to follow mode).
  - Reconnects on docker-daemon disconnect with bounded backoff.
  - Emitted ``LogLine.stream`` is always empty. Python's ``logging``
    module (and many CLI tools) writes *all* levels to stderr by default,
    so the stdout/stderr split does not carry severity information. We
    drop it here to match what Kubernetes can provide and to avoid the
    UI falsely painting INFO-level pfcon logs as errors. Severity is
    derived from log-line content downstream.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import aiodocker

from chris_streaming.common.container_naming import resolve_job_type
from chris_streaming.common.schemas import JobType

logger = logging.getLogger(__name__)

_MAX_RECONNECT_WAIT = 60


@dataclass
class LogLine:
    """A single enriched log line ready for serialization.

    When ``eos=True``, this record is an End-of-Stream sentinel for the
    named container: Docker (or Kubernetes) has closed the log stream
    after draining all buffered output. ``line`` and ``stream`` are
    unused on EOS records.

    ``stream`` is retained in the schema for future use but is currently
    always empty — see the module docstring for why.
    """
    job_id: str
    job_type: JobType
    container_name: str
    stream: str
    line: str
    timestamp: datetime
    eos: bool = False


def _parse_docker_log_line(raw: str) -> tuple[datetime, str, str]:
    """Parse a Docker log line in the form '<rfc3339-ts> <message>\\n'.

    Returns (timestamp, stream, message). We cannot distinguish stdout vs
    stderr from the demuxed text stream alone; aiodocker gives us them via
    separate iterators when ``stdout``/``stderr`` are both requested, so
    callers tag the stream themselves.
    """
    # Docker prefixes each line with an RFC3339 nano-precision timestamp.
    # Example: "2026-01-15T12:01:02.123456789Z hello\n"
    ts_str, _, message = raw.partition(" ")
    try:
        ts = _parse_rfc3339(ts_str)
    except ValueError:
        ts = datetime.now(timezone.utc)
        message = raw
    # Strip trailing newline
    if message.endswith("\n"):
        message = message[:-1]
    return ts, "stdout", message


def _parse_rfc3339(s: str) -> datetime:
    """Parse a Docker RFC3339 timestamp with nanosecond precision."""
    # Python's fromisoformat doesn't accept nanoseconds in <3.12 reliably.
    # Truncate to microseconds (drop nanoseconds) and normalize trailing Z.
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    # Remove sub-microsecond digits
    if "." in s:
        head, _, tail = s.partition(".")
        # tail is like "123456789+00:00" — split on sign
        for i, ch in enumerate(tail):
            if ch in "+-":
                digits = tail[:i]
                tz = tail[i:]
                break
        else:
            digits = tail
            tz = ""
        digits = digits[:6]  # truncate to microseconds
        s = f"{head}.{digits}{tz}"
    return datetime.fromisoformat(s)


class DockerLogTailer:
    """Tails logs from all ChRIS job containers on this Docker host."""

    def __init__(
        self,
        label_filter: str,
        label_value: str,
    ) -> None:
        self._label = f"{label_filter}={label_value}"
        self._docker: Optional[aiodocker.Docker] = None
        self._tasks: dict[str, asyncio.Task] = {}
        self._queue: asyncio.Queue[LogLine] = asyncio.Queue(maxsize=10_000)
        self._running = False

    async def connect(self) -> None:
        self._docker = aiodocker.Docker()
        logger.info("Log tailer connected to Docker daemon")

    async def close(self) -> None:
        self._running = False
        for task in list(self._tasks.values()):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()
        if self._docker is not None:
            await self._docker.close()
            self._docker = None

    async def __aenter__(self) -> "DockerLogTailer":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def stream(self) -> AsyncIterator[LogLine]:
        """Yield enriched log lines. Runs until ``close()`` is called."""
        self._running = True
        # Kick off the event-watch supervisor in the background
        supervisor = asyncio.create_task(self._supervise())
        try:
            while self._running:
                try:
                    line = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                    yield line
                except asyncio.TimeoutError:
                    continue
        finally:
            supervisor.cancel()
            try:
                await supervisor
            except asyncio.CancelledError:
                pass

    async def _supervise(self) -> None:
        """Discover ChRIS containers and keep per-container tail tasks alive."""
        backoff = 1.0
        while self._running:
            try:
                await self._bootstrap_existing()
                await self._watch_events()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Log tailer supervisor error: %s", e, exc_info=True)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _MAX_RECONNECT_WAIT)

    async def _bootstrap_existing(self) -> None:
        """On (re)connect, pick up already-running ChRIS containers."""
        assert self._docker is not None
        containers = await self._docker.containers.list(
            all=False,
            filters={"label": [self._label]},
        )
        for c in containers:
            await self._spawn_tail(c.id)

    async def _watch_events(self) -> None:
        """Watch Docker events for new ChRIS containers; spawn tailers."""
        assert self._docker is not None
        subscriber = self._docker.events.subscribe(
            filters={
                "type": ["container"],
                "label": [self._label],
                "event": ["start", "create"],
            },
        )
        while self._running:
            event = await subscriber.get()
            if event is None:
                # Docker event stream dropped
                return
            if event.get("Type") != "container":
                continue
            action = event.get("Action")
            if action not in ("start", "create"):
                continue
            cid = event.get("Actor", {}).get("ID") or event.get("id")
            if cid:
                await self._spawn_tail(cid)

    async def _spawn_tail(self, container_id: str) -> None:
        """Start a follow-mode log task for this container (idempotent)."""
        if container_id in self._tasks and not self._tasks[container_id].done():
            return
        task = asyncio.create_task(self._tail_one(container_id))
        self._tasks[container_id] = task

    async def _tail_one(self, container_id: str) -> None:
        """Stream logs from one container until it stops.

        Uses a single combined stdout+stderr stream. When Docker closes
        the stream (container exited and buffered output drained), emit
        an EOS sentinel so downstream knows this container's logs are
        complete. Errors skip EOS and defer to the cleanup quiescence
        backstop.
        """
        assert self._docker is not None
        emit_eos = False
        name = ""
        job_id = ""
        job_type: Optional[JobType] = None
        try:
            container = await self._docker.containers.get(container_id)
            info = await container.show()
            labels = (info.get("Config") or {}).get("Labels") or {}
            name = (info.get("Name") or "").lstrip("/")
            if not name:
                return
            job_id, job_type = resolve_job_type(name, labels)

            async for chunk in container.log(
                stdout=True,
                stderr=True,
                follow=True,
                timestamps=True,
            ):
                if not chunk:
                    continue
                for line in chunk.splitlines():
                    if not line:
                        continue
                    ts, _, message = _parse_docker_log_line(line)
                    await self._queue.put(
                        LogLine(
                            job_id=job_id,
                            job_type=job_type,
                            container_name=name,
                            stream="",
                            line=message,
                            timestamp=ts,
                        )
                    )
            emit_eos = True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("Tail error for container %s: %s", container_id, e)
        finally:
            if emit_eos and job_type is not None:
                await self._queue.put(
                    LogLine(
                        job_id=job_id,
                        job_type=job_type,
                        container_name=name,
                        stream="",
                        line="",
                        timestamp=datetime.now(timezone.utc),
                        eos=True,
                    )
                )
            self._tasks.pop(container_id, None)
