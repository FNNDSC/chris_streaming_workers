"""
Docker event watcher that produces StatusEvent objects from Docker daemon events.

Connects to the Docker daemon via the Unix socket, filters container events
by the ChRIS label (org.chrisproject.miniChRIS=plugininstance), and maps
Docker lifecycle events to pfcon-compatible JobStatus values.

Resilience features:
  - On startup, lists all matching containers and emits their current status
  - Reconnects with exponential backoff if the event stream drops
  - Stateless: no local state, safe to restart at any time
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Optional

import aiodocker

from chris_streaming.common.container_naming import resolve_job_type
from chris_streaming.common.pfcon_status import docker_event_to_status, docker_state_to_status
from chris_streaming.common.schemas import JobStatus, StatusEvent
from chris_streaming.common.settings import EventForwarderSettings
from .watcher import Watcher

logger = logging.getLogger(__name__)

# Docker event types we care about. `destroy` is included so we can detect
# containers that vanished without firing `die` (stuck/overloaded docker)
# and report them as `undefined`.
_RELEVANT_EVENTS = {"create", "start", "die", "kill", "oom", "destroy"}

# Terminal JobStatus values that mean "no further status needed for this
# container" — once observed we won't synthesize `undefined` on destroy.
_TERMINAL_STATUSES = frozenset({
    JobStatus.finishedSuccessfully,
    JobStatus.finishedWithError,
    JobStatus.undefined,
})

# Reconnect backoff
_MAX_RECONNECT_WAIT = 60


class DockerWatcher(Watcher):
    """Watches Docker daemon events for ChRIS job containers."""

    def __init__(self, settings: EventForwarderSettings):
        self._settings = settings
        self._docker: Optional[aiodocker.Docker] = None
        self._label_filter = f"{settings.docker_label_filter}={settings.docker_label_value}"
        # Last emitted status per container id. Used to:
        #   1. synthesize `undefined` on `destroy` events that follow no
        #      terminal event (container vanished unexpectedly).
        #   2. deduplicate events produced by the periodic reconciler.
        self._last_status: dict[str, JobStatus] = {}

    async def connect(self) -> None:
        self._docker = aiodocker.Docker()
        logger.info("Connected to Docker daemon")

    async def close(self) -> None:
        if self._docker:
            await self._docker.close()
            self._docker = None

    async def watch(self) -> AsyncIterator[StatusEvent]:
        """
        Yield status events. First emits initial state of all matching
        containers, then streams live events with auto-reconnect. When
        ``docker_reconcile_seconds`` is non-zero, also runs a periodic
        reconciler that inspects tracked containers and emits an event if
        the current mapped state disagrees with what was last emitted.
        """
        if self._settings.emit_initial_state:
            async for event in self._emit_initial_state():
                yield event

        queue: asyncio.Queue[StatusEvent] = asyncio.Queue()

        async def run_events() -> None:
            reconnect_attempt = 0
            while True:
                try:
                    async for event in self._watch_events():
                        await queue.put(event)
                        reconnect_attempt = 0
                except (aiodocker.exceptions.DockerError, ConnectionError, OSError) as e:
                    reconnect_attempt += 1
                    wait = min(2 ** reconnect_attempt, _MAX_RECONNECT_WAIT)
                    logger.warning(
                        "Docker event stream disconnected: %s. Reconnecting in %ds (attempt %d)",
                        e, wait, reconnect_attempt,
                    )
                    await asyncio.sleep(wait)
                    try:
                        await self.close()
                    except Exception:
                        pass
                    await self.connect()
                    async for event in self._emit_initial_state():
                        await queue.put(event)

        events_task = asyncio.create_task(run_events())
        reconciler_task: Optional[asyncio.Task] = None
        interval = float(self._settings.docker_reconcile_seconds or 0.0)
        if interval > 0:
            async def run_reconciler() -> None:
                while True:
                    await asyncio.sleep(interval)
                    try:
                        async for event in self._reconcile():
                            await queue.put(event)
                    except Exception as e:
                        logger.warning("Docker reconciler sweep failed: %s", e)

            reconciler_task = asyncio.create_task(run_reconciler())
            logger.info("Docker reconciler enabled (every %.1fs)", interval)

        try:
            while True:
                yield await queue.get()
        finally:
            events_task.cancel()
            if reconciler_task is not None:
                reconciler_task.cancel()

    async def _emit_initial_state(self) -> AsyncIterator[StatusEvent]:
        """List all matching containers and emit their current status."""
        filters = {"label": [self._label_filter]}
        containers = await self._docker.containers.list(all=True, filters=filters)
        logger.info("Initial state: found %d matching containers", len(containers))

        for container in containers:
            info = await container.show()
            event = self._container_inspect_to_event(info)
            if event is not None:
                cid = info.get("Id", "")
                if cid:
                    self._last_status[cid] = event.status
                yield event

    async def _watch_events(self) -> AsyncIterator[StatusEvent]:
        """Stream Docker events filtered by label."""
        subscriber = self._docker.events.subscribe(filters={
            "type": ["container"],
            "label": [self._label_filter],
        })
        try:
            while True:
                raw_event = await subscriber.get()
                if raw_event is None:
                    # Stream closed
                    return

                # aiodocker uses "Action" (not "status") for the event type
                event_action = raw_event.get("Action", "")
                if event_action not in _RELEVANT_EVENTS:
                    continue

                status_event = await self._docker_event_to_status_event(raw_event)
                if status_event is not None:
                    yield status_event
        finally:
            await subscriber.close()

    async def _docker_event_to_status_event(self, raw: dict) -> Optional[StatusEvent]:
        """Convert a raw aiodocker event dict to a StatusEvent."""
        event_action = raw.get("Action", "")
        actor = raw.get("Actor", {})
        container_id = actor.get("ID", "")
        attrs = actor.get("Attributes", {})
        container_name = attrs.get("name", "")
        labels = {k: v for k, v in attrs.items()
                  if k.startswith("org.chrisproject.")}

        # Get exit code from die events
        exit_code = None
        if event_action == "die":
            exit_code_str = attrs.get("exitCode", "")
            try:
                exit_code = int(exit_code_str)
            except (ValueError, TypeError):
                exit_code = -1

        # `destroy` only produces a status event when the container vanished
        # without a prior terminal event — then we synthesize `undefined`.
        if event_action == "destroy":
            last = self._last_status.get(container_id)
            self._last_status.pop(container_id, None)
            if last in _TERMINAL_STATUSES:
                return None
            job_status = JobStatus.undefined
        else:
            job_status = docker_event_to_status(event_action, exit_code)
            if job_status is None:
                return None

        job_id, job_type = resolve_job_type(container_name, labels)

        # Get image and cmd from container inspect for richer events
        image = attrs.get("image", "")
        cmd = ""
        try:
            info = await self._docker.containers.get(container_id)
            detail = await info.show()
            cmd_list = detail.get("Config", {}).get("Cmd") or []
            cmd = " ".join(cmd_list)
            if not image:
                image = detail.get("Config", {}).get("Image", "")
        except Exception:
            pass

        # aiodocker provides timeNano (nanoseconds since epoch)
        time_nano = raw.get("timeNano", 0)
        if time_nano:
            timestamp = datetime.fromtimestamp(time_nano / 1e9, tz=timezone.utc)
        else:
            timestamp = datetime.now(timezone.utc)

        # Destroy already popped the container from tracking; don't re-add.
        if container_id and event_action != "destroy":
            self._last_status[container_id] = job_status

        return StatusEvent(
            job_id=job_id,
            job_type=job_type,
            status=job_status,
            image=image,
            cmd=cmd,
            message=event_action,
            exit_code=exit_code,
            timestamp=timestamp,
            source="docker",
        )

    async def _reconcile(self) -> AsyncIterator[StatusEvent]:
        """Inspect tracked containers and emit events for state drift.

        For every container matching our label filter, computes the current
        mapped status from its inspect payload and yields a StatusEvent if
        the result differs from what we last emitted for that container id.
        Catches containers that ended up in a degenerate state without
        firing the usual die/kill/oom events (e.g. under overloaded Docker).
        """
        filters = {"label": [self._label_filter]}
        containers = await self._docker.containers.list(all=True, filters=filters)
        for container in containers:
            info = await container.show()
            event = self._container_inspect_to_event(info)
            if event is None:
                continue
            cid = info.get("Id", "")
            if self._last_status.get(cid) == event.status:
                continue
            if cid:
                self._last_status[cid] = event.status
            yield event

    def _container_inspect_to_event(self, info: dict) -> Optional[StatusEvent]:
        """Convert a container inspect dict to a StatusEvent (for initial state)."""
        name = info.get("Name", "").lstrip("/")
        labels = info.get("Config", {}).get("Labels", {})
        state = info.get("State", {})

        job_id, job_type = resolve_job_type(name, labels)
        status = docker_state_to_status(state)

        cmd_list = info.get("Config", {}).get("Cmd") or []
        image = info.get("Config", {}).get("Image", "")

        exit_code = state.get("ExitCode")
        ts_str = state.get("FinishedAt", "")
        if ts_str == "0001-01-01T00:00:00Z":
            ts_str = state.get("StartedAt", "")

        try:
            timestamp = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            timestamp = datetime.now(timezone.utc)

        return StatusEvent(
            job_id=job_id,
            job_type=job_type,
            status=status,
            image=image,
            cmd=" ".join(cmd_list),
            message=state.get("Status", ""),
            exit_code=exit_code,
            timestamp=timestamp,
            source="docker",
        )
