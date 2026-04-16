"""Tests for chris_streaming.event_forwarder.docker_watcher."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock

from chris_streaming.common.schemas import JobStatus, JobType
from chris_streaming.common.settings import EventForwarderSettings
from chris_streaming.event_forwarder.docker_watcher import DockerWatcher


class TestDockerWatcher:
    def _make_watcher(self) -> DockerWatcher:
        settings = EventForwarderSettings(emit_initial_state=False)
        return DockerWatcher(settings)

    def test_label_filter(self):
        settings = EventForwarderSettings(
            docker_label_filter="org.chrisproject.miniChRIS",
            docker_label_value="plugininstance",
        )
        watcher = DockerWatcher(settings)
        assert watcher._label_filter == "org.chrisproject.miniChRIS=plugininstance"

    def test_container_inspect_to_event_running(self):
        watcher = self._make_watcher()
        info = {
            "Name": "/test-job-1",
            "Config": {
                "Labels": {"org.chrisproject.job_type": "plugin"},
                "Cmd": ["python", "app.py"],
                "Image": "ghcr.io/fnndsc/pl-test:latest",
            },
            "State": {
                "Running": True,
                "Status": "running",
                "ExitCode": 0,
                "StartedAt": "2026-01-15T12:00:00Z",
                "FinishedAt": "0001-01-01T00:00:00Z",
            },
        }
        event = watcher._container_inspect_to_event(info)
        assert event is not None
        assert event.job_id == "test-job-1"
        assert event.job_type == JobType.plugin
        assert event.status == JobStatus.started
        assert event.image == "ghcr.io/fnndsc/pl-test:latest"

    def test_container_inspect_to_event_exited_success(self):
        watcher = self._make_watcher()
        info = {
            "Name": "/test-job-1-copy",
            "Config": {
                "Labels": {},
                "Cmd": ["cp", "-r", "src", "dst"],
                "Image": "ghcr.io/fnndsc/pfcon:latest",
            },
            "State": {
                "Status": "exited",
                "ExitCode": 0,
                "StartedAt": "2026-01-15T12:00:00Z",
                "FinishedAt": "2026-01-15T12:01:00Z",
            },
        }
        event = watcher._container_inspect_to_event(info)
        assert event.job_id == "test-job-1"
        assert event.job_type == JobType.copy
        assert event.status == JobStatus.finishedSuccessfully

    def test_container_inspect_to_event_exited_error(self):
        watcher = self._make_watcher()
        info = {
            "Name": "/test-job-1",
            "Config": {"Labels": {}, "Cmd": None, "Image": ""},
            "State": {
                "Status": "exited",
                "ExitCode": 1,
                "StartedAt": "2026-01-15T12:00:00Z",
                "FinishedAt": "2026-01-15T12:01:00Z",
            },
        }
        event = watcher._container_inspect_to_event(info)
        assert event.status == JobStatus.finishedWithError
        assert event.exit_code == 1

    async def test_docker_event_to_status_event_start(self):
        watcher = self._make_watcher()
        mock_docker = AsyncMock()
        watcher._docker = mock_docker

        container_mock = AsyncMock()
        container_mock.show = AsyncMock(return_value={
            "Config": {"Cmd": ["python", "run.py"], "Image": "img:latest"},
        })
        mock_docker.containers.get = AsyncMock(return_value=container_mock)

        raw = {
            "Action": "start",
            "Actor": {
                "ID": "abc123",
                "Attributes": {
                    "name": "test-job-1",
                    "image": "img:latest",
                    "org.chrisproject.job_type": "plugin",
                },
            },
            "timeNano": int(datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1e9),
        }

        event = await watcher._docker_event_to_status_event(raw)
        assert event is not None
        assert event.status == JobStatus.started
        assert event.job_id == "test-job-1"
        assert event.job_type == JobType.plugin

    async def test_docker_event_to_status_event_die_exit_0(self):
        watcher = self._make_watcher()
        mock_docker = AsyncMock()
        watcher._docker = mock_docker

        container_mock = AsyncMock()
        container_mock.show = AsyncMock(return_value={
            "Config": {"Cmd": None, "Image": "img:latest"},
        })
        mock_docker.containers.get = AsyncMock(return_value=container_mock)

        raw = {
            "Action": "die",
            "Actor": {
                "ID": "abc123",
                "Attributes": {"name": "test-job-1", "exitCode": "0"},
            },
            "timeNano": 0,
        }

        event = await watcher._docker_event_to_status_event(raw)
        assert event is not None
        assert event.status == JobStatus.finishedSuccessfully
        assert event.exit_code == 0

    async def test_docker_event_to_status_event_irrelevant_action(self):
        watcher = self._make_watcher()
        raw = {
            "Action": "rename",
            "Actor": {"ID": "abc", "Attributes": {"name": "test-job-1"}},
            "timeNano": 0,
        }
        event = await watcher._docker_event_to_status_event(raw)
        assert event is None

    async def test_destroy_without_prior_terminal_emits_undefined(self):
        """Container vanishes without die/kill/oom → undefined (stuck Docker)."""
        watcher = self._make_watcher()
        mock_docker = AsyncMock()
        watcher._docker = mock_docker
        container_mock = AsyncMock()
        container_mock.show = AsyncMock(return_value={
            "Config": {"Cmd": None, "Image": "img:latest"},
        })
        mock_docker.containers.get = AsyncMock(return_value=container_mock)

        # Container was previously 'started' (never saw die)
        watcher._last_status["abc123"] = JobStatus.started
        raw = {
            "Action": "destroy",
            "Actor": {
                "ID": "abc123",
                "Attributes": {
                    "name": "test-job-1",
                    "org.chrisproject.job_type": "plugin",
                },
            },
            "timeNano": 0,
        }
        event = await watcher._docker_event_to_status_event(raw)
        assert event is not None
        assert event.status == JobStatus.undefined
        # Container id is forgotten after destroy is processed
        assert "abc123" not in watcher._last_status

    async def test_destroy_after_terminal_is_deduped(self):
        """destroy following a die event should NOT re-emit a terminal."""
        watcher = self._make_watcher()
        watcher._last_status["abc123"] = JobStatus.finishedSuccessfully
        raw = {
            "Action": "destroy",
            "Actor": {"ID": "abc123", "Attributes": {"name": "test-job-1"}},
            "timeNano": 0,
        }
        event = await watcher._docker_event_to_status_event(raw)
        assert event is None
        assert "abc123" not in watcher._last_status

    async def test_events_track_last_status(self):
        """Emitted events must seed _last_status so destroy can dedup."""
        watcher = self._make_watcher()
        mock_docker = AsyncMock()
        watcher._docker = mock_docker
        container_mock = AsyncMock()
        container_mock.show = AsyncMock(return_value={
            "Config": {"Cmd": None, "Image": "img:latest"},
        })
        mock_docker.containers.get = AsyncMock(return_value=container_mock)

        raw = {
            "Action": "die",
            "Actor": {
                "ID": "abc123",
                "Attributes": {"name": "test-job-1", "exitCode": "1"},
            },
            "timeNano": 0,
        }
        event = await watcher._docker_event_to_status_event(raw)
        assert event.status == JobStatus.finishedWithError
        assert watcher._last_status["abc123"] == JobStatus.finishedWithError

    async def test_reconciler_emits_undefined_for_drifted_container(self):
        """Reconciler emits when inspect disagrees with last known status."""
        watcher = self._make_watcher()
        mock_docker = AsyncMock()
        watcher._docker = mock_docker

        # Container in an undefined state (e.g. 'removing') with no die event.
        container_mock = AsyncMock()
        container_mock.show = AsyncMock(return_value={
            "Id": "abc123",
            "Name": "/test-job-1",
            "Config": {
                "Labels": {"org.chrisproject.job_type": "plugin"},
                "Cmd": None,
                "Image": "img:latest",
            },
            "State": {
                "Status": "removing",
                "ExitCode": 0,
                "StartedAt": "2026-01-15T12:00:00Z",
                "FinishedAt": "0001-01-01T00:00:00Z",
            },
        })
        mock_docker.containers.list = AsyncMock(return_value=[container_mock])

        # Last seen: started. Reconciler should notice drift → emit undefined.
        watcher._last_status["abc123"] = JobStatus.started

        events = []
        async for e in watcher._reconcile():
            events.append(e)

        assert len(events) == 1
        assert events[0].status == JobStatus.undefined
        assert watcher._last_status["abc123"] == JobStatus.undefined

    async def test_reconciler_skips_unchanged_containers(self):
        """No drift → no event, no dupe noise."""
        watcher = self._make_watcher()
        mock_docker = AsyncMock()
        watcher._docker = mock_docker

        container_mock = AsyncMock()
        container_mock.show = AsyncMock(return_value={
            "Id": "abc123",
            "Name": "/test-job-1",
            "Config": {"Labels": {}, "Cmd": None, "Image": ""},
            "State": {
                "Running": True,
                "Status": "running",
                "ExitCode": 0,
                "StartedAt": "2026-01-15T12:00:00Z",
                "FinishedAt": "0001-01-01T00:00:00Z",
            },
        })
        mock_docker.containers.list = AsyncMock(return_value=[container_mock])
        watcher._last_status["abc123"] = JobStatus.started

        events = [e async for e in watcher._reconcile()]
        assert events == []
