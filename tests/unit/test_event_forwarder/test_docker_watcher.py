"""Tests for chris_streaming.event_forwarder.docker_watcher."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

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
            "Action": "destroy",
            "Actor": {"ID": "abc", "Attributes": {"name": "test-job-1"}},
            "timeNano": 0,
        }
        event = await watcher._docker_event_to_status_event(raw)
        assert event is None
