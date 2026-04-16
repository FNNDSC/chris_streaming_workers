"""Tests for chris_streaming.event_forwarder.k8s_watcher."""

from types import SimpleNamespace
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from chris_streaming.common.schemas import JobStatus
from chris_streaming.common.settings import EventForwarderSettings
from chris_streaming.event_forwarder.k8s_watcher import K8sWatcher


def _make_job(name="test-job-1", labels=None, pod_labels=None,
              active=None, succeeded=None, failed=None,
              completion_time=None, conditions=None):
    labels = labels or {"org.chrisproject.job_type": "plugin"}
    pod_labels = pod_labels or labels
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            labels=labels,
            resource_version="1",
            creation_timestamp=datetime(2026, 1, 15, tzinfo=timezone.utc),
        ),
        spec=SimpleNamespace(
            template=SimpleNamespace(
                metadata=SimpleNamespace(labels=pod_labels),
                spec=SimpleNamespace(containers=[
                    SimpleNamespace(image="img:latest", command=["python"]),
                ]),
            ),
        ),
        status=SimpleNamespace(
            conditions=conditions,
            active=active,
            succeeded=succeeded,
            failed=failed,
            completion_time=completion_time,
            start_time=datetime(2026, 1, 15, tzinfo=timezone.utc),
        ),
    )


def _terminated_pod(exit_code=0):
    return SimpleNamespace(
        status=SimpleNamespace(
            container_statuses=[
                SimpleNamespace(state=SimpleNamespace(
                    terminated=SimpleNamespace(exit_code=exit_code),
                )),
            ],
        ),
    )


class TestK8sWatcher:
    def _make_watcher(self):
        return K8sWatcher(EventForwarderSettings(compute_env="kubernetes"))

    async def test_job_inactive_no_conditions_emits_undefined(self):
        """Replicates pfcon's 'inactive' → undefined branch."""
        watcher = self._make_watcher()
        watcher._core_api = MagicMock()
        watcher._core_api.list_namespaced_pod = AsyncMock(
            return_value=SimpleNamespace(items=[]),
        )

        # active/succeeded=None but failed=0 ⇒ not the "all None" edge case;
        # no Failed condition, no completion_time ⇒ undefined.
        job = _make_job(active=None, succeeded=None, failed=0, conditions=[])
        event = await watcher._k8s_job_to_event(job)

        assert event is not None
        assert event.status == JobStatus.undefined
        # exit_code fetch attempted; returned None because no pods.
        assert event.exit_code is None

    async def test_undefined_status_fetches_exit_code_when_pod_terminated(self):
        watcher = self._make_watcher()
        watcher._core_api = MagicMock()
        watcher._core_api.list_namespaced_pod = AsyncMock(
            return_value=SimpleNamespace(items=[_terminated_pod(exit_code=137)]),
        )

        job = _make_job(active=None, succeeded=None, failed=0, conditions=[])
        event = await watcher._k8s_job_to_event(job)

        assert event.status == JobStatus.undefined
        assert event.exit_code == 137

    async def test_finished_successfully_fetches_exit_code(self):
        watcher = self._make_watcher()
        watcher._core_api = MagicMock()
        watcher._core_api.list_namespaced_pod = AsyncMock(
            return_value=SimpleNamespace(items=[_terminated_pod(exit_code=0)]),
        )

        job = _make_job(
            succeeded=1,
            completion_time=datetime(2026, 1, 15, 12, tzinfo=timezone.utc),
        )
        event = await watcher._k8s_job_to_event(job)

        assert event.status == JobStatus.finishedSuccessfully
        assert event.exit_code == 0

    async def test_running_job_does_not_fetch_exit_code(self):
        watcher = self._make_watcher()
        watcher._core_api = MagicMock()
        watcher._core_api.list_namespaced_pod = AsyncMock()

        job = _make_job(active=1)
        event = await watcher._k8s_job_to_event(job)

        assert event.status == JobStatus.started
        watcher._core_api.list_namespaced_pod.assert_not_called()
        assert event.exit_code is None
