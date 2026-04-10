"""Tests for chris_streaming.common.pfcon_status."""

from types import SimpleNamespace

from chris_streaming.common.pfcon_status import (
    docker_event_to_status,
    docker_state_to_status,
    k8s_job_to_status,
)
from chris_streaming.common.schemas import JobStatus


# ── docker_event_to_status ────────────────────────────────────────────────

class TestDockerEventToStatus:
    def test_create(self):
        assert docker_event_to_status("create") == JobStatus.notStarted

    def test_start(self):
        assert docker_event_to_status("start") == JobStatus.started

    def test_die_exit_0(self):
        assert docker_event_to_status("die", exit_code=0) == JobStatus.finishedSuccessfully

    def test_die_exit_nonzero(self):
        assert docker_event_to_status("die", exit_code=1) == JobStatus.finishedWithError

    def test_die_exit_none(self):
        assert docker_event_to_status("die", exit_code=None) == JobStatus.finishedWithError

    def test_kill(self):
        assert docker_event_to_status("kill") == JobStatus.finishedWithError

    def test_oom(self):
        assert docker_event_to_status("oom") == JobStatus.finishedWithError

    def test_destroy_ignored(self):
        assert docker_event_to_status("destroy") is None

    def test_unknown_event_ignored(self):
        assert docker_event_to_status("pause") is None
        assert docker_event_to_status("attach") is None


# ── docker_state_to_status ────────────────────────────────────────────────

class TestDockerStateToStatus:
    def test_running(self):
        assert docker_state_to_status({"Running": True}) == JobStatus.started

    def test_paused(self):
        assert docker_state_to_status({"Paused": True}) == JobStatus.started

    def test_created(self):
        assert docker_state_to_status({"Status": "created"}) == JobStatus.notStarted

    def test_oom_killed(self):
        assert docker_state_to_status({"OOMKilled": True}) == JobStatus.finishedWithError

    def test_dead(self):
        assert docker_state_to_status({"Dead": True}) == JobStatus.finishedWithError

    def test_exited_success(self):
        state = {"Status": "exited", "ExitCode": 0}
        assert docker_state_to_status(state) == JobStatus.finishedSuccessfully

    def test_exited_error(self):
        state = {"Status": "exited", "ExitCode": 137}
        assert docker_state_to_status(state) == JobStatus.finishedWithError

    def test_exited_no_exit_code(self):
        state = {"Status": "exited"}
        assert docker_state_to_status(state) == JobStatus.finishedWithError

    def test_unknown_state(self):
        assert docker_state_to_status({}) == JobStatus.undefined


# ── k8s_job_to_status ─────────────────────────────────────────────────────

def _k8s_status(
    conditions=None,
    completion_time=None,
    succeeded=None,
    active=None,
    failed=None,
):
    return SimpleNamespace(
        conditions=conditions,
        completion_time=completion_time,
        succeeded=succeeded,
        active=active,
        failed=failed,
    )


class TestK8sJobToStatus:
    def test_failed_condition(self):
        cond = SimpleNamespace(type="Failed", status="True")
        status = _k8s_status(conditions=[cond])
        assert k8s_job_to_status(status) == JobStatus.finishedWithError

    def test_completed(self):
        from datetime import datetime
        status = _k8s_status(completion_time=datetime.now(), succeeded=1)
        assert k8s_job_to_status(status) == JobStatus.finishedSuccessfully

    def test_active(self):
        status = _k8s_status(active=1)
        assert k8s_job_to_status(status) == JobStatus.started

    def test_all_none_just_created(self):
        status = _k8s_status()
        assert k8s_job_to_status(status) == JobStatus.started

    def test_undefined_fallthrough(self):
        status = _k8s_status(active=0, succeeded=0, failed=1)
        assert k8s_job_to_status(status) == JobStatus.undefined

    def test_non_failed_condition_ignored(self):
        cond = SimpleNamespace(type="Complete", status="True")
        status = _k8s_status(conditions=[cond], active=1)
        assert k8s_job_to_status(status) == JobStatus.started
