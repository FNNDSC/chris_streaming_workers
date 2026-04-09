"""
Maps native Docker / Kubernetes states to pfcon's JobStatus enum.

Extracted from pfcon's dockermgr.py and kubernetesmgr.py so that the
Event Forwarder produces the same status values as pfcon itself.
"""

from __future__ import annotations

from .schemas import JobStatus


# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------

def docker_event_to_status(event_status: str, exit_code: int | None = None) -> JobStatus | None:
    """
    Map a Docker event 'status' field to a pfcon JobStatus.

    Docker events we care about:
      create   -> notStarted
      start    -> started
      die      -> finishedSuccessfully (exit 0) or finishedWithError (non-zero)
      kill     -> finishedWithError
      oom      -> finishedWithError
      destroy  -> ignored (container removed, we already saw 'die')

    Returns None for events that don't represent a status change.
    """
    match event_status:
        case "create":
            return JobStatus.notStarted
        case "start":
            return JobStatus.started
        case "die":
            if exit_code is not None and exit_code == 0:
                return JobStatus.finishedSuccessfully
            return JobStatus.finishedWithError
        case "kill" | "oom":
            return JobStatus.finishedWithError
        case _:
            return None


def docker_state_to_status(state: dict) -> JobStatus:
    """
    Map a Docker container's full State dict (from inspect) to a pfcon JobStatus.

    This replicates pfcon's _get_status_from() in dockermgr.py lines 115-128.
    Used on startup to emit the current state of existing containers.
    """
    if state.get("Running") or state.get("Paused"):
        return JobStatus.started
    if state.get("Status") == "created":
        return JobStatus.notStarted
    if state.get("OOMKilled") or state.get("Dead"):
        return JobStatus.finishedWithError
    if state.get("Status") == "exited":
        if state.get("ExitCode", -1) == 0:
            return JobStatus.finishedSuccessfully
        return JobStatus.finishedWithError
    return JobStatus.undefined


# ---------------------------------------------------------------------------
# Kubernetes
# ---------------------------------------------------------------------------

def k8s_job_to_status(job_status) -> JobStatus:
    """
    Map a Kubernetes Job status object to a pfcon JobStatus.

    Replicates pfcon's KubernetesManager.get_job_info() logic from
    kubernetesmgr.py lines 85-129.

    Args:
        job_status: A kubernetes_asyncio V1JobStatus object with attributes:
                    conditions, completion_time, succeeded, active, failed.
    """
    if job_status.conditions:
        for condition in job_status.conditions:
            if condition.type == "Failed" and condition.status == "True":
                return JobStatus.finishedWithError

    if job_status.completion_time is not None and job_status.succeeded:
        return JobStatus.finishedSuccessfully

    if job_status.active:
        return JobStatus.started

    # Edge case: all fields are None (job just created)
    if (job_status.active is None
            and job_status.succeeded is None
            and job_status.failed is None):
        return JobStatus.started

    return JobStatus.undefined
