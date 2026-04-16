"""
Kubernetes event watcher that produces StatusEvent objects from the K8s API.

Watches Kubernetes Jobs matching a label selector, maps their status to
pfcon-compatible JobStatus values. Uses the kubernetes-asyncio library
for async watch streams.

Resilience features:
  - On startup, lists all matching Jobs and emits their current status
  - Uses resourceVersion for efficient watch resumption
  - Reconnects with exponential backoff on watch stream disconnection
  - Stateless: safe to restart at any time
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Optional

from kubernetes_asyncio import client as k_client, config as k_config, watch as k_watch
from kubernetes_asyncio.client.exceptions import ApiException

from chris_streaming.common.container_naming import resolve_job_type
from chris_streaming.common.pfcon_status import k8s_job_to_status
from chris_streaming.common.schemas import StatusEvent, JobStatus
from chris_streaming.common.settings import EventForwarderSettings
from .watcher import Watcher

logger = logging.getLogger(__name__)

_MAX_RECONNECT_WAIT = 60

# Pod waiting reasons we treat as terminal failures. The Job controller
# only marks the Job "Failed" after backoffLimit retries, which can leave
# the workflow hanging for minutes. Detecting these on the pod lets us
# fail fast and let cleanup proceed.
_TERMINAL_POD_WAITING_REASONS = frozenset({
    "ImagePullBackOff",
    "ErrImagePull",
    "InvalidImageName",
    "CreateContainerConfigError",
    "CreateContainerError",
})


class K8sWatcher(Watcher):
    """Watches Kubernetes Jobs for ChRIS job status changes."""

    def __init__(self, settings: EventForwarderSettings):
        self._settings = settings
        self._batch_api: Optional[k_client.BatchV1Api] = None
        self._core_api: Optional[k_client.CoreV1Api] = None
        self._resource_version: Optional[str] = None
        # Track last-seen status per job to only emit on change
        self._last_status: dict[str, JobStatus] = {}
        # Jobs for which we already emitted a pod-derived failure event,
        # so we don't re-emit on every pod MODIFIED event.
        self._pod_failure_emitted: set[str] = set()

    async def connect(self) -> None:
        try:
            k_config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes config")
        except k_config.ConfigException:
            await k_config.load_kube_config()
            logger.info("Loaded local kubeconfig")
        self._batch_api = k_client.BatchV1Api()
        self._core_api = k_client.CoreV1Api()

    async def close(self) -> None:
        if self._batch_api:
            await self._batch_api.api_client.close()
            self._batch_api = None
        if self._core_api:
            await self._core_api.api_client.close()
            self._core_api = None

    async def watch(self) -> AsyncIterator[StatusEvent]:
        """Yield status events from K8s Jobs and Pods with auto-reconnect.

        Two producers feed a shared queue: the Job watcher (primary status
        source) and the Pod watcher (fast-failure for image-pull errors).
        Both auto-reconnect independently with exponential backoff.
        """
        if self._settings.emit_initial_state:
            async for event in self._emit_initial_state():
                yield event

        queue: asyncio.Queue[StatusEvent | BaseException] = asyncio.Queue()

        async def run_producer(name: str, producer):
            reconnect_attempt = 0
            while True:
                try:
                    async for event in producer():
                        await queue.put(event)
                        reconnect_attempt = 0
                except ApiException as e:
                    # 410 Gone ⇒ resourceVersion expired. A relist is the only
                    # way forward; clear state and re-run initial listing so
                    # we pick up any status transitions we missed while the
                    # watch was stale.
                    if e.status == 410 and name == "Jobs":
                        logger.warning(
                            "Jobs watch: resourceVersion expired (410), relisting"
                        )
                        self._resource_version = None
                        try:
                            async for ev in self._emit_initial_state():
                                await queue.put(ev)
                        except Exception as list_err:
                            logger.warning(
                                "Relist after 410 failed: %s", list_err,
                            )
                        reconnect_attempt = 0
                        continue
                    reconnect_attempt += 1
                    wait = min(2 ** reconnect_attempt, _MAX_RECONNECT_WAIT)
                    logger.warning(
                        "%s watch disconnected: %s. Reconnecting in %ds (attempt %d)",
                        name, e, wait, reconnect_attempt,
                    )
                    await asyncio.sleep(wait)
                except Exception as e:
                    reconnect_attempt += 1
                    wait = min(2 ** reconnect_attempt, _MAX_RECONNECT_WAIT)
                    logger.warning(
                        "%s watch disconnected: %s. Reconnecting in %ds (attempt %d)",
                        name, e, wait, reconnect_attempt,
                    )
                    await asyncio.sleep(wait)

        jobs_task = asyncio.create_task(run_producer("Jobs", self._watch_jobs))
        pods_task = asyncio.create_task(run_producer("Pods", self._watch_pods))

        try:
            while True:
                event = await queue.get()
                yield event
        finally:
            jobs_task.cancel()
            pods_task.cancel()

    async def _emit_initial_state(self) -> AsyncIterator[StatusEvent]:
        """List all matching K8s Jobs and emit their current status."""
        namespace = self._settings.k8s_namespace
        label_selector = self._settings.k8s_label_selector

        job_list = await self._batch_api.list_namespaced_job(
            namespace=namespace,
            label_selector=label_selector,
        )
        self._resource_version = job_list.metadata.resource_version
        logger.info("Initial K8s state: found %d matching jobs", len(job_list.items))

        for job in job_list.items:
            event = await self._k8s_job_to_event(job)
            if event is not None:
                self._last_status[job.metadata.name] = event.status
                yield event

    async def _watch_jobs(self) -> AsyncIterator[StatusEvent]:
        """Watch K8s Jobs for status changes using the Watch API."""
        namespace = self._settings.k8s_namespace
        label_selector = self._settings.k8s_label_selector

        w = k_watch.Watch()
        kwargs = {
            "namespace": namespace,
            "label_selector": label_selector,
            "timeout_seconds": 0,  # infinite watch
        }
        if self._resource_version:
            kwargs["resource_version"] = self._resource_version

        async for raw_event in w.stream(
            self._batch_api.list_namespaced_job, **kwargs
        ):
            event_type = raw_event["type"]
            job = raw_event["object"]

            # Update resource version for resume
            if job.metadata.resource_version:
                self._resource_version = job.metadata.resource_version

            if event_type in ("ADDED", "MODIFIED"):
                status_event = await self._k8s_job_to_event(job)
                if status_event is None:
                    continue

                # Only emit if status actually changed
                job_key = job.metadata.name
                prev = self._last_status.get(job_key)
                if prev == status_event.status:
                    continue
                status_event.previous_status = prev
                self._last_status[job_key] = status_event.status
                yield status_event

            elif event_type == "DELETED":
                # Clean up tracking
                self._last_status.pop(job.metadata.name, None)
                self._pod_failure_emitted.discard(job.metadata.name)

    async def _watch_pods(self) -> AsyncIterator[StatusEvent]:
        """Watch pods for unrecoverable image/config errors.

        Emits ``finishedWithError`` for the owning Job as soon as any of its
        pods lands in a terminal waiting state (ImagePullBackOff, etc.).
        Deduped per Job so we emit at most one failure event, and only if
        the Job watcher hasn't already reported a terminal status.
        """
        namespace = self._settings.k8s_namespace
        label_selector = self._settings.k8s_label_selector

        w = k_watch.Watch()
        async for raw_event in w.stream(
            self._core_api.list_namespaced_pod,
            namespace=namespace,
            label_selector=label_selector,
            timeout_seconds=0,
        ):
            if raw_event["type"] not in ("ADDED", "MODIFIED"):
                continue
            pod = raw_event["object"]
            event = self._pod_failure_event(pod)
            if event is not None:
                yield event

    def _pod_failure_event(self, pod) -> Optional[StatusEvent]:
        """Return a failure StatusEvent if a pod is stuck in a terminal waiting state."""
        owners = pod.metadata.owner_references or []
        job_owner = next((o for o in owners if o.kind == "Job"), None)
        if job_owner is None:
            return None
        job_name = job_owner.name

        # If the Job watcher already emitted a terminal status, defer to it.
        last = self._last_status.get(job_name)
        if last in (JobStatus.finishedSuccessfully, JobStatus.finishedWithError):
            return None
        if job_name in self._pod_failure_emitted:
            return None

        statuses = list(pod.status.container_statuses or []) + list(
            pod.status.init_container_statuses or []
        )
        terminated_exit_code: Optional[int] = None
        for cs in statuses:
            terminated = getattr(cs.state, "terminated", None) if cs.state else None
            if terminated is not None and terminated.exit_code is not None:
                terminated_exit_code = int(terminated.exit_code)
                break
        for cs in statuses:
            waiting = getattr(cs.state, "waiting", None) if cs.state else None
            reason = getattr(waiting, "reason", None) if waiting else None
            if reason in _TERMINAL_POD_WAITING_REASONS:
                message = getattr(waiting, "message", "") or f"pod {reason}"
                logger.warning(
                    "Pod %s for Job %s is %s: %s — emitting finishedWithError",
                    pod.metadata.name, job_name, reason, message,
                )
                self._pod_failure_emitted.add(job_name)
                self._last_status[job_name] = JobStatus.finishedWithError

                # Parse from the owning Job's name, not the pod name — the
                # pod carries a random suffix that breaks name-based parsing.
                labels = {**(pod.metadata.labels or {})}
                job_id, job_type = resolve_job_type(job_name, labels)
                # job_id is further cleaned by pfcon's name convention:
                # plugin Jobs are named `{job_id}` exactly, others carry a
                # `-{type}` suffix already handled by parse_container_name.
                image = cs.image or ""
                return StatusEvent(
                    job_id=job_id,
                    job_type=job_type,
                    status=JobStatus.finishedWithError,
                    image=image,
                    cmd="",
                    message=f"{reason}: {message}",
                    exit_code=terminated_exit_code,
                    timestamp=datetime.now(timezone.utc),
                    source="kubernetes",
                )
        return None

    async def _k8s_job_to_event(self, job) -> Optional[StatusEvent]:
        """Convert a K8s Job object to a StatusEvent."""
        name = job.metadata.name
        labels = job.metadata.labels or {}
        pod_labels = {}
        if job.spec.template and job.spec.template.metadata:
            pod_labels = job.spec.template.metadata.labels or {}

        # Merge job-level and pod-level labels for job_type resolution
        all_labels = {**labels, **pod_labels}
        job_id, job_type = resolve_job_type(name, all_labels)

        status = k8s_job_to_status(job.status)

        image = ""
        cmd = ""
        containers = (job.spec.template.spec.containers or []) if job.spec.template else []
        if containers:
            image = containers[0].image or ""
            cmd = " ".join(containers[0].command or [])

        timestamp = (
            job.status.completion_time
            or job.status.start_time
            or job.metadata.creation_timestamp
            or datetime.now(timezone.utc)
        )
        if hasattr(timestamp, "tzinfo") and timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)

        exit_code = None
        if status in (
            JobStatus.finishedSuccessfully,
            JobStatus.finishedWithError,
            JobStatus.undefined,
        ):
            exit_code = await self._fetch_exit_code(name)

        return StatusEvent(
            job_id=job_id,
            job_type=job_type,
            status=status,
            image=image,
            cmd=cmd,
            message="",
            exit_code=exit_code,
            timestamp=timestamp,
            source="kubernetes",
        )

    async def _fetch_exit_code(self, job_name: str) -> Optional[int]:
        """Look up the exit code of the terminated container for a Job's pod.

        K8s carries exit codes on the pod, not the Job. Queries pods owned by
        the Job and returns the first terminated main-container exit code.
        Returns None if no pod is found or none have terminated yet.
        """
        try:
            pods = await self._core_api.list_namespaced_pod(
                namespace=self._settings.k8s_namespace,
                label_selector=f"job-name={job_name}",
            )
        except Exception as e:
            logger.warning("Failed to fetch pods for Job %s: %s", job_name, e)
            return None

        for pod in pods.items:
            for cs in pod.status.container_statuses or []:
                terminated = getattr(cs.state, "terminated", None) if cs.state else None
                if terminated is not None and terminated.exit_code is not None:
                    return int(terminated.exit_code)
        return None
