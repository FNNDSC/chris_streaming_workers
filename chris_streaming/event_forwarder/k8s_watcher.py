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

from chris_streaming.common.container_naming import resolve_job_type
from chris_streaming.common.pfcon_status import k8s_job_to_status
from chris_streaming.common.schemas import StatusEvent, JobStatus
from chris_streaming.common.settings import EventForwarderSettings
from .watcher import Watcher

logger = logging.getLogger(__name__)

_MAX_RECONNECT_WAIT = 60


class K8sWatcher(Watcher):
    """Watches Kubernetes Jobs for ChRIS job status changes."""

    def __init__(self, settings: EventForwarderSettings):
        self._settings = settings
        self._batch_api: Optional[k_client.BatchV1Api] = None
        self._core_api: Optional[k_client.CoreV1Api] = None
        self._resource_version: Optional[str] = None
        # Track last-seen status per job to only emit on change
        self._last_status: dict[str, JobStatus] = {}

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
        """Yield status events from K8s Jobs with auto-reconnect."""
        if self._settings.emit_initial_state:
            async for event in self._emit_initial_state():
                yield event

        reconnect_attempt = 0
        while True:
            try:
                async for event in self._watch_jobs():
                    yield event
                    reconnect_attempt = 0
            except Exception as e:
                reconnect_attempt += 1
                wait = min(2 ** reconnect_attempt, _MAX_RECONNECT_WAIT)
                logger.warning(
                    "K8s watch disconnected: %s. Reconnecting in %ds (attempt %d)",
                    e, wait, reconnect_attempt,
                )
                await asyncio.sleep(wait)
                try:
                    await self.close()
                except Exception:
                    pass
                await self.connect()
                # Re-list after reconnect to catch missed events
                async for event in self._emit_initial_state():
                    yield event

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
            event = self._k8s_job_to_event(job)
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
                status_event = self._k8s_job_to_event(job)
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

    def _k8s_job_to_event(self, job) -> Optional[StatusEvent]:
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

        return StatusEvent(
            job_id=job_id,
            job_type=job_type,
            status=status,
            image=image,
            cmd=cmd,
            message="",
            timestamp=timestamp,
            source="kubernetes",
        )
