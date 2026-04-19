"""
Kubernetes log tailer.

Watches Pods matching a label selector, opens a follow-mode log stream
per pod via CoreV1Api, and yields :class:`LogLine` objects.

Notes:
  - Each Pod may have multiple containers; we stream the first (main)
    container. Plugin containers in ChRIS are single-container pods.
  - Streams close automatically when the pod terminates.
  - Replay-safety is enforced by a per-tailer "completed" ledger: once a
    pod's tail has exited, subsequent MODIFIED events (phase transitions,
    finalizer/label updates, or a watch reconnect's relist) do not re-spawn
    a tailer. Without the ledger, ``read_namespaced_pod_log(follow=True)``
    re-streams from container start every time.
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
from chris_streaming.common.schemas import JobType
from .tailer import LogLine, _parse_docker_log_line

logger = logging.getLogger(__name__)

_MAX_RECONNECT_WAIT = 60


class K8sLogTailer:
    """Tails logs from all ChRIS job Pods in a namespace."""

    def __init__(
        self,
        namespace: str,
        label_selector: str,
    ) -> None:
        self._namespace = namespace
        self._label_selector = label_selector
        self._core: Optional[k_client.CoreV1Api] = None
        self._tasks: dict[str, asyncio.Task] = {}
        # Pods whose tail has already exited (success or non-retryable error).
        # Prevents duplicate streaming when K8s fires further MODIFIED events
        # on a terminated pod or when the watch reconnects and relists.
        self._completed: set[str] = set()
        self._queue: asyncio.Queue[LogLine] = asyncio.Queue(maxsize=10_000)
        self._running = False

    async def connect(self) -> None:
        try:
            k_config.load_incluster_config()
            logger.info("K8s log tailer: loaded in-cluster config")
        except k_config.ConfigException:
            await k_config.load_kube_config()
            logger.info("K8s log tailer: loaded local kubeconfig")
        self._core = k_client.CoreV1Api()

    async def close(self) -> None:
        self._running = False
        for task in list(self._tasks.values()):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()
        if self._core is not None:
            await self._core.api_client.close()
            self._core = None

    async def __aenter__(self) -> "K8sLogTailer":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def stream(self) -> AsyncIterator[LogLine]:
        """Yield enriched log lines until ``close()`` is called."""
        self._running = True
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
        """Discover running pods and keep a tail task per running pod."""
        backoff = 1.0
        while self._running:
            try:
                await self._bootstrap_running()
                await self._watch_pods()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except ApiException as e:
                logger.error("K8s log tailer API error: %s", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _MAX_RECONNECT_WAIT)
            except Exception as e:
                logger.error("K8s log tailer error: %s", e, exc_info=True)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _MAX_RECONNECT_WAIT)

    async def _bootstrap_running(self) -> None:
        """List pods with started containers and spawn tail tasks for each."""
        assert self._core is not None
        pods = await self._core.list_namespaced_pod(
            namespace=self._namespace,
            label_selector=self._label_selector,
        )
        for pod in pods.items:
            if _container_started(pod):
                await self._spawn_tail(pod)

    async def _watch_pods(self) -> None:
        """Watch for Pods whose container has started and spawn tail tasks."""
        assert self._core is not None
        w = k_watch.Watch()
        async for raw in w.stream(
            self._core.list_namespaced_pod,
            namespace=self._namespace,
            label_selector=self._label_selector,
            timeout_seconds=0,
        ):
            ev_type = raw["type"]
            pod = raw["object"]
            if ev_type in ("ADDED", "MODIFIED") and _container_started(pod):
                await self._spawn_tail(pod)
            elif ev_type == "DELETED":
                # Pod is gone from the cluster — drop it from the ledger so the
                # set doesn't grow unboundedly over the tailer's lifetime.
                name = pod.metadata.name if pod.metadata else None
                if name:
                    self._completed.discard(name)

    async def _spawn_tail(self, pod) -> None:
        pod_name = pod.metadata.name
        if pod_name in self._completed:
            return
        if pod_name in self._tasks and not self._tasks[pod_name].done():
            return
        self._tasks[pod_name] = asyncio.create_task(self._tail_one(pod))

    async def _tail_one(self, pod) -> None:
        """Stream log lines from the first container of ``pod``.

        When the pod's log stream closes (pod terminated + logs drained),
        emit an EOS sentinel so downstream can mark this container's logs
        as complete. Errors skip EOS and defer to the cleanup quiescence
        backstop.
        """
        assert self._core is not None
        pod_name = pod.metadata.name
        emit_eos = False
        # Only true when we hit a transient condition where the next MODIFIED
        # event should be allowed to spawn a fresh tail. All other exits
        # (success, 404, unexpected errors) mark the pod as completed so we
        # don't replay logs on later events.
        retry_possible = False
        owner_name = ""
        job_id = ""
        job_type: Optional[JobType] = None
        try:
            containers = (pod.spec.containers or []) if pod.spec else []
            if not containers:
                return
            container_name = containers[0].name

            labels = pod.metadata.labels or {}
            # Resolve job_id/job_type from the pod's owning Job if present,
            # otherwise from the pod name directly.
            owner_name = _owning_job_name(pod) or pod_name
            job_id, job_type = resolve_job_type(owner_name, labels)

            resp = await self._core.read_namespaced_pod_log(
                name=pod_name,
                namespace=self._namespace,
                container=container_name,
                follow=True,
                timestamps=True,
                _preload_content=False,
            )
            async for chunk in resp.content.iter_any():
                if not chunk:
                    continue
                text = chunk.decode("utf-8", errors="replace")
                for raw_line in text.splitlines():
                    if not raw_line:
                        continue
                    ts, _, message = _parse_docker_log_line(raw_line + "\n")
                    await self._queue.put(
                        LogLine(
                            job_id=job_id,
                            job_type=job_type,
                            container_name=owner_name,
                            # K8s merges stdout+stderr into a single log file
                            # before we see it; the distinction cannot be
                            # recovered through read_namespaced_pod_log.
                            stream="",
                            line=message,
                            timestamp=ts,
                        )
                    )
            emit_eos = True
        except asyncio.CancelledError:
            raise
        except ApiException as e:
            if e.status == 400 and _is_container_waiting_error(e):
                # The container-state gate should prevent this, but a quick
                # waiting→running→waiting transition (e.g., a crashloop
                # between MODIFIED events) can race us. Leave the pod out of
                # the completed ledger so the next MODIFIED event can retry.
                logger.debug("Pod %s not yet ready for log stream: %s", pod_name, e.reason)
                retry_possible = True
            elif e.status == 404:
                # Pod deleted between spawn and the log API call (e.g., pfcon
                # cleanup). Nothing more to read; don't retry.
                logger.debug("Pod %s deleted before log stream could open", pod_name)
            else:
                logger.warning("Tail error for pod %s: %s", pod_name, e)
        except Exception as e:
            logger.warning("Tail error for pod %s: %s", pod_name, e)
        finally:
            if emit_eos and job_type is not None:
                await self._queue.put(
                    LogLine(
                        job_id=job_id,
                        job_type=job_type,
                        container_name=owner_name,
                        stream="",
                        line="",
                        timestamp=datetime.now(timezone.utc),
                        eos=True,
                    )
                )
            if not retry_possible:
                self._completed.add(pod_name)
            self._tasks.pop(pod_name, None)


def _owning_job_name(pod) -> Optional[str]:
    """Return the owning Job's name, if any."""
    owners = (pod.metadata.owner_references or []) if pod.metadata else []
    for o in owners:
        if o.kind == "Job" and o.name:
            return o.name
    return None


def _container_started(pod) -> bool:
    """Return True once the pod's main container has started.

    ``read_namespaced_pod_log`` rejects requests against a container still
    in ``waiting`` state (ContainerCreating, ImagePullBackOff, etc.) with a
    400 BadRequest, so we gate on container state rather than pod phase —
    pod phase stays ``Pending`` for the entire image-pull/volume-mount
    window during which the container is not yet loggable.

    Returns True when the first container's state is ``running`` or
    ``terminated`` (terminated containers still have readable logs).
    """
    if not pod.status:
        return False
    statuses = pod.status.container_statuses or []
    if not statuses:
        return False
    state = statuses[0].state
    if state is None:
        return False
    return (
        getattr(state, "running", None) is not None
        or getattr(state, "terminated", None) is not None
    )


def _is_container_waiting_error(exc: ApiException) -> bool:
    """Detect the 'container is waiting to start' 400 from the K8s log API."""
    body = (exc.body or "") if hasattr(exc, "body") else ""
    return "is waiting to start" in body

