"""
Kubernetes leader election via the ``coordination.k8s.io/v1`` Lease API.

The Event Forwarder and Log Forwarder are pure event sources: each replica
watches Kubernetes independently and emits events. With multiple replicas
running at once the same event is produced by each, which is not safe —
the ``event_id`` of some events (log lines, pod-derived failures, EOS
markers) includes ``datetime.now()``, so dedup on retry cannot catch
cross-replica duplicates.

Rather than solving cross-replica dedup we let only one replica emit at a
time. Losing the Lease immediately cancels the follower body; the new
leader takes over by reading the current cluster state (initial list,
XADD stream positions) so no events are missed.

Rollout semantics: the Deployment's ``strategy: Recreate`` guarantees that
the old replica is fully torn down before the new one starts — but during
voluntary eviction or crash-loops, the lease TTL is the backstop that
caps duplicate-emission windows.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


def _default_identity() -> str:
    """Stable identity for this process.

    Prefer ``POD_NAME`` (injected via Downward API) so holder identity is
    human-readable in ``kubectl describe lease``. Fall back to hostname +
    random suffix so two identical containers never collide.
    """
    pod_name = os.environ.get("POD_NAME")
    if pod_name:
        return pod_name
    return f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"


class LeaderElection:
    """Acquire and hold a ``coordination.k8s.io/v1`` Lease.

    Usage::

        election = LeaderElection(namespace="chris-streaming", name="event-forwarder")
        await election.run(body)

    ``body`` is an async callable that receives a ``cancel_event``
    (``asyncio.Event``) it must honour — when leadership is lost, the
    event is set and ``body`` must exit promptly.
    """

    def __init__(
        self,
        *,
        namespace: str,
        name: str,
        identity: str | None = None,
        lease_duration_seconds: int = 15,
        renew_deadline_seconds: int = 10,
        retry_period_seconds: int = 2,
    ):
        if renew_deadline_seconds >= lease_duration_seconds:
            raise ValueError(
                "renew_deadline_seconds must be < lease_duration_seconds"
            )
        self._namespace = namespace
        self._name = name
        self._identity = identity or _default_identity()
        self._lease_duration = lease_duration_seconds
        self._renew_deadline = renew_deadline_seconds
        self._retry_period = retry_period_seconds
        self._coord_api = None

    @property
    def identity(self) -> str:
        return self._identity

    async def _load_config(self) -> None:
        from kubernetes_asyncio import client as k_client, config as k_config

        try:
            k_config.load_incluster_config()
        except k_config.ConfigException:
            await k_config.load_kube_config()
        self._coord_api = k_client.CoordinationV1Api()

    async def _close(self) -> None:
        if self._coord_api is not None:
            await self._coord_api.api_client.close()
            self._coord_api = None

    async def run(
        self,
        body: Callable[[asyncio.Event], Awaitable[None]],
    ) -> None:
        """Keep trying to acquire leadership and run ``body`` while leader.

        Returns when ``body`` returns normally. If leadership is lost
        mid-run, ``body`` is cancelled and the loop resumes, trying to
        acquire the lease again.
        """
        await self._load_config()
        try:
            while True:
                acquired = await self._acquire()
                if not acquired:
                    await asyncio.sleep(self._retry_period)
                    continue

                logger.info(
                    "Acquired leader lease %s/%s as %s",
                    self._namespace, self._name, self._identity,
                )

                cancel_event = asyncio.Event()
                renew_task = asyncio.create_task(
                    self._renew_loop(cancel_event),
                    name=f"lease-renew-{self._name}",
                )
                body_task = asyncio.create_task(
                    body(cancel_event),
                    name=f"leader-body-{self._name}",
                )

                done, pending = await asyncio.wait(
                    {renew_task, body_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                # Signal the body to stop and tear down the renew loop.
                cancel_event.set()
                for t in pending:
                    t.cancel()
                for t in pending:
                    try:
                        await t
                    except (asyncio.CancelledError, Exception):
                        pass

                # Best-effort: release the lease on clean body exit so the
                # next replica can take over immediately instead of waiting
                # for TTL expiry. If we got here because renewal failed,
                # the lease is already abandoned.
                if body_task in done and not body_task.cancelled():
                    try:
                        body_exc = body_task.exception()
                    except asyncio.CancelledError:
                        body_exc = None
                    await self._release()
                    if body_exc is not None:
                        raise body_exc
                    return
                # Renewal failed: loop and try to re-acquire.
                logger.warning(
                    "Lost leader lease %s/%s, re-contesting",
                    self._namespace, self._name,
                )
        finally:
            await self._close()

    async def _acquire(self) -> bool:
        """Try to acquire the lease. Returns True on success."""
        from kubernetes_asyncio.client.exceptions import ApiException

        try:
            lease = await self._coord_api.read_namespaced_lease(
                name=self._name, namespace=self._namespace,
            )
        except ApiException as e:
            if e.status == 404:
                return await self._create_lease()
            logger.warning("Lease read failed: %s", e)
            return False
        except Exception as e:
            logger.warning("Lease read failed: %s", e)
            return False

        spec = lease.spec
        now = datetime.now(timezone.utc)
        holder = getattr(spec, "holder_identity", None)
        renew_time = getattr(spec, "renew_time", None)
        duration = getattr(spec, "lease_duration_seconds", None) or self._lease_duration

        if holder == self._identity:
            return await self._renew(lease)

        if holder and renew_time is not None:
            if renew_time.tzinfo is None:
                renew_time = renew_time.replace(tzinfo=timezone.utc)
            if now < renew_time + timedelta(seconds=duration):
                # Lease still valid; someone else holds it.
                return False

        # Lease is either unheld or expired; take it.
        return await self._take_over(lease)

    async def _create_lease(self) -> bool:
        from kubernetes_asyncio import client as k_client
        from kubernetes_asyncio.client.exceptions import ApiException

        body = k_client.V1Lease(
            metadata=k_client.V1ObjectMeta(
                name=self._name, namespace=self._namespace,
            ),
            spec=k_client.V1LeaseSpec(
                holder_identity=self._identity,
                lease_duration_seconds=self._lease_duration,
                acquire_time=datetime.now(timezone.utc),
                renew_time=datetime.now(timezone.utc),
                lease_transitions=1,
            ),
        )
        try:
            await self._coord_api.create_namespaced_lease(
                namespace=self._namespace, body=body,
            )
            return True
        except ApiException as e:
            if e.status == 409:
                return False  # Another replica beat us to it.
            logger.warning("Lease create failed: %s", e)
            return False

    async def _take_over(self, lease) -> bool:
        from kubernetes_asyncio.client.exceptions import ApiException

        transitions = (getattr(lease.spec, "lease_transitions", 0) or 0) + 1
        lease.spec.holder_identity = self._identity
        lease.spec.lease_duration_seconds = self._lease_duration
        lease.spec.acquire_time = datetime.now(timezone.utc)
        lease.spec.renew_time = datetime.now(timezone.utc)
        lease.spec.lease_transitions = transitions
        try:
            await self._coord_api.replace_namespaced_lease(
                name=self._name, namespace=self._namespace, body=lease,
            )
            return True
        except ApiException as e:
            # 409 conflict: another replica took it first; retry next tick.
            if e.status == 409:
                return False
            logger.warning("Lease take-over failed: %s", e)
            return False

    async def _renew(self, lease) -> bool:
        from kubernetes_asyncio.client.exceptions import ApiException

        lease.spec.renew_time = datetime.now(timezone.utc)
        try:
            await self._coord_api.replace_namespaced_lease(
                name=self._name, namespace=self._namespace, body=lease,
            )
            return True
        except ApiException as e:
            logger.warning("Lease renew failed: %s", e)
            return False

    async def _renew_loop(self, cancel_event: asyncio.Event) -> None:
        """Renew the lease periodically; set cancel_event on failure."""
        from kubernetes_asyncio.client.exceptions import ApiException

        last_renewed = datetime.now(timezone.utc)
        try:
            while not cancel_event.is_set():
                await asyncio.sleep(self._retry_period)
                try:
                    lease = await self._coord_api.read_namespaced_lease(
                        name=self._name, namespace=self._namespace,
                    )
                except (ApiException, Exception) as e:
                    logger.warning("Lease read for renew failed: %s", e)
                    if (datetime.now(timezone.utc) - last_renewed
                            > timedelta(seconds=self._renew_deadline)):
                        return
                    continue

                if getattr(lease.spec, "holder_identity", None) != self._identity:
                    logger.warning(
                        "Lost leadership: lease now held by %s",
                        getattr(lease.spec, "holder_identity", None),
                    )
                    return

                ok = await self._renew(lease)
                if ok:
                    last_renewed = datetime.now(timezone.utc)
                elif (datetime.now(timezone.utc) - last_renewed
                        > timedelta(seconds=self._renew_deadline)):
                    logger.warning(
                        "Failed to renew lease for >%ds; stepping down",
                        self._renew_deadline,
                    )
                    return
        except asyncio.CancelledError:
            raise

    async def _release(self) -> None:
        """Best-effort release: clear holder_identity so the next replica can take over."""
        from kubernetes_asyncio.client.exceptions import ApiException

        try:
            lease = await self._coord_api.read_namespaced_lease(
                name=self._name, namespace=self._namespace,
            )
            if getattr(lease.spec, "holder_identity", None) != self._identity:
                return
            lease.spec.holder_identity = None
            lease.spec.renew_time = None
            await self._coord_api.replace_namespaced_lease(
                name=self._name, namespace=self._namespace, body=lease,
            )
        except ApiException as e:
            logger.debug("Lease release failed (non-fatal): %s", e)
        except Exception as e:
            logger.debug("Lease release failed (non-fatal): %s", e)
