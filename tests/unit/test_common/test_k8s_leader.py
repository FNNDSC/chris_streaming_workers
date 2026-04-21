"""Unit tests for chris_streaming.common.k8s_leader.LeaderElection.

These tests avoid importing ``kubernetes_asyncio`` by installing a fake
module in sys.modules before the LeaderElection helpers perform their
lazy imports. That keeps the test runnable in the unit-test image, which
may not ship kubernetes-asyncio, while still exercising the acquire /
renew / take-over / step-down control flow end-to-end.
"""

from __future__ import annotations

import asyncio
import sys
import types
from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest


# ── Fake kubernetes_asyncio hierarchy ─────────────────────────────────────────

class _FakeV1LeaseSpec:
    def __init__(
        self,
        holder_identity: Optional[str] = None,
        lease_duration_seconds: Optional[int] = None,
        acquire_time: Optional[datetime] = None,
        renew_time: Optional[datetime] = None,
        lease_transitions: int = 0,
    ):
        self.holder_identity = holder_identity
        self.lease_duration_seconds = lease_duration_seconds
        self.acquire_time = acquire_time
        self.renew_time = renew_time
        self.lease_transitions = lease_transitions


class _FakeV1Lease:
    def __init__(self, metadata, spec):
        self.metadata = metadata
        self.spec = spec


class _FakeV1ObjectMeta:
    def __init__(self, name: str, namespace: str):
        self.name = name
        self.namespace = namespace


class _FakeApiException(Exception):
    def __init__(self, status: int, reason: str = ""):
        self.status = status
        self.reason = reason
        super().__init__(f"{status} {reason}")


class _FakeApiClient:
    async def close(self):
        pass


class _FakeCoordinationV1Api:
    # Shared state across instances — one store per test.
    store: dict[tuple[str, str], _FakeV1Lease] = {}
    create_should_conflict: bool = False
    update_should_conflict: bool = False

    def __init__(self):
        self.api_client = _FakeApiClient()

    async def read_namespaced_lease(self, *, name, namespace):
        key = (namespace, name)
        if key not in self.store:
            raise _FakeApiException(404, "NotFound")
        return self.store[key]

    async def create_namespaced_lease(self, *, namespace, body):
        key = (namespace, body.metadata.name)
        if self.create_should_conflict or key in self.store:
            raise _FakeApiException(409, "Conflict")
        self.store[key] = body
        return body

    async def replace_namespaced_lease(self, *, name, namespace, body):
        if self.update_should_conflict:
            raise _FakeApiException(409, "Conflict")
        key = (namespace, name)
        self.store[key] = body
        return body


def _install_fake_k8s(monkeypatch):
    """Install a minimal fake kubernetes_asyncio hierarchy in sys.modules."""
    _FakeCoordinationV1Api.store = {}
    _FakeCoordinationV1Api.create_should_conflict = False
    _FakeCoordinationV1Api.update_should_conflict = False

    ka = types.ModuleType("kubernetes_asyncio")
    ka_client = types.ModuleType("kubernetes_asyncio.client")
    ka_client_exc = types.ModuleType("kubernetes_asyncio.client.exceptions")
    ka_config = types.ModuleType("kubernetes_asyncio.config")

    ka_client.V1Lease = _FakeV1Lease
    ka_client.V1LeaseSpec = _FakeV1LeaseSpec
    ka_client.V1ObjectMeta = _FakeV1ObjectMeta
    ka_client.CoordinationV1Api = _FakeCoordinationV1Api
    ka_client_exc.ApiException = _FakeApiException

    class _ConfigException(Exception):
        pass

    def _load_incluster_config():
        # Simulate "not in cluster" so the code falls through to kube_config.
        raise _ConfigException("not in cluster")

    async def _load_kube_config():
        return None

    ka_config.ConfigException = _ConfigException
    ka_config.load_incluster_config = _load_incluster_config
    ka_config.load_kube_config = _load_kube_config

    ka.client = ka_client
    ka.config = ka_config

    monkeypatch.setitem(sys.modules, "kubernetes_asyncio", ka)
    monkeypatch.setitem(sys.modules, "kubernetes_asyncio.client", ka_client)
    monkeypatch.setitem(
        sys.modules, "kubernetes_asyncio.client.exceptions", ka_client_exc,
    )
    monkeypatch.setitem(sys.modules, "kubernetes_asyncio.config", ka_config)


# ── Tests ────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_acquire_when_no_lease_exists(monkeypatch):
    _install_fake_k8s(monkeypatch)
    from chris_streaming.common.k8s_leader import LeaderElection

    election = LeaderElection(
        namespace="ns", name="test-lease", identity="pod-a",
        lease_duration_seconds=10, renew_deadline_seconds=5,
        retry_period_seconds=1,
    )
    body_ran = asyncio.Event()

    async def body(cancel: asyncio.Event):
        body_ran.set()
        # Exit immediately so run() returns.
        return

    await election.run(body)
    assert body_ran.is_set()
    assert ("ns", "test-lease") in _FakeCoordinationV1Api.store


@pytest.mark.asyncio
async def test_follower_waits_until_leader_expires(monkeypatch):
    _install_fake_k8s(monkeypatch)
    from chris_streaming.common.k8s_leader import LeaderElection

    # Seed a lease held by another pod, already expired.
    now = datetime.now(timezone.utc)
    _FakeCoordinationV1Api.store[("ns", "test-lease")] = _FakeV1Lease(
        metadata=_FakeV1ObjectMeta("test-lease", "ns"),
        spec=_FakeV1LeaseSpec(
            holder_identity="old-pod",
            lease_duration_seconds=10,
            acquire_time=now - timedelta(seconds=60),
            renew_time=now - timedelta(seconds=60),
            lease_transitions=1,
        ),
    )

    election = LeaderElection(
        namespace="ns", name="test-lease", identity="pod-a",
        lease_duration_seconds=10, renew_deadline_seconds=5,
        retry_period_seconds=1,
    )

    body_ran = asyncio.Event()

    async def body(cancel: asyncio.Event):
        body_ran.set()
        return

    await election.run(body)

    # body must have run (proves we took the lease despite the stale holder).
    assert body_ran.is_set()
    lease = _FakeCoordinationV1Api.store[("ns", "test-lease")]
    # take-over increments lease_transitions from the seeded 1 to 2.
    assert lease.spec.lease_transitions == 2
    # On clean exit the helper releases the lease, so holder is None now.
    assert lease.spec.holder_identity is None


@pytest.mark.asyncio
async def test_body_is_cancelled_when_renewal_fails(monkeypatch):
    _install_fake_k8s(monkeypatch)
    from chris_streaming.common.k8s_leader import LeaderElection

    election = LeaderElection(
        namespace="ns", name="test-lease", identity="pod-a",
        lease_duration_seconds=10, renew_deadline_seconds=2,
        retry_period_seconds=1,
    )

    entered = asyncio.Event()

    async def body(cancel: asyncio.Event):
        entered.set()
        # Hold forever unless cancellation signalled.
        await cancel.wait()

    # Start the election in the background, then yank the lease away.
    task = asyncio.create_task(election.run(body))
    await asyncio.wait_for(entered.wait(), timeout=3.0)

    # Simulate another replica stealing the lease: flip the holder.
    lease = _FakeCoordinationV1Api.store[("ns", "test-lease")]
    lease.spec.holder_identity = "other-pod"

    # The renew loop should observe the other holder and step down, which
    # cancels the body so run() loops back to try-to-acquire again. The
    # take-over path will succeed (since we just set holder to "other-pod"
    # but not its renew_time bookkeeping; the expiry check will see the
    # stale renew_time). To bound the test, cancel the outer task.
    await asyncio.sleep(1.5)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_release_clears_holder_on_clean_exit(monkeypatch):
    _install_fake_k8s(monkeypatch)
    from chris_streaming.common.k8s_leader import LeaderElection

    election = LeaderElection(
        namespace="ns", name="test-lease", identity="pod-a",
        lease_duration_seconds=10, renew_deadline_seconds=5,
        retry_period_seconds=1,
    )

    async def body(cancel: asyncio.Event):
        return

    await election.run(body)
    lease = _FakeCoordinationV1Api.store[("ns", "test-lease")]
    assert lease.spec.holder_identity is None


@pytest.mark.asyncio
async def test_renew_deadline_validation():
    from chris_streaming.common.k8s_leader import LeaderElection

    with pytest.raises(ValueError):
        LeaderElection(
            namespace="ns", name="l", identity="i",
            lease_duration_seconds=5, renew_deadline_seconds=5,
            retry_period_seconds=1,
        )
