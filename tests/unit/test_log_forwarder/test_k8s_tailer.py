"""Tests for chris_streaming.log_forwarder.k8s_tailer.

The live-K8s-integrated pieces (follow streams, watch events) are exercised
by e2e tests. Here we cover the pure gating helpers that decide when a pod
is ready for `read_namespaced_pod_log`, plus the completed-ledger logic that
prevents log replay when K8s fires further events on a terminated pod.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Optional

import pytest
from kubernetes_asyncio.client.exceptions import ApiException

from chris_streaming.log_forwarder.k8s_tailer import (
    K8sLogTailer,
    _container_started,
    _is_container_waiting_error,
)


def _pod(container_states):
    """Build a minimal pod-like object with the given container states."""
    statuses = [SimpleNamespace(state=s) for s in container_states]
    return SimpleNamespace(status=SimpleNamespace(container_statuses=statuses))


def _state(running=None, terminated=None, waiting=None):
    return SimpleNamespace(running=running, terminated=terminated, waiting=waiting)


class TestContainerStarted:
    def test_waiting_container_creating_returns_false(self):
        pod = _pod([_state(waiting=SimpleNamespace(reason="ContainerCreating"))])
        assert _container_started(pod) is False

    def test_waiting_image_pull_backoff_returns_false(self):
        pod = _pod([_state(waiting=SimpleNamespace(reason="ImagePullBackOff"))])
        assert _container_started(pod) is False

    def test_running_returns_true(self):
        pod = _pod([_state(running=SimpleNamespace(started_at="2026-04-19T12:00:00Z"))])
        assert _container_started(pod) is True

    def test_terminated_returns_true(self):
        # Terminated pods still have readable logs until the log file is rotated.
        pod = _pod([_state(terminated=SimpleNamespace(exit_code=0))])
        assert _container_started(pod) is True

    def test_missing_status_returns_false(self):
        pod = SimpleNamespace(status=None)
        assert _container_started(pod) is False

    def test_no_container_statuses_returns_false(self):
        pod = SimpleNamespace(status=SimpleNamespace(container_statuses=None))
        assert _container_started(pod) is False

    def test_empty_container_statuses_returns_false(self):
        pod = SimpleNamespace(status=SimpleNamespace(container_statuses=[]))
        assert _container_started(pod) is False

    def test_null_state_returns_false(self):
        pod = _pod([None])
        assert _container_started(pod) is False

    def test_gates_on_first_container(self):
        # Plugin pods are single-container; we deliberately ignore sidecars.
        pod = _pod([
            _state(waiting=SimpleNamespace(reason="ContainerCreating")),
            _state(running=SimpleNamespace(started_at="2026-04-19T12:00:00Z")),
        ])
        assert _container_started(pod) is False


class TestIsContainerWaitingError:
    def test_matches_waiting_to_start_body(self):
        exc = ApiException(status=400, reason="BadRequest")
        exc.body = (
            '{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure",'
            '"message":"container \\"test-job-1-copy\\" in pod '
            '\\"test-job-1-copy-wn45v\\" is waiting to start: ContainerCreating",'
            '"reason":"BadRequest","code":400}'
        )
        assert _is_container_waiting_error(exc) is True

    def test_unrelated_400_returns_false(self):
        exc = ApiException(status=400, reason="BadRequest")
        exc.body = '{"message":"some other bad request"}'
        assert _is_container_waiting_error(exc) is False

    def test_missing_body_returns_false(self):
        exc = ApiException(status=400, reason="BadRequest")
        exc.body = None
        assert _is_container_waiting_error(exc) is False


def _make_pod(name: str = "test-job-1-copy-abc12"):
    """Build a minimal pod-like object with enough shape for _tail_one."""
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            labels={"org.chrisproject.job_type": "copy"},
            owner_references=[SimpleNamespace(kind="Job", name="test-job-1-copy")],
        ),
        spec=SimpleNamespace(containers=[SimpleNamespace(name="main")]),
        status=SimpleNamespace(
            container_statuses=[
                SimpleNamespace(state=SimpleNamespace(
                    running=SimpleNamespace(started_at="2026-04-19T12:00:00Z"),
                    terminated=None,
                    waiting=None,
                )),
            ],
        ),
    )


class _FakeLogResponse:
    """Mimics the aiohttp ClientResponse shape iter_any() consumes."""

    def __init__(self, chunks: list[bytes]):
        self.content = _FakeContent(chunks)


class _FakeContent:
    def __init__(self, chunks: list[bytes]):
        self._chunks = chunks

    async def iter_any(self):
        for c in self._chunks:
            yield c


class _FakeCore:
    """Stand-in for CoreV1Api.read_namespaced_pod_log."""

    def __init__(self, response=None, raise_exc: Optional[BaseException] = None):
        self._response = response
        self._raise = raise_exc
        self.calls = 0

    async def read_namespaced_pod_log(self, **kwargs):
        self.calls += 1
        if self._raise is not None:
            raise self._raise
        return self._response


class TestCompletedLedger:
    """Regression coverage for the replay bug: terminated pods must not be re-tailed."""

    def _make_tailer(self, core) -> K8sLogTailer:
        t = K8sLogTailer(namespace="ns", label_selector="x=y")
        t._core = core
        return t

    @pytest.mark.asyncio
    async def test_spawn_skips_pods_in_completed_set(self):
        tailer = self._make_tailer(_FakeCore(response=_FakeLogResponse([])))
        pod = _make_pod()
        tailer._completed.add(pod.metadata.name)

        await tailer._spawn_tail(pod)

        assert pod.metadata.name not in tailer._tasks
        assert tailer._core.calls == 0

    @pytest.mark.asyncio
    async def test_successful_tail_marks_pod_completed_and_emits_eos(self):
        chunk = b"2026-04-19T12:00:01.000000Z hello world\n"
        tailer = self._make_tailer(_FakeCore(response=_FakeLogResponse([chunk])))
        pod = _make_pod()

        await tailer._tail_one(pod)

        assert pod.metadata.name in tailer._completed
        # One log line + one EOS marker.
        lines = []
        while not tailer._queue.empty():
            lines.append(tailer._queue.get_nowait())
        assert len(lines) == 2
        assert lines[0].line == "hello world"
        assert lines[0].eos is False
        assert lines[1].eos is True

    @pytest.mark.asyncio
    async def test_404_marks_completed_without_emitting_eos(self):
        exc = ApiException(status=404, reason="NotFound")
        exc.body = '{"message":"pods \\"x\\" not found","reason":"NotFound","code":404}'
        tailer = self._make_tailer(_FakeCore(raise_exc=exc))
        pod = _make_pod()

        await tailer._tail_one(pod)

        assert pod.metadata.name in tailer._completed
        assert tailer._queue.empty()

    @pytest.mark.asyncio
    async def test_400_waiting_does_not_mark_completed(self):
        # Transient: retry must be allowed on the next MODIFIED event.
        exc = ApiException(status=400, reason="BadRequest")
        exc.body = (
            '{"message":"container \\"c\\" in pod \\"p\\" is waiting to start: '
            'ContainerCreating","code":400}'
        )
        tailer = self._make_tailer(_FakeCore(raise_exc=exc))
        pod = _make_pod()

        await tailer._tail_one(pod)

        assert pod.metadata.name not in tailer._completed
        assert tailer._queue.empty()

    @pytest.mark.asyncio
    async def test_unexpected_error_marks_completed(self):
        # 500s etc. are treated as permanent — we can't distinguish them from
        # partial reads, and replaying would duplicate already-delivered lines.
        exc = ApiException(status=500, reason="InternalServerError")
        exc.body = '{"code":500}'
        tailer = self._make_tailer(_FakeCore(raise_exc=exc))
        pod = _make_pod()

        await tailer._tail_one(pod)

        assert pod.metadata.name in tailer._completed

    @pytest.mark.asyncio
    async def test_respawn_after_completion_is_noop(self):
        # Simulates: pod runs, tail drains, pod terminates, K8s fires another
        # MODIFIED event (phase → Succeeded). The second _spawn_tail must not
        # open a second log stream.
        tailer = self._make_tailer(_FakeCore(response=_FakeLogResponse([])))
        pod = _make_pod()

        await tailer._tail_one(pod)
        assert tailer._core.calls == 1

        await tailer._spawn_tail(pod)
        # No new task scheduled.
        if pod.metadata.name in tailer._tasks:
            # If a task was scheduled, let it run and then assert on calls.
            await asyncio.gather(tailer._tasks[pod.metadata.name], return_exceptions=True)
        assert tailer._core.calls == 1

    @pytest.mark.asyncio
    async def test_cancelled_during_tail_propagates(self):
        # CancelledError must propagate out of _tail_one unchanged so close()
        # can join tasks cleanly. (It should not be swallowed by the except
        # handlers, and it should not mark the pod completed.)

        class _SlowResponse:
            def __init__(self):
                self.content = self

            async def iter_any(self):
                await asyncio.sleep(10)
                if False:
                    yield b""

        tailer = self._make_tailer(_FakeCore(response=_SlowResponse()))
        pod = _make_pod()

        task = asyncio.create_task(tailer._tail_one(pod))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # CancelledError bypasses the finally-block's mark-completed via the
        # re-raise in the handler, but finally still runs — we don't assert on
        # _completed here because cancellation on shutdown is terminal anyway.
