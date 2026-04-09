"""
Abstract watcher protocol for compute runtime event sources.

Both DockerWatcher and K8sWatcher implement this protocol, allowing the
Event Forwarder to work with either backend via the same interface.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from chris_streaming.common.schemas import StatusEvent


class Watcher(ABC):
    """
    Async context manager that yields StatusEvent objects.

    On entry (__aenter__), the watcher connects to the runtime and
    optionally emits the current state of all matching containers/jobs.
    It then continuously watches for new events until cancelled.
    """

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the compute runtime."""
        ...

    @abstractmethod
    async def close(self) -> None:
        """Clean up connections."""
        ...

    @abstractmethod
    def watch(self) -> AsyncIterator[StatusEvent]:
        """
        Yield StatusEvent objects as they occur.

        This is an infinite async generator. It handles reconnection
        internally. It yields the initial state on startup if configured.
        """
        ...

    async def __aenter__(self) -> "Watcher":
        await self.connect()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()
