"""Tests for chris_streaming.status_consumer.consumer."""

from unittest.mock import AsyncMock

import pytest

from chris_streaming.status_consumer.consumer import StatusMessageHandler


class TestStatusMessageHandler:
    async def test_call_deserializes_and_notifies(self, sample_status_event):
        notifier = AsyncMock()
        handler = StatusMessageHandler(notifier)

        raw = sample_status_event.serialize()
        await handler(raw)

        notifier.notify.assert_awaited_once()
        # The notifier is called with the deserialized StatusEvent
        arg = notifier.notify.call_args[0][0]
        assert arg.job_id == sample_status_event.job_id
        assert arg.status == sample_status_event.status

    async def test_call_propagates_deserialize_error(self):
        notifier = AsyncMock()
        handler = StatusMessageHandler(notifier)
        with pytest.raises(Exception):
            await handler(b"not-json")

    async def test_call_propagates_notify_error(self, sample_status_event):
        notifier = AsyncMock()
        notifier.notify = AsyncMock(side_effect=Exception("boom"))
        handler = StatusMessageHandler(notifier)
        with pytest.raises(Exception, match="boom"):
            await handler(sample_status_event.serialize())

    async def test_reclaim_returns_true_on_success(self, sample_status_event):
        notifier = AsyncMock()
        handler = StatusMessageHandler(notifier)

        ok = await handler.reclaim(sample_status_event.serialize())
        assert ok is True
        notifier.notify.assert_awaited_once()

    async def test_reclaim_returns_false_on_failure(self, sample_status_event):
        notifier = AsyncMock()
        notifier.notify = AsyncMock(side_effect=Exception("fail"))
        handler = StatusMessageHandler(notifier)

        ok = await handler.reclaim(sample_status_event.serialize())
        assert ok is False

    async def test_reclaim_returns_false_on_bad_payload(self):
        notifier = AsyncMock()
        handler = StatusMessageHandler(notifier)

        ok = await handler.reclaim(b"not-json")
        assert ok is False
