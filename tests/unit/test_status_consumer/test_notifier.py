"""Tests for chris_streaming.status_consumer.notifier."""

from unittest.mock import MagicMock, patch

from chris_streaming.common.schemas import JobStatus, JobType, StatusEvent
from chris_streaming.status_consumer.notifier import StatusNotifier


class TestStatusNotifier:
    async def test_notify_sends_celery_task(self, sample_status_event):
        with patch("chris_streaming.status_consumer.notifier.Celery") as MockCelery:
            mock_app = MagicMock()
            MockCelery.return_value = mock_app

            notifier = StatusNotifier("redis://localhost:6379/0")
            await notifier.notify(sample_status_event)

            mock_app.send_task.assert_called_once_with(
                "chris_streaming.sse_service.tasks.process_job_status",
                kwargs={"event_data": sample_status_event.model_dump(mode="json")},
                queue="status-processing",
            )

    async def test_notify_serializes_event_correctly(self):
        with patch("chris_streaming.status_consumer.notifier.Celery") as MockCelery:
            mock_app = MagicMock()
            MockCelery.return_value = mock_app

            notifier = StatusNotifier("redis://localhost:6379/0")
            event = StatusEvent(
                job_id="j1",
                job_type=JobType.copy,
                status=JobStatus.notStarted,
            )
            await notifier.notify(event)

            call_kwargs = mock_app.send_task.call_args[1]["kwargs"]["event_data"]
            assert call_kwargs["job_id"] == "j1"
            assert call_kwargs["job_type"] == "copy"
            assert call_kwargs["status"] == "notStarted"
