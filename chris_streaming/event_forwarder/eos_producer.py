"""
End-of-Stream (EOS) marker producer.

After a container dies, waits for Fluent Bit to flush remaining logs,
then produces an EOS LogEvent to the job-logs Kafka topic. The Log
Consumer uses this marker as a *hint* that all logs for the container
have likely been written to OpenSearch, and sets a Redis key that
cleanup_containers can fast-path on.

IMPORTANT — EOS is a best-effort flush hint, not a correctness guarantee.
Kafka's ordering guarantee applies per producer within a partition, not
across producers. Fluent Bit and the Event Forwarder are independent
producers writing to the same partition (same job_id key), so the EOS
marker can in principle arrive before some of Fluent Bit's in-flight log
lines under broker backpressure, client retries, or if Fluent Bit is
still buffering when the delay expires.

The ``EOS_DELAY_SECONDS`` default (10s) should comfortably exceed Fluent
Bit's Refresh_Interval + Flush interval under normal load. When the hint
fails or is never delivered, ``cleanup_containers`` falls back to a
terminal-status quiescence check (see ``SSEServiceSettings.eos_quiescence_seconds``)
so cleanup does not block on EOS alone.
"""

from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaProducer

from chris_streaming.common.schemas import LogEvent, JobType, kafka_key_for_job

logger = logging.getLogger(__name__)


class EOSProducer:
    """Produces delayed EOS markers to the job-logs Kafka topic."""

    def __init__(
        self,
        producer: AIOKafkaProducer,
        topic: str,
        delay_seconds: float = 10.0,
    ):
        self._producer = producer
        self._topic = topic
        self._delay = delay_seconds
        self._pending: dict[str, asyncio.Task] = {}

    def schedule_eos(self, job_id: str, job_type: JobType) -> None:
        """Schedule a delayed EOS marker for a job container."""
        task_key = f"{job_id}:{job_type.value}"
        if task_key in self._pending:
            logger.debug("EOS already scheduled for %s", task_key)
            return
        task = asyncio.create_task(self._delayed_send(job_id, job_type))
        self._pending[task_key] = task
        task.add_done_callback(lambda _: self._pending.pop(task_key, None))
        logger.info(
            "Scheduled EOS marker for %s in %.1fs",
            task_key, self._delay,
        )

    async def _delayed_send(self, job_id: str, job_type: JobType) -> None:
        """Wait for Fluent Bit flush, then produce EOS marker."""
        await asyncio.sleep(self._delay)
        eos_event = LogEvent(
            job_id=job_id,
            job_type=job_type,
            eos=True,
        )
        key = kafka_key_for_job(job_id)
        await self._producer.send_and_wait(
            self._topic,
            value=eos_event.serialize_for_kafka(),
            key=key,
        )
        logger.info(
            "Produced EOS marker: job=%s type=%s",
            job_id, job_type.value,
        )

    async def cancel_all(self) -> None:
        """Cancel all pending EOS tasks (for shutdown)."""
        for task in self._pending.values():
            task.cancel()
        if self._pending:
            await asyncio.gather(*self._pending.values(), return_exceptions=True)
        self._pending.clear()
