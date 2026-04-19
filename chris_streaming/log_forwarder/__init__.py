"""
Log Forwarder service.

Tails container logs for ChRIS job containers and XADDs each line as a
``LogEvent`` to the sharded ``stream:job-logs:{shard}`` Redis Streams.
When a container's log stream reaches EOF, emits a final ``LogEvent``
with ``eos=True`` on the same shard so the Log Consumer can mark the
container's logs as durable.

Replaces the Fluent Bit + Lua pipeline with a small Python service that
reuses the common ``RedisStreamProducer`` abstraction. Mirrors the role
of ``event_forwarder`` for the logs pipeline.
"""
