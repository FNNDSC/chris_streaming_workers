# ChRIS Streaming Workers

Event-driven streaming workers that replace ChRIS CUBE's polling-based job observability with a push/event-driven architecture. This repository implements four long-running async Python services — **Event Forwarder**, **Log Forwarder**, **Status Consumer** and **Log Consumer** — backed by **Redis Streams**, and the surrounding infrastructure to demonstrate them working within proper event and log pipelines.

## Overview

In the current ChRIS architecture, CUBE polls pfcon every 5 seconds for every active plugin instance to check status and retrieve logs. This generates O(N) API calls per poll cycle (where N = active jobs), producing excessive database queries, enqueued Celery tasks, and heavy pressure on the Kubernetes/Docker API.

This repository introduces a push-based alternative with two separate pipelines and event-driven workflow orchestration, all carried on **Redis Streams**:

**Event Pipeline** (status changes):
```
Docker/K8s Runtime → Event Forwarder → Redis Stream [stream:job-status:{shard}] → Status Consumer → Celery → Celery Worker → PostgreSQL + confirmed_* XADD back to status stream
```

**Log Pipeline** (container output):
```
Docker/K8s Runtime → Log Forwarder → Redis Stream [stream:job-logs:{shard}] → Log Consumer → Quickwit
                                   └ (EOS on log-stream EOF) ┘                              └ SET logs_flushed (post-flush) ┘
```

**Workflow Orchestration** (job lifecycle):
```
UI → POST /api/jobs/{id}/run → SSE Service → Celery start_workflow → pfcon (copy)
                                                    ↓
Docker events → Event Forwarder → Redis Stream → Status Consumer → Celery process_job_status
                                                    ↓
                                          Workflow state machine:
                                          copy → plugin → upload → delete → cleanup → completed
                                                    ↓
                                          Each transition → job_workflow_events row + XADD stream:job-workflow:{shard}
                                                    ↓
                                          cleanup_containers waits for logs_flushed → pfcon DELETE
```

All three streams feed a real-time broadcast layer with historical replay:
```
Redis Streams (status, logs, workflow) → SSE Service StreamDispatcher (ungrouped XREAD) → Browser (EventSource)
PostgreSQL + Quickwit → SSE Service → Browser (historical replay on connect, deduped against live by event_id)
```

Streams are **sharded** on a stable `md5(job_id) mod N` hash so all events for a given job live on one shard and preserve order, while total throughput scales with the number of shards. Consumers acquire a **lease** per shard (`SET NX PX` with heartbeat refresh) so every shard has exactly one live reader at a time. A background `PendingReclaimer` uses `XAUTOCLAIM`/`XPENDING` to recover messages left in the PEL by crashed consumers and routes them to a DLQ after `N` delivery attempts.

### The four core workers

- **Event Forwarder** (`compute-event-forwarder`) — Async daemon that watches Docker daemon events (or Kubernetes Job API) for ChRIS job containers, maps native container states to pfcon's `JobStatus` enum (`notStarted`, `started`, `finishedSuccessfully`, `finishedWithError`, `undefined`), and `XADD`s structured status events to the sharded `stream:job-status:{shard}` stream. Stateless, idempotent, restart-safe with auto-reconnect.

- **Log Forwarder** (`compute-log-forwarder`) — Tails container stdout/stderr directly from the compute runtime (aiodocker for Docker, kubernetes-asyncio for K8s), filters by the same `org.chrisproject.miniChRIS=plugininstance` label selector used everywhere else, and `XADD`s each line to the sharded `stream:job-logs:{shard}` stream. When a container's log stream reaches EOF (container exited + buffers drained), emits a final `LogEvent` with `eos=true` on the same shard — this is the signal the Log Consumer uses to mark the container's logs as durable. Replaces the previous FluentBit-based log pipeline.

- **Status Consumer** (`compute-status-consumer`) — Reads status events via `XREADGROUP` from the sharded status streams, then schedules Celery tasks for DB persistence, terminal-status confirmation (the Celery worker re-emits `confirmed_*` events via XADD back to the same status stream so SSE clients and CUBE see them), and workflow advancement. `confirmed_*` events that land on the stream are dropped here to avoid a processing loop. Each replica acquires a lease for a subset of shards; a `PendingReclaimer` recovers PEL entries from crashed workers and routes to `stream:job-status-dlq` after the configured retry budget.

- **Log Consumer** (`compute-log-consumer`) — Batched Redis Streams consumer that reads log events from `stream:job-logs:{shard}` (lines and EOS markers, both produced by the Log Forwarder) and ingests them into Quickwit (`/api/v1/{index}/ingest?commit=force`) for durable storage and search. Live fan-out to SSE clients comes from the same stream — the SSE service runs its own ungrouped `XREAD` on `stream:job-logs:*` and sees every entry independently. When an EOS marker appears in the batch, SETs `job:{id}:{type}:logs_flushed` (TTL 1h) **after** the Quickwit commit succeeds so the key cannot fire ahead of the data it attests to. Configurable batch size and flush interval. Horizontal scaling is safe: all replicas share a single consumer group, and a `PendingReclaimer` sweep (with atomic `XCLAIM`) recovers entries left in the PEL by crashed replicas.

### Supporting components

The repository also includes supporting infrastructure and a pilot test environment to demonstrate the full pipeline end-to-end:

- **Redis** (single instance by default, with `appendonly yes` + `appendfsync everysec` for durability) — carries Streams (event + log + workflow transport) and the Celery broker. SSE fan-out is done via ungrouped `XREAD` directly on the same sharded streams, so every replica sees every event independently with no separate Pub/Sub plane. An opt-in Sentinel HA topology is provided at [kubernetes/20-infra/redis-ha.yaml](kubernetes/20-infra/redis-ha.yaml) (3 Redis replicas + 3 Sentinels), selectable via a `redis+sentinel://` URL.
- **Quickwit** for log storage and historical replay.
- **PostgreSQL** for durable status tracking (written by the Celery Worker).
- **SSE Service** (FastAPI app) that streams events to browsers via SSE, replays historical events from PostgreSQL/Quickwit on connect, exposes REST endpoints for workflow submission and status queries, and exposes a `/metrics` endpoint that reports per-shard stream depth, PEL depth, and DLQ length for both pipelines.
- **Celery Worker** that processes status confirmations, orchestrates the workflow state machine (copy → plugin → upload → delete → cleanup), calls pfcon to advance steps, honours pfcon's `requires_copy_job` / `requires_upload_job` flags to skip optional steps, and waits for log flush (or terminal-status quiescence) before container cleanup. At cleanup time it computes the overall workflow status (`finishedSuccessfully`, `finishedWithError`, or `failed`) from the recorded per-step outcomes.
- **pfcon** (`ghcr.io/fnndsc/pfcon:latest`, which includes `org.chrisproject.job_type` labels) as the job control plane.
- **Test UI** for submitting jobs via the SSE service and watching status + logs stream in real-time.

For a detailed view of all data flows, message schemas, Redis Streams topology, resilience properties, and the confirmed status flow, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Project structure

```
chris_streaming_workers/
├── pyproject.toml                              # Python deps: redis.asyncio, aiodocker, pydantic, fastapi, etc.
├── docker-compose.yml                          # Full application stack
├── docker-compose.test.yml                     # Test stack (unit/integration/e2e)
├── Justfile                                    # Task runner (just up, just run all-tests, just k8s-up, ...)
├── .env                                        # Credentials & config
├── .gitignore
├── .dockerignore
├── README.md
│
├── Dockerfile.event_forwarder                  # Image: localhost/fnndsc/compute-event-forwarder
├── Dockerfile.log_consumer                     # Image: localhost/fnndsc/compute-log-consumer
├── Dockerfile.status_consumer                  # Image: localhost/fnndsc/compute-status-consumer
├── Dockerfile.log_forwarder                    # Image: localhost/fnndsc/compute-log-forwarder
├── Dockerfile.sse_service                      # SSE + Celery worker image
│
├── chris_streaming/                            # Python package root
│   ├── __init__.py
│   │
│   ├── common/                                 # Shared modules
│   │   ├── __init__.py
│   │   ├── redis_stream.py                     # Redis client factory, ShardRouter, ShardLeaseManager, PendingReclaimer
│   │   ├── stream_metrics.py                   # Per-shard XLEN/PEL/DLQ snapshot for /metrics
│   │   ├── schemas.py                          # Pydantic models: StatusEvent, LogEvent, JobStatus
│   │   ├── settings.py                         # pydantic-settings for env var parsing
│   │   ├── pfcon_status.py                     # Docker/K8s state → pfcon JobStatus mapping
│   │   └── container_naming.py                 # job_id ↔ container_name parsing
│   │
│   ├── event_forwarder/                        # Produces to stream:job-status
│   │   ├── __init__.py
│   │   ├── __main__.py                         # python -m chris_streaming.event_forwarder
│   │   ├── watcher.py                          # Abstract async watcher protocol
│   │   ├── docker_watcher.py                   # Docker event stream → StatusEvent
│   │   ├── k8s_watcher.py                      # K8s Job watch API → StatusEvent
│   │   └── producer.py                         # XADD producer with idempotence + dedup
│   │
│   ├── status_consumer/                        # Consumes stream:job-status
│   │   ├── __init__.py
│   │   ├── __main__.py                         # python -m chris_streaming.status_consumer
│   │   ├── consumer.py                         # XREADGROUP + lease + reclaimer + DLQ
│   │   └── notifier.py                         # Celery task scheduling
│   │
│   ├── log_consumer/                           # Consumes stream:job-logs
│   │   ├── __init__.py
│   │   ├── __main__.py                         # python -m chris_streaming.log_consumer
│   │   ├── consumer.py                         # Batched XREADGROUP consumer
│   │   └── quickwit_writer.py                  # Quickwit ingest wrapper (commit=force)
│   │
│   ├── log_forwarder/                          # Produces to stream:job-logs (lines + EOS)
│   │   ├── __init__.py
│   │   ├── __main__.py                         # python -m chris_streaming.log_forwarder
│   │   ├── forwarder.py                        # Serializes LogLine → LogEvent and XADDs
│   │   ├── tailer.py                           # aiodocker log stream → LogLine (+ EOS on EOF)
│   │   └── k8s_tailer.py                       # kubernetes-asyncio pod logs → LogLine (+ EOS on EOF)
│   │
│   └── sse_service/                            # FastAPI SSE service + Celery tasks
│       ├── __init__.py
│       ├── __main__.py                         # python -m chris_streaming.sse_service
│       ├── app.py                              # FastAPI app with CORS
│       ├── routes.py                           # SSE + REST endpoints (run, workflow, history, metrics)
│       ├── redis_subscriber.py                 # Async Redis subscriber with historical replay
│       ├── pfcon_client.py                     # Synchronous HTTP client for pfcon REST API
│       └── tasks.py                            # Celery tasks: process_job_status, start_workflow, cleanup_containers
│
├── config/
│   ├── quickwit/
│   │   └── job-logs-index.yaml                 # Quickwit index config (raw tokenizer on keyword fields)
│   └── init-test-data.sh                       # Create sample files in storeBase
│
├── test_ui/
│   ├── Dockerfile                              # nginx serving static + reverse proxy
│   ├── nginx.conf                              # Proxy /pfcon/ → pfcon, /sse/ → SSE service
│   └── static/
│       ├── index.html                          # Job submission + status + log viewer
│       └── app.js                              # SSE service client + EventSource SSE client
│
├── tests/
│   ├── conftest.py
│   ├── unit/                                   # Pure unit tests (no network)
│   ├── integration/                            # Redis, Quickwit, PostgreSQL integration tests
│   └── e2e/                                    # Full-stack workflow tests
│
└── kubernetes/                                 # Kubernetes manifests (parallel to docker-compose)
    ├── README.md                               # K8s deployment and testing guide
    ├── kustomization.yaml                      # Kustomize entrypoint
    ├── 00-namespace.yaml
    ├── 20-infra/                               # Redis (+ optional redis-ha), Quickwit, Postgres
    ├── 30-pfcon/                               # pfcon with KubernetesManager
    ├── 40-workers/                             # event-forwarder, status/log consumers, log-forwarder, SSE, celery
    ├── 50-ui/                                  # Test UI (nginx)
    └── tests/                                  # Unit/integration/e2e test Jobs + integration stack
```

## Services in docker-compose

All services run on a single `streaming` Docker network.

| # | Service | Image | Role | Exposed Ports |
|---|---------|-------|------|---------------|
| 1 | `redis` | `redis:7-alpine` | Streams transport (status/logs/workflow) + Celery broker (AOF everysec) | 6379 |
| 2 | `quickwit` | `quickwit/quickwit:latest` | Log storage and search (Tantivy index, `/api/v1`) | 7280 |
| 3 | `postgres` | `postgres:16-alpine` | Celery worker DB | 5433 |
| 4 | `pfcon` | `ghcr.io/fnndsc/pfcon:latest` | Job control plane (fslink mode) | 30005 |
| 5 | `init-test-data` | `alpine:latest` | Creates sample fslink test data (run-once) | — |
| 6 | `event-forwarder` | `localhost/fnndsc/compute-event-forwarder` | Docker events → `stream:job-status` | — |
| 7 | `log-forwarder` | `localhost/fnndsc/compute-log-forwarder` | Docker container logs → `stream:job-logs` | — |
| 8 | `status-consumer` | `localhost/fnndsc/compute-status-consumer` | `stream:job-status` → Celery | — |
| 9 | `log-consumer` | `localhost/fnndsc/compute-log-consumer` | `stream:job-logs` → Quickwit | — |
| 10 | `sse-service` | Built from `Dockerfile.sse_service` | FastAPI SSE streaming + `/metrics` | 8080 |
| 11 | `celery-worker` | Built from `Dockerfile.sse_service` | Celery status processing + PostgreSQL | — |
| 12 | `test-ui` | Built from `test_ui/Dockerfile` | nginx + static HTML/JS test app | 8888 |

### Dependency graph

```
init-test-data ──→ pfcon
redis ──→ event-forwarder
      ├──→ log-forwarder
      ├──→ status-consumer
      └──→ log-consumer    (also needs quickwit)
redis + postgres + pfcon ──→ celery-worker
redis + postgres ──→ sse-service
sse-service ──→ test-ui
```

### Redis Streams design

All streams use `md5(job_id) mod N` as the shard key, guaranteeing per-job ordering.

| Stream base | Shards | Trim policy | Writers | Readers |
|-------------|--------|-------------|---------|---------|
| `stream:job-status:{shard}` | `STREAM_NUM_SHARDS` (default 8) | `MAXLEN ~ STREAM_STATUS_MAXLEN` (default 1M) | Event Forwarder | Status Consumer |
| `stream:job-logs:{shard}` | `STREAM_NUM_SHARDS` (default 8) | `MAXLEN ~ STREAM_LOGS_MAXLEN` (default 5M) | Log Forwarder (lines + EOS markers) | Log Consumer |
| `stream:job-status-dlq` | 1 | `MAXLEN ~ STREAM_DLQ_MAXLEN` (default 100k) | Status Consumer (via reclaimer) | (manual inspection) |
| `stream:job-logs-dlq` | 1 | `MAXLEN ~ STREAM_DLQ_MAXLEN` (default 100k) | Log Consumer (via reclaimer) | (manual inspection) |

Consumer groups: `status-consumer-group`, `log-consumer-group` (created with `MKSTREAM` on startup). Per-shard `lease:<stream>:<group>` keys gate which replica owns which shard. A `PendingReclaimer` background task uses `XAUTOCLAIM`/`XPENDING` to recover entries left in the PEL by crashed consumers and routes messages to the DLQ after `RECLAIM_MAX_DELIVERIES`.

## Development and testing

### Kubernetes deployment

A parallel Kubernetes pipeline is available under [kubernetes/](kubernetes/) and
wrapped by `just k8s-*` recipes. See [kubernetes/README.md](kubernetes/README.md)
for the full guide (bring-up, teardown, in-cluster test runs). The docker-compose
pipeline described below is unaffected.

### Prerequisites

- Docker and Docker Compose v2
- The `ghcr.io/fnndsc/pfcon:latest` image (includes `org.chrisproject.job_type` label support)

### Start everything

```bash
just up
```

or without just:
```bash
docker compose up --build -d
```

This builds the custom service images, pulls infrastructure images (including pfcon), creates test data, and launches all services.

### Access points

| URL | Service |
|-----|---------|
| http://localhost:8888 | Test UI — submit jobs, watch status + logs |
| http://localhost:8080/health | SSE Service health check |
| http://localhost:8080/metrics | Per-shard stream depth / PEL / DLQ snapshot |
| http://localhost:8080/api/jobs/{job_id}/run | Submit a workflow (POST) |
| http://localhost:8080/api/jobs/{job_id}/workflow | Workflow status (GET) |
| http://localhost:8080/api/jobs/{job_id}/status/history | Status history (GET) |
| http://localhost:8080/events/{job_id}/status | SSE status stream (with historical replay) |
| http://localhost:8080/events/{job_id}/logs | SSE log stream (with historical replay) |
| http://localhost:8080/events/{job_id}/all | SSE combined stream (with historical replay) |
| http://localhost:8080/logs/{job_id}/history | Historical logs from Quickwit |
| http://localhost:30005/api/v1/ | pfcon API (direct) |
| http://localhost:7280 | Quickwit API (UI at `/ui`) |

### Run a test job via the UI

1. Open http://localhost:8888
2. The form is pre-filled with defaults for `pl-simpledsapp` against the test data
3. Click **Run Full Workflow** — the UI will:
   - Submit a single `POST /sse/api/jobs/{job_id}/run` request to the SSE service
   - The SSE service schedules the workflow via Celery, which orchestrates copy → plugin → upload → delete → cleanup automatically
4. Watch the **Status Events** panel for real-time SSE status updates from the event pipeline
5. Watch the **Container Logs** panel for real-time log lines from the log pipeline
6. Watch the **Step Tracker** for workflow progression

### Run a test job via curl

```bash
# Submit a workflow (single request — the SSE service orchestrates everything)
curl -s -X POST http://localhost:8080/api/jobs/my-job-1/run \
  -H 'Content-Type: application/json' \
  -d '{
    "image": "ghcr.io/fnndsc/pl-simpledsapp:2.1.0",
    "entrypoint": ["simpledsapp"],
    "type": "ds",
    "args": ["--dummyFloat", "3.5", "--sleepLength", "5"]
  }'

# Check workflow status
curl -s http://localhost:8080/api/jobs/my-job-1/workflow | jq

# Get status history
curl -s http://localhost:8080/api/jobs/my-job-1/status/history | jq

# Stream SSE events (including historical replay for late-connecting clients)
curl -N http://localhost:8080/events/my-job-1/all

# Observe stream / PEL / DLQ depth
curl -s http://localhost:8080/metrics | jq
```

### Inspect the pipeline internals

```bash
# List streams and their lengths
docker compose exec redis redis-cli --scan --pattern 'stream:job-*' | \
  xargs -I {} sh -c 'echo "{} -> $(docker compose exec -T redis redis-cli XLEN {})"'

# Inspect a shard's consumer-group state
docker compose exec redis redis-cli XINFO GROUPS stream:job-status:0
docker compose exec redis redis-cli XPENDING stream:job-status:0 status-consumer-group

# Tail new entries from a status shard (blocks)
docker compose exec redis redis-cli XREAD BLOCK 0 COUNT 10 STREAMS stream:job-status:0 \$

# DLQ inspection
docker compose exec redis redis-cli XLEN stream:job-status-dlq
docker compose exec redis redis-cli XRANGE stream:job-status-dlq - + COUNT 10

# Query PostgreSQL for job statuses
docker compose exec postgres psql -U chris chris_streaming \
  -c "SELECT job_id, job_type, status, updated_at FROM job_status ORDER BY updated_at;"

# Query Quickwit for logs
curl -s -X POST -H 'Content-Type: application/json' \
  'http://localhost:7280/api/v1/job-logs/search' \
  -d '{"query": "job_id:\"test-job-1\"", "max_hits": 20, "sort_by": "timestamp"}' \
  | jq '.hits[].line'
```

### Stop and clean up

```bash
just down              # stop services
just nuke              # stop and remove all volumes (full reset)
```

or without just:
```bash
docker compose down    # stop services
docker compose down -v # stop and remove all volumes (full reset)
```

### Running automated tests

Tests are split into three levels — **unit tests** (no infrastructure, fast), **integration tests** (require Redis, Quickwit, PostgreSQL), and **end-to-end tests** (require the full application stack). All run entirely in Docker via `docker-compose.test.yml` and `Dockerfile.test`, using Docker Compose profiles to start only the services needed for each level.

#### Using `just` (recommended)

Install [just](https://github.com/casey/just), then:

```bash
# Run unit tests (no infrastructure needed, fast)
just run unit-tests

# Run integration tests (spins up Redis, Quickwit, PostgreSQL)
just run integration-tests

# Run E2E tests (starts full application stack, submits real workflows)
just run e2e-tests

# Run all test levels sequentially (useful for CI/CD)
just run all-tests
```

#### Using Docker Compose directly

```bash
# Unit tests only
docker compose -f docker-compose.test.yml build unit-tests
docker compose -f docker-compose.test.yml run --rm unit-tests

# Integration tests (starts infrastructure, runs tests, stops infrastructure)
docker compose -f docker-compose.test.yml --profile integration build
docker compose -f docker-compose.test.yml --profile integration up -d --wait
docker compose -f docker-compose.test.yml run --rm integration-tests
docker compose -f docker-compose.test.yml --profile integration down

# E2E tests (requires the full stack from docker-compose.yml)
docker compose up --build -d
docker compose -f docker-compose.test.yml build e2e-tests
docker compose -f docker-compose.test.yml --profile e2e run --rm e2e-tests
docker compose down
```

#### Test structure

```
tests/
├── conftest.py                           # Shared fixtures (sample events)
├── unit/                                 # No external services required
│   ├── test_common/                      # schemas, pfcon_status, container_naming, settings, redis_stream, stream_metrics
│   ├── test_event_forwarder/             # producer, docker_watcher, k8s_watcher
│   ├── test_status_consumer/             # consumer, notifier
│   ├── test_log_consumer/                # consumer, quickwit_writer, reclaim
│   ├── test_log_forwarder/               # forwarder, tailer
│   └── test_sse_service/                 # app, routes, pfcon_client, tasks, dispatcher
├── integration/                          # Requires Docker Compose infrastructure
│   ├── test_quickwit.py                  # Quickwit ingest + search against live instance
│   ├── test_log_consumer_reclaim.py      # PEL reclaim + DLQ via PendingReclaimer
│   ├── test_postgres.py                  # Schema creation and upsert logic
│   └── test_sse_health.py                # FastAPI health endpoint
└── e2e/                                  # Requires full application stack
    └── test_workflow_e2e.py              # Submit workflows, verify SSE events + logs
```

- **Unit tests** use `fakeredis` and mocks exclusively — no network or container dependencies.
- **Integration tests** are marked with `@pytest.mark.integration` and test real service interactions (Redis Streams XADD/XREADGROUP/XACK, Quickwit ingest + search, PostgreSQL upserts, reclaimer + DLQ behavior).
- **E2E tests** are marked with `@pytest.mark.e2e` and exercise the full pipeline: submit a job via `POST /api/jobs/{id}/run`, verify status events arrive through the SSE stream, check workflow completion, and confirm logs appear in Quickwit. Includes tests for successful workflows, failure scenarios (bad image), and historical replay for late-connecting SSE clients.


## Configuration

All services are configured via environment variables. The `.env` file provides defaults for the Docker Compose deployment.

### Shared Redis Streams settings

Used by: Event Forwarder, Log Forwarder, Status Consumer, Log Consumer, SSE Service.

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection URL. Set to `redis+sentinel://host1:26379,host2:26379/mymaster/0` to resolve the current master via a Sentinel cluster. |
| `STREAM_STATUS_BASE` | `stream:job-status` | Status stream base; sharded as `{base}:{i}` |
| `STREAM_LOGS_BASE` | `stream:job-logs` | Log stream base; sharded as `{base}:{i}` |
| `STREAM_STATUS_DLQ` | `stream:job-status-dlq` | Status dead-letter stream |
| `STREAM_LOGS_DLQ` | `stream:job-logs-dlq` | Log dead-letter stream |
| `STREAM_NUM_SHARDS` | `8` | Number of shards per stream base (stable `md5(job_id) mod N`) |
| `STREAM_STATUS_MAXLEN` | `1000000` | Approximate per-shard cap applied via `XADD MAXLEN ~` |
| `STREAM_LOGS_MAXLEN` | `5000000` | Approximate per-shard cap applied via `XADD MAXLEN ~` |
| `STREAM_DLQ_MAXLEN` | `100000` | Approximate DLQ cap |
| `RECLAIM_MIN_IDLE_MS` | `30000` | PEL entries idle longer than this are eligible for `XAUTOCLAIM` |
| `RECLAIM_SWEEP_INTERVAL_MS` | `10000` | How often the reclaimer runs |
| `RECLAIM_MAX_DELIVERIES` | `5` | After N deliveries, the reclaimer moves the message to the DLQ |
| `LEASE_TTL_MS` | `15000` | Shard-lease TTL |
| `LEASE_REFRESH_INTERVAL_MS` | `5000` | How often the owner refreshes its lease |
| `LEASE_ACQUIRE_INTERVAL_MS` | `2000` | How often a replica tries to pick up an unowned shard |

### Event Forwarder

| Variable | Default | Description |
|----------|---------|-------------|
| `COMPUTE_ENV` | `docker` | `docker` or `kubernetes` |
| `DOCKER_LABEL_FILTER` | `org.chrisproject.miniChRIS` | Docker label key to filter containers |
| `DOCKER_LABEL_VALUE` | `plugininstance` | Expected value for the filter label |
| `K8S_NAMESPACE` | `default` | Kubernetes namespace (when `COMPUTE_ENV=kubernetes`) |
| `K8S_LABEL_SELECTOR` | `chrisproject.org/role=plugininstance` | K8s label selector (K8s-idiomatic `domain/key` form; differs from the Docker flat-key label) |
| `EMIT_INITIAL_STATE` | `true` | Emit current state of all containers on startup |
| `DOCKER_RECONCILE_SECONDS` | `0.0` | If > 0, periodically inspect tracked containers and re-emit a status if the mapped state disagrees with last-emitted (0 disables) |

Requires the Docker socket mounted at `/var/run/docker.sock` (Docker mode) or in-cluster K8s config (Kubernetes mode).

### Log Forwarder

| Variable | Default | Description |
|----------|---------|-------------|
| `COMPUTE_ENV` | `docker` | `docker` or `kubernetes` |
| `DOCKER_LABEL_FILTER` | `org.chrisproject.miniChRIS` | Docker label key to filter containers |
| `DOCKER_LABEL_VALUE` | `plugininstance` | Expected value for the filter label |
| `K8S_NAMESPACE` | `default` | Kubernetes namespace (when `COMPUTE_ENV=kubernetes`) |
| `K8S_LABEL_SELECTOR` | `chrisproject.org/role=plugininstance` | K8s label selector (K8s-idiomatic `domain/key` form; differs from the Docker flat-key label) |

Requires the Docker socket (Docker mode) or in-cluster K8s config (Kubernetes mode).

### Status Consumer

| Variable | Default | Description |
|----------|---------|-------------|
| `STATUS_CONSUMER_GROUP` | `status-consumer-group` | Consumer group name |
| `HANDLER_RETRIES` | `3` | In-process retries before a message is left in the PEL for reclaim |
| `CELERY_BROKER_URL` | `redis://redis:6379/0` | Celery broker URL |

### Log Consumer

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_CONSUMER_GROUP` | `log-consumer-group` | Consumer group name |
| `QUICKWIT_URL` | `http://quickwit:7280` | Quickwit endpoint |
| `QUICKWIT_INDEX` | `job-logs` | Quickwit index name |
| `BATCH_MAX_SIZE` | `200` | Max messages per batch before flush |
| `BATCH_MAX_WAIT_SECONDS` | `2.0` | Max seconds before flushing a partial batch |

### SSE Service

| Variable | Default | Description |
|----------|---------|-------------|
| `HOST` | `0.0.0.0` | Bind address |
| `PORT` | `8080` | Bind port |
| `QUICKWIT_URL` | `http://quickwit:7280` | Quickwit for historical log queries |
| `QUICKWIT_INDEX` | `job-logs` | Quickwit index name (must match the Log Consumer) |
| `CELERY_BROKER_URL` | `redis://redis:6379/0` | Celery broker |
| `DB_DSN` | `postgresql://chris:chris1234@postgres:5432/chris_streaming` | PostgreSQL connection string |
| `PFCON_URL` | `http://pfcon:30005` | pfcon API base URL |
| `PFCON_USER` | `pfcon` | pfcon API username |
| `PFCON_PASSWORD` | `pfcon1234` | pfcon API password |
| `EOS_QUIESCENCE_SECONDS` | `10.0` | Fallback window used by `cleanup_containers`: if no `logs_flushed` key appears, a step whose terminal status has been stable for this long is treated as drained. EOS is a hint, not a hard gate. |
| `STATUS_CONSUMER_GROUP` | `status-consumer-group` | Used by `/metrics` to read PEL depth — must match the Status Consumer's value |
| `LOG_CONSUMER_GROUP` | `log-consumer-group` | Used by `/metrics` to read PEL depth — must match the Log Consumer's value |

### Celery Worker

Uses the same image as SSE Service. Runs with:
```
celery -A chris_streaming.sse_service.tasks worker -l info -Q status-processing -c 2
```

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://redis:6379/0` | Redis URL for XADDing `confirmed_*` back to the status stream, XADDing workflow events, and checking logs_flushed keys |
| `CELERY_BROKER_URL` | `redis://redis:6379/0` | Celery broker |
| `DB_DSN` | `postgresql://chris:chris1234@postgres:5432/chris_streaming` | PostgreSQL connection string |
| `PFCON_URL` | `http://pfcon:30005` | pfcon API base URL |
| `PFCON_USER` | `pfcon` | pfcon API username |
| `PFCON_PASSWORD` | `pfcon1234` | pfcon API password |

### pfcon

| Variable | Default | Description |
|----------|---------|-------------|
| `APPLICATION_MODE` | `development` | Enables `DevConfig` with hardcoded test credentials |
| `PFCON_INNETWORK` | `true` | In-network mode (containers share a volume) |
| `STORAGE_ENV` | `fslink` | Filesystem with ChRIS link expansion |
| `CONTAINER_ENV` | `docker` | Schedule containers via Docker API |
| `JOB_LABELS` | `org.chrisproject.miniChRIS=plugininstance` | Labels applied to all job containers |
| `REMOVE_JOBS` | `yes` | Remove containers on DELETE |

### PostgreSQL

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_DB` | `chris_streaming` | Database name (used by Celery Worker) |
| `POSTGRES_USER` | `chris` | Database user |
| `POSTGRES_PASSWORD` | `chris1234` | Database password |

### Redis durability and HA

The `redis:7-alpine` container is launched with `--appendonly yes --appendfsync everysec` everywhere Redis runs (docker-compose, `kubernetes/20-infra/redis.yaml`, and `kubernetes/tests/integration-stack.yaml`). This bounds data loss to roughly one second of writes on an `fsync` gap without the throughput cost of `appendfsync always`.

For production HA, apply `kubernetes/20-infra/redis-ha.yaml` instead of (or in addition to) the default single-node manifest. It ships:

- A 3-replica Redis StatefulSet (1 primary + 2 replicas, via an init container that sets `replicaof` for ordinal > 0).
- A 3-replica Sentinel StatefulSet pointing at the same primary.
- Sentinel service at `redis-sentinel:26379` with master name `mymaster`.

Workers opt in by switching `REDIS_URL` to `redis+sentinel://redis-sentinel:26379/mymaster/0`; `create_redis_client()` detects the scheme and resolves the master via `redis.asyncio.sentinel.Sentinel`, so no code changes are needed to move between single-node and Sentinel topologies.
