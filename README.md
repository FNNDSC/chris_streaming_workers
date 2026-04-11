# ChRIS Streaming Workers

Event-driven streaming workers that replace ChRIS CUBE's polling-based job observability with a push/event-driven architecture. This repository implements three core long-running Kafka worker processes — **Event Forwarder**, **Status Consumer**, and **Log Consumer** — and the surrounding infrastructure to demonstrate them working within proper event and log pipelines.

## Overview

In the current ChRIS architecture, CUBE polls pfcon every 5 seconds for every active plugin instance to check status and retrieve logs. This generates O(N) API calls per poll cycle (where N = active jobs), producing excessive database queries, enqueued Celery tasks, and heavy pressure on the Kubernetes/Docker API.

This repository introduces a push-based alternative with two separate pipelines and event-driven workflow orchestration:

**Event Pipeline** (status changes):
```
Docker/K8s Runtime → Event Forwarder → Kafka [job-status-events] → Status Consumer → Celery → Celery Worker → PostgreSQL + Redis Pub/Sub
```

**Log Pipeline** (container output):
```
Docker/K8s Runtime → Fluent Bit → Kafka [job-logs] → Log Consumer → OpenSearch + Redis Pub/Sub
Event Forwarder → (delayed EOS marker) → Kafka [job-logs] → Log Consumer → Redis logs_flushed key
```

**Workflow Orchestration** (job lifecycle):
```
UI → POST /api/jobs/{id}/run → SSE Service → Celery start_workflow → pfcon (copy)
                                                    ↓
Docker events → Event Forwarder → Kafka → Status Consumer → Celery process_job_status
                                                    ↓
                                          Workflow state machine:
                                          copy → plugin → upload → delete → cleanup → completed
                                                    ↓
                                          cleanup_containers waits for logs_flushed → pfcon DELETE
```

Both pipelines feed into a real-time streaming layer with historical replay:
```
Redis Pub/Sub → SSE Service → Browser (EventSource)
PostgreSQL + OpenSearch → SSE Service → Browser (historical replay on connect)
```

### The three core workers

- **Event Forwarder** (`compute_event_forwarder`) — Async daemon that watches Docker daemon events (or Kubernetes Job API) for ChRIS job containers, maps native container states to pfcon's `JobStatus` enum (`notStarted`, `started`, `finishedSuccessfully`, `finishedWithError`, `undefined`), and produces structured status events to Kafka. For terminal events, also schedules delayed EOS (End-of-Stream) markers to the `job-logs` topic as a best-effort hint that Fluent Bit has flushed all logs for a container (see the EOS section in [ARCHITECTURE.md](ARCHITECTURE.md) — cleanup does not depend on this alone). Stateless, idempotent, restart-safe with auto-reconnect.

- **Status Consumer** (`compute_status_consumer`) — Kafka consumer that reads status events and schedules Celery tasks for DB persistence, Redis Pub/Sub publishing, terminal status confirmation, and workflow advancement. Failed messages go to a dead-letter topic after configurable retries.

- **Log Consumer** (`compute_logs_consumer`) — Batched Kafka consumer that reads log events (produced by Fluent Bit and EOS markers from Event Forwarder), bulk-writes to OpenSearch for durable storage and search, and publishes to Redis Pub/Sub for real-time log streaming. When an EOS marker is received, flushes the current batch and sets a Redis key (`job:{id}:{type}:logs_flushed`) to signal that all logs have been written to OpenSearch. Configurable batch size and flush interval.

### Supporting components

The repository also includes supporting infrastructure and a pilot test environment to demonstrate the full pipeline end-to-end:

- **Kafka** (KRaft mode) with SASL/PLAIN authentication, per-service users, and ACLs
- **Fluent Bit** reading Docker container logs, filtering by ChRIS labels, and forwarding to Kafka
- **OpenSearch** for log storage and historical replay
- **Redis** for Pub/Sub fan-out and Celery broker
- **PostgreSQL** for durable status tracking (written by the Celery Worker)
- **SSE Service** (FastAPI app) that streams events to browsers via SSE, replays historical events from PostgreSQL/OpenSearch on connect, and exposes REST endpoints for workflow submission and status queries
- **Celery Worker** that processes status confirmations, orchestrates the workflow state machine (copy → plugin → upload → delete → cleanup), calls pfcon to advance steps, honours pfcon's `requires_copy_job` / `requires_upload_job` flags to skip optional steps, and waits for log flush (or terminal-status quiescence) before container cleanup. At cleanup time it computes the overall workflow status (`finishedSuccessfully`, `finishedWithError`, or `failed`) from the recorded per-step outcomes.
- **pfcon** (`ghcr.io/fnndsc/pfcon:latest`, which includes `org.chrisproject.job_type` labels) as the job control plane
- **Test UI** for submitting jobs via the SSE service and watching status + logs stream in real-time

For a detailed view of all data flows, message schemas, Kafka topic design, resilience properties, and the confirmed status flow, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Project structure

```
chris_streaming_workers/
├── pyproject.toml                              # Python deps: aiokafka, aiodocker, pydantic, fastapi, etc.
├── docker-compose.yml                          # 14 services
├── .env                                        # Credentials & config
├── .gitignore
├── .dockerignore
├── README.md
│
├── Dockerfile.event_forwarder                  # Image: localhost/fnndsc/compute_event_forwarder
├── Dockerfile.log_consumer                     # Image: localhost/fnndsc/compute_logs_consumer
├── Dockerfile.status_consumer                  # Image: localhost/fnndsc/compute_status_consumer
├── Dockerfile.sse_service                      # SSE + Celery worker image
│
├── chris_streaming/                            # Python package root
│   ├── __init__.py
│   │
│   ├── common/                                 # Shared modules (high code reuse)
│   │   ├── __init__.py
│   │   ├── kafka.py                            # Async Kafka producer/consumer factory
│   │   ├── schemas.py                          # Pydantic models: StatusEvent, LogEvent, JobStatus
│   │   ├── settings.py                         # pydantic-settings for env var parsing
│   │   ├── pfcon_status.py                     # Docker/K8s state → pfcon JobStatus mapping
│   │   └── container_naming.py                 # job_id ↔ container_name parsing
│   │
│   ├── event_forwarder/                        # Produces to job-status-events + EOS markers
│   │   ├── __init__.py
│   │   ├── __main__.py                         # python -m chris_streaming.event_forwarder
│   │   ├── watcher.py                          # Abstract async watcher protocol
│   │   ├── docker_watcher.py                   # Docker event stream → StatusEvent
│   │   ├── k8s_watcher.py                      # K8s Job watch API → StatusEvent
│   │   ├── producer.py                         # Kafka producer with idempotence + dedup
│   │   └── eos_producer.py                     # Delayed EOS markers → Kafka job-logs
│   │
│   ├── status_consumer/                        # Consumes job-status-events
│   │   ├── __init__.py
│   │   ├── __main__.py                         # python -m chris_streaming.status_consumer
│   │   ├── consumer.py                         # Kafka consumer with retry + DLQ
│   │   └── notifier.py                         # Redis Pub/Sub + Celery task scheduling
│   │
│   ├── log_consumer/                           # Consumes job-logs
│   │   ├── __init__.py
│   │   ├── __main__.py                         # python -m chris_streaming.log_consumer
│   │   ├── consumer.py                         # Batched Kafka consumer
│   │   ├── opensearch_writer.py                # Bulk writes with daily index rotation
│   │   └── redis_publisher.py                  # Per-job Pub/Sub fan-out
│   │
│   └── sse_service/                            # FastAPI SSE service + Celery tasks
│       ├── __init__.py
│       ├── __main__.py                         # python -m chris_streaming.sse_service
│       ├── app.py                              # FastAPI app with CORS
│       ├── routes.py                           # SSE + REST endpoints (run, workflow, history)
│       ├── redis_subscriber.py                 # Async Redis subscriber with historical replay
│       ├── pfcon_client.py                     # Synchronous HTTP client for pfcon REST API
│       └── tasks.py                            # Celery tasks: process_job_status, start_workflow, cleanup_containers
│
├── config/
│   ├── kafka/
│   │   ├── server.properties                   # KRaft broker config (SASL/PLAIN, ACLs)
│   │   ├── kafka_server_jaas.conf              # SASL/PLAIN user credentials
│   │   ├── init-kafka.sh                       # Create topics and ACLs (run-once)
│   │   └── admin.properties                    # SASL config for kafka CLI tools
│   ├── fluent-bit/
│   │   ├── fluent-bit.conf                     # Input, filter, output pipeline
│   │   ├── parsers.conf                        # Docker JSON log parser
│   │   └── enrich.lua                          # Lua filter: metadata + schema reshaping
│   ├── opensearch/
│   │   └── index-template.json                 # job-logs-* index mapping
│   └── init-test-data.sh                       # Create sample files in storeBase
│
├── test_ui/
│   ├── Dockerfile                              # nginx serving static + reverse proxy
│   ├── nginx.conf                              # Proxy /pfcon/ → pfcon, /sse/ → SSE service
│   └── static/
│       ├── index.html                          # Job submission + status + log viewer
│       └── app.js                              # SSE service client + EventSource SSE client
│
└── tests/
    └── __init__.py
```

## Services in docker-compose

All 14 services run on a single `streaming` Docker network.

| # | Service | Image | Role | Exposed Ports |
|---|---------|-------|------|---------------|
| 1 | `kafka` | `apache/kafka:3.9.0` | KRaft broker with SASL/PLAIN | 9092 |
| 2 | `kafka-init` | `apache/kafka:3.9.0` | Creates topics and ACLs (run-once) | — |
| 3 | `opensearch` | `opensearchproject/opensearch:2.18.0` | Log storage and search | 9200 |
| 4 | `redis` | `redis:7-alpine` | Pub/Sub fan-out + Celery broker | 6379 |
| 5 | `postgres` | `postgres:16-alpine` | Celery worker DB | 5433 |
| 6 | `fluent-bit` | `fluent/fluent-bit:3.2` | Docker log files → Kafka `job-logs` | 2020 (metrics) |
| 7 | `pfcon` | `ghcr.io/fnndsc/pfcon:latest` | Job control plane (fslink mode) | 30005 |
| 8 | `init-test-data` | `alpine:latest` | Creates sample fslink test data (run-once) | — |
| 9 | `event-forwarder` | `localhost/fnndsc/compute_event_forwarder` | Docker events → Kafka `job-status-events` | — |
| 10 | `status-consumer` | `localhost/fnndsc/compute_status_consumer` | Kafka → Celery | — |
| 11 | `log-consumer` | `localhost/fnndsc/compute_logs_consumer` | Kafka → OpenSearch + Redis | — |
| 12 | `sse-service` | Built from `Dockerfile.sse_service` | FastAPI SSE streaming | 8080 |
| 13 | `celery-worker` | Built from `Dockerfile.sse_service` | Celery status processing + PostgreSQL | — |
| 14 | `test-ui` | Built from `test_ui/Dockerfile` | nginx + static HTML/JS test app | 8888 |

### Dependency graph

```
init-test-data ──→ pfcon
kafka ──→ kafka-init ──→ event-forwarder
                     ├──→ status-consumer
                     ├──→ log-consumer    (also needs opensearch, redis)
                     └──→ fluent-bit
redis + postgres + pfcon ──→ celery-worker
redis + postgres ──→ sse-service
sse-service ──→ test-ui
```

### Kafka design

| Topic | Partitions | Retention | Key | Purpose |
|-------|-----------|-----------|-----|---------|
| `job-status-events` | 12 | 3 days | `job_id` | Status transitions from Event Forwarder |
| `job-logs` | 12 | 3 days | `job_id` | Log lines from Fluent Bit |
| `job-status-events-dlq` | 3 | 7 days | `job_id` | Failed status messages |
| `job-logs-dlq` | 3 | 7 days | `job_id` | Failed log messages |

Partitioning by `job_id` guarantees ordering of all events for a single job.

SASL/PLAIN users (defined in `kafka_server_jaas.conf`): `event-forwarder` (write events + write EOS markers to job-logs), `log-producer` (Fluent Bit writes logs), `status-consumer` (read events), `log-consumer` (read logs). Each user has ACLs restricting them to only the operations they need.

## Development and testing

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

This builds the custom service images, pulls infrastructure images (including pfcon), initializes Kafka topics/users, creates test data, and launches all services.

### Access points

| URL | Service |
|-----|---------|
| http://localhost:8888 | Test UI — submit jobs, watch status + logs |
| http://localhost:8080/health | SSE Service health check |
| http://localhost:8080/api/jobs/{job_id}/run | Submit a workflow (POST) |
| http://localhost:8080/api/jobs/{job_id}/workflow | Workflow status (GET) |
| http://localhost:8080/api/jobs/{job_id}/status/history | Status history (GET) |
| http://localhost:8080/events/{job_id}/status | SSE status stream (with historical replay) |
| http://localhost:8080/events/{job_id}/logs | SSE log stream (with historical replay) |
| http://localhost:8080/events/{job_id}/all | SSE combined stream (with historical replay) |
| http://localhost:8080/logs/{job_id}/history | Historical logs from OpenSearch |
| http://localhost:30005/api/v1/ | pfcon API (direct) |
| http://localhost:9200 | OpenSearch API |

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
```

### Inspect the pipeline internals

```bash
# Check Kafka topics
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --command-config /etc/kafka/admin.properties --list

# Read status events from Kafka
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --consumer.config /etc/kafka/admin.properties \
  --topic job-status-events --from-beginning --max-messages 10

# Query PostgreSQL for job statuses
docker compose exec postgres psql -U chris chris_streaming \
  -c "SELECT job_id, job_type, status, updated_at FROM job_status ORDER BY updated_at;"

# Query OpenSearch for logs
curl -s 'http://localhost:9200/job-logs-*/_search?q=job_id:test-job-1&sort=timestamp:asc&size=20' | jq '.hits.hits[]._source.line'

# Check Fluent Bit metrics
curl -s http://localhost:2020/api/v1/metrics
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

Tests are split into three levels — **unit tests** (no infrastructure, fast), **integration tests** (require Kafka, Redis, OpenSearch, PostgreSQL), and **end-to-end tests** (require the full application stack). All run entirely in Docker via `docker-compose.test.yml` and `Dockerfile.test`, using Docker Compose profiles to start only the services needed for each level.

#### Using `just` (recommended)

Install [just](https://github.com/casey/just), then:

```bash
# Run unit tests (no infrastructure needed, ~1s)
just run unit-tests

# Run integration tests (spins up Kafka, Redis, OpenSearch, PostgreSQL)
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
│   ├── test_common/                      # schemas, pfcon_status, container_naming, settings, kafka
│   ├── test_event_forwarder/             # producer, eos_producer, docker_watcher
│   ├── test_status_consumer/             # consumer, notifier
│   ├── test_log_consumer/                # consumer, opensearch_writer, redis_publisher
│   └── test_sse_service/                 # app, routes, pfcon_client, tasks
├── integration/                          # Requires Docker Compose infrastructure
│   ├── test_kafka_roundtrip.py           # Produce/consume through real Kafka
│   ├── test_redis_pubsub.py             # Pub/Sub and key operations
│   ├── test_opensearch_writer.py         # Bulk writes and queries
│   ├── test_postgres.py                  # Schema creation and upsert logic
│   └── test_sse_health.py               # FastAPI health endpoint
└── e2e/                                  # Requires full application stack
    └── test_workflow_e2e.py              # Submit workflows, verify SSE events + logs
```

- **Unit tests** use mocks exclusively — no network or container dependencies.
- **Integration tests** are marked with `@pytest.mark.integration` and test real service interactions (Kafka produce/consume, Redis Pub/Sub, OpenSearch bulk writes, PostgreSQL upserts).
- **E2E tests** are marked with `@pytest.mark.e2e` and exercise the full pipeline: submit a job via `POST /api/jobs/{id}/run`, verify status events arrive through the SSE stream, check workflow completion, and confirm logs appear in OpenSearch. Includes tests for successful workflows, failure scenarios (bad image), and historical replay for late-connecting SSE clients.


## Configuration

All services are configured via environment variables. The `.env` file provides defaults for the Docker Compose deployment.

### Shared Kafka settings

Used by: Event Forwarder, Status Consumer, Log Consumer

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |
| `KAFKA_SECURITY_PROTOCOL` | `SASL_PLAINTEXT` | `SASL_PLAINTEXT` for dev, `SASL_SSL` for production |
| `KAFKA_SASL_MECHANISM` | `PLAIN` | SASL mechanism |
| `KAFKA_SASL_USERNAME` | *(per service)* | SASL/PLAIN username |
| `KAFKA_SASL_PASSWORD` | *(per service)* | SASL/PLAIN password |
| `KAFKA_TOPIC_STATUS` | `job-status-events` | Status events topic |
| `KAFKA_TOPIC_LOGS` | `job-logs` | Log events topic |
| `KAFKA_TOPIC_STATUS_DLQ` | `job-status-events-dlq` | Status dead-letter topic |
| `KAFKA_TOPIC_LOGS_DLQ` | `job-logs-dlq` | Logs dead-letter topic |

### Event Forwarder

| Variable | Default | Description |
|----------|---------|-------------|
| `COMPUTE_ENV` | `docker` | `docker` or `kubernetes` |
| `DOCKER_LABEL_FILTER` | `org.chrisproject.miniChRIS` | Docker label key to filter containers |
| `DOCKER_LABEL_VALUE` | `plugininstance` | Expected value for the filter label |
| `K8S_NAMESPACE` | `default` | Kubernetes namespace (when `COMPUTE_ENV=kubernetes`) |
| `K8S_LABEL_SELECTOR` | `org.chrisproject.miniChRIS=plugininstance` | K8s label selector |
| `EMIT_INITIAL_STATE` | `true` | Emit current state of all containers on startup |
| `EOS_DELAY_SECONDS` | `10.0` | Delay before sending EOS marker to job-logs (seconds) |
| `KAFKA_SASL_USERNAME` | `event-forwarder` | Kafka SASL/PLAIN user |
| `KAFKA_SASL_PASSWORD` | `event-forwarder-secret` | Kafka SASL/PLAIN password |

Requires the Docker socket mounted at `/var/run/docker.sock` (Docker mode) or in-cluster K8s config (Kubernetes mode).

### Status Consumer

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_SASL_USERNAME` | `status-consumer` | Kafka SASL/PLAIN user |
| `KAFKA_SASL_PASSWORD` | `status-consumer-secret` | Kafka SASL/PLAIN password |
| `KAFKA_CONSUMER_GROUP` | `status-consumer-group` | Kafka consumer group ID |
| `CELERY_BROKER_URL` | `redis://redis:6379/0` | Celery broker URL |
| `MAX_RETRIES` | `3` | Retries before sending to DLQ |

### Log Consumer

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_SASL_USERNAME` | `log-consumer` | Kafka SASL/PLAIN user |
| `KAFKA_SASL_PASSWORD` | `log-consumer-secret` | Kafka SASL/PLAIN password |
| `KAFKA_CONSUMER_GROUP` | `log-consumer-group` | Kafka consumer group ID |
| `OPENSEARCH_URL` | `http://opensearch:9200` | OpenSearch endpoint |
| `REDIS_URL` | `redis://redis:6379/0` | Redis URL for Pub/Sub |
| `BATCH_MAX_SIZE` | `200` | Max messages per batch before flush |
| `BATCH_MAX_WAIT_SECONDS` | `2.0` | Max seconds before flushing a partial batch |

### SSE Service

| Variable | Default | Description |
|----------|---------|-------------|
| `HOST` | `0.0.0.0` | Bind address |
| `PORT` | `8080` | Bind port |
| `REDIS_URL` | `redis://redis:6379/0` | Redis URL for Pub/Sub subscriptions |
| `OPENSEARCH_URL` | `http://opensearch:9200` | OpenSearch for historical log queries |
| `CELERY_BROKER_URL` | `redis://redis:6379/0` | Celery broker |
| `DB_DSN` | `postgresql://chris:chris1234@postgres:5432/chris_streaming` | PostgreSQL connection string |
| `PFCON_URL` | `http://pfcon:30005` | pfcon API base URL |
| `PFCON_USER` | `pfcon` | pfcon API username |
| `PFCON_PASSWORD` | `pfcon1234` | pfcon API password |
| `EOS_QUIESCENCE_SECONDS` | `10.0` | Fallback window used by `cleanup_containers`: if no `logs_flushed` key appears, a step whose terminal status has been stable for this long is treated as drained. EOS is a hint, not a hard gate. |

### Celery Worker

Uses the same image as SSE Service. Runs with:
```
celery -A chris_streaming.sse_service.tasks worker -l info -Q status-processing -c 2
```

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://redis:6379/0` | Redis URL for publishing confirmed statuses and checking logs_flushed keys |
| `CELERY_BROKER_URL` | `redis://redis:6379/0` | Celery broker |
| `DB_DSN` | `postgresql://chris:chris1234@postgres:5432/chris_streaming` | PostgreSQL connection string |
| `PFCON_URL` | `http://pfcon:30005` | pfcon API base URL |
| `PFCON_USER` | `pfcon` | pfcon API username |
| `PFCON_PASSWORD` | `pfcon1234` | pfcon API password |

### Fluent Bit

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | `kafka:9092` | Kafka broker address |
| `KAFKA_SASL_USERNAME` | `log-producer` | Kafka SASL/PLAIN user |
| `KAFKA_SASL_PASSWORD` | `log-producer-secret` | Kafka SASL/PLAIN password |

Requires `/var/lib/docker/containers` and `/var/run/docker.sock` mounted read-only.

### pfcon

| Variable | Default | Description |
|----------|---------|-------------|
| `APPLICATION_MODE` | `development` | Enables `DevConfig` with hardcoded test credentials |
| `PFCON_INNETWORK` | `true` | In-network mode (containers share a volume) |
| `STORAGE_ENV` | `fslink` | Filesystem with ChRIS link expansion |
| `CONTAINER_ENV` | `docker` | Schedule containers via Docker API |
| `JOB_LABELS` | `org.chrisproject.miniChRIS=plugininstance` | Labels applied to all job containers |
| `REMOVE_JOBS` | `yes` | Remove containers on DELETE |

### Kafka broker

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_ADMIN_PASSWORD` | `admin-secret` | SASL/PLAIN password for the admin super-user |

### PostgreSQL

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_DB` | `chris_streaming` | Database name (used by Celery Worker) |
| `POSTGRES_USER` | `chris` | Database user |
| `POSTGRES_PASSWORD` | `chris1234` | Database password |

### Production TLS and authentication for Kafka

The dev environment uses `SASL_PLAINTEXT` with `SASL/PLAIN` (credentials in cleartext over the wire). For production:

1. Switch authentication from `SASL/PLAIN` to `SASL/SCRAM-SHA-512` (hashed credentials)
2. Generate CA, broker, and client certificates
3. Configure the Kafka broker with `ssl.keystore.location`, `ssl.truststore.location`
4. Change `KAFKA_SECURITY_PROTOCOL` to `SASL_SSL` and `KAFKA_SASL_MECHANISM` to `SCRAM-SHA-512` on all clients
5. Distribute client keystores/truststores to each service container
6. Update Fluent Bit `rdkafka.security.protocol` to `SASL_SSL`, `rdkafka.sasl.mechanism` to `SCRAM-SHA-512`, and add `rdkafka.ssl.ca.location`
