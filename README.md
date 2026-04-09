# ChRIS Streaming Workers

Event-driven streaming workers that replace ChRIS CUBE's polling-based job observability with a push/event-driven architecture. This repository implements three core long-running Kafka worker processes — **Event Forwarder**, **Status Consumer**, and **Log Consumer** — and the surrounding infrastructure to demonstrate them working within proper event and log pipelines.

## Overview

In the current ChRIS architecture, CUBE polls pfcon every 5 seconds for every active plugin instance to check status and retrieve logs. This generates O(N) API calls per poll cycle (where N = active jobs), producing excessive database queries, enqueued Celery tasks, and heavy pressure on the Kubernetes/Docker API.

This repository introduces a push-based alternative with two separate pipelines:

**Event Pipeline** (status changes):
```
Docker/K8s Runtime → Event Forwarder → Kafka [job-status-events] → Status Consumer → PostgreSQL + Redis Pub/Sub + Celery
```

**Log Pipeline** (container output):
```
Docker/K8s Runtime → Fluent Bit → Kafka [job-logs] → Log Consumer → OpenSearch + Redis Pub/Sub
```

Both pipelines feed into a real-time streaming layer:
```
Redis Pub/Sub → SSE Service → Browser (EventSource)
```

### The three core workers

- **Event Forwarder** (`compute_event_forwarder`) — Async daemon that watches Docker daemon events (or Kubernetes Job API) for ChRIS job containers, maps native container states to pfcon's `JobStatus` enum (`notStarted`, `started`, `finishedSuccessfully`, `finishedWithError`, `undefined`), and produces structured status events to Kafka. Stateless, idempotent, restart-safe with auto-reconnect.

- **Status Consumer** (`compute_status_consumer`) — Kafka consumer that reads status events, upserts them to PostgreSQL, publishes to Redis Pub/Sub for real-time delivery, and schedules Celery confirmation tasks for terminal statuses. Failed messages go to a dead-letter topic after configurable retries.

- **Log Consumer** (`compute_logs_consumer`) — Batched Kafka consumer that reads log events (produced by Fluent Bit), bulk-writes to OpenSearch for durable storage and search, and publishes to Redis Pub/Sub for real-time log streaming. Configurable batch size and flush interval.

### Supporting components

The repository also includes supporting infrastructure and a pilot test environment to demonstrate the full pipeline end-to-end:

- **Kafka** (KRaft mode) with SASL/PLAIN authentication, per-service users, and ACLs
- **Fluent Bit** reading Docker container logs, filtering by ChRIS labels, and forwarding to Kafka
- **OpenSearch** for log storage and historical replay
- **Redis** for Pub/Sub fan-out and Celery broker
- **PostgreSQL** for durable status tracking
- **SSE Service** (pilot FastAPI app) that subscribes to Redis and streams events to browsers
- **Celery Worker** that processes terminal status confirmations
- **pfcon** (`ghcr.io/fnndsc/pfcon:latest`, which includes `org.chrisproject.job_type` labels) as the job control plane
- **Test UI** for submitting jobs to pfcon and watching status + logs stream in real-time

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
│   ├── event_forwarder/                        # Produces to job-status-events
│   │   ├── __init__.py
│   │   ├── __main__.py                         # python -m chris_streaming.event_forwarder
│   │   ├── watcher.py                          # Abstract async watcher protocol
│   │   ├── docker_watcher.py                   # Docker event stream → StatusEvent
│   │   ├── k8s_watcher.py                      # K8s Job watch API → StatusEvent
│   │   └── producer.py                         # Kafka producer with idempotence + dedup
│   │
│   ├── status_consumer/                        # Consumes job-status-events
│   │   ├── __init__.py
│   │   ├── __main__.py                         # python -m chris_streaming.status_consumer
│   │   ├── consumer.py                         # Kafka consumer with retry + DLQ
│   │   ├── db.py                               # asyncpg PostgreSQL upserts + schema
│   │   └── notifier.py                         # Redis Pub/Sub + Celery task scheduling
│   │
│   ├── log_consumer/                           # Consumes job-logs
│   │   ├── __init__.py
│   │   ├── __main__.py                         # python -m chris_streaming.log_consumer
│   │   ├── consumer.py                         # Batched Kafka consumer
│   │   ├── opensearch_writer.py                # Bulk writes with daily index rotation
│   │   └── redis_publisher.py                  # Per-job Pub/Sub fan-out
│   │
│   └── sse_service/                            # Pilot FastAPI SSE service
│       ├── __init__.py
│       ├── __main__.py                         # python -m chris_streaming.sse_service
│       ├── app.py                              # FastAPI app with CORS
│       ├── routes.py                           # SSE + history endpoints
│       ├── redis_subscriber.py                 # Async Redis subscriber per SSE connection
│       └── tasks.py                            # Celery confirm_job_status task
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
│       └── app.js                              # pfcon client + EventSource SSE client
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
| 5 | `postgres` | `postgres:16-alpine` | Status consumer DB | 5433 |
| 6 | `fluent-bit` | `fluent/fluent-bit:3.2` | Docker log files → Kafka `job-logs` | 2020 (metrics) |
| 7 | `pfcon` | `ghcr.io/fnndsc/pfcon:latest` | Job control plane (fslink mode) | 30005 |
| 8 | `init-test-data` | `alpine:latest` | Creates sample fslink test data (run-once) | — |
| 9 | `event-forwarder` | `localhost/fnndsc/compute_event_forwarder` | Docker events → Kafka `job-status-events` | — |
| 10 | `status-consumer` | `localhost/fnndsc/compute_status_consumer` | Kafka → PostgreSQL + Redis + Celery | — |
| 11 | `log-consumer` | `localhost/fnndsc/compute_logs_consumer` | Kafka → OpenSearch + Redis | — |
| 12 | `sse-service` | Built from `Dockerfile.sse_service` | FastAPI SSE streaming | 8080 |
| 13 | `celery-worker` | Built from `Dockerfile.sse_service` | Celery confirmation tasks | — |
| 14 | `test-ui` | Built from `test_ui/Dockerfile` | nginx + static HTML/JS test app | 8888 |

### Dependency graph

```
init-test-data ──→ pfcon
kafka ──→ kafka-init ──→ event-forwarder
                     ├──→ status-consumer (also needs postgres, redis)
                     ├──→ log-consumer    (also needs opensearch, redis)
                     └──→ fluent-bit
redis ──→ sse-service
      ──→ celery-worker
pfcon + sse-service ──→ test-ui
```

### Kafka design

| Topic | Partitions | Retention | Key | Purpose |
|-------|-----------|-----------|-----|---------|
| `job-status-events` | 12 | 3 days | `job_id` | Status transitions from Event Forwarder |
| `job-logs` | 12 | 3 days | `job_id` | Log lines from Fluent Bit |
| `job-status-events-dlq` | 3 | 7 days | `job_id` | Failed status messages |
| `job-logs-dlq` | 3 | 7 days | `job_id` | Failed log messages |

Partitioning by `job_id` guarantees ordering of all events for a single job.

SASL/PLAIN users (defined in `kafka_server_jaas.conf`): `event-forwarder` (write events), `log-producer` (Fluent Bit writes logs), `status-consumer` (read events), `log-consumer` (read logs). Each user has ACLs restricting them to only the operations they need.

## Development and testing

### Prerequisites

- Docker and Docker Compose v2
- The `ghcr.io/fnndsc/pfcon:latest` image (includes `org.chrisproject.job_type` label support)

### Start everything

```bash
cd /path/to/chris_streaming_workers
docker compose up --build
```

This builds the custom service images, pulls infrastructure images (including pfcon), initializes Kafka topics/users, creates test data, and launches all services.

### Access points

| URL | Service |
|-----|---------|
| http://localhost:8888 | Test UI — submit jobs, watch status + logs |
| http://localhost:8080/health | SSE Service health check |
| http://localhost:8080/events/{job_id}/status | SSE status stream (direct) |
| http://localhost:8080/events/{job_id}/logs | SSE log stream (direct) |
| http://localhost:8080/logs/{job_id}/history | Historical logs from OpenSearch |
| http://localhost:30005/api/v1/ | pfcon API (direct) |
| http://localhost:9200 | OpenSearch API |

### Run a test job via the UI

1. Open http://localhost:8888
2. The form is pre-filled with defaults for `pl-simpledsapp` against the test data
3. Click **Run Full Workflow** — the UI will:
   - Authenticate with pfcon
   - Schedule copy → poll → schedule plugin → poll → get files → upload (no-op) → delete → cleanup
4. Watch the **Status Events** panel for real-time SSE status updates from the event pipeline
5. Watch the **Container Logs** panel for real-time log lines from the log pipeline

### Run a test job via curl

```bash
# Authenticate
TOKEN=$(curl -s -X POST http://localhost:30005/api/v1/auth-token/ \
  -H 'Content-Type: application/json' \
  -d '{"pfcon_user":"pfcon","pfcon_password":"pfcon1234"}' | jq -r '.token')

# Schedule copy
curl -s -X POST http://localhost:30005/api/v1/copyjobs/ \
  -H "Authorization: Bearer $TOKEN" \
  -d 'jid=test-job-1&input_dirs=home/user/cube&output_dir=home/user/cube_out'

# Poll copy until finishedSuccessfully
curl -s http://localhost:30005/api/v1/copyjobs/test-job-1/ \
  -H "Authorization: Bearer $TOKEN"

# Schedule plugin
curl -s -X POST http://localhost:30005/api/v1/pluginjobs/ \
  -H "Authorization: Bearer $TOKEN" \
  -d 'jid=test-job-1&args=--prefix&args=le&auid=cube&number_of_workers=1&cpu_limit=1000&memory_limit=200&gpu_limit=0&image=fnndsc/pl-simpledsapp&entrypoint=python3&entrypoint=/usr/local/bin/simpledsapp&type=ds&input_dirs=home/user/cube&output_dir=home/user/cube_out'

# Meanwhile, stream SSE events in another terminal:
curl -N http://localhost:8080/events/test-job-1/all
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
docker compose down           # stop services
docker compose down -v        # stop and remove all volumes (full reset)
```

### Local Python development

For working on the Python code outside Docker:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[all,dev]"

# Run linter
ruff check chris_streaming/

# Run tests (requires infrastructure services running)
pytest tests/
```

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
| `KAFKA_SASL_USERNAME` | `event-forwarder` | Kafka SASL/PLAIN user |
| `KAFKA_SASL_PASSWORD` | `event-forwarder-secret` | Kafka SASL/PLAIN password |

Requires the Docker socket mounted at `/var/run/docker.sock` (Docker mode) or in-cluster K8s config (Kubernetes mode).

### Status Consumer

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_SASL_USERNAME` | `status-consumer` | Kafka SASL/PLAIN user |
| `KAFKA_SASL_PASSWORD` | `status-consumer-secret` | Kafka SASL/PLAIN password |
| `KAFKA_CONSUMER_GROUP` | `status-consumer-group` | Kafka consumer group ID |
| `DB_DSN` | `postgresql://chris:chris1234@postgres:5432/chris_streaming` | PostgreSQL connection string |
| `REDIS_URL` | `redis://redis:6379/0` | Redis URL for Pub/Sub |
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
| `CELERY_BROKER_URL` | `redis://redis:6379/0` | Celery broker for the confirmation worker |

### Celery Worker

Uses the same image as SSE Service. Runs with:
```
celery -A chris_streaming.sse_service.tasks worker -l info -Q confirmation -c 2
```

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://redis:6379/0` | Redis URL for publishing confirmed statuses |
| `CELERY_BROKER_URL` | `redis://redis:6379/0` | Celery broker |

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
| `POSTGRES_DB` | `chris_streaming` | Database name |
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
