# Kubernetes deployment

This directory contains Kubernetes manifests that bring up the same stack
`docker-compose.yml` defines (Redis + Quickwit + PostgreSQL + pfcon +
streaming workers + SSE + test UI) on a local Kubernetes cluster. Tested on
Docker Desktop's built-in K8s.

Transport for both job-status events and container log lines is Redis Streams
(sharded, with consumer groups, PEL, and DLQ). The Log Forwarder worker
replaces Fluent Bit — it watches Pods via the K8s API and streams logs with
`read_namespaced_pod_log(follow=True)` directly into the log-stream shards.

The top-level `justfile` provides `just k8s-*` recipes that mirror the
docker-compose workflow. All commands are idempotent and safe to re-run.

## Prerequisites

- Docker Desktop with Kubernetes enabled (or any local K8s cluster)
- `kubectl` configured to point at that cluster
- [just](https://github.com/casey/just)
- Port `5005` free on `127.0.0.1` (used by the local Docker registry)

## Commands

### Bring the full stack up

```bash
just k8s-up
```

This builds all service images, pushes them to a local Docker registry at
`localhost:5005`, applies every manifest via Kustomize, and waits for all
deployments and StatefulSets to become ready.

### Tear it down

```bash
just k8s-down    # delete app resources (PVCs preserved — data survives)
just k8s-nuke    # full reset: delete app + PVCs + test namespace + local registry
```

### Tail logs for a component

```bash
just k8s-logs sse-service        # by `app=` label
just k8s-logs event-forwarder
just k8s-logs celery-worker
```

### Access points

| URL | Service |
|-----|---------|
| http://localhost:30888 | Test UI |
| http://localhost:30080 | SSE service (REST + SSE endpoints) |
| http://localhost:30005 | pfcon API |

The SSE REST/SSE paths are the same as the docker-compose deployment — see
the main [README.md](../README.md) for the full endpoint list.

### Running automated tests

Tests run in-cluster via Kubernetes Jobs, using the same `chris_streaming_tests`
image and same three levels (unit / integration / e2e) as `just run`. Integration
infra lives in its own `chris-streaming-test` namespace that is created and
destroyed per invocation.

```bash
# Unit tests — no infrastructure needed (fast)
just k8s-run unit-tests

# Integration tests — spins up an ephemeral Redis/Quickwit/Postgres
# stack in the chris-streaming-test namespace, then tears it down
just k8s-run integration-tests

# E2E tests — requires the full app stack (calls `just k8s-up` first)
just k8s-run e2e-tests

# Run all three levels sequentially (CI/CD)
just k8s-run all-tests
```

Each recipe streams the test Job logs and exits with the Job's status.

## Local image registry

Images are pushed to a local Docker registry container (`chris-k8s-registry`)
bound to `127.0.0.1:5005`. We avoid port 5000 because it conflicts with macOS
AirPlay Receiver on modern macOS.

`just k8s-up` ensures the registry container is running, builds all service
images, and pushes them as `localhost:5005/fnndsc/<name>:dev`. Manifests
reference these tags with `imagePullPolicy: Always`, so re-pushing `:dev`
followed by a rollout restart picks up new code:

```bash
just k8s-build                                     # rebuild + push all images
kubectl rollout restart deployment/<name> -n chris-streaming
```

## Label conventions

pfcon's `KubernetesManager` translates `job_type` → `chrisproject.org/job-type`
on every pod it spawns. Our `JOB_LABELS` env applies `chrisproject.org/role=
plugininstance` to the same pods. The streaming workers select on
`chrisproject.org/role=plugininstance` and read `chrisproject.org/job-type` to
identify copy/plugin/upload/delete steps.

## Namespaces

- `chris-streaming` — the application itself. pfcon schedules plugininstance
  Jobs here too (`JOB_NAMESPACE=chris-streaming`) so the event forwarder and
  log forwarder can watch/read them.
- `chris-streaming-test` — ephemeral integration infra + test Jobs. Created
  and destroyed per `just k8s-run` invocation so test state never leaks.

## Redis topology

`kubernetes/20-infra/redis.yaml` runs a single Redis StatefulSet with
`--appendonly yes --appendfsync everysec`. `redis-ha.yaml` is opt-in: a
primary + 2 replicas + 3 Sentinels for failover. To switch the workers
onto the HA topology, apply `redis-ha.yaml` and update their `REDIS_URL`
env to a `redis+sentinel://` URL pointing at the Sentinel service.

All four workers share the same stream topology via env:

- `stream:job-status:{0..N}` — status events (Event Forwarder → Status
  Consumer → Celery task → PostgreSQL + Pub/Sub).
- `stream:job-logs:{0..N}` — container log lines (Log Forwarder → Log
  Consumer → Quickwit + Pub/Sub), plus `eos=true` markers on each
  container's final log entry.

Shard count, consumer group names, batch sizes, and reclaim/lease TTLs
are read from env at pod startup — see the main [README.md](../README.md)
for the full configuration reference.

## Resilience notes

- `celery-worker` has an `initContainer` that blocks on TCP readiness of
  postgres, redis, and pfcon before Celery's `worker_init` fires.
- The event forwarder watches both Jobs *and* Pods. If a plugininstance pod
  enters a terminal waiting state (`ImagePullBackOff`, `ErrImagePull`,
  `InvalidImageName`, `CreateContainerConfigError`, `CreateContainerError`)
  the forwarder emits `finishedWithError` for the owning Job immediately —
  the workflow advances to cleanup instead of hanging until Job `backoffLimit`
  is exhausted.
