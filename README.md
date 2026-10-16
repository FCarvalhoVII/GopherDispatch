# GopherQueue

GopherQueue is a Go backend for creating and asynchronously processing jobs. It is designed as a production-minded exercise in concurrency and distributed systems, not a CRUD application. This release implements **v0.1 — Foundation**: an HTTP API, PostgreSQL persistence, a concurrent worker pool, simulated handlers, basic HTTP safeguards, graceful shutdown, and tests.

> Current status: v0.1. Redis, weighted scheduling, retry backoff, idempotency, metrics, and tracing are part of the target design, but are intentionally deferred to later versions to keep the evolution verifiable.

## Architecture

```text
Client
  |
  v
+---------+       +----------------+
| Go API  | ----> | PostgreSQL     |
+---------+       | durable jobs   |
                  +-------+--------+
                          | atomic claim
                          v
                  +----------------+
                  | Worker Pool    |
                  | N goroutines   |
                  +-------+--------+
                          |
                          v
                  Handler registry
```

```text
cmd/api/                       API composition root
cmd/worker/                    worker composition root
internal/config/               environment configuration
internal/httpapi/              routes and middleware
internal/job/                  domain, service, and repository contract
internal/storage/postgres/     PostgreSQL implementation
internal/worker/               pool and handlers
migrations/                    SQL migrations
docs/adr/                      architecture decision records
```

Interfaces exist only at boundaries with concrete reasons to vary: persistence and handlers. Composition remains explicit in both binaries.

## Flows

Creation: the client sends `POST /jobs`; the API limits and decodes the body, validates type, priority, and payload, and stores the job as `pending`.

Processing: each goroutine requests a job. A single SQL statement uses `FOR UPDATE SKIP LOCKED` and changes the state to `processing`, eliminating the gap between selecting and reserving a job. The registry selects a handler by type, a timeout context wraps execution, and the job becomes `completed` or `failed`.

Lookup: `GET /jobs/{id}` reads the durable source of truth in PostgreSQL.

## Running locally

Requirements: Docker with Compose. The image uses Go 1.27, the current stable release selected for this project.

```bash
cp .env.example .env
docker compose up --build
```

PostgreSQL applies the first migration when its volume is created. If a migration changes during development, deliberately recreate the volume with `docker compose down -v` — this deletes local data.

Check the services:

```bash
curl http://localhost:8080/health
curl http://localhost:8080/ready
```

Create and retrieve a job:

```bash
curl -i -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"send-email","priority":"high","payload":{"email":"john@example.com","template":"welcome"}}'

curl http://localhost:8080/jobs/JOB_ID
```

Registered simulated types are `send-email`, `generate-report`, and `webhook-delivery`.

## Configuration

| Variable | Default | Purpose |
|---|---:|---|
| `DATABASE_URL` | required | PostgreSQL DSN |
| `HTTP_PORT` | `8080` | API port |
| `WORKER_COUNT` | `4` | consumer goroutines |
| `WORKER_POLL_INTERVAL` | `500ms` | pause when no jobs are available |
| `MAX_JOB_ATTEMPTS` | `3` | persisted value for the retry evolution |
| `JOB_TIMEOUT` | `30s` | per-handler limit |
| `SHUTDOWN_TIMEOUT` | `15s` | drain deadline |
| `REQUEST_BODY_LIMIT` | `1048576` | maximum accepted API body in bytes |
| `RATE_LIMIT_PER_MINUTE` | `120` | simple per-IP, per-instance limit |

## Concurrency and delivery guarantee

The worker pool has one loop per goroutine and does not create a goroutine per job. Cancellation stops new claims; already claimed work drains until the job timeout or the global shutdown deadline. `SKIP LOCKED` permits concurrency across processes without an additional distributed lock.

The system targets **at-least-once** delivery. In v0.1, a crash after a claim can leave a job in `processing`; leases and recovery arrive in v0.5. Once recovery exists, a crash after an external side effect and before committing the job state can repeat the handler. Exactly-once would require a shared transaction across PostgreSQL and every external side effect, which is generally unavailable. Real handlers must therefore be idempotent.

The `idempotency_key` column is reserved in the schema, but HTTP deduplication will only be enabled in v0.3. This version does not promise idempotency.

## Priority, retry, and timeout

In v0.1, SQL prefers `high`, then `medium`, then `low`, while keeping FIFO order within each class. Continuous high-priority load can cause starvation; this is a known limitation. v0.2 replaces this ordering with Redis queues and fair weighted scheduling.

Automatic retries, exponential backoff, jitter, and a dead-letter queue will be implemented in v0.3. Today, a failure ends in `failed`. Per-job timeout already applies to the handler; handlers must honor their context.

## Security and observability

The API includes a request-body limit, strict JSON decoding, validation, server timeouts, panic recovery, and a simple local rate limit. Logs are JSON; worker events include `job_id`, type, priority, attempt, worker, and duration. Payloads and credentials are never logged.

Prometheus and OpenTelemetry are planned for v0.4. The in-memory rate limiter is not a distributed defense; production should also enforce limits at the ingress or gateway.

## Tests and quality

```bash
make test
make test-race
make benchmark
make lint
```

The integration test is opt-in and requires a migrated PostgreSQL instance:

```bash
TEST_DATABASE_URL='postgres://gopherqueue:gopherqueue@localhost:5432/gopherqueue?sslmode=disable' \
go test -tags=integration ./internal/storage/postgres
```

CI runs formatting, `go vet`, regular tests, and the race detector. `BenchmarkSimulatedHandler` provides the initial benchmark infrastructure; the scheduler receives a dedicated benchmark in v0.2.

## Trade-offs and current limitations

- PostgreSQL is both the queue and source of truth in v0.1: it reduces components and provides atomic claims, but polling costs queries and the current order is not fair.
- There is no retry, DLQ, lease, or automatic recovery of abandoned jobs.
- `Idempotency-Key` deduplication is not implemented yet.
- PostgreSQL's entrypoint applies migrations only to new volumes; production should use a dedicated migration tool.
- Prometheus metrics and OpenTelemetry tracing are not exposed yet.
- Handlers only simulate work and do not produce external side effects.

## Roadmap

- **v0.2 — Queue:** Redis, high/medium/low queues, a starvation-free weighted scheduler, and scheduler benchmarks.
- **v0.3 — Reliability:** retries, deterministically tested jittered backoff, DLQ, and idempotency with a configurable window.
- **v0.4 — Observability:** Prometheus metrics, queue depth, and OpenTelemetry traces between API and worker.
- **v0.5 — Distributed Workers:** leases, heartbeat/recovery, and failure tests.
- **v1.0:** hardening, load tests, releases, and open-source-ready documentation.

Detailed decisions are available in [`docs/adr`](docs/adr/).
