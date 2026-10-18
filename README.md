# GopherQueue

GopherQueue is a Go backend for durable, asynchronous job processing. **v0.2 — Queue & Worker Pool** adds Redis Streams, priority-aware scheduling, a PostgreSQL transactional outbox, and bounded concurrent workers while retaining PostgreSQL as the authoritative job store.

## Architecture

```mermaid
flowchart LR
  C[Client] --> A[HTTP API]
  A -->|one PostgreSQL transaction| P[(PostgreSQL jobs)]
  A -->|same transaction| O[(job_outbox)]
  O --> OP[Outbox publisher]
  OP --> RH[Redis Stream: high]
  OP --> RM[Redis Stream: medium]
  OP --> RL[Redis Stream: low]
  RH & RM & RL --> S[Weighted scheduler]
  S --> W[Concurrent worker pool]
  W -->|conditional execution claim| P
  W --> H[Handler]
  H -->|terminal state persisted| P
  W -->|XACK after persistence| RH & RM & RL
```

The API and worker are independent processes. The API writes a job and an outbox event atomically; its background publisher converts outbox entries into small Redis messages containing only `job_id` and priority. Workers load the durable job after receiving a stream message.

## Running locally

```bash
cp .env.example .env
docker compose up --build
```

The Compose stack starts `api`, `worker`, PostgreSQL, and Redis. PostgreSQL initialization applies all migrations only to a new volume. To deliberately reset local data after changing migrations:

```bash
docker compose down -v
docker compose up --build
```

Create a job:

```bash
curl -i -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"send-email","priority":"high","payload":{"email":"john@example.com"}}'
```

Then retrieve it with `GET /jobs/{id}`. Supported simulated handlers are `send-email`, `generate-report`, and `webhook-delivery`.

## Queueing, priorities, and concurrency

Each priority has its own Redis Stream:

- `gopherqueue:jobs:high`
- `gopherqueue:jobs:medium`
- `gopherqueue:jobs:low`

Workers join the configured Redis Consumer Group. A mutex-protected deterministic cycle allocates reads with default weights `high=5`, `medium=3`, and `low=1`. An empty stream returns immediately, so it does not hold capacity while another priority has work. Weights express consumption opportunities, not a strict global execution order across concurrent workers.

`WORKER_COUNT` bounds active handler execution. Every worker has a unique consumer identity derived from container hostname, process ID, and worker number, so scaled worker containers do not share a Redis consumer name.

## PostgreSQL and Redis consistency

Job creation inserts `jobs` and `job_outbox` in one PostgreSQL transaction. If Redis is unavailable after that commit, the unpublished event remains durable and the publisher retries it on later polls. Multiple publishers safely claim events with `FOR UPDATE SKIP LOCKED`.

Redis publication and marking an event published cannot be atomic together. A publisher crash after `XADD` and before marking the outbox event may publish the same job twice. This is intentional at-least-once behavior: workers make a conditional PostgreSQL claim, and terminal duplicate deliveries are acknowledged without re-running the handler.

## Claims, acknowledgment, and recovery

Before invoking a handler, a worker atomically updates a job from `pending` to `processing`. It records `processing_owner` and `claimed_at`, creating a path to lease-based recovery in v0.5. Only the worker that wins this transition executes the handler.

Messages are acknowledged only after PostgreSQL stores `completed` or `failed`. A database failure leaves the Redis message pending. An ACK failure may cause a harmless duplicate delivery; a terminal job is acknowledged without executing again.

At startup, workers use `XAUTOCLAIM` for messages idle longer than `PENDING_IDLE_TIMEOUT`. Recovery is deliberately conservative: if PostgreSQL says the job is still `processing`, the message remains pending and is not re-executed. This prevents concurrent duplicates, but a job stranded by a crashed worker is not automatically recovered until v0.5 introduces execution leases.

## Configuration

| Variable | Default | Purpose |
|---|---:|---|
| `DATABASE_URL` | required | PostgreSQL DSN |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `REDIS_CONSUMER_GROUP` | `gopherqueue-workers` | stream consumer group |
| `WORKER_COUNT` | `4` | maximum active worker goroutines |
| `PRIORITY_HIGH_WEIGHT` | `5` | high-priority scheduling opportunities |
| `PRIORITY_MEDIUM_WEIGHT` | `3` | medium-priority scheduling opportunities |
| `PRIORITY_LOW_WEIGHT` | `1` | low-priority scheduling opportunities |
| `WORKER_POLL_INTERVAL` | `100ms` | pause after an empty/erroring queue read |
| `OUTBOX_POLL_INTERVAL` | `1s` | publisher retry interval |
| `OUTBOX_BATCH_SIZE` | `50` | events claimed per publisher cycle |
| `PENDING_IDLE_TIMEOUT` | `30s` | minimum idle time before pending recovery |
| `JOB_TIMEOUT` | `30s` | per-handler timeout |
| `SHUTDOWN_TIMEOUT` | `15s` | graceful-drain deadline |

Existing HTTP controls, including `REQUEST_BODY_LIMIT` and `RATE_LIMIT_PER_MINUTE`, remain supported.

## Graceful shutdown

The API stops accepting HTTP traffic and cancels its publisher. Workers stop new stream reads when signaled, allow already claimed handlers to use their bounded job context, persist terminal results, and ACK only finalized messages. Unfinished jobs remain pending in Redis and recoverable according to the conservative policy above.

## Testing

```bash
make lint
make test
make test-race
make benchmark
```

Unit tests cover the weighted cycle, outbox publication behavior, handler completion and ACK ordering, duplicate delivery behavior, configuration, and existing HTTP behavior. PostgreSQL integration tests remain opt-in:

```bash
TEST_DATABASE_URL='postgres://gopherqueue:gopherqueue@localhost:5432/gopherqueue?sslmode=disable' \
go test -tags=integration ./internal/storage/postgres
```

## Known limitations

- v0.2 provides at-least-once delivery, never exactly-once execution.
- Job execution leases and automatic recovery of jobs stranded in `processing` are deferred to v0.5.
- A publisher crash in the Redis-publication/Outbox-confirmation window can produce a duplicate stream message.
- Handler failures become `failed`; retries, exponential backoff, jitter, DLQ, idempotency keys, and manual retry are v0.3 work.
- Redis and PostgreSQL integration tests require locally available services.

## ADRs

Architecture decisions are documented in [`docs/adr`](docs/adr/), including delivery semantics, priority scheduling, Redis Streams, transactional outbox, locking, and worker concurrency.
