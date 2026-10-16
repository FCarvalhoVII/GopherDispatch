# ADR 0001: Job delivery semantics

## Context

Committing a job state and executing a handler's external side effect do not share an atomic transaction.

## Decision

Target at-least-once delivery. Handlers must accept a stable key (the job ID) and implement idempotency. v0.1 does not recover abandoned claims; leases will be added before claiming the complete guarantee across failures.

## Consequences

Jobs can be repeated after failure recovery. The implementation avoids promising exactly-once semantics and makes the handler responsibility explicit.

## Alternatives considered

Exactly-once was rejected because it cannot be generically guaranteed between a database and external services. At-most-once would lose work during crashes.
