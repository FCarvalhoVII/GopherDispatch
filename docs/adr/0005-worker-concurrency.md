# ADR 0005: Bounded worker concurrency and execution claims

## Context

Messages may be duplicated by retries, recovery, or duplicate outbox publication. Multiple processes can receive or recover related messages.

## Decision

Run a fixed number of goroutines and transition a job from `pending` to `processing` with a conditional PostgreSQL update before handler execution. Store the owner and claim timestamp. Terminal duplicate deliveries are acknowledged; active deliveries are left pending.

## Consequences

Only one live worker executes a pending job. The system does not re-execute a job whose owner crashed until lease recovery is implemented, favoring safety over automatic recovery in v0.2.

## Alternatives Considered

Redis-only locking creates two sources of execution state. Re-executing every idle pending message risks concurrent external side effects.
