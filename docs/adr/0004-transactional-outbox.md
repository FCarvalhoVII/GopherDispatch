# ADR 0004: Transactional outbox for queue publication

## Context

PostgreSQL and Redis cannot participate in one transaction. Persisting a job and publishing it directly can lose work if Redis fails after the database commit.

## Decision

Insert a job and a job-specific outbox event in the same PostgreSQL transaction. Background publishers claim unpublished events, publish a job reference to Redis, then mark the event published.

## Consequences

Committed jobs are eventually publishable after transient Redis failures. A crash after Redis publication but before marking the event published may duplicate a stream entry; the worker claim makes that safe under at-least-once delivery.

## Alternatives Considered

Dual writes without an outbox can lose jobs. Distributed two-phase commit is disproportionate and unsupported by Redis.
