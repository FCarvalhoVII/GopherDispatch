# ADR 0002: Weighted priority scheduling

## Context

High-priority work should receive more opportunities, but strict priority can indefinitely starve medium and low queues.

## Decision

Use a mutex-protected, deterministic weighted cycle. The defaults are high=5, medium=3, and low=1. Each worker asks the shared scheduler for its next stream before attempting a non-blocking Consumer Group read.

## Consequences

Every nonempty priority receives recurring scheduling opportunities. Empty streams do not block other streams. Concurrent workers mean the exact global order is not guaranteed, but bounded scheduling opportunities are preserved.

## Alternatives Considered

Strict priority was rejected because it starves lower priorities. A local priority heap would not coordinate across worker processes.
