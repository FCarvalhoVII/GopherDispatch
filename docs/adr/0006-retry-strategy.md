# ADR 0006: Retry strategy

## Context

Transient failures need retries without retry storms, while permanent failures need a terminal outcome.

## Decision

v0.2 preserves the v0.1 behavior: a handler failure becomes `failed`. v0.3 will persist `next_attempt_at`, use capped exponential backoff with jitter, and move exhausted jobs to `dead_letter`.

## Consequences

The queue and outbox can safely support future retries without changing their delivery semantics. Automatic retries and manual retry endpoints are intentionally not part of v0.2.

## Alternatives Considered

Immediate retries amplify outages. Fixed delays synchronize workers. Full workflow engines are excessive for this scope.
