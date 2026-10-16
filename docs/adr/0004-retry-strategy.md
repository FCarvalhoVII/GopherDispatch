# ADR 0004: Retry strategy

## Context

Transient failures need retries without causing retry storms, while permanent failures need to stop.

## Decision

In v0.3, persist `next_attempt_at`, use capped exponential backoff with jitter, and transition to `dead_letter` on reaching `max_attempts`. The random source will be injectable for deterministic tests.

## Consequences

v0.1 marks failures as `failed` without retry. The evolution will require an index of available jobs and an explicit manual reprocessing policy.

## Alternatives considered

Immediate retries were rejected because they amplify failures. Fixed backoff synchronizes workers. Full workflow libraries are excessive for this scope.
