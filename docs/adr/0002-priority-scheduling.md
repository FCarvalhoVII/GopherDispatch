# ADR 0002: Priority scheduling

## Context

Jobs have high, medium, and low priorities, but strict preference can cause starvation.

## Decision

In the foundation, order jobs in PostgreSQL by priority and creation time. In v0.2, move consumption to three Redis queues and use configurable weighted cycles.

## Consequences

v0.1 is simple but can starve lower-priority jobs under continuous load. The later weighted scheduler will retain preference while providing progress for lower-priority queues.

## Alternatives considered

A local heap does not coordinate multiple instances. Permanent strict priority was rejected as the final solution.
