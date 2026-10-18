# ADR 0003: Redis Streams and Consumer Groups

## Context

The queue needs durable broker messages, explicit acknowledgments, consumer coordination, and visibility into pending work.

## Decision

Use one Redis Stream per priority and a shared Consumer Group. Workers use `XREADGROUP`, acknowledge only after a durable PostgreSQL terminal state, and use conservative `XAUTOCLAIM` recovery for idle pending messages.

## Consequences

Consumer Groups distribute new messages but do not provide exactly-once handler execution. PostgreSQL claims remain necessary. Pending entries offer recovery visibility, but stranded `processing` jobs await lease support in v0.5.

## Alternatives Considered

Redis Lists lack Consumer Group pending-entry tracking and native idle-message recovery. A separate broker adds complexity beyond v0.2.
