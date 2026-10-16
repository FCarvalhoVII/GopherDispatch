# ADR 0003: Job locking strategy

## Context

Multiple goroutines and processes can compete for the same job.

## Decision

Claim and transition a job in one PostgreSQL statement using `FOR UPDATE SKIP LOCKED`. v0.5 will add expiring leases and recovery for dead processes.

## Consequences

Normal claims do not process the same record concurrently and workers do not block one another. A crash after a claim leaves the job in `processing` in v0.1.

## Alternatives considered

A Redis lock would add coordination and divergent state too early. Advisory locks would require holding sessions and still need recovery.
