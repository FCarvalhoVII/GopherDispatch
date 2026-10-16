CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TYPE job_priority AS ENUM ('high', 'medium', 'low');
CREATE TYPE job_status AS ENUM ('pending', 'processing', 'completed', 'failed', 'dead_letter');

CREATE TABLE jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    priority job_priority NOT NULL,
    status job_status NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    idempotency_key VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE INDEX jobs_pending_order_idx ON jobs (priority, created_at) WHERE status = 'pending';

