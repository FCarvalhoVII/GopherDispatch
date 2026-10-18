ALTER TABLE jobs ADD COLUMN IF NOT EXISTS processing_owner VARCHAR(255);
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS job_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    priority job_priority NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    locked_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS job_outbox_unpublished_idx ON job_outbox (created_at) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS jobs_processing_claim_idx ON jobs (claimed_at) WHERE status = 'processing';
