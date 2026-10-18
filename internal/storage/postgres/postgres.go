package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/FCarvalhoVII/GopherDispatch/internal/job"
	"github.com/FCarvalhoVII/GopherDispatch/internal/outbox"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct{ pool *pgxpool.Pool }

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close()                         { s.pool.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) Ready(ctx context.Context) error {
	var one int
	return s.pool.QueryRow(ctx, `SELECT 1 WHERE to_regclass('public.jobs') IS NOT NULL`).Scan(&one)
}

const columns = `id::text, type, payload, priority, status, attempts, max_attempts, created_at, updated_at, started_at, completed_at`

func scanJob(row pgx.Row) (job.Job, error) {
	var j job.Job
	err := row.Scan(&j.ID, &j.Type, &j.Payload, &j.Priority, &j.Status, &j.Attempts, &j.MaxAttempts, &j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, job.ErrNotFound
	}
	return j, err
}

func (s *Store) Create(ctx context.Context, in job.CreateInput) (job.Job, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return job.Job{}, fmt.Errorf("begin create transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	q := `INSERT INTO jobs (type, payload, priority, max_attempts) VALUES ($1,$2,$3,$4) RETURNING ` + columns
	j, err := scanJob(tx.QueryRow(ctx, q, in.Type, in.Payload, in.Priority, in.MaxAttempts))
	if err != nil {
		return job.Job{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO job_outbox (job_id, priority) VALUES ($1, $2)`, j.ID, j.Priority); err != nil {
		return job.Job{}, fmt.Errorf("insert outbox event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return job.Job{}, fmt.Errorf("commit create transaction: %w", err)
	}
	return j, nil
}

func (s *Store) Get(ctx context.Context, id string) (job.Job, error) {
	return scanJob(s.pool.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE id::text=$1`, id))
}

func (s *Store) Claim(ctx context.Context, id, owner string) (job.Job, error) {
	q := `UPDATE jobs SET status='processing', attempts=attempts+1, started_at=now(), claimed_at=now(), processing_owner=$2, updated_at=now()
		WHERE id::text=$1 AND status='pending' RETURNING ` + columns
	j, err := scanJob(s.pool.QueryRow(ctx, q, id, owner))
	if !errors.Is(err, job.ErrNotFound) {
		return j, err
	}
	current, getErr := s.Get(ctx, id)
	if getErr != nil {
		return job.Job{}, getErr
	}
	if current.Status == job.StatusProcessing {
		return current, job.ErrAlreadyProcessing
	}
	return current, job.ErrTerminal
}

func (s *Store) Complete(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE jobs SET status='completed', completed_at=now(), updated_at=now() WHERE id::text=$1 AND status='processing'`, id)
	return err
}

func (s *Store) Fail(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE jobs SET status='failed', updated_at=now() WHERE id::text=$1 AND status='processing'`, id)
	return err
}

func (s *Store) ClaimOutbox(ctx context.Context, limit int, staleAfter time.Duration) ([]outbox.Event, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin outbox claim: %w", err)
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `WITH candidates AS (
		SELECT id FROM job_outbox WHERE published_at IS NULL AND (locked_at IS NULL OR locked_at < now() - $2::interval)
		ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT $1
	) UPDATE job_outbox SET locked_at=now() WHERE id IN (SELECT id FROM candidates)
	RETURNING id::text, job_id::text, priority`, limit, staleAfter.String())
	if err != nil {
		return nil, fmt.Errorf("claim outbox events: %w", err)
	}
	defer rows.Close()
	events := make([]outbox.Event, 0, limit)
	for rows.Next() {
		var event outbox.Event
		if err := rows.Scan(&event.ID, &event.JobID, &event.Priority); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit outbox claim: %w", err)
	}
	return events, nil
}

func (s *Store) MarkOutboxPublished(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE job_outbox SET published_at=now(), locked_at=NULL WHERE id::text=$1 AND published_at IS NULL`, id)
	return err
}
