package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/FCarvalhoVII/GopherDispatch/internal/job"
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
	q := `INSERT INTO jobs (type, payload, priority, max_attempts) VALUES ($1,$2,$3,$4) RETURNING ` + columns
	return scanJob(s.pool.QueryRow(ctx, q, in.Type, in.Payload, in.Priority, in.MaxAttempts))
}

func (s *Store) Get(ctx context.Context, id string) (job.Job, error) {
	return scanJob(s.pool.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE id::text=$1`, id))
}

func (s *Store) ClaimNext(ctx context.Context) (job.Job, error) {
	q := `WITH candidate AS (
		SELECT id FROM jobs WHERE status='pending'
		ORDER BY CASE priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END, created_at
		FOR UPDATE SKIP LOCKED LIMIT 1
	) UPDATE jobs SET status='processing', attempts=attempts+1, started_at=now(), updated_at=now()
	WHERE id=(SELECT id FROM candidate) RETURNING ` + columns
	return scanJob(s.pool.QueryRow(ctx, q))
}

func (s *Store) Complete(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE jobs SET status='completed', completed_at=now(), updated_at=now() WHERE id::text=$1 AND status='processing'`, id)
	return err
}

func (s *Store) Fail(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE jobs SET status='failed', updated_at=now() WHERE id::text=$1 AND status='processing'`, id)
	return err
}
