package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/felip/gopherqueue/internal/job"
)

type recordingRepo struct {
	mu                sync.Mutex
	completed, failed int
}

func (*recordingRepo) Create(context.Context, job.CreateInput) (job.Job, error) {
	return job.Job{}, nil
}
func (*recordingRepo) Get(context.Context, string) (job.Job, error) {
	return job.Job{}, job.ErrNotFound
}
func (*recordingRepo) ClaimNext(context.Context) (job.Job, error) { return job.Job{}, job.ErrNotFound }
func (r *recordingRepo) Complete(context.Context, string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completed++
	return nil
}
func (r *recordingRepo) Fail(context.Context, string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failed++
	return nil
}

type handlerFunc func(context.Context, job.Job) error

func (f handlerFunc) Handle(ctx context.Context, j job.Job) error { return f(ctx, j) }

func TestProcessCompletes(t *testing.T) {
	t.Parallel()
	repo := &recordingRepo{}
	p := NewPool(repo, Registry{"ok": handlerFunc(func(context.Context, job.Job) error { return nil })}, 1, time.Millisecond, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.process(context.Background(), 1, job.Job{ID: "1", Type: "ok"})
	if repo.completed != 1 || repo.failed != 0 {
		t.Fatalf("completed=%d failed=%d", repo.completed, repo.failed)
	}
}

func TestProcessTimeout(t *testing.T) {
	t.Parallel()
	repo := &recordingRepo{}
	p := NewPool(repo, Registry{"slow": handlerFunc(func(ctx context.Context, _ job.Job) error { <-ctx.Done(); return ctx.Err() })}, 1, time.Millisecond, 5*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.process(context.Background(), 1, job.Job{ID: "1", Type: "slow"})
	if repo.failed != 1 {
		t.Fatalf("failed=%d", repo.failed)
	}
}

func TestSimulatedHandlerHonorsCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := (SimulatedHandler{Delay: time.Second}).Handle(ctx, job.Job{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}

func BenchmarkSimulatedHandler(b *testing.B) {
	h := SimulatedHandler{}
	for i := 0; i < b.N; i++ {
		if err := h.Handle(context.Background(), job.Job{}); err != nil {
			b.Fatal(err)
		}
	}
}
