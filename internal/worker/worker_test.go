package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/FCarvalhoVII/GopherDispatch/internal/job"
	"github.com/FCarvalhoVII/GopherDispatch/internal/queue"
	"github.com/FCarvalhoVII/GopherDispatch/internal/scheduler"
)

type recordingRepo struct {
	mu                        sync.Mutex
	completed, failed, claims int
	claimErr                  error
	claimed                   job.Job
}

func (r *recordingRepo) Claim(_ context.Context, _ string, _ string) (job.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.claims++
	if r.claimErr != nil {
		return job.Job{}, r.claimErr
	}
	return r.claimed, nil
}
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

type fakeQueue struct{ acked int }

func (*fakeQueue) EnsureGroups(context.Context) error { return nil }
func (*fakeQueue) Read(context.Context, job.Priority, string) (queue.Message, error) {
	return queue.Message{}, queue.ErrNoMessage
}
func (q *fakeQueue) Ack(context.Context, queue.Message) error { q.acked++; return nil }
func (*fakeQueue) Recover(context.Context, job.Priority, string, time.Duration) ([]queue.Message, error) {
	return nil, nil
}

type handlerFunc func(context.Context, job.Job) error

func (f handlerFunc) Handle(ctx context.Context, j job.Job) error { return f(ctx, j) }

func newTestPool(t *testing.T, repo *recordingRepo, q *fakeQueue, handlers Registry) *Pool {
	t.Helper()
	schedule, err := scheduler.New(scheduler.Weights{High: 1, Medium: 1, Low: 1})
	if err != nil {
		t.Fatal(err)
	}
	return NewPool(repo, q, schedule, handlers, 1, time.Millisecond, 20*time.Millisecond, time.Second, "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
}
func TestProcessCompletesAndAcknowledgesAfterPersistence(t *testing.T) {
	repo := &recordingRepo{claimed: job.Job{ID: "1", Type: "ok", Priority: job.PriorityHigh}}
	q := &fakeQueue{}
	newTestPool(t, repo, q, Registry{"ok": handlerFunc(func(context.Context, job.Job) error { return nil })}).process(context.Background(), 1, "test-1", queue.Message{ID: "m1", JobID: "1", Priority: job.PriorityHigh, Stream: queue.StreamHigh})
	if repo.completed != 1 || repo.failed != 0 || q.acked != 1 {
		t.Fatalf("completed=%d failed=%d acked=%d", repo.completed, repo.failed, q.acked)
	}
}
func TestDuplicateActiveDeliveryIsNotAcknowledged(t *testing.T) {
	repo := &recordingRepo{claimErr: job.ErrAlreadyProcessing}
	q := &fakeQueue{}
	newTestPool(t, repo, q, nil).process(context.Background(), 1, "test-1", queue.Message{ID: "m1", JobID: "1", Priority: job.PriorityHigh, Stream: queue.StreamHigh})
	if q.acked != 0 {
		t.Fatal("active duplicate was acknowledged")
	}
}
func TestTerminalDuplicateIsAcknowledged(t *testing.T) {
	repo := &recordingRepo{claimErr: job.ErrTerminal}
	q := &fakeQueue{}
	newTestPool(t, repo, q, nil).process(context.Background(), 1, "test-1", queue.Message{ID: "m1", JobID: "1", Priority: job.PriorityHigh, Stream: queue.StreamHigh})
	if q.acked != 1 {
		t.Fatal("terminal duplicate was not acknowledged")
	}
}
func TestSimulatedHandlerHonorsCancellation(t *testing.T) {
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
