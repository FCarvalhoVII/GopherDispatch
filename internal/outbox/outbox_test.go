package outbox

import (
	"context"
	"errors"
	"github.com/FCarvalhoVII/GopherDispatch/internal/job"
	"io"
	"log/slog"
	"testing"
	"time"
)

type fakeRepo struct {
	events []Event
	marked []string
}

func (r *fakeRepo) ClaimOutbox(context.Context, int, time.Duration) ([]Event, error) {
	return r.events, nil
}
func (r *fakeRepo) MarkOutboxPublished(_ context.Context, id string) error {
	r.marked = append(r.marked, id)
	return nil
}

type fakeQueue struct {
	err       error
	published []job.Job
}

func (q *fakeQueue) Publish(_ context.Context, j job.Job) error {
	if q.err != nil {
		return q.err
	}
	q.published = append(q.published, j)
	return nil
}
func TestPublishOnceMarksDeliveredOnlyAfterPublish(t *testing.T) {
	repo := &fakeRepo{events: []Event{{ID: "e1", JobID: "j1", Priority: job.PriorityHigh}}}
	q := &fakeQueue{}
	NewPublisher(repo, q, time.Second, 10, slog.New(slog.NewTextHandler(io.Discard, nil))).PublishOnce(context.Background())
	if len(repo.marked) != 1 || len(q.published) != 1 {
		t.Fatalf("marked=%d published=%d", len(repo.marked), len(q.published))
	}
}
func TestPublishFailureLeavesEventUnpublished(t *testing.T) {
	repo := &fakeRepo{events: []Event{{ID: "e1", JobID: "j1", Priority: job.PriorityHigh}}}
	q := &fakeQueue{err: errors.New("redis down")}
	NewPublisher(repo, q, time.Second, 10, slog.New(slog.NewTextHandler(io.Discard, nil))).PublishOnce(context.Background())
	if len(repo.marked) != 0 {
		t.Fatal("failed publish was marked")
	}
}
