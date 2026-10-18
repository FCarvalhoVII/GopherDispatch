package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/FCarvalhoVII/GopherDispatch/internal/job"
	"github.com/FCarvalhoVII/GopherDispatch/internal/queue"
)

type Event struct {
	ID, JobID string
	Priority  job.Priority
}
type Repository interface {
	ClaimOutbox(context.Context, int, time.Duration) ([]Event, error)
	MarkOutboxPublished(context.Context, string) error
}

type Publisher struct {
	repo     Repository
	queue    queue.Publisher
	interval time.Duration
	batch    int
	log      *slog.Logger
}

func NewPublisher(repo Repository, queue queue.Publisher, interval time.Duration, batch int, log *slog.Logger) *Publisher {
	return &Publisher{repo: repo, queue: queue, interval: interval, batch: batch, log: log}
}

func (p *Publisher) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		p.PublishOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (p *Publisher) PublishOnce(ctx context.Context) {
	events, err := p.repo.ClaimOutbox(ctx, p.batch, 2*p.interval)
	if err != nil {
		if ctx.Err() == nil {
			p.log.Error("claim outbox events", "error", err)
		}
		return
	}
	for _, event := range events {
		if err := p.queue.Publish(ctx, job.Job{ID: event.JobID, Priority: event.Priority}); err != nil {
			p.log.Error("publish outbox event", "event_id", event.ID, "job_id", event.JobID, "error", err)
			continue
		}
		if err := p.repo.MarkOutboxPublished(ctx, event.ID); err != nil {
			p.log.Error("mark outbox event published", "event_id", event.ID, "job_id", event.JobID, "error", err)
			continue
		}
		p.log.Info("job published", "job_id", event.JobID, "priority", event.Priority)
	}
}

func (e Event) String() string { return fmt.Sprintf("outbox event %s for job %s", e.ID, e.JobID) }
