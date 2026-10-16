package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/felip/gopherqueue/internal/job"
)

type Handler interface {
	Handle(context.Context, job.Job) error
}
type Registry map[string]Handler

type Pool struct {
	repo                     job.Repository
	handlers                 Registry
	count                    int
	pollInterval, jobTimeout time.Duration
	log                      *slog.Logger
}

func NewPool(repo job.Repository, handlers Registry, count int, pollInterval, jobTimeout time.Duration, log *slog.Logger) *Pool {
	return &Pool{repo: repo, handlers: handlers, count: count, pollInterval: pollInterval, jobTimeout: jobTimeout, log: log}
}

func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 1; i <= p.count; i++ {
		wg.Add(1)
		go func(id int) { defer wg.Done(); p.runWorker(ctx, id) }(i)
	}
	wg.Wait()
}

func (p *Pool) runWorker(ctx context.Context, workerID int) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		j, err := p.repo.ClaimNext(ctx)
		if errors.Is(err, job.ErrNotFound) {
			timer.Reset(p.pollInterval)
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.log.Error("claim job", "worker_id", workerID, "error", err)
			timer.Reset(p.pollInterval)
			continue
		}
		p.process(ctx, workerID, j)
		timer.Reset(0)
	}
}

func (p *Pool) process(parent context.Context, workerID int, j job.Job) {
	start := time.Now()
	h, ok := p.handlers[j.Type]
	attrs := []any{"job_id", j.ID, "job_type", j.Type, "priority", j.Priority, "attempt", j.Attempts, "worker_id", workerID}
	if !ok {
		finishCtx, finishCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer finishCancel()
		_ = p.repo.Fail(finishCtx, j.ID)
		p.log.Error("unknown job type", attrs...)
		return
	}
	// A shutdown stops new claims but lets work already claimed drain until its own timeout.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), p.jobTimeout)
	defer cancel()
	err := h.Handle(ctx, j)
	attrs = append(attrs, "duration", time.Since(start))
	finishCtx, finishCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer finishCancel()
	if err != nil {
		if markErr := p.repo.Fail(finishCtx, j.ID); markErr != nil {
			p.log.Error("mark job failed", "job_id", j.ID, "error", markErr)
		}
		p.log.Error("job failed", append(attrs, "error", err)...)
		return
	}
	if err := p.repo.Complete(finishCtx, j.ID); err != nil {
		p.log.Error("mark job complete", "job_id", j.ID, "error", err)
		return
	}
	p.log.Info("job completed", attrs...)
}

type SimulatedHandler struct{ Delay time.Duration }

func (h SimulatedHandler) Handle(ctx context.Context, _ job.Job) error {
	t := time.NewTimer(h.Delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("handler canceled: %w", ctx.Err())
	case <-t.C:
		return nil
	}
}
