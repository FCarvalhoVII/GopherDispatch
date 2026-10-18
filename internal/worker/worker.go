package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/FCarvalhoVII/GopherDispatch/internal/job"
	"github.com/FCarvalhoVII/GopherDispatch/internal/queue"
	"github.com/FCarvalhoVII/GopherDispatch/internal/scheduler"
)

type Handler interface {
	Handle(context.Context, job.Job) error
}
type Registry map[string]Handler

type Pool struct {
	repo                                  job.ExecutionRepository
	queue                                 queue.Consumer
	scheduler                             *scheduler.Scheduler
	handlers                              Registry
	count                                 int
	pollInterval, jobTimeout, pendingIdle time.Duration
	consumerBase                          string
	log                                   *slog.Logger
}

func NewPool(repo job.ExecutionRepository, consumer queue.Consumer, scheduler *scheduler.Scheduler, handlers Registry, count int, pollInterval, jobTimeout, pendingIdle time.Duration, consumerBase string, log *slog.Logger) *Pool {
	return &Pool{repo: repo, queue: consumer, scheduler: scheduler, handlers: handlers, count: count, pollInterval: pollInterval, jobTimeout: jobTimeout, pendingIdle: pendingIdle, consumerBase: consumerBase, log: log}
}

func (p *Pool) Run(ctx context.Context) {
	if err := p.queue.EnsureGroups(ctx); err != nil {
		p.log.Error("ensure Redis consumer groups", "error", err)
		return
	}
	var wg sync.WaitGroup
	for i := 1; i <= p.count; i++ {
		wg.Add(1)
		go func(id int) { defer wg.Done(); p.runWorker(ctx, id) }(i)
	}
	wg.Wait()
}

func (p *Pool) runWorker(ctx context.Context, workerID int) {
	consumer := fmt.Sprintf("%s-%d", p.consumerBase, workerID)
	p.recoverPending(ctx, workerID, consumer)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		priority := p.scheduler.Next()
		message, err := p.queue.Read(ctx, priority, consumer)
		if errors.Is(err, queue.ErrNoMessage) {
			if !wait(ctx, p.pollInterval) {
				return
			}
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.log.Error("consume queue message", "worker_id", workerID, "consumer_id", consumer, "priority", priority, "error", err)
			if !wait(ctx, p.pollInterval) {
				return
			}
			continue
		}
		p.process(ctx, workerID, consumer, message)
	}
}

func (p *Pool) recoverPending(ctx context.Context, workerID int, consumer string) {
	for _, priority := range []job.Priority{job.PriorityHigh, job.PriorityMedium, job.PriorityLow} {
		messages, err := p.queue.Recover(ctx, priority, consumer, p.pendingIdle)
		if err != nil {
			p.log.Error("recover pending messages", "priority", priority, "consumer_id", consumer, "error", err)
			continue
		}
		for _, message := range messages {
			p.process(ctx, workerID, consumer, message)
		}
	}
}

func (p *Pool) process(parent context.Context, workerID int, consumer string, message queue.Message) {
	attrs := []any{"job_id", message.JobID, "priority", message.Priority, "worker_id", workerID, "consumer_id", consumer, "stream", message.Stream}
	if message.JobID == "" {
		p.log.Error("malformed queue message", append(attrs, "message_id", message.ID)...)
		p.ack(parent, message, attrs)
		return
	}
	j, err := p.repo.Claim(parent, message.JobID, consumer)
	if errors.Is(err, job.ErrTerminal) {
		p.log.Info("duplicate terminal delivery", attrs...)
		p.ack(parent, message, attrs)
		return
	}
	if errors.Is(err, job.ErrAlreadyProcessing) {
		p.log.Warn("job already processing; leaving message pending", attrs...)
		return
	}
	if errors.Is(err, job.ErrNotFound) {
		p.log.Error("queue message references missing job", attrs...)
		p.ack(parent, message, attrs)
		return
	}
	if err != nil {
		p.log.Error("claim job", append(attrs, "error", err)...)
		return
	}
	start := time.Now()
	attrs = append(attrs, "job_type", j.Type, "attempt", j.Attempts)
	h, ok := p.handlers[j.Type]
	if !ok {
		p.finish(message, j, fmt.Errorf("unknown job type %q", j.Type), attrs)
		return
	}
	handlerCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), p.jobTimeout)
	defer cancel()
	p.finish(message, j, h.Handle(handlerCtx, j), append(attrs, "duration", time.Since(start)))
}

func (p *Pool) finish(message queue.Message, j job.Job, handlerErr error, attrs []any) {
	finishCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if handlerErr != nil {
		if err := p.repo.Fail(finishCtx, j.ID); err != nil {
			p.log.Error("mark job failed", append(attrs, "error", err)...)
			return
		}
		p.log.Error("job failed", append(attrs, "error", handlerErr)...)
	} else {
		if err := p.repo.Complete(finishCtx, j.ID); err != nil {
			p.log.Error("mark job completed", append(attrs, "error", err)...)
			return
		}
		p.log.Info("job completed", attrs...)
	}
	p.ack(finishCtx, message, attrs)
}

func (p *Pool) ack(ctx context.Context, message queue.Message, attrs []any) {
	if err := p.queue.Ack(ctx, message); err != nil {
		p.log.Error("ack queue message", append(attrs, "message_id", message.ID, "error", err)...)
		return
	}
	p.log.Info("message acknowledged", append(attrs, "message_id", message.ID)...)
}

func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

type SimulatedHandler struct{ Delay time.Duration }

func (h SimulatedHandler) Handle(ctx context.Context, _ job.Job) error {
	timer := time.NewTimer(h.Delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("handler canceled: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}
