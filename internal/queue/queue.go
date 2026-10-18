package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/FCarvalhoVII/GopherDispatch/internal/job"
	"github.com/redis/go-redis/v9"
)

const (
	StreamHigh   = "gopherqueue:jobs:high"
	StreamMedium = "gopherqueue:jobs:medium"
	StreamLow    = "gopherqueue:jobs:low"
)

var ErrNoMessage = errors.New("no queue message available")

func StreamFor(priority job.Priority) (string, error) {
	switch priority {
	case job.PriorityHigh:
		return StreamHigh, nil
	case job.PriorityMedium:
		return StreamMedium, nil
	case job.PriorityLow:
		return StreamLow, nil
	default:
		return "", fmt.Errorf("invalid priority %q", priority)
	}
}

type Message struct {
	ID, Stream, JobID string
	Priority          job.Priority
}

type Publisher interface {
	Publish(context.Context, job.Job) error
}
type Consumer interface {
	EnsureGroups(context.Context) error
	Read(context.Context, job.Priority, string) (Message, error)
	Ack(context.Context, Message) error
	Recover(context.Context, job.Priority, string, time.Duration) ([]Message, error)
}

type Redis struct {
	client *redis.Client
	group  string
}

func Open(ctx context.Context, url, group string) (*Redis, error) {
	options, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis URL: %w", err)
	}
	client := redis.NewClient(options)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return &Redis{client: client, group: group}, nil
}

func (r *Redis) Close() error                   { return r.client.Close() }
func (r *Redis) Ping(ctx context.Context) error { return r.client.Ping(ctx).Err() }

func (r *Redis) Publish(ctx context.Context, j job.Job) error {
	stream, err := StreamFor(j.Priority)
	if err != nil {
		return err
	}
	return r.client.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{"job_id": j.ID, "priority": string(j.Priority)}}).Err()
}

func (r *Redis) EnsureGroups(ctx context.Context) error {
	for _, stream := range []string{StreamHigh, StreamMedium, StreamLow} {
		err := r.client.XGroupCreateMkStream(ctx, stream, r.group, "0").Err()
		if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
			return fmt.Errorf("create group for %s: %w", stream, err)
		}
	}
	return nil
}

func (r *Redis) Read(ctx context.Context, priority job.Priority, consumer string) (Message, error) {
	stream, err := StreamFor(priority)
	if err != nil {
		return Message{}, err
	}
	entries, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: r.group, Consumer: consumer, Streams: []string{stream, ">"}, Count: 1, NoAck: false}).Result()
	if errors.Is(err, redis.Nil) || len(entries) == 0 || len(entries[0].Messages) == 0 {
		return Message{}, ErrNoMessage
	}
	if err != nil {
		return Message{}, fmt.Errorf("read %s: %w", stream, err)
	}
	return decode(stream, priority, entries[0].Messages[0]), nil
}

func (r *Redis) Ack(ctx context.Context, m Message) error {
	return r.client.XAck(ctx, m.Stream, r.group, m.ID).Err()
}

func (r *Redis) Recover(ctx context.Context, priority job.Priority, consumer string, minIdle time.Duration) ([]Message, error) {
	stream, err := StreamFor(priority)
	if err != nil {
		return nil, err
	}
	msgs, _, err := r.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: stream, Group: r.group, Consumer: consumer, MinIdle: minIdle, Start: "0-0", Count: 10}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("recover %s: %w", stream, err)
	}
	result := make([]Message, 0, len(msgs))
	for _, m := range msgs {
		result = append(result, decode(stream, priority, m))
	}
	return result, nil
}

func decode(stream string, priority job.Priority, raw redis.XMessage) Message {
	m := Message{ID: raw.ID, Stream: stream, Priority: priority}
	if value, ok := raw.Values["job_id"].(string); ok {
		m.JobID = value
	}
	return m
}
