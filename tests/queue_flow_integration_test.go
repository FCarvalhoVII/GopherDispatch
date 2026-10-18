//go:build integration

package tests

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/FCarvalhoVII/GopherDispatch/internal/httpapi"
	"github.com/FCarvalhoVII/GopherDispatch/internal/job"
	"github.com/FCarvalhoVII/GopherDispatch/internal/outbox"
	"github.com/FCarvalhoVII/GopherDispatch/internal/queue"
	"github.com/FCarvalhoVII/GopherDispatch/internal/scheduler"
	"github.com/FCarvalhoVII/GopherDispatch/internal/storage/postgres"
	"github.com/FCarvalhoVII/GopherDispatch/internal/worker"
)

func TestHTTPOutboxRedisWorkerFlow(t *testing.T) {
	databaseURL, redisURL := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_REDIS_URL")
	if databaseURL == "" || redisURL == "" {
		t.Skip("TEST_DATABASE_URL and TEST_REDIS_URL are required")
	}
	ctx := context.Background()
	store, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	group := "gopherqueue-integration-" + time.Now().UTC().Format("20060102150405.000000000")
	redisQueue, err := queue.Open(ctx, redisURL, group)
	if err != nil {
		t.Fatal(err)
	}
	defer redisQueue.Close()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := job.NewService(store, 3)
	api := httpapi.NewHandler(service, func(*http.Request) error { return nil }, log, 1024).Routes(100)
	request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"type":"integration-handler","priority":"high","payload":{"test":true}}`))
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("create job returned %d: %s", response.Code, response.Body.String())
	}
	jobID := strings.Split(strings.Split(response.Body.String(), `"id":"`)[1], `"`)[0]

	outbox.NewPublisher(store, redisQueue, time.Second, 50, log).PublishOnce(ctx)
	schedule, err := scheduler.New(scheduler.Weights{High: 5, Medium: 3, Low: 1})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{}, 1)
	handlers := worker.Registry{"integration-handler": handlerFunc(func(context.Context, job.Job) error { done <- struct{}{}; return nil })}
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := worker.NewPool(store, redisQueue, schedule, handlers, 1, 10*time.Millisecond, time.Second, time.Hour, "integration", log)
	go pool.Run(runCtx)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not invoked")
	}
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		j, err := store.Get(ctx, jobID)
		if err == nil && j.Status == job.StatusCompleted {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("job was not persisted as completed")
}

type handlerFunc func(context.Context, job.Job) error

func (f handlerFunc) Handle(ctx context.Context, j job.Job) error { return f(ctx, j) }
