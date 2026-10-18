package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/FCarvalhoVII/GopherDispatch/internal/config"
	"github.com/FCarvalhoVII/GopherDispatch/internal/queue"
	"github.com/FCarvalhoVII/GopherDispatch/internal/scheduler"
	"github.com/FCarvalhoVII/GopherDispatch/internal/storage/postgres"
	workerpkg "github.com/FCarvalhoVII/GopherDispatch/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		log.Error("load config", "error", err)
		os.Exit(1)
	}
	root, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	store, err := postgres.Open(root, cfg.DatabaseURL)
	if err != nil {
		log.Error("open database", "error", err)
		os.Exit(1)
	}
	defer store.Close()
	redisQueue, err := queue.Open(root, cfg.RedisURL, cfg.RedisConsumerGroup)
	if err != nil {
		log.Error("open Redis", "error", err)
		os.Exit(1)
	}
	defer redisQueue.Close()
	priorityScheduler, err := scheduler.New(scheduler.Weights{High: cfg.PriorityHighWeight, Medium: cfg.PriorityMediumWeight, Low: cfg.PriorityLowWeight})
	if err != nil {
		log.Error("create scheduler", "error", err)
		os.Exit(1)
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "worker"
	}
	handlers := workerpkg.Registry{
		"send-email":       workerpkg.SimulatedHandler{Delay: 100 * time.Millisecond},
		"generate-report":  workerpkg.SimulatedHandler{Delay: 250 * time.Millisecond},
		"webhook-delivery": workerpkg.SimulatedHandler{Delay: 150 * time.Millisecond},
	}
	consumerBase := fmt.Sprintf("%s-%d", hostname, os.Getpid())
	pool := workerpkg.NewPool(store, redisQueue, priorityScheduler, handlers, cfg.WorkerCount, cfg.WorkerPollInterval, cfg.JobTimeout, cfg.PendingIdleTimeout, consumerBase, log)
	log.Info("worker pool started", "workers", cfg.WorkerCount)
	done := make(chan struct{})
	go func() { pool.Run(root); close(done) }()
	<-root.Done()
	select {
	case <-done:
		log.Info("worker pool stopped")
	case <-time.After(cfg.ShutdownTimeout):
		log.Warn("worker shutdown deadline exceeded")
	}
}
