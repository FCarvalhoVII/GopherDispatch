package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/FCarvalhoVII/GopherDispatch/internal/config"
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
	handlers := workerpkg.Registry{
		"send-email":       workerpkg.SimulatedHandler{Delay: 100 * time.Millisecond},
		"generate-report":  workerpkg.SimulatedHandler{Delay: 250 * time.Millisecond},
		"webhook-delivery": workerpkg.SimulatedHandler{Delay: 150 * time.Millisecond},
	}
	pool := workerpkg.NewPool(store, handlers, cfg.WorkerCount, cfg.WorkerPollInterval, cfg.JobTimeout, log)
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
