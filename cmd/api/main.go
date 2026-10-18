package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/FCarvalhoVII/GopherDispatch/internal/config"
	"github.com/FCarvalhoVII/GopherDispatch/internal/httpapi"
	"github.com/FCarvalhoVII/GopherDispatch/internal/job"
	"github.com/FCarvalhoVII/GopherDispatch/internal/outbox"
	"github.com/FCarvalhoVII/GopherDispatch/internal/queue"
	"github.com/FCarvalhoVII/GopherDispatch/internal/storage/postgres"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		log.Error("load config", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	store, err := postgres.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("open database", "error", err)
		os.Exit(1)
	}
	defer store.Close()
	redisQueue, err := queue.Open(ctx, cfg.RedisURL, cfg.RedisConsumerGroup)
	if err != nil {
		log.Error("open Redis", "error", err)
		os.Exit(1)
	}
	defer redisQueue.Close()
	go outbox.NewPublisher(store, redisQueue, cfg.OutboxPollInterval, cfg.OutboxBatchSize, log).Run(ctx)
	service := job.NewService(store, cfg.MaxJobAttempts)
	h := httpapi.NewHandler(service, func(r *http.Request) error {
		c, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		return store.Ready(c)
	}, log, cfg.RequestBodyLimit)
	srv := &http.Server{Addr: ":" + cfg.HTTPPort, Handler: h.Routes(cfg.RateLimitPerMinute), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		log.Info("api listening", "address", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("serve", "error", err)
			stop()
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "error", err)
	}
}
