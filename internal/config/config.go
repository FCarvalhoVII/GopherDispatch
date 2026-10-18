package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPPort             string
	DatabaseURL          string
	RedisURL             string
	RedisConsumerGroup   string
	WorkerCount          int
	WorkerPollInterval   time.Duration
	OutboxPollInterval   time.Duration
	OutboxBatchSize      int
	PendingIdleTimeout   time.Duration
	PriorityHighWeight   int
	PriorityMediumWeight int
	PriorityLowWeight    int
	MaxJobAttempts       int
	JobTimeout           time.Duration
	ShutdownTimeout      time.Duration
	RequestBodyLimit     int64
	RateLimitPerMinute   int
}

func Load() (Config, error) {
	c := Config{
		HTTPPort: "8080", RedisURL: "redis://localhost:6379/0", RedisConsumerGroup: "gopherqueue-workers",
		WorkerCount: 4, WorkerPollInterval: 100 * time.Millisecond, OutboxPollInterval: time.Second, OutboxBatchSize: 50, PendingIdleTimeout: 30 * time.Second,
		PriorityHighWeight: 5, PriorityMediumWeight: 3, PriorityLowWeight: 1,
		MaxJobAttempts: 3, JobTimeout: 30 * time.Second, ShutdownTimeout: 15 * time.Second,
		RequestBodyLimit: 1 << 20, RateLimitPerMinute: 120,
	}
	c.DatabaseURL = os.Getenv("DATABASE_URL")
	if c.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is required")
	}
	c.HTTPPort = envString("HTTP_PORT", c.HTTPPort)
	c.RedisURL = envString("REDIS_URL", c.RedisURL)
	c.RedisConsumerGroup = envString("REDIS_CONSUMER_GROUP", c.RedisConsumerGroup)
	var err error
	if c.WorkerCount, err = envInt("WORKER_COUNT", c.WorkerCount); err != nil {
		return Config{}, err
	}
	if c.MaxJobAttempts, err = envInt("MAX_JOB_ATTEMPTS", c.MaxJobAttempts); err != nil {
		return Config{}, err
	}
	if c.RateLimitPerMinute, err = envInt("RATE_LIMIT_PER_MINUTE", c.RateLimitPerMinute); err != nil {
		return Config{}, err
	}
	if c.OutboxBatchSize, err = envInt("OUTBOX_BATCH_SIZE", c.OutboxBatchSize); err != nil {
		return Config{}, err
	}
	if c.PriorityHighWeight, err = envInt("PRIORITY_HIGH_WEIGHT", c.PriorityHighWeight); err != nil {
		return Config{}, err
	}
	if c.PriorityMediumWeight, err = envInt("PRIORITY_MEDIUM_WEIGHT", c.PriorityMediumWeight); err != nil {
		return Config{}, err
	}
	if c.PriorityLowWeight, err = envInt("PRIORITY_LOW_WEIGHT", c.PriorityLowWeight); err != nil {
		return Config{}, err
	}
	if c.RequestBodyLimit, err = envInt64("REQUEST_BODY_LIMIT", c.RequestBodyLimit); err != nil {
		return Config{}, err
	}
	if c.WorkerPollInterval, err = envDuration("WORKER_POLL_INTERVAL", c.WorkerPollInterval); err != nil {
		return Config{}, err
	}
	if c.OutboxPollInterval, err = envDuration("OUTBOX_POLL_INTERVAL", c.OutboxPollInterval); err != nil {
		return Config{}, err
	}
	if c.PendingIdleTimeout, err = envDuration("PENDING_IDLE_TIMEOUT", c.PendingIdleTimeout); err != nil {
		return Config{}, err
	}
	if c.JobTimeout, err = envDuration("JOB_TIMEOUT", c.JobTimeout); err != nil {
		return Config{}, err
	}
	if c.ShutdownTimeout, err = envDuration("SHUTDOWN_TIMEOUT", c.ShutdownTimeout); err != nil {
		return Config{}, err
	}
	if c.RedisURL == "" || c.RedisConsumerGroup == "" {
		return Config{}, errors.New("REDIS_URL and REDIS_CONSUMER_GROUP must not be empty")
	}
	if c.WorkerCount < 1 || c.MaxJobAttempts < 1 || c.RequestBodyLimit < 1 || c.RateLimitPerMinute < 1 || c.OutboxBatchSize < 1 || c.PriorityHighWeight < 1 || c.PriorityMediumWeight < 1 || c.PriorityLowWeight < 1 {
		return Config{}, errors.New("numeric configuration values must be positive")
	}
	return c, nil
}

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func envInt(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return n, nil
}
func envInt64(key string, fallback int64) (int64, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return n, nil
}
func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration: %w", key, err)
	}
	return d, nil
}
