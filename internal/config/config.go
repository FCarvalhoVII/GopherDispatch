package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPPort           string
	DatabaseURL        string
	WorkerCount        int
	WorkerPollInterval time.Duration
	MaxJobAttempts     int
	JobTimeout         time.Duration
	ShutdownTimeout    time.Duration
	RequestBodyLimit   int64
	RateLimitPerMinute int
}

func Load() (Config, error) {
	c := Config{
		HTTPPort: "8080", WorkerCount: 4, WorkerPollInterval: 500 * time.Millisecond,
		MaxJobAttempts: 3, JobTimeout: 30 * time.Second, ShutdownTimeout: 15 * time.Second,
		RequestBodyLimit: 1 << 20, RateLimitPerMinute: 120,
	}
	c.DatabaseURL = os.Getenv("DATABASE_URL")
	if c.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is required")
	}
	c.HTTPPort = envString("HTTP_PORT", c.HTTPPort)
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
	if c.RequestBodyLimit, err = envInt64("REQUEST_BODY_LIMIT", c.RequestBodyLimit); err != nil {
		return Config{}, err
	}
	if c.WorkerPollInterval, err = envDuration("WORKER_POLL_INTERVAL", c.WorkerPollInterval); err != nil {
		return Config{}, err
	}
	if c.JobTimeout, err = envDuration("JOB_TIMEOUT", c.JobTimeout); err != nil {
		return Config{}, err
	}
	if c.ShutdownTimeout, err = envDuration("SHUTDOWN_TIMEOUT", c.ShutdownTimeout); err != nil {
		return Config{}, err
	}
	if c.WorkerCount < 1 || c.MaxJobAttempts < 1 || c.RequestBodyLimit < 1 || c.RateLimitPerMinute < 1 {
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
