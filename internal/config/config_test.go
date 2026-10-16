package config

import "testing"

func TestLoadRequiresDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	if _, err := Load(); err == nil {
		t.Fatal("Load() succeeded without DATABASE_URL")
	}
}

func TestLoadRejectsInvalidWorkerCount(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("WORKER_COUNT", "zero")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted invalid WORKER_COUNT")
	}
}
