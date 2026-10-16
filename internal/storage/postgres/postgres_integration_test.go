//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/felip/gopherqueue/internal/job"
)

func TestCreateClaimComplete(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	store, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	j, err := store.Create(ctx, job.CreateInput{Type: "send-email", Payload: json.RawMessage(`{}`), Priority: job.PriorityHigh, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimNext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if claimed.ID != j.ID || claimed.Status != job.StatusProcessing {
		t.Fatalf("unexpected claimed job: %+v", claimed)
	}
	if err := store.Complete(ctx, j.ID); err != nil {
		t.Fatal(err)
	}
}
