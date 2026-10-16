package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/felip/gopherqueue/internal/job"
)

type fakeRepo struct{ created job.Job }

func (f *fakeRepo) Create(_ context.Context, in job.CreateInput) (job.Job, error) {
	f.created = job.Job{ID: "job-1", Type: in.Type, Priority: in.Priority, Status: job.StatusPending, MaxAttempts: in.MaxAttempts}
	return f.created, nil
}
func (f *fakeRepo) Get(_ context.Context, id string) (job.Job, error) {
	if id != f.created.ID {
		return job.Job{}, job.ErrNotFound
	}
	return f.created, nil
}
func (*fakeRepo) ClaimNext(context.Context) (job.Job, error) { return job.Job{}, job.ErrNotFound }
func (*fakeRepo) Complete(context.Context, string) error     { return nil }
func (*fakeRepo) Fail(context.Context, string) error         { return nil }

func TestCreateAndGetJob(t *testing.T) {
	t.Parallel()
	repo := &fakeRepo{}
	h := NewHandler(job.NewService(repo, 3), func(*http.Request) error { return nil }, slog.New(slog.NewTextHandler(io.Discard, nil)), 1024).Routes(100)
	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"type":"send-email","priority":"high","payload":{"to":"a@example.com"}}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("POST status = %d, body=%s", w.Code, w.Body.String())
	}
	var created map[string]any
	if err := json.NewDecoder(w.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	if created["id"] != "job-1" {
		t.Fatalf("id = %v", created["id"])
	}

	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/jobs/job-1", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("GET status = %d", w.Code)
	}
}

func TestCreateRejectsOversizedBody(t *testing.T) {
	t.Parallel()
	h := NewHandler(job.NewService(&fakeRepo{}, 3), func(*http.Request) error { return nil }, slog.New(slog.NewTextHandler(io.Discard, nil)), 8).Routes(100)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"type":"too large"}`)))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", w.Code)
	}
}
