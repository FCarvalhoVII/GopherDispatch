package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/felip/gopherqueue/internal/job"
)

type Handler struct {
	service   *job.Service
	ready     func(*http.Request) error
	log       *slog.Logger
	bodyLimit int64
}

func NewHandler(service *job.Service, ready func(*http.Request) error, log *slog.Logger, bodyLimit int64) *Handler {
	return &Handler{service: service, ready: ready, log: log, bodyLimit: bodyLimit}
}

func (h *Handler) Routes(rateLimit int) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", h.createJob)
	mux.HandleFunc("GET /jobs/{id}", h.getJob)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /ready", h.readiness)
	return recoverMiddleware(h.log, newRateLimiter(rateLimit, time.Minute).wrap(mux))
}

type createRequest struct {
	Type        string          `json:"type"`
	Priority    job.Priority    `json:"priority"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts int             `json:"maxAttempts,omitempty"`
}

func (h *Handler) createJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, h.bodyLimit)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req createRequest
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "body must contain one JSON object")
		return
	}
	in := job.CreateInput{Type: strings.TrimSpace(req.Type), Priority: req.Priority, Payload: req.Payload, MaxAttempts: req.MaxAttempts}
	if in.MaxAttempts == 0 {
		// The service applies the configured default. Use a valid sentinel for
		// request validation without duplicating that configuration here.
		in.MaxAttempts = 1
	}
	if err := in.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	in.MaxAttempts = req.MaxAttempts
	j, err := h.service.Create(r.Context(), in)
	if err != nil {
		h.log.Error("create job", "error", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"id": j.ID, "status": j.Status})
}

func (h *Handler) getJob(w http.ResponseWriter, r *http.Request) {
	j, err := h.service.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, job.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		h.log.Error("get job", "error", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (h *Handler) readiness(w http.ResponseWriter, r *http.Request) {
	if err := h.ready(r); err != nil {
		writeError(w, http.StatusServiceUnavailable, "not ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
