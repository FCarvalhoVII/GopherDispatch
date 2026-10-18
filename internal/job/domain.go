package job

import (
	"encoding/json"
	"errors"
	"time"
)

type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityMedium Priority = "medium"
	PriorityLow    Priority = "low"
)

func (p Priority) Valid() bool { return p == PriorityHigh || p == PriorityMedium || p == PriorityLow }

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
	StatusDeadLetter Status = "dead_letter"
)

type Job struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	Priority    Priority        `json:"priority"`
	Status      Status          `json:"status"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"maxAttempts"`
	CreatedAt   time.Time       `json:"createdAt"`
	UpdatedAt   time.Time       `json:"updatedAt"`
	StartedAt   *time.Time      `json:"startedAt,omitempty"`
	CompletedAt *time.Time      `json:"completedAt,omitempty"`
}

var (
	ErrNotFound          = errors.New("job not found")
	ErrAlreadyProcessing = errors.New("job is already processing")
	ErrTerminal          = errors.New("job is already in a terminal state")
)

type CreateInput struct {
	Type        string
	Payload     json.RawMessage
	Priority    Priority
	MaxAttempts int
}

func (in CreateInput) Validate() error {
	if in.Type == "" || len(in.Type) > 100 {
		return errors.New("type is required and must be at most 100 characters")
	}
	if !in.Priority.Valid() {
		return errors.New("priority must be high, medium, or low")
	}
	if len(in.Payload) == 0 || !json.Valid(in.Payload) {
		return errors.New("payload must be valid JSON")
	}
	if in.MaxAttempts < 1 {
		return errors.New("maxAttempts must be positive")
	}
	return nil
}
