// Package scheduler selects queue priorities using a deterministic weighted cycle.
package scheduler

import (
	"errors"
	"sync"

	"github.com/FCarvalhoVII/GopherDispatch/internal/job"
)

type Weights struct{ High, Medium, Low int }

func (w Weights) Validate() error {
	if w.High < 1 || w.Medium < 1 || w.Low < 1 {
		return errors.New("priority weights must be positive")
	}
	return nil
}

type Scheduler struct {
	mu    sync.Mutex
	cycle []job.Priority
	index int
}

func New(weights Weights) (*Scheduler, error) {
	if err := weights.Validate(); err != nil {
		return nil, err
	}
	cycle := make([]job.Priority, 0, weights.High+weights.Medium+weights.Low)
	for range weights.High {
		cycle = append(cycle, job.PriorityHigh)
	}
	for range weights.Medium {
		cycle = append(cycle, job.PriorityMedium)
	}
	for range weights.Low {
		cycle = append(cycle, job.PriorityLow)
	}
	return &Scheduler{cycle: cycle}, nil
}

func (s *Scheduler) Next() job.Priority {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.cycle[s.index]
	s.index = (s.index + 1) % len(s.cycle)
	return p
}
