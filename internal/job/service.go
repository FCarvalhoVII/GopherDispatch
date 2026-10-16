package job

import (
	"context"
	"fmt"
)

type Service struct {
	repo               Repository
	defaultMaxAttempts int
}

func NewService(repo Repository, defaultMaxAttempts int) *Service {
	return &Service{repo: repo, defaultMaxAttempts: defaultMaxAttempts}
}

func (s *Service) Create(ctx context.Context, in CreateInput) (Job, error) {
	if in.MaxAttempts == 0 {
		in.MaxAttempts = s.defaultMaxAttempts
	}
	if err := in.Validate(); err != nil {
		return Job{}, err
	}
	j, err := s.repo.Create(ctx, in)
	if err != nil {
		return Job{}, fmt.Errorf("create job: %w", err)
	}
	return j, nil
}

func (s *Service) Get(ctx context.Context, id string) (Job, error) {
	j, err := s.repo.Get(ctx, id)
	if err != nil {
		return Job{}, fmt.Errorf("get job: %w", err)
	}
	return j, nil
}
