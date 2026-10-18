package job

import "context"

type Repository interface {
	Create(context.Context, CreateInput) (Job, error)
	Get(context.Context, string) (Job, error)
}

type ExecutionRepository interface {
	Claim(context.Context, string, string) (Job, error)
	Complete(context.Context, string) error
	Fail(context.Context, string) error
}
