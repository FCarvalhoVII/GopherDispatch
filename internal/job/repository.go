package job

import "context"

type Repository interface {
	Create(context.Context, CreateInput) (Job, error)
	Get(context.Context, string) (Job, error)
	ClaimNext(context.Context) (Job, error)
	Complete(context.Context, string) error
	Fail(context.Context, string) error
}
