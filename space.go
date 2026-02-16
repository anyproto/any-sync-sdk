package syncsdk

import "context"

type SpaceCreateOption func(*spaceCreateOptions)
type ObjectCreateOption func(*objectCreateOptions)
type ObjectDeriveOption func(*objectDeriveOptions)

type spaceCreateOptions struct{}
type objectCreateOptions struct {
	changeType string
}
type objectDeriveOptions struct{}

func WithChangeType(t string) ObjectCreateOption {
	return func(o *objectCreateOptions) {
		o.changeType = t
	}
}

// ResolvedObjectCreateOptions holds the resolved values of ObjectCreateOption functions.
type ResolvedObjectCreateOptions struct {
	ChangeType string
}

// ResolveObjectCreateOptions applies the given ObjectCreateOption functions and returns the resolved values.
func ResolveObjectCreateOptions(opts []ObjectCreateOption) ResolvedObjectCreateOptions {
	var o objectCreateOptions
	for _, opt := range opts {
		opt(&o)
	}
	return ResolvedObjectCreateOptions{
		ChangeType: o.changeType,
	}
}

type Space interface {
	ID() string
	GetObject(ctx context.Context, objectID string) (Object, error)
	CreateObject(ctx context.Context, opts ...ObjectCreateOption) (Object, error)
	DeriveObject(ctx context.Context, opts ...ObjectDeriveOption) (Object, error)
	DeleteObject(ctx context.Context, objectID string) error
	ListObjectIDs(ctx context.Context) ([]string, error)
	KeyValue() KeyValue
	Subscribe(handler Handler) (unsubscribe func())
	Close(ctx context.Context) error
}
