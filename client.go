package syncsdk

import "context"

// Client provides the top-level API for the any-sync SDK.
type Client interface {
	CreateSpace(ctx context.Context, opts ...SpaceCreateOption) (Space, error)
	DeriveSpace(ctx context.Context, spaceType string) (Space, error)
	OpenSpace(ctx context.Context, spaceID string) (Space, error)
	Subscribe(handler Handler) (unsubscribe func())
	Close(ctx context.Context) error
}

// New is a convenience alias. Due to Go import cycle constraints, the real
// implementation lives in github.com/anyproto/any-sync-sdk/client. Use
// client.New() for the full implementation.
func New(_ context.Context, _ Config) (Client, error) {
	return nil, ErrInvalidConfig
}
