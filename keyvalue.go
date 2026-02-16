package syncsdk

import "context"

type KeyValue interface {
	Set(ctx context.Context, key string, value []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Iterate(ctx context.Context, fn func(key string, value []byte) bool) error
}
