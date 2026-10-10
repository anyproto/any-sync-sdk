package spaceobjects

import (
	"context"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app/ocache"
	"github.com/stretchr/testify/require"
)

// releaseObject declines TryClose while busy, the way an Object does
// while a handler holds its tree locked.
type releaseObject struct {
	busy   bool
	closed int
}

func (o *releaseObject) Close() error {
	o.closed++
	return nil
}

func (o *releaseObject) TryClose(time.Duration) (bool, error) {
	if o.busy {
		return false, nil
	}
	o.closed++
	return true, nil
}

// Release closes a resident object at once instead of at the cache's
// TTL; one that declines stays resident, and an id that is not
// resident is a no-op.
func TestStoreRelease(t *testing.T) {
	ctx := context.Background()
	objs := map[string]*releaseObject{"idle": {}, "busy": {busy: true}}
	s := &Store{cache: ocache.New(
		func(_ context.Context, id string) (ocache.Object, error) { return objs[id], nil },
		ocache.WithTTL(objectCacheTTL),
		ocache.WithGCPeriod(objectCacheGC),
	)}
	t.Cleanup(func() { _ = s.cache.Close() })
	for id := range objs {
		_, err := s.cache.Get(ctx, id)
		require.NoError(t, err)
	}

	s.Release("idle")
	s.Release("busy")
	s.Release("absent")

	require.Equal(t, 1, objs["idle"].closed)
	_, err := s.cache.Pick(ctx, "idle")
	require.ErrorIs(t, err, ocache.ErrNotExists)
	require.Zero(t, objs["busy"].closed)
	_, err = s.cache.Pick(ctx, "busy")
	require.NoError(t, err)
}
