package anysyncx

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app/ocache"
	"github.com/anyproto/any-sync/commonspace"
	"github.com/stretchr/testify/require"
)

type fakeSpace struct{ id string }

func (f *fakeSpace) Id() string                           { return f.id }
func (f *fakeSpace) Inner() commonspace.Space             { return nil }
func (f *fakeSpace) SyncHeads(context.Context) error      { return nil }
func (f *fakeSpace) Close() error                         { return nil }
func (f *fakeSpace) TryClose(time.Duration) (bool, error) { return true, nil }

// PeekSpace answers at once in every cache state: absent, loading,
// loaded and removed. A load in flight is a miss, not a wait.
func TestPeekSpace_NeverWaits(t *testing.T) {
	ctx := context.Background()
	release := make(chan struct{})
	loading := make(chan struct{})
	var once sync.Once
	a := &App{}
	a.spaceCache = ocache.New(func(ctx context.Context, id string) (ocache.Object, error) {
		once.Do(func() { close(loading) })
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &fakeSpace{id: id}, nil
	}, ocache.WithTTL(spaceLoaderTTL), ocache.WithGCPeriod(spaceLoaderGC)).(spaceCache)
	t.Cleanup(func() { _ = a.spaceCache.Close() })

	_, ok := a.PeekSpace("sp")
	require.False(t, ok, "absent")

	loaded := make(chan error, 1)
	go func() {
		_, err := a.GetSpace(ctx, "sp")
		loaded <- err
	}()
	select {
	case <-loading:
	case <-time.After(5 * time.Second):
		t.Fatal("the load never started")
	}
	peeked := make(chan bool, 1)
	go func() {
		_, ok := a.PeekSpace("sp")
		peeked <- ok
	}()
	select {
	case ok := <-peeked:
		require.False(t, ok, "loading")
	case <-time.After(5 * time.Second):
		t.Fatal("PeekSpace waited on the load in flight")
	}

	close(release)
	require.NoError(t, <-loaded)
	h, ok := a.PeekSpace("sp")
	require.True(t, ok, "loaded")
	require.Equal(t, "sp", h.Id())

	require.NoError(t, a.EvictSpace(ctx, "sp"))
	_, ok = a.PeekSpace("sp")
	require.False(t, ok, "removed")
}
