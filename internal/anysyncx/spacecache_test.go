package anysyncx

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app/ocache"
	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
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

// scriptedLoad is one answer of scriptedCache: the error, and whether
// the load ran on the caller's ctx or was another caller's, shared.
type scriptedLoad struct {
	err error
	own bool
}

// scriptedCache answers each Get with the next scripted load, then a
// space from a load of the caller's own.
type scriptedCache struct {
	spaceCache
	loads []scriptedLoad
	calls int
}

func (c *scriptedCache) Get(ctx context.Context, id string) (ocache.Object, error) {
	c.calls++
	own, err := true, error(nil)
	if c.calls <= len(c.loads) {
		own, err = c.loads[c.calls-1].own, c.loads[c.calls-1].err
	}
	if tok, ok := ctx.Value(loadTokenKey{}).(*loadToken); ok && own {
		tok.ran.Store(true)
	}
	if err != nil {
		return nil, err
	}
	return &fakeSpace{id: id}, nil
}

// GetSpaceCreating loads again after a failed load it shared with
// another caller, up to createLoadAttempts loads; a failure of a load it
// ran itself is final.
func TestGetSpaceCreating_ReloadsAfterAForeignFailure(t *testing.T) {
	ctx := context.Background()
	miss := fmt.Errorf("anysyncx: NewSpace sp: %w", spacesyncproto.ErrSpaceMissing)
	boom := errors.New("boom")
	foreign := func(err error) scriptedLoad { return scriptedLoad{err: err} }
	cases := []struct {
		name      string
		loads     []scriptedLoad
		wantErr   error
		wantCalls int
	}{
		{"foreign miss", []scriptedLoad{foreign(miss)}, nil, 2},
		{"foreign network error", []scriptedLoad{foreign(boom)}, nil, 2},
		{"bounded", []scriptedLoad{foreign(miss), foreign(miss), foreign(miss), foreign(miss)}, spacesyncproto.ErrSpaceMissing, createLoadAttempts},
		{"own failure", []scriptedLoad{{err: boom, own: true}}, boom, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cache := &scriptedCache{loads: tc.loads}
			a := &App{spaceCache: cache}
			h, err := a.GetSpaceCreating(ctx, "sp")
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, "sp", h.Id())
			}
			require.Equal(t, tc.wantCalls, cache.calls)
		})
	}

	// GetSpace itself never reloads.
	cache := &scriptedCache{loads: []scriptedLoad{foreign(miss)}}
	_, err := (&App{spaceCache: cache}).GetSpace(ctx, "sp")
	require.ErrorIs(t, err, spacesyncproto.ErrSpaceMissing)
	require.Equal(t, 1, cache.calls)
}
