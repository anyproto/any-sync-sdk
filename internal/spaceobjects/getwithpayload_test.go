package spaceobjects

import (
	"context"
	"errors"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree/updatelistener"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/stretchr/testify/require"
)

// payloadStore is a Store whose cache runs load for every object load
// and hands back one prebuilt object on success.
func payloadStore(t *testing.T, load func(ctx context.Context) error) (*Store, *object.Object) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, t.TempDir()+"/test.db", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	store := NewStore(nil, db, nil, "spaceA", nil, nil, nil, nil)
	ctrl, err := crdt.NewController(ctx, "obj", db, crdt.HandlerReg{Name: "blocks", Handler: &crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}})
	require.NoError(t, err)
	obj, err := object.New(object.Config{SpaceId: "spaceA", Controller: ctrl},
		func(updatelistener.UpdateListener) (objecttree.ObjectTree, error) {
			return &closableStubTree{stubTree{id: "obj"}}, nil
		})
	require.NoError(t, err)
	_ = store.cache.Close()
	store.cache = ocache.New(func(ctx context.Context, _ string) (ocache.Object, error) {
		if err := load(ctx); err != nil {
			return nil, err
		}
		return obj, nil
	})
	return store, obj
}

// scriptedLoad is one object load: foreign leaves the caller's payload
// unread, as a load another caller ran does; err is its outcome.
type scriptedLoad struct {
	foreign bool
	err     error
}

var (
	errForeign = errors.New("foreign load failed")
	errOwn     = errors.New("own load failed")
)

func TestGetWithPayload(t *testing.T) {
	ctx := context.Background()
	foreignFail := scriptedLoad{foreign: true, err: errForeign}
	repeat := func(l scriptedLoad, n int) []scriptedLoad {
		out := make([]scriptedLoad, n)
		for i := range out {
			out[i] = l
		}
		return out
	}
	cases := []struct {
		name      string
		loads     []scriptedLoad // in order; a load past the end is the caller's own and succeeds
		wantLoads int
		wantErr   error
	}{
		{name: "the caller's own load", wantLoads: 1},
		{name: "a foreign load's success is the caller's result", loads: []scriptedLoad{{foreign: true}}, wantLoads: 1},
		{name: "a foreign load's failure is not the caller's, so Get runs again", loads: []scriptedLoad{foreignFail}, wantLoads: 2},
		{name: "the caller's own failure is returned", loads: []scriptedLoad{{err: errOwn}}, wantLoads: 1, wantErr: errOwn},
		{name: "the own failure after a foreign one is the one returned", loads: []scriptedLoad{foreignFail, {err: errOwn}}, wantLoads: 2, wantErr: errOwn},
		{name: "Get runs again a bounded number of times", loads: repeat(foreignFail, maxForeignRetries+2), wantLoads: 1 + maxForeignRetries, wantErr: errForeign},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			loads := 0
			store, want := payloadStore(t, func(ctx context.Context) error {
				loads++
				var l scriptedLoad
				if loads <= len(tc.loads) {
					l = tc.loads[loads-1]
				}
				if !l.foreign {
					require.NotNil(t, loadPayloadFromCtx(ctx))
				}
				return l.err
			})
			got, err := store.getWithPayload(ctx, "obj", &treestorage.TreeStorageCreatePayload{})
			require.Equal(t, tc.wantLoads, loads)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Same(t, want, got)
		})
	}
}

// A ctx cancelled while a foreign load fails ends the loop.
func TestGetWithPayload_CancelledCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loads := 0
	store, _ := payloadStore(t, func(context.Context) error {
		loads++
		cancel()
		return errForeign
	})
	_, err := store.getWithPayload(ctx, "obj", &treestorage.TreeStorageCreatePayload{})
	require.ErrorIs(t, err, errForeign)
	require.Equal(t, 1, loads)
}

// The cache's LoadFunc marks the payload as run before its first step
// can fail: an own load that fails at GetSpace is not taken for another
// caller's.
func TestLoad_MarksPayloadFirst(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, t.TempDir()+"/test.db", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	store := NewStore(&anysyncx.App{}, db, nil, "spaceA", nil, nil, nil, nil)
	lp := &loadPayload{payload: &treestorage.TreeStorageCreatePayload{}}
	_, err = store.load(context.WithValue(ctx, loadPayloadKey{}, lp), "obj")
	require.ErrorContains(t, err, "get space")
	require.True(t, lp.ran.Load())
}

// countingCache counts the Gets a cache answers.
type countingCache struct {
	ocache.OCache
	gets int
}

func (c *countingCache) Get(ctx context.Context, id string) (ocache.Object, error) {
	c.gets++
	return c.OCache.Get(ctx, id)
}

// A closed cache answers without a load; the caller does not ask again.
func TestGetWithPayload_ClosedCache(t *testing.T) {
	store, _ := payloadStore(t, func(context.Context) error { return nil })
	require.NoError(t, store.cache.Close())
	cache := &countingCache{OCache: store.cache}
	store.cache = cache
	_, err := store.getWithPayload(context.Background(), "obj", &treestorage.TreeStorageCreatePayload{})
	require.ErrorIs(t, err, ocache.ErrClosed)
	require.Equal(t, 1, cache.gets)
}
