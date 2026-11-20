package readsync

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonspace/object/keyvalue/keyvaluestorage/innerstorage"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/readstate"
)

// coldSpace stands in for a space that is not loaded: the resolver
// refuses until the loader ran, and the loader waits on release.
type coldSpace struct {
	resident atomic.Bool
	once     sync.Once
	loading  chan struct{}
	release  chan struct{}
}

func newColdSpace() *coldSpace {
	return &coldSpace{loading: make(chan struct{}), release: make(chan struct{})}
}

func (c *coldSpace) resolve(_ context.Context, _, changeId string) ([]string, string, bool, error) {
	if !c.resident.Load() {
		return nil, "", false, readstate.ErrSpaceNotResident
	}
	return nil, "v" + changeId, true, nil
}

func (c *coldSpace) load(ctx context.Context) error {
	c.once.Do(func() { close(c.loading) })
	select {
	case <-c.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	c.resident.Store(true)
	return nil
}

func (c *coldSpace) awaitLoading(t *testing.T) {
	t.Helper()
	select {
	case <-c.loading:
	case <-time.After(5 * time.Second):
		t.Fatal("the merge never loaded the space")
	}
}

// A merge whose gap walk finds the space cold loads it with the write
// lock released: a write on the account DB completes while the load
// waits on the (unreachable) sync node, and the merge lands once the
// space is resident.
func TestMerge_ColdSpaceLoadsOutsideWriteTx(t *testing.T) {
	space := newColdSpace()
	f := newFixtureWith(t, space.resolve, space.load)
	f.OnKeyValues(plainDecryptor, []innerstorage.KeyValue{
		frontierKV(kvKey(testSpace, testObj), "peer-other", []string{"c1"}),
	})
	space.awaitLoading(t)

	write := make(chan error, 1)
	go func() {
		tx, err := f.db.WriteTx(ctx)
		if err == nil {
			err = tx.Commit()
		}
		write <- err
	}()
	select {
	case err := <-write:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("a write waited on the merge's space load")
	}

	close(space.release)
	require.Eventually(t, func() bool {
		heads, _, err := f.eng.Frontier(ctx, testObj)
		return err == nil && len(heads) == 1 && heads[0] == "c1"
	}, 5*time.Second, 10*time.Millisecond)
}

// Close returns while a merge waits on a space load: the worker's ctx
// is cancelled and the load gives up.
func TestClose_CancelsMergeSpaceLoad(t *testing.T) {
	space := newColdSpace()
	f := newFixtureWith(t, space.resolve, space.load)
	f.OnKeyValues(plainDecryptor, []innerstorage.KeyValue{
		frontierKV(kvKey(testSpace, testObj), "peer-other", []string{"c1"}),
	})
	space.awaitLoading(t)

	closed := make(chan struct{})
	go func() {
		f.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close waited on the merge's space load")
	}
}
