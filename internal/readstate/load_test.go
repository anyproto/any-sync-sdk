package readstate

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/require"
)

// coldFixture is an engine over a space that starts cold: the resolver
// refuses until the loader ran, and the loader waits on release.
type coldFixture struct {
	*Engine
	db       anystore.DB
	resident atomic.Bool
	loads    atomic.Int32
	loading  chan struct{} // closed when the loader first runs
	release  chan struct{} // the loader returns once closed
	loadErr  error         // returned by the loader instead of making the space resident
	stayCold bool          // the loader returns without making the space resident
}

func newColdFixture(t *testing.T) *coldFixture {
	t.Helper()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "readstate.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	f := &coldFixture{db: db, loading: make(chan struct{}), release: make(chan struct{})}
	seq := uint64(1000)
	f.Engine = New(db, "space1", func(context.Context) (uint64, error) {
		seq++
		return seq, nil
	}, f.resolve, f.load)
	require.NoError(t, f.Open(ctx))
	return f
}

func (f *coldFixture) resolve(_ context.Context, _, changeId string) ([]string, string, bool, error) {
	if !f.resident.Load() {
		return nil, "", false, ErrSpaceNotResident
	}
	return nil, "v" + changeId, true, nil
}

func (f *coldFixture) load(context.Context) error {
	if f.loads.Add(1) == 1 {
		close(f.loading)
	}
	<-f.release
	if f.loadErr != nil {
		return f.loadErr
	}
	if !f.stayCold {
		f.resident.Store(true)
	}
	return nil
}

func (f *coldFixture) mergeAsync(heads ...string) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- f.WriteTx(ctx, func(txCtx context.Context) error {
			_, err := f.MergeHeads(txCtx, "obj", heads)
			return err
		})
	}()
	return done
}

func (f *coldFixture) awaitLoading(t *testing.T, merged <-chan error) {
	t.Helper()
	select {
	case <-f.loading:
	case err := <-merged:
		t.Fatalf("merge returned before the loader ran: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the loader never ran")
	}
}

func awaitErr(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not finish", what)
		return nil
	}
}

func (f *coldFixture) frontier(t *testing.T) []string {
	t.Helper()
	heads, _, err := f.Frontier(ctx, "obj")
	require.NoError(t, err)
	return heads
}

// A merge whose gap walk finds the space cold loads it with the write
// lock released: a concurrent write on the DB completes while the load
// waits, and the merge reruns once the space is resident.
func TestWriteTx_ColdSpaceLoadsOutsideTx(t *testing.T) {
	f := newColdFixture(t)
	merged := f.mergeAsync("c1")
	f.awaitLoading(t, merged)

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
		t.Fatal("a concurrent write waited on the merge's space load")
	}

	close(f.release)
	require.NoError(t, awaitErr(t, merged, "merge"))
	require.Equal(t, []string{"c1"}, f.frontier(t))
	require.EqualValues(t, 1, f.loads.Load())
}

// The loader's error is the merge's error; nothing was committed.
func TestWriteTx_LoaderErrorReturned(t *testing.T) {
	f := newColdFixture(t)
	f.loadErr = errors.New("sync node unreachable")
	close(f.release)
	require.ErrorIs(t, awaitErr(t, f.mergeAsync("c1"), "merge"), f.loadErr)
	require.Empty(t, f.frontier(t))
}

// A space still cold after the load is not loaded again: the refusal
// reaches the caller after one attempt.
func TestWriteTx_StillColdAfterLoad(t *testing.T) {
	f := newColdFixture(t)
	f.stayCold = true
	close(f.release)
	require.ErrorIs(t, awaitErr(t, f.mergeAsync("c1"), "merge"), ErrSpaceNotResident)
	require.EqualValues(t, 1, f.loads.Load())
	require.Empty(t, f.frontier(t))
}

// Without a loader the refusal reaches the caller.
func TestWriteTx_NoLoader(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "readstate.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	e := New(db, "space1", func(context.Context) (uint64, error) { return 1, nil },
		func(context.Context, string, string) ([]string, string, bool, error) {
			return nil, "", false, ErrSpaceNotResident
		}, nil)
	require.NoError(t, e.Open(ctx))
	err = e.WriteTx(ctx, func(txCtx context.Context) error {
		_, err := e.MergeHeads(txCtx, "obj", []string{"c1"})
		return err
	})
	require.ErrorIs(t, err, ErrSpaceNotResident)
}

// Frontier compaction resolves members too. Over the cap with the space
// cold, the merge loads the space and ranks members by version instead
// of dropping them in arbitrary order.
func TestWriteTx_CompactionLoadsColdSpace(t *testing.T) {
	f := newColdFixture(t)
	n := maxFrontierSize + 1
	heads := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("c%03d", i)
		heads = append(heads, id)
		require.NoError(t, f.WriteTx(ctx, func(txCtx context.Context) error {
			return f.TrackChange(txCtx, Track{
				ObjectId: "obj", ChangeId: id, VersionId: "v" + id, ApplySeq: uint64(i + 1),
				RecordIds: []string{"r-" + id}, Tags: []string{"message"}, Tracked: true,
			})
		}))
	}
	// Every head has an unread row, so the walk resolves nothing; only
	// the compaction reaches the resolver.
	merged := f.mergeAsync(heads...)
	f.awaitLoading(t, merged)
	close(f.release)
	require.NoError(t, awaitErr(t, merged, "merge"))
	require.EqualValues(t, 1, f.loads.Load())
	got := f.frontier(t)
	require.Len(t, got, maxFrontierSize)
	require.NotContains(t, got, "c000", "the oldest member is the one dropped")
}
