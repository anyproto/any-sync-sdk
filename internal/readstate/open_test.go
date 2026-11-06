package readstate

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/require"
)

// An opened engine does no DDL: a read from outside a tx completes while
// another goroutine holds the engine's write tx, and the in-tx call that
// follows finds the collections ready. With a lazy first-call open, the
// out-of-tx caller created the collections and waited on the write lock
// the in-tx caller held, while the in-tx caller waited on the open.
func TestOpen_ReadOutsideHeldWriteTx(t *testing.T) {
	f := newFixture(t)
	seeded := make(chan error, 1)
	err := f.WriteTx(ctx, func(txCtx context.Context) error {
		go func() {
			_, err := f.Seeded(ctx, "obj")
			seeded <- err
		}()
		select {
		case err := <-seeded:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Seeded outside the tx waited on the held write tx")
		}
		_, err := f.MergeHeads(txCtx, "obj", []string{"c1"})
		return err
	})
	require.NoError(t, err)
}

func TestOpen_NotOpenGate(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "readstate.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	e := New(db, "space1", nil, nil, nil)

	calls := map[string]func() error{
		"Seeded":        func() error { _, err := e.Seeded(ctx, "obj"); return err },
		"TrackChange":   func() error { return e.TrackChange(ctx, mkTrack("obj", "c1", "v01", nil)) },
		"MarkRead":      func() error { _, err := e.MarkRead(ctx, "obj", []string{"c1"}); return err },
		"MarkReadUpTo":  func() error { _, err := e.MarkReadUpTo(ctx, "obj", ""); return err },
		"MergeHeads":    func() error { _, err := e.MergeHeads(ctx, "obj", []string{"c1"}); return err },
		"ClearRecords":  func() error { return e.ClearRecords(ctx, "obj", []string{"r1"}, 1) },
		"UnreadEntries": func() error { _, _, err := e.UnreadEntries(ctx, "obj"); return err },
		"Counts":        func() error { _, err := e.Counts(ctx, "obj"); return err },
		"Frontier":      func() error { _, _, err := e.Frontier(ctx, "obj"); return err },
		"ChangedSince":  func() error { _, err := e.ChangedSince(ctx, 0, 0); return err },
		"MarkSeeded":    func() error { return e.MarkSeeded(ctx, "obj") },
		"SeedFrontier":  func() error { _, err := e.SeedFrontier(ctx, "obj", []string{"c1"}); return err },
	}
	for name, call := range calls {
		require.ErrorIs(t, call(), ErrNotOpen, name)
	}

	require.NoError(t, e.Open(ctx))
	require.NoError(t, e.Open(ctx), "a second Open is a no-op")
	seeded, err := e.Seeded(ctx, "obj")
	require.NoError(t, err)
	require.False(t, seeded)
}
