package readstate

import (
	"context"
	"errors"
	"github.com/anyproto/any-sync-sdk/space"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// twoSpaces opens one DB with an engine per space, like the shared SDK
// DB, so a purge of one space can be checked against the other.
func twoSpaces(t *testing.T) (anystore.DB, *Engine, *Engine) {
	t.Helper()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "readstate.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	seq := uint64(1000)
	next := func(context.Context) (uint64, error) { seq++; return seq, nil }
	resolve := func(_ context.Context, _, _ string) ([]string, string, bool, error) { return nil, "", false, nil }
	return db, New(db, "space1", next, resolve), New(db, "space2", next, resolve)
}

func unreadCount(t *testing.T, e *Engine, objectId string) int {
	t.Helper()
	entries, _, err := e.UnreadEntries(ctx, objectId)
	require.NoError(t, err)
	return len(entries)
}

func stateRows(t *testing.T, db anystore.DB) int {
	t.Helper()
	coll, err := db.OpenCollection(ctx, StateCollectionName)
	require.NoError(t, err)
	n, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	return n
}

func TestPurgeObject_DropsUnreadAndState(t *testing.T) {
	db, e1, e2 := twoSpaces(t)
	require.NoError(t, e1.TrackChange(ctx, mkTrack("objA", "a1", "v01", nil, "msg")))
	require.NoError(t, e1.TrackChange(ctx, mkTrack("objA", "a2", "v02", []string{"a1"}, "msg")))
	require.NoError(t, e1.TrackChange(ctx, mkTrack("objB", "b1", "v01", nil, "msg")))
	require.NoError(t, e2.TrackChange(ctx, mkTrack("objC", "c1", "v01", nil, "msg")))
	_, err := e1.SeedFrontier(ctx, "objA", []string{"a2"})
	require.NoError(t, err)
	seeded, err := e1.Seeded(ctx, "objA")
	require.NoError(t, err)
	require.True(t, seeded)

	require.NoError(t, PurgeObject(ctx, db, "objA"))

	seeded, err = e1.Seeded(ctx, "objA")
	require.NoError(t, err)
	assert.False(t, seeded, "a purged object seeds again on its next first sight")
	counts, err := e1.Counts(ctx, "objA")
	require.NoError(t, err)
	assert.Empty(t, counts)
	assert.Zero(t, unreadCount(t, e1, "objA"))
	assert.Equal(t, 1, unreadCount(t, e1, "objB"), "other objects keep their rows")
	assert.Equal(t, 1, unreadCount(t, e2, "objC"))
	assert.Equal(t, 2, stateRows(t, db))

	// Idempotent, and a never-tracked id is not an error.
	require.NoError(t, PurgeObject(ctx, db, "objA"))
	require.NoError(t, PurgeObject(ctx, db, "never"))
}

func TestPurgeObject_NoCollectionsYet(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "fresh.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, PurgeObject(ctx, db, "objA"))
	n, err := PurgeSpace(ctx, db, "space1")
	require.NoError(t, err)
	assert.Zero(t, n)
}

func TestPurgeSpace_DropsSpaceRowsInChunks(t *testing.T) {
	db, e1, e2 := twoSpaces(t)
	prev := purgeSpaceChunk
	purgeSpaceChunk = 2
	t.Cleanup(func() { purgeSpaceChunk = prev })

	for _, obj := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, e1.TrackChange(ctx, mkTrack("obj"+obj, obj+"1", "v01", nil, "msg")))
		require.NoError(t, e1.TrackChange(ctx, mkTrack("obj"+obj, obj+"2", "v02", []string{obj + "1"}, "msg")))
	}
	// An object with a state row but no unread rows (all self-authored).
	self := mkTrack("objSelf", "s1", "v01", nil)
	self.SelfAuthored = true
	require.NoError(t, e1.TrackChange(ctx, self))
	require.NoError(t, e2.TrackChange(ctx, mkTrack("objX", "x1", "v01", nil, "msg")))
	require.Equal(t, 7, stateRows(t, db))

	n, err := PurgeSpace(ctx, db, "space1")
	require.NoError(t, err)
	assert.Equal(t, 6, n)
	for _, obj := range []string{"a", "b", "c", "d", "e"} {
		assert.Zero(t, unreadCount(t, e1, "obj"+obj))
	}
	assert.Equal(t, 1, stateRows(t, db), "only the other space's row remains")
	assert.Equal(t, 1, unreadCount(t, e2, "objX"))
	unread, err := db.OpenCollection(ctx, UnreadCollectionName)
	require.NoError(t, err)
	left, err := unread.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, left)

	n, err = PurgeSpace(ctx, db, "space1")
	require.NoError(t, err)
	assert.Zero(t, n, "second pass finds nothing")
}

// A mark or merge on a deleted object stops at the resolver's answer
// and persists nothing: the published frontiers of a purged object
// keep arriving, and parking their heads would re-create the row the
// purge removed.
func TestMergeHeads_DeletedObjectPersistsNothing(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "readstate.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	seq := uint64(1000)
	e := New(db, "space1", func(context.Context) (uint64, error) { seq++; return seq, nil },
		func(_ context.Context, objectId, _ string) ([]string, string, bool, error) {
			if objectId == "gone" {
				return nil, "", false, ErrObjectDeleted
			}
			return nil, "", false, nil
		})

	_, err = e.MergeHeads(ctx, "gone", []string{"h1"})
	assert.True(t, errors.Is(err, ErrObjectDeleted))
	assert.True(t, errors.Is(err, space.ErrObjectDeleted))
	_, err = e.MarkRead(ctx, "gone", []string{"h1"})
	assert.True(t, errors.Is(err, ErrObjectDeleted))
	assert.Zero(t, stateRows(t, db), "no state row for a deleted object")

	// A not-yet-arrived head on a live object still parks as pending.
	res, err := e.MergeHeads(ctx, "live", []string{"h1"})
	require.NoError(t, err)
	assert.Equal(t, []string{"h1"}, res.Pending)
	assert.Equal(t, 1, stateRows(t, db))
}
