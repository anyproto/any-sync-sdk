package crdt

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryChangedObjects_FiltersBySpaceAndSince(t *testing.T) {
	_, coll := openMetaColl(t)
	require.NoError(t, ensureMetaIndexes(ctx, coll))

	// Space A objects at various AddSeqs.
	require.NoError(t, PersistMeta(ctx, coll, "a1", 5, 5, nil, "spaceA"))
	require.NoError(t, PersistMeta(ctx, coll, "a2", 12, 12, nil, "spaceA"))
	require.NoError(t, PersistMeta(ctx, coll, "a3", 20, 20, nil, "spaceA"))
	// A different space — must never leak into spaceA results.
	require.NoError(t, PersistMeta(ctx, coll, "b1", 99, 99, nil, "spaceB"))
	// The per-space watermark row (keyed space:<id>, no `sp`) — excluded.
	require.NoError(t, PersistSpaceMaxAddSeq(ctx, coll, "spaceA", 1000))

	got, err := QueryChangedObjects(ctx, coll, "spaceA", 10, 0)
	require.NoError(t, err)
	require.Len(t, got, 2)
	// Ordered ascending by ApplySeq.
	assert.Equal(t, ObjectSeq{ObjectId: "a2", ApplySeq: 12}, got[0])
	assert.Equal(t, ObjectSeq{ObjectId: "a3", ApplySeq: 20}, got[1])
}

func TestQueryChangedObjects_PaginatesByAddSeq(t *testing.T) {
	_, coll := openMetaColl(t)
	require.NoError(t, ensureMetaIndexes(ctx, coll))

	for i, id := range []string{"o1", "o2", "o3", "o4"} {
		require.NoError(t, PersistMeta(ctx, coll, id, uint64((i+1)*10), uint64((i+1)*10), nil, "s"))
	}

	page1, err := QueryChangedObjects(ctx, coll, "s", 0, 2)
	require.NoError(t, err)
	require.Len(t, page1, 2)
	assert.Equal(t, "o1", page1[0].ObjectId)
	assert.Equal(t, "o2", page1[1].ObjectId)

	// Resume from the last ApplySeq of page1.
	page2, err := QueryChangedObjects(ctx, coll, "s", page1[1].ApplySeq, 2)
	require.NoError(t, err)
	require.Len(t, page2, 2)
	assert.Equal(t, "o3", page2[0].ObjectId)
	assert.Equal(t, "o4", page2[1].ObjectId)
}

func TestQueryChangedObjects_SkipsUnscopedRows(t *testing.T) {
	_, coll := openMetaColl(t)
	require.NoError(t, ensureMetaIndexes(ctx, coll))

	// A row written before space-scoping (no `sp`) — invisible until its
	// next change backfills the field (lazy "index from now on").
	require.NoError(t, PersistMeta(ctx, coll, "legacy", 50, 50, nil, ""))
	require.NoError(t, PersistMeta(ctx, coll, "scoped", 60, 60, nil, "s"))

	got, err := QueryChangedObjects(ctx, coll, "s", 0, 0)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "scoped", got[0].ObjectId)
}

func TestMaxObjectApplySeq(t *testing.T) {
	_, coll := openMetaColl(t)
	require.NoError(t, ensureMetaIndexes(ctx, coll))

	got, err := MaxObjectApplySeq(ctx, coll, "s")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), got, "empty space")

	require.NoError(t, PersistMeta(ctx, coll, "o1", 10, 10, nil, "s"))
	require.NoError(t, PersistMeta(ctx, coll, "o2", 35, 35, nil, "s"))
	require.NoError(t, PersistMeta(ctx, coll, "other", 99, 99, nil, "otherSpace"))

	got, err = MaxObjectApplySeq(ctx, coll, "s")
	require.NoError(t, err)
	assert.Equal(t, uint64(35), got, "max within space, ignores other spaces")
}

// TestBackfillApplySeq seeds applySeq := addSeq on legacy rows exactly
// once, leaving rows that already carry an applySeq untouched.
func TestBackfillApplySeq(t *testing.T) {
	_, coll := openMetaColl(t)
	require.NoError(t, ensureMetaIndexes(ctx, coll))

	// Legacy rows: addSeq only (pre-applySeq schema).
	require.NoError(t, PersistMeta(ctx, coll, "old1", 5, 0, nil, "s"))
	require.NoError(t, PersistMeta(ctx, coll, "old2", 9, 0, nil, "s"))
	// A post-applySeq row — backfill must not touch it.
	require.NoError(t, PersistMeta(ctx, coll, "new1", 3, 12, nil, "s"))
	// Another space's legacy row — out of scope.
	require.NoError(t, PersistMeta(ctx, coll, "other", 50, 0, nil, "other"))

	require.NoError(t, BackfillApplySeq(ctx, coll, "s"))

	got, err := QueryChangedObjects(ctx, coll, "s", 0, 0)
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, ObjectSeq{ObjectId: "old1", ApplySeq: 5}, got[0])
	assert.Equal(t, ObjectSeq{ObjectId: "old2", ApplySeq: 9}, got[1])
	assert.Equal(t, ObjectSeq{ObjectId: "new1", ApplySeq: 12}, got[2])

	maxSeq, err := MaxObjectApplySeq(ctx, coll, "s")
	require.NoError(t, err)
	assert.Equal(t, uint64(12), maxSeq)

	// Idempotent — the flag short-circuits, values unchanged.
	require.NoError(t, BackfillApplySeq(ctx, coll, "s"))
	again, err := QueryChangedObjects(ctx, coll, "s", 0, 0)
	require.NoError(t, err)
	assert.Equal(t, got, again)

	// The other space stays unbackfilled until its own call.
	other, err := QueryChangedObjects(ctx, coll, "other", 0, 0)
	require.NoError(t, err)
	assert.Empty(t, other)
}
