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
	require.NoError(t, PersistMeta(ctx, coll, "a1", 5, nil, "spaceA"))
	require.NoError(t, PersistMeta(ctx, coll, "a2", 12, nil, "spaceA"))
	require.NoError(t, PersistMeta(ctx, coll, "a3", 20, nil, "spaceA"))
	// A different space — must never leak into spaceA results.
	require.NoError(t, PersistMeta(ctx, coll, "b1", 99, nil, "spaceB"))
	// The per-space watermark row (keyed space:<id>, no `sp`) — excluded.
	require.NoError(t, PersistSpaceMaxAddSeq(ctx, coll, "spaceA", 1000))

	got, err := QueryChangedObjects(ctx, coll, "spaceA", 10, 0)
	require.NoError(t, err)
	require.Len(t, got, 2)
	// Ordered ascending by AddSeq.
	assert.Equal(t, ObjectSeq{ObjectId: "a2", AddSeq: 12}, got[0])
	assert.Equal(t, ObjectSeq{ObjectId: "a3", AddSeq: 20}, got[1])
}

func TestQueryChangedObjects_PaginatesByAddSeq(t *testing.T) {
	_, coll := openMetaColl(t)
	require.NoError(t, ensureMetaIndexes(ctx, coll))

	for i, id := range []string{"o1", "o2", "o3", "o4"} {
		require.NoError(t, PersistMeta(ctx, coll, id, uint64((i+1)*10), nil, "s"))
	}

	page1, err := QueryChangedObjects(ctx, coll, "s", 0, 2)
	require.NoError(t, err)
	require.Len(t, page1, 2)
	assert.Equal(t, "o1", page1[0].ObjectId)
	assert.Equal(t, "o2", page1[1].ObjectId)

	// Resume from the last AddSeq of page1.
	page2, err := QueryChangedObjects(ctx, coll, "s", page1[1].AddSeq, 2)
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
	require.NoError(t, PersistMeta(ctx, coll, "legacy", 50, nil, ""))
	require.NoError(t, PersistMeta(ctx, coll, "scoped", 60, nil, "s"))

	got, err := QueryChangedObjects(ctx, coll, "s", 0, 0)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "scoped", got[0].ObjectId)
}

func TestMaxObjectAddSeq(t *testing.T) {
	_, coll := openMetaColl(t)
	require.NoError(t, ensureMetaIndexes(ctx, coll))

	got, err := MaxObjectAddSeq(ctx, coll, "s")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), got, "empty space")

	require.NoError(t, PersistMeta(ctx, coll, "o1", 10, nil, "s"))
	require.NoError(t, PersistMeta(ctx, coll, "o2", 35, nil, "s"))
	require.NoError(t, PersistMeta(ctx, coll, "other", 99, nil, "otherSpace"))

	got, err = MaxObjectAddSeq(ctx, coll, "s")
	require.NoError(t, err)
	assert.Equal(t, uint64(35), got, "max within space, ignores other spaces")
}
