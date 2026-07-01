package crdt

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersistDeletionMark_StampsAndProjects(t *testing.T) {
	_, coll := openMetaColl(t)
	require.NoError(t, ensureMetaIndexes(ctx, coll))

	// A content-bearing object row: q=5, as=5, handler versions.
	require.NoError(t, PersistMeta(ctx, coll, "o1", 5, 5, map[string]int{"ds": 3}, "spaceA"))

	// Deletion stamps del=true and bumps as to a fresh, higher seq.
	require.NoError(t, PersistDeletionMark(ctx, coll, "o1", "spaceA", 9))

	got, err := QueryChangedObjects(ctx, coll, "spaceA", 0, 0)
	require.NoError(t, err)
	require.Equal(t, []ObjectSeq{{ObjectId: "o1", ApplySeq: 9, Deleted: true}}, got)

	// q (addSeq watermark) and hv are preserved; as advanced to 9.
	addSeq, applySeq, hv, err := LoadMeta(ctx, coll, "o1")
	require.NoError(t, err)
	assert.Equal(t, uint64(5), addSeq)
	assert.Equal(t, uint64(9), applySeq)
	assert.Equal(t, map[string]int{"ds": 3}, hv)
}

func TestMetaExists_SpaceScoped(t *testing.T) {
	_, coll := openMetaColl(t)
	require.NoError(t, PersistMeta(ctx, coll, "o1", 1, 1, nil, "spaceA"))

	ok, err := MetaExists(ctx, coll, "o1", "spaceA")
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = MetaExists(ctx, coll, "o1", "spaceB") // right id, wrong space
	require.NoError(t, err)
	assert.False(t, ok)

	ok, err = MetaExists(ctx, coll, "missing", "spaceA")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestLoadOrInitGeneration_MintsStableUnique(t *testing.T) {
	_, coll := openMetaColl(t)

	g1, err := LoadOrInitGeneration(ctx, coll, "spaceA")
	require.NoError(t, err)
	require.NotEmpty(t, g1)
	b, err := hex.DecodeString(g1)
	require.NoError(t, err)
	assert.Len(t, b, 12, "bson-style 12-byte ObjectID")

	g1again, err := LoadOrInitGeneration(ctx, coll, "spaceA")
	require.NoError(t, err)
	assert.Equal(t, g1, g1again, "stable across reads")

	g2, err := LoadOrInitGeneration(ctx, coll, "spaceB")
	require.NoError(t, err)
	assert.NotEqual(t, g1, g2, "distinct per space")
}

func TestSpaceDeletedGate_RoundTripPreservesSiblings(t *testing.T) {
	_, coll := openMetaColl(t)

	// Missing row → zero gate (forces a reconcile sweep).
	head, ver, err := LoadSpaceDeletedGate(ctx, coll, "spaceA")
	require.NoError(t, err)
	assert.Equal(t, "", head)
	assert.Equal(t, 0, ver)

	// Siblings on the same space:<id> row must survive a gate write.
	require.NoError(t, PersistSpaceMaxAddSeq(ctx, coll, "spaceA", 1000))
	gen, err := LoadOrInitGeneration(ctx, coll, "spaceA")
	require.NoError(t, err)

	require.NoError(t, PersistSpaceDeletedGate(ctx, coll, "spaceA", "HEAD-abc", 2))

	head, ver, err = LoadSpaceDeletedGate(ctx, coll, "spaceA")
	require.NoError(t, err)
	assert.Equal(t, "HEAD-abc", head)
	assert.Equal(t, 2, ver)

	q, err := LoadSpaceMaxAddSeq(ctx, coll, "spaceA")
	require.NoError(t, err)
	assert.Equal(t, uint64(1000), q, "forward-catchup watermark preserved")

	genAfter, err := LoadOrInitGeneration(ctx, coll, "spaceA")
	require.NoError(t, err)
	assert.Equal(t, gen, genAfter, "generation preserved")
}

func TestApplySeqAllocator_Seed(t *testing.T) {
	calls := 0
	a := NewApplySeqAllocator(func(context.Context) (uint64, error) {
		calls++
		return 42, nil
	})
	require.NoError(t, a.Seed(ctx))
	require.NoError(t, a.Seed(ctx)) // idempotent

	next, err := a.Next(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(43), next, "allocates above the seed")
	assert.Equal(t, 1, calls, "seedFn runs exactly once")
}
