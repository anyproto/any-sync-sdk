package crdt

import (
	"context"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newApplySeqController builds a Controller wired with an allocator
// seeded at `seed`, the standard DefaultHandler dataset, and a _meta
// collection — the minimal applySeq-enabled apply environment.
func newApplySeqController(t *testing.T, seed uint64) (*Controller, anystore.Collection) {
	t.Helper()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "applyseq.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctrl, err := NewController(ctx, "obj1", db, HandlerReg{Name: testDS, Handler: DefaultHandler{}, Schema: dynSchema})
	require.NoError(t, err)
	ctrl.SetSpaceId("s")
	ctrl.SetApplySeqAllocator(NewApplySeqAllocator(func(context.Context) (uint64, error) {
		return seed, nil
	}))
	metaColl, err := db.Collection(ctx, MetaCollectionName)
	require.NoError(t, err)
	require.NoError(t, ensureMetaIndexes(ctx, metaColl))
	return ctrl, metaColl
}

func applySeqChange(versionId VersionId, recId string, ops ...Op) Change {
	return Change{
		ObjectId: "obj1", Dataset: testDS, DataVersion: testDataVersion,
		ChangeId: "ch-" + string(versionId), VersionId: versionId,
		Records: []RecordChange{{Id: recId, Upsert: true, Ops: ops}},
	}
}

// Every apply — DAG-borne or Local — allocates an ascending applySeq,
// stamps it on the written records, and persists the per-object max in
// the same tx. The allocator seeds past the supplied max.
func TestApplySeq_StampsAndPersists(t *testing.T) {
	ctrl, metaColl := newApplySeqController(t, 100)
	arena := &anyenc.Arena{}

	// DAG-style change.
	res, err := ctrl.ApplyChangeWithResult(ctx, applySeqChange("v1", "r1",
		Op{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("a")}))
	require.NoError(t, err)
	assert.Equal(t, uint64(101), res.ApplySeq, "first allocation past the seed")

	rec := ctrl.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	assert.Equal(t, 101, rec.GetInt(ApplySeqField))

	// Local change (no DAG, no AddSeq) — the axis still advances. The
	// dataset schema must declare the field local for the class check.
	res2, err := ctrl.ApplyChangeWithResult(ctx, applySeqChange("v2", "r1",
		Op{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("b")}))
	require.NoError(t, err)
	assert.Equal(t, uint64(102), res2.ApplySeq)

	rec = ctrl.Get(ctx, testDS, "r1")
	assert.Equal(t, 102, rec.GetInt(ApplySeqField), "record stamp advanced")

	// Per-object meta watermark persisted; feed sees the object.
	_, maxApply, _, err := LoadMeta(ctx, metaColl, "obj1")
	require.NoError(t, err)
	assert.Equal(t, uint64(102), maxApply)

	rows, err := QueryChangedObjects(ctx, metaColl, "s", 101, 0)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, ObjectSeq{ObjectId: "obj1", ApplySeq: 102}, rows[0])
}

// A tombstoning change carries the applySeq onto the tombstone so
// deletion streaming surfaces it past a cursor.
func TestApplySeq_OnTombstone(t *testing.T) {
	ctrl, _ := newApplySeqController(t, 0)
	arena := &anyenc.Arena{}

	_, err := ctrl.ApplyChangeWithResult(ctx, applySeqChange("v1", "r1",
		Op{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("a")}))
	require.NoError(t, err)

	res, err := ctrl.ApplyChangeWithResult(ctx, applySeqChange("v2", "r1", Op{Type: OpDelete}))
	require.NoError(t, err)
	assert.Equal(t, uint64(2), res.ApplySeq)

	tomb := ctrl.Get(ctx, testDS, "r1")
	require.NotNil(t, tomb)
	require.NotNil(t, tomb.Get(DeletedAtField), "tombstoned")
	assert.Equal(t, 2, tomb.GetInt(ApplySeqField))
}

// Without an allocator (unit-test / no-feed mode) nothing is stamped
// and the result carries zero — fully backward compatible.
func TestApplySeq_NoAllocatorIsNoop(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "noalloc.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ctrl, err := NewController(ctx, "obj1", db, HandlerReg{Name: testDS, Handler: DefaultHandler{}, Schema: dynSchema})
	require.NoError(t, err)
	arena := &anyenc.Arena{}

	res, err := ctrl.ApplyChangeWithResult(ctx, applySeqChange("v1", "r1",
		Op{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("a")}))
	require.NoError(t, err)
	assert.Zero(t, res.ApplySeq)
	rec := ctrl.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	assert.Nil(t, rec.Get(ApplySeqField))
}

// The allocator seeds lazily exactly once and hands out strictly
// ascending values afterwards.
func TestApplySeqAllocator_SeedOnce(t *testing.T) {
	calls := 0
	a := NewApplySeqAllocator(func(context.Context) (uint64, error) {
		calls++
		return 41, nil
	})
	n1, err := a.Next(ctx)
	require.NoError(t, err)
	n2, err := a.Next(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), n1)
	assert.Equal(t, uint64(43), n2)
	assert.Equal(t, 1, calls, "seedFn runs once")
}
