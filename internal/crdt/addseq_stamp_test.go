package crdt

import (
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordAddSeq reads the _addSeq watermark off a materialised record.
// Returns 0 when the field is absent.
func recordAddSeq(t *testing.T, st *Controller, id string) uint64 {
	t.Helper()
	rec := st.Get(ctx, testDS, id)
	require.NotNil(t, rec)
	return uint64(rec.GetInt(AddSeqField))
}

func TestStampAddSeq_OnCreateAndModify(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	create := makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "x"}),
	})
	create.AddSeq = 5
	require.NoError(t, st.ApplyChange(ctx, create))
	assert.Equal(t, uint64(5), recordAddSeq(t, st, "r1"), "stamped on create")

	modify := makeChange("v2", "r1", Op{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("y")})
	modify.AddSeq = 9
	require.NoError(t, st.ApplyChange(ctx, modify))
	assert.Equal(t, uint64(9), recordAddSeq(t, st, "r1"), "advanced on modify")
}

func TestStampAddSeq_Monotonic_LowerReplayDoesNotRegress(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	high := makeUpsert("v2", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "x"}),
	})
	high.AddSeq = 20
	require.NoError(t, st.ApplyChange(ctx, high))
	assert.Equal(t, uint64(20), recordAddSeq(t, st, "r1"))

	// Replay an older change (lower AddSeq, lower version) — applies
	// idempotently via gating but must not regress _addSeq.
	low := makeChange("v1", "r1", Op{Type: OpSet, Path: []string{"other"}, Payload: arena.NewString("z")})
	low.AddSeq = 7
	require.NoError(t, st.ApplyChange(ctx, low))
	assert.Equal(t, uint64(20), recordAddSeq(t, st, "r1"), "lower replay does not regress")
}

func TestStampAddSeq_OnTombstone(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	create := makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "x"}),
	})
	create.AddSeq = 3
	require.NoError(t, st.ApplyChange(ctx, create))

	del := makeChange("v2", "r1", Op{Type: OpDelete})
	del.AddSeq = 8
	require.NoError(t, st.ApplyChange(ctx, del))

	// Tombstone still carries the latest AddSeq so a delete surfaces in
	// "changed since N" scans.
	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	assert.NotNil(t, rec.Get(DeletedAtField), "is a tombstone")
	assert.Equal(t, uint64(8), uint64(rec.GetInt(AddSeqField)))
}

func TestStampAddSeq_ZeroIsNoOp(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// Default makeUpsert leaves AddSeq=0 — nothing meaningful to stamp.
	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "x"}),
	})))
	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	assert.Nil(t, rec.Get(AddSeqField), "no _addSeq stamped for zero AddSeq")
}

// The builtin _addSeq index is ensured when the per-object collection
// first materialises through an apply.
func TestStampAddSeq_BuiltinIndexEnsured(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	c, err := NewController(ctx, "obj1", db, HandlerReg{Name: testDS, Handler: DefaultHandler{}, Schema: dynSchema})
	require.NoError(t, err)

	arena := &anyenc.Arena{}
	require.NoError(t, c.ApplyChange(ctx, makeUpsert("v1", "r1",
		Op{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("x")})))

	coll, err := db.OpenCollection(ctx, "obj1_"+testDS)
	require.NoError(t, err)
	assert.Contains(t, indexNames(coll.GetIndexes()), "idx__addSeq")
}
