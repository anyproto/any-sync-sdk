package crdt

import (
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChangedSince pins the precondition read: a record stamped above
// the value is a change, nothing above it is not, and a reindex keeps
// the watermark, so the answers hold while one is pending.
func TestChangedSince(t *testing.T) {
	ctrl, _ := newApplySeqController(t, 100)
	arena := &anyenc.Arena{}

	res, err := ctrl.ApplyChangeWithResult(ctx, applySeqChange("v1", "r1",
		Op{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("a")}))
	require.NoError(t, err)

	changed, err := ctrl.ChangedSince(ctx, testDS, res.ApplySeq)
	require.NoError(t, err)
	assert.False(t, changed, "nothing above the highest stamp")

	changed, err = ctrl.ChangedSince(ctx, testDS, res.ApplySeq-1)
	require.NoError(t, err)
	assert.True(t, changed, "the record's stamp is above an older value")

	require.NoError(t, ctrl.ResetForReindex(ctx, nil))
	changed, err = ctrl.ChangedSince(ctx, testDS, res.ApplySeq-1)
	require.NoError(t, err)
	assert.True(t, changed, "the record's stamp is still above an older value")
	changed, err = ctrl.ChangedSince(ctx, testDS, res.ApplySeq)
	require.NoError(t, err)
	assert.False(t, changed, "nothing above the kept watermark")
}

// A keyed dataset keeps every object's rows in one collection: the
// precondition scan is bounded to this object's rows, so another
// object's later write is not a change here.
func TestChangedSince_KeyedScopedToObject(t *testing.T) {
	f := newKeyedFixture(t)
	alloc := NewApplySeqAllocator(nil)
	c1 := f.controller(t, "obj1", DefaultHandler{})
	c1.SetApplySeqAllocator(alloc)
	c2 := f.controller(t, "obj2", DefaultHandler{})
	c2.SetApplySeqAllocator(alloc)
	a := &anyenc.Arena{}

	res, err := c1.ApplyChangeWithResult(ctx, keyedChange("obj1", "v1",
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "v", "x")}}))
	require.NoError(t, err)
	own := res.ApplySeq
	_, err = c2.ApplyChangeWithResult(ctx, keyedChange("obj2", "v1",
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "v", "y")}}))
	require.NoError(t, err)
	// Move obj1's watermark past its keyed row so the scan runs.
	other := keyedChange("obj1", "v2", RecordChange{Id: "n1", Upsert: true, Ops: []Op{setOp(a, "v", "z")}})
	other.Dataset = stampNotes
	_, err = c1.ApplyChangeWithResult(ctx, other)
	require.NoError(t, err)

	changed, err := c1.ChangedSince(ctx, keyedSamples, own)
	require.NoError(t, err)
	assert.False(t, changed, "obj2's row is not a change of obj1's dataset")
	changed, err = c1.ChangedSince(ctx, keyedSamples, own-1)
	require.NoError(t, err)
	assert.True(t, changed)
}

func TestChangedSince_SharedObjectRow(t *testing.T) {
	ctrl, _ := newStampFixture(t, notesSchema)
	ctrl.SetApplySeqAllocator(NewApplySeqAllocator(nil))
	createObjectsRow(t, ctrl, "v1", 1)
	seq := uint64(ctrl.Get(ctx, stampShared, stampObjectId).GetInt(ApplySeqField))
	require.NotZero(t, seq)
	changed, err := ctrl.ChangedSince(ctx, stampShared, seq)
	require.NoError(t, err)
	require.False(t, changed)

	// A different object shares this collection, but not this precondition.
	coll, err := ctrl.openForRead(ctx, stampShared)
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"other","_applySeq":999}`)))
	changed, err = ctrl.ChangedSince(ctx, stampShared, seq)
	require.NoError(t, err)
	require.False(t, changed)

	// A synced dataset change stamps this object's row as well.
	a := &anyenc.Arena{}
	require.NoError(t, ctrl.ApplyChange(ctx, stampChange(stampNotes, "v2", 2,
		RecordChange{Id: "body", Upsert: true, Ops: []Op{setOp(a, "name", "edited")}})))
	changed, err = ctrl.ChangedSince(ctx, stampShared, seq)
	require.NoError(t, err)
	require.True(t, changed)
	latest := uint64(ctrl.Get(ctx, stampShared, stampObjectId).GetInt(ApplySeqField))
	changed, err = ctrl.ChangedSince(ctx, stampShared, latest)
	require.NoError(t, err)
	require.False(t, changed, "another object's row must not poison the guard")
}
