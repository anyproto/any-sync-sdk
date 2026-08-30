package crdt

import (
	"errors"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// ObjectStamper: a synced change on any per-object dataset stamps the
// object's row in the shared dataset (the objects row's modifiedAt).

const (
	stampObjectId = "obj-stamp"
	stampShared   = "objects"
	stampNotes    = "notes"
)

// stampHandler is the shared dataset's handler: DefaultHandler plus an
// ObjectStamper that stamps `modifiedAt` = the change Timestamp (and
// `modifiedBy` = the change Creator when one is set) and counts
// invocations.
type stampHandler struct {
	DefaultHandler
	calls int
}

func (h *stampHandler) StampObject(ctx *ChangeCtx, sink *Sink) {
	h.calls++
	a := &anyenc.Arena{}
	sink.DeriveOnce(Op{Type: OpSet, Path: []string{"modifiedAt"}, Payload: a.NewNumberFloat64(float64(ctx.Change.Timestamp))})
	if ctx.Change.Creator != "" {
		sink.DeriveOnce(Op{Type: OpSet, Path: []string{"modifiedBy"}, Payload: a.NewString(ctx.Change.Creator)})
	}
}

// notesSchema declares one local and one account field next to the
// dynamic synced keyspace, so Local / Injected changes can land ops.
var notesSchema = schema.Dataset{
	Fields: []schema.Field{
		{Id: "unread", Name: "Unread", Schema: schema.Leaf(schema.KindBoolean), Scope: schema.ScopeLocal},
		{Id: "pinned", Name: "Pinned", Schema: schema.Leaf(schema.KindBoolean), Scope: schema.ScopeAccount},
	},
	Dynamic: true,
}

func newStampFixture(t *testing.T, notes schema.Dataset) (*Controller, *stampHandler) {
	return newStampFixtureWith(t, notes, DefaultHandler{})
}

func newStampFixtureWith(t *testing.T, notes schema.Dataset, notesHandler Handler) (*Controller, *stampHandler) {
	t.Helper()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	sharedColl, err := db.Collection(ctx, "space_objects")
	require.NoError(t, err)
	h := &stampHandler{}
	ctrl, err := NewControllerWithShared(ctx, stampObjectId, db, SharedCollections{stampShared: sharedColl},
		HandlerReg{Name: stampShared, Handler: h, Schema: dynSchema},
		HandlerReg{Name: stampNotes, Handler: notesHandler, Schema: notes},
	)
	require.NoError(t, err)
	return ctrl, h
}

// rejectBadKeyHandler rejects any op that writes `bad` — as a
// single-path op or as a key of a multi-field $set — so the controller
// exercises its per-key salvage.
type rejectBadKeyHandler struct{ DefaultHandler }

func (rejectBadKeyHandler) BeforeModify(_ *ChangeCtx, _ *RecordChange, op *Op, _ *Sink) error {
	if len(op.Path) == 1 && op.Path[0] == "bad" {
		return errors.New("bad key")
	}
	if len(op.Path) == 0 && op.Payload != nil && op.Payload.Type() == anyenc.TypeObject && op.Payload.Get("bad") != nil {
		return errors.New("bad key")
	}
	return nil
}

// stampChange builds a change on dataset signed by "acct-<ver>", so
// the stamper derives the modifiedAt / modifiedBy pair as the
// production objects handler does.
func stampChange(dataset string, ver VersionId, ts int64, recs ...RecordChange) Change {
	return Change{
		ObjectId:    stampObjectId,
		Dataset:     dataset,
		ChangeId:    "ch-" + string(ver),
		VersionId:   ver,
		Timestamp:   ts,
		Creator:     "acct-" + string(ver),
		DataVersion: dataset + "-v1",
		Records:     recs,
	}
}

// opPaths lists the paths of ops, for order-insensitive assertions.
func opPaths(ops []Op) [][]string {
	paths := make([][]string, 0, len(ops))
	for _, op := range ops {
		paths = append(paths, op.Path)
	}
	return paths
}

func setOp(a *anyenc.Arena, path, val string) Op {
	return Op{Type: OpSet, Path: []string{path}, Payload: a.NewString(val)}
}

func createObjectsRow(t *testing.T, ctrl *Controller, ver VersionId, ts int64) {
	t.Helper()
	a := &anyenc.Arena{}
	require.NoError(t, ctrl.ApplyChange(ctx, stampChange(stampShared, ver, ts,
		RecordChange{Id: stampObjectId, Upsert: true, Ops: []Op{setOp(a, "name", "n")}})))
}

func modifiedAtOf(t *testing.T, ctrl *Controller) float64 {
	t.Helper()
	row := ctrl.Get(ctx, stampShared, stampObjectId)
	require.NotNil(t, row, "objects row present")
	v := row.Get("modifiedAt")
	require.NotNil(t, v, "modifiedAt present")
	return v.GetFloat64()
}

func TestObjectStamp_DatasetWriteStampsSharedRow(t *testing.T) {
	ctrl, h := newStampFixture(t, notesSchema)
	a := &anyenc.Arena{}
	createObjectsRow(t, ctrl, "v1", 100)
	assert.Equal(t, 0, h.calls, "a write to the shared dataset itself never invokes the stamper")
	require.Nil(t, ctrl.Get(ctx, stampShared, stampObjectId).Get("modifiedAt"), "test handler stamps only via StampObject")

	write := stampChange(stampNotes, "v2", 200,
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "text", "hello")}})
	write.TraceIds = []string{"trace-1"}
	res, err := ctrl.ApplyChangeWithResult(ctx, write)
	require.NoError(t, err)
	assert.Equal(t, 1, h.calls)
	assert.EqualValues(t, 200, modifiedAtOf(t, ctrl))
	row := ctrl.Get(ctx, stampShared, stampObjectId)
	assert.Equal(t, VersionId("v2"), GetRecordVersion(row, "modifiedAt"),
		"stamp carries the triggering change's VersionId")
	assert.Nil(t, row.Get(TracesKey), "the change's trace ids stay on its own records")
	assert.NotNil(t, ctrl.Get(ctx, stampNotes, "r1").Get(TracesKey))
	require.Len(t, res.ObjectStamps, 1)
	assert.Equal(t, stampShared, res.ObjectStamps[0].Dataset)
	assert.Equal(t, stampObjectId, res.ObjectStamps[0].RowId)
	assert.ElementsMatch(t, [][]string{{"modifiedAt"}, {"modifiedBy"}}, opPaths(res.ObjectStamps[0].Ops))
	assert.Equal(t, "acct-v2", row.GetString("modifiedBy"))

	// The notes record itself is untouched by the stamp.
	rec := ctrl.Get(ctx, stampNotes, "r1")
	require.NotNil(t, rec)
	assert.Nil(t, rec.Get("modifiedAt"))
}

// A stamper that derives several ops lands them as one row update
// under one VersionId and reports them together; an out-of-order older
// change loses the gate for all of them and reports none.
func TestObjectStamp_MultiOpStampLandsAndReportsTogether(t *testing.T) {
	ctrl, _ := newStampFixture(t, notesSchema)
	a := &anyenc.Arena{}
	createObjectsRow(t, ctrl, "v1", 100)

	newer := stampChange(stampNotes, "v3", 300, RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "text", "b")}})
	newer.Creator = "acct-b"
	res, err := ctrl.ApplyChangeWithResult(ctx, newer)
	require.NoError(t, err)
	require.Len(t, res.ObjectStamps, 1)
	assert.ElementsMatch(t, [][]string{{"modifiedAt"}, {"modifiedBy"}}, opPaths(res.ObjectStamps[0].Ops))
	row := ctrl.Get(ctx, stampShared, stampObjectId)
	assert.EqualValues(t, 300, row.Get("modifiedAt").GetFloat64())
	assert.Equal(t, "acct-b", row.GetString("modifiedBy"))
	assert.Equal(t, VersionId("v3"), GetRecordVersion(row, "modifiedAt"))
	assert.Equal(t, VersionId("v3"), GetRecordVersion(row, "modifiedBy"))

	older := stampChange(stampNotes, "v2", 200, RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "text", "a")}})
	older.Creator = "acct-a"
	res, err = ctrl.ApplyChangeWithResult(ctx, older)
	require.NoError(t, err)
	assert.Empty(t, res.ObjectStamps, "a gated-out stamp is not reported")
	row = ctrl.Get(ctx, stampShared, stampObjectId)
	assert.EqualValues(t, 300, row.Get("modifiedAt").GetFloat64())
	assert.Equal(t, "acct-b", row.GetString("modifiedBy"), "older change must not regress either stamp")
}

// Stamps on a row whose leaves sit at different versions can land one
// op and lose another; only the ops that took the gate are reported,
// since the live dispatcher ships reported payloads verbatim.
func TestObjectStamp_SplitGateReportsOnlyTakenOps(t *testing.T) {
	ctrl, _ := newStampFixture(t, notesSchema)
	a := &anyenc.Arena{}
	createObjectsRow(t, ctrl, "v1", 100)

	// An unsigned change stamps modifiedAt alone (the test stamper
	// derives modifiedBy only for a signed change).
	unsigned := stampChange(stampNotes, "v5", 500, RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "text", "u")}})
	unsigned.Creator = ""
	res, err := ctrl.ApplyChangeWithResult(ctx, unsigned)
	require.NoError(t, err)
	require.Len(t, res.ObjectStamps, 1)
	assert.Equal(t, [][]string{{"modifiedAt"}}, opPaths(res.ObjectStamps[0].Ops))

	// An older signed change: modifiedAt loses to v5, modifiedBy lands.
	res, err = ctrl.ApplyChangeWithResult(ctx, stampChange(stampNotes, "v3", 300,
		RecordChange{Id: "r1", Ops: []Op{setOp(a, "text", "s")}}))
	require.NoError(t, err)
	require.Len(t, res.ObjectStamps, 1)
	assert.Equal(t, [][]string{{"modifiedBy"}}, opPaths(res.ObjectStamps[0].Ops), "the gated-out modifiedAt is not reported")
	row := ctrl.Get(ctx, stampShared, stampObjectId)
	assert.EqualValues(t, 500, row.Get("modifiedAt").GetFloat64())
	assert.Equal(t, "acct-v3", row.GetString("modifiedBy"))
}

func TestObjectStamp_MultiRecordChangeStampsOnce(t *testing.T) {
	ctrl, h := newStampFixture(t, notesSchema)
	a := &anyenc.Arena{}
	createObjectsRow(t, ctrl, "v1", 100)
	res, err := ctrl.ApplyChangeWithResult(ctx, stampChange(stampNotes, "v2", 200,
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "text", "a")}},
		RecordChange{Id: "r2", Upsert: true, Ops: []Op{setOp(a, "text", "b")}},
		RecordChange{Id: "r3", Upsert: true, Ops: []Op{setOp(a, "text", "c")}}))
	require.NoError(t, err)
	assert.Equal(t, 1, h.calls)
	assert.Len(t, res.ObjectStamps, 1)
}

func TestObjectStamp_AbsentRowNotCreated(t *testing.T) {
	ctrl, h := newStampFixture(t, notesSchema)
	a := &anyenc.Arena{}
	res, err := ctrl.ApplyChangeWithResult(ctx, stampChange(stampNotes, "v1", 100,
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "text", "hello")}}))
	require.NoError(t, err)
	assert.Equal(t, 1, h.calls, "stamper runs; the strict update is what skips")
	assert.Nil(t, ctrl.Get(ctx, stampShared, stampObjectId), "no objects row materialized by a stamp")
	assert.Empty(t, res.ObjectStamps)
	assert.NotNil(t, ctrl.Get(ctx, stampNotes, "r1"), "the dataset write itself landed")
}

func TestObjectStamp_TombstoneUntouched(t *testing.T) {
	ctrl, _ := newStampFixture(t, notesSchema)
	a := &anyenc.Arena{}
	createObjectsRow(t, ctrl, "v1", 100)
	require.NoError(t, ctrl.ApplyChange(ctx, stampChange(stampShared, "v2", 200,
		RecordChange{Id: stampObjectId, Ops: []Op{{Type: OpDelete}}})))
	before := ctrl.Get(ctx, stampShared, stampObjectId)
	require.NotNil(t, before.Get(DeletedAtField), "row is a tombstone")

	res, err := ctrl.ApplyChangeWithResult(ctx, stampChange(stampNotes, "v3", 300,
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "text", "hello")}}))
	require.NoError(t, err)
	assert.Empty(t, res.ObjectStamps)
	after := ctrl.Get(ctx, stampShared, stampObjectId)
	assert.NotNil(t, after.Get(DeletedAtField))
	assert.Nil(t, after.Get("modifiedAt"))
	assert.Equal(t, before.String(), after.String(), "tombstone bytes unchanged")
}

func TestObjectStamp_LocalAndInjectedNeverStamp(t *testing.T) {
	ctrl, h := newStampFixture(t, notesSchema)
	a := &anyenc.Arena{}
	createObjectsRow(t, ctrl, "v1", 100)
	require.NoError(t, ctrl.ApplyChange(ctx, stampChange(stampNotes, "v2", 200,
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "text", "hello")}})))
	require.Equal(t, 1, h.calls)

	local := stampChange(stampNotes, "l1", 300,
		RecordChange{Id: "r1", Ops: []Op{{Type: OpSet, Path: []string{"unread"}, Payload: a.NewTrue()}}})
	local.Local = true
	res, err := ctrl.ApplyChangeWithResult(ctx, local)
	require.NoError(t, err)
	require.Empty(t, res.Rejections, "the local field write itself lands")
	assert.Empty(t, res.ObjectStamps)

	injected := stampChange(stampNotes, "a1", 400,
		RecordChange{Id: "r1", Ops: []Op{{Type: OpSet, Path: []string{"pinned"}, Payload: a.NewTrue()}}})
	injected.Injected = true
	res, err = ctrl.ApplyChangeWithResult(ctx, injected)
	require.NoError(t, err)
	require.Empty(t, res.Rejections)
	assert.Empty(t, res.ObjectStamps)

	assert.Equal(t, 1, h.calls, "neither route invokes the stamper")
	assert.EqualValues(t, 200, modifiedAtOf(t, ctrl))
}

func TestObjectStamp_NothingLandedNoStamp(t *testing.T) {
	declared := schema.Dataset{Fields: []schema.Field{
		{Id: "text", Name: "Text", Schema: schema.Leaf(schema.KindString), Scope: schema.ScopeSynced},
	}}
	ctrl, h := newStampFixture(t, declared)
	a := &anyenc.Arena{}
	createObjectsRow(t, ctrl, "v1", 100)

	// Strict update of an absent record: whole-record drop.
	res, err := ctrl.ApplyChangeWithResult(ctx, stampChange(stampNotes, "v2", 200,
		RecordChange{Id: "missing", Ops: []Op{setOp(a, "text", "x")}}))
	require.NoError(t, err)
	require.NotEmpty(t, res.Rejections)
	assert.Equal(t, 0, h.calls)
	assert.Empty(t, res.ObjectStamps)

	require.NoError(t, ctrl.ApplyChange(ctx, stampChange(stampNotes, "v3", 300,
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "text", "seed")}})))
	require.Equal(t, 1, h.calls)

	// Every op rejected by the field-class gate: nothing reaches the
	// record.
	res, err = ctrl.ApplyChangeWithResult(ctx, stampChange(stampNotes, "v4", 400,
		RecordChange{Id: "r1", Ops: []Op{setOp(a, "undeclared", "x")}}))
	require.NoError(t, err)
	require.NotEmpty(t, res.Rejections)
	assert.Equal(t, 1, h.calls)
	assert.Empty(t, res.ObjectStamps)
	assert.EqualValues(t, 300, modifiedAtOf(t, ctrl))

	// One op of two survives: that is a write.
	res, err = ctrl.ApplyChangeWithResult(ctx, stampChange(stampNotes, "v5", 500,
		RecordChange{Id: "r1", Ops: []Op{setOp(a, "undeclared", "x"), setOp(a, "text", "ok")}}))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.Equal(t, 2, h.calls)
	assert.EqualValues(t, 500, modifiedAtOf(t, ctrl))
}

// A multi-field op the handler rejects per key still lands its
// surviving keys — that is a write, so the object is stamped.
func TestObjectStamp_PerKeySalvageStamps(t *testing.T) {
	ctrl, h := newStampFixtureWith(t, notesSchema, rejectBadKeyHandler{})
	a := &anyenc.Arena{}
	createObjectsRow(t, ctrl, "v1", 100)
	require.NoError(t, ctrl.ApplyChange(ctx, stampChange(stampNotes, "v2", 200,
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "text", "seed")}})))
	require.Equal(t, 1, h.calls)

	multi := a.NewObject()
	multi.Set("good", a.NewString("y"))
	multi.Set("bad", a.NewString("z"))
	res, err := ctrl.ApplyChangeWithResult(ctx, stampChange(stampNotes, "v3", 300,
		RecordChange{Id: "r1", Ops: []Op{{Type: OpSet, Payload: multi}}}))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1, "the bad key is shed")
	rec := ctrl.Get(ctx, stampNotes, "r1")
	assert.Equal(t, "y", rec.GetString("good"))
	assert.Nil(t, rec.Get("bad"))
	assert.Equal(t, 2, h.calls)
	assert.Len(t, res.ObjectStamps, 1)
	assert.EqualValues(t, 300, modifiedAtOf(t, ctrl))

	// Every key rejected: not a write.
	onlyBad := a.NewObject()
	onlyBad.Set("bad", a.NewString("z"))
	res, err = ctrl.ApplyChangeWithResult(ctx, stampChange(stampNotes, "v4", 400,
		RecordChange{Id: "r1", Ops: []Op{{Type: OpSet, Payload: onlyBad}}}))
	require.NoError(t, err)
	require.NotEmpty(t, res.Rejections)
	assert.Equal(t, 2, h.calls)
	assert.Empty(t, res.ObjectStamps)
	assert.EqualValues(t, 300, modifiedAtOf(t, ctrl))
}

// A stamp that loses the LWW gate (an older change delivered late) is
// applied but not reported: subscribers must not receive a $set with
// the stale value.
func TestObjectStamp_GatedOutStampNotReported(t *testing.T) {
	ctrl, _ := newStampFixture(t, notesSchema)
	a := &anyenc.Arena{}
	createObjectsRow(t, ctrl, "v1", 100)
	res, err := ctrl.ApplyChangeWithResult(ctx, stampChange(stampNotes, "v3", 300,
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "text", "c")}}))
	require.NoError(t, err)
	require.Len(t, res.ObjectStamps, 1)

	res, err = ctrl.ApplyChangeWithResult(ctx, stampChange(stampNotes, "v2", 200,
		RecordChange{Id: "r2", Upsert: true, Ops: []Op{setOp(a, "text", "b")}}))
	require.NoError(t, err)
	assert.Empty(t, res.ObjectStamps, "older stamp lost the gate")
	assert.EqualValues(t, 300, modifiedAtOf(t, ctrl))
	assert.NotNil(t, ctrl.Get(ctx, stampNotes, "r2"), "the dataset write itself landed")
}

// Deleting an already-deleted record writes nothing and stamps nothing.
func TestObjectStamp_RedeleteNoStamp(t *testing.T) {
	ctrl, _ := newStampFixture(t, notesSchema)
	a := &anyenc.Arena{}
	createObjectsRow(t, ctrl, "v1", 100)
	require.NoError(t, ctrl.ApplyChange(ctx, stampChange(stampNotes, "v2", 200,
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "text", "hello")}})))
	require.NoError(t, ctrl.ApplyChange(ctx, stampChange(stampNotes, "v3", 300,
		RecordChange{Id: "r1", Ops: []Op{{Type: OpDelete}}})))
	require.EqualValues(t, 300, modifiedAtOf(t, ctrl))

	res, err := ctrl.ApplyChangeWithResult(ctx, stampChange(stampNotes, "v4", 400,
		RecordChange{Id: "r1", Ops: []Op{{Type: OpDelete}}}))
	require.NoError(t, err)
	assert.Empty(t, res.ObjectStamps)
	assert.EqualValues(t, 300, modifiedAtOf(t, ctrl))
}

// An upsert with no ops still materializes the record: a write.
func TestObjectStamp_ZeroOpUpsertCreates(t *testing.T) {
	ctrl, _ := newStampFixture(t, notesSchema)
	createObjectsRow(t, ctrl, "v1", 100)
	res, err := ctrl.ApplyChangeWithResult(ctx, stampChange(stampNotes, "v2", 200,
		RecordChange{Id: "r1", Upsert: true}))
	require.NoError(t, err)
	require.NotNil(t, ctrl.Get(ctx, stampNotes, "r1"))
	assert.Len(t, res.ObjectStamps, 1)
	assert.EqualValues(t, 200, modifiedAtOf(t, ctrl))
}

func TestObjectStamp_DeleteIsAWrite(t *testing.T) {
	ctrl, _ := newStampFixture(t, notesSchema)
	a := &anyenc.Arena{}
	createObjectsRow(t, ctrl, "v1", 100)
	require.NoError(t, ctrl.ApplyChange(ctx, stampChange(stampNotes, "v2", 200,
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "text", "hello")}})))
	require.NoError(t, ctrl.ApplyChange(ctx, stampChange(stampNotes, "v3", 300,
		RecordChange{Id: "r1", Ops: []Op{{Type: OpDelete}}})))
	assert.EqualValues(t, 300, modifiedAtOf(t, ctrl))
}

// Out-of-order delivery: the stamp is LWW-gated on the change's
// VersionId like any field, so an older change arriving later never
// regresses the row, and the shared row's own writes interleave with
// dataset writes under the same rule.
func TestObjectStamp_OutOfOrderKeepsNewest(t *testing.T) {
	ctrl, _ := newStampFixture(t, notesSchema)
	a := &anyenc.Arena{}
	createObjectsRow(t, ctrl, "v1", 100)
	require.NoError(t, ctrl.ApplyChange(ctx, stampChange(stampNotes, "v3", 300,
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "text", "c")}})))
	require.NoError(t, ctrl.ApplyChange(ctx, stampChange(stampNotes, "v2", 200,
		RecordChange{Id: "r2", Upsert: true, Ops: []Op{setOp(a, "text", "b")}})))
	assert.EqualValues(t, 300, modifiedAtOf(t, ctrl), "older dataset change must not regress the stamp")
	assert.Equal(t, VersionId("v3"), GetRecordVersion(ctrl.Get(ctx, stampShared, stampObjectId), "modifiedAt"))

	require.NoError(t, ctrl.ApplyChange(ctx, stampChange(stampNotes, "v5", 500,
		RecordChange{Id: "r1", Ops: []Op{setOp(a, "text", "e")}})))
	require.NoError(t, ctrl.ApplyChange(ctx, stampChange(stampNotes, "v4", 400,
		RecordChange{Id: "r1", Ops: []Op{setOp(a, "text", "d")}})))
	assert.EqualValues(t, 500, modifiedAtOf(t, ctrl))
}

// Cost of the stamp on the dataset-write hot path (cold restore replays
// every change through here): one extra UpdateId on the shared row per
// change. Compare with BenchmarkApply_DatasetWrite_NoStamper.
func BenchmarkApply_DatasetWrite_WithObjectStamp(b *testing.B) {
	benchDatasetWrite(b, true)
}

func BenchmarkApply_DatasetWrite_NoStamper(b *testing.B) {
	benchDatasetWrite(b, false)
}

func benchDatasetWrite(b *testing.B, stamper bool) {
	db, err := anystore.Open(ctx, filepath.Join(b.TempDir(), "bench.db"), nil)
	require.NoError(b, err)
	defer func() { _ = db.Close() }()
	sharedColl, err := db.Collection(ctx, "space_objects")
	require.NoError(b, err)
	var shared Handler = DefaultHandler{}
	if stamper {
		shared = &stampHandler{}
	}
	// The production objects collection indexes modifiedAt; include it
	// so the stamp pays the index update too.
	ctrl, err := NewControllerWithShared(ctx, stampObjectId, db, SharedCollections{stampShared: sharedColl},
		HandlerReg{Name: stampShared, Handler: shared, Schema: dynSchema,
			Indexes: []anystore.IndexInfo{{Name: "idx_modifiedAt", Fields: []string{"modifiedAt"}}}},
		HandlerReg{Name: stampNotes, Handler: DefaultHandler{}, Schema: dynSchema},
	)
	require.NoError(b, err)
	a := &anyenc.Arena{}
	require.NoError(b, ctrl.ApplyChange(ctx, stampChange(stampShared, "v0", 1,
		RecordChange{Id: stampObjectId, Upsert: true, Ops: []Op{setOp(a, "name", "n")}})))
	require.NoError(b, ctrl.ApplyChange(ctx, stampChange(stampNotes, "v1", 2,
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "text", "seed")}})))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ver := VersionId("w" + padVersion(i))
		ch := stampChange(stampNotes, ver, int64(i+3),
			RecordChange{Id: "r1", Ops: []Op{setOp(a, "text", "t")}})
		if err := ctrl.ApplyChange(ctx, ch); err != nil {
			b.Fatal(err)
		}
	}
}

// padVersion renders i as a fixed-width decimal so VersionIds compare
// in apply order.
func padVersion(i int) string {
	const width = 10
	s := make([]byte, width)
	for j := width - 1; j >= 0; j-- {
		s[j] = byte('0' + i%10)
		i /= 10
	}
	return string(s)
}

// Only a SHARED dataset's handler can stamp: on a controller without
// shared collections the same handler never runs as a stamper.
func TestObjectStamp_NonSharedNeverRegisters(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	h := &stampHandler{}
	ctrl, err := NewController(ctx, stampObjectId, db,
		HandlerReg{Name: stampShared, Handler: h, Schema: dynSchema},
		HandlerReg{Name: stampNotes, Handler: DefaultHandler{}, Schema: dynSchema},
	)
	require.NoError(t, err)
	a := &anyenc.Arena{}
	require.NoError(t, ctrl.ApplyChange(ctx, stampChange(stampShared, "v1", 100,
		RecordChange{Id: stampObjectId, Upsert: true, Ops: []Op{setOp(a, "name", "n")}})))
	require.NoError(t, ctrl.ApplyChange(ctx, stampChange(stampNotes, "v2", 200,
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "text", "hello")}})))
	assert.Equal(t, 0, h.calls)
	assert.Nil(t, ctrl.Get(ctx, stampShared, stampObjectId).Get("modifiedAt"))
}

// The absent-row memo: once a stamp finds no row the stamper is skipped
// until a change on the shared dataset itself (the row's creation)
// applies; after that stamps land again.
func TestObjectStamp_AbsentMemoClearedByRowCreation(t *testing.T) {
	ctrl, h := newStampFixture(t, notesSchema)
	a := &anyenc.Arena{}
	require.NoError(t, ctrl.ApplyChange(ctx, stampChange(stampNotes, "v1", 100,
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "text", "a")}})))
	require.NoError(t, ctrl.ApplyChange(ctx, stampChange(stampNotes, "v2", 200,
		RecordChange{Id: "r2", Upsert: true, Ops: []Op{setOp(a, "text", "b")}})))
	assert.Equal(t, 1, h.calls, "second write skips the stamper: the row is known absent")

	createObjectsRow(t, ctrl, "v3", 300)
	res, err := ctrl.ApplyChangeWithResult(ctx, stampChange(stampNotes, "v4", 400,
		RecordChange{Id: "r3", Upsert: true, Ops: []Op{setOp(a, "text", "c")}}))
	require.NoError(t, err)
	assert.Equal(t, 2, h.calls)
	assert.Len(t, res.ObjectStamps, 1)
	assert.EqualValues(t, 400, modifiedAtOf(t, ctrl))
}

// A re-index wipes the materialized rows and replays the tree in order;
// the stamp state must come out identical.
func TestObjectStamp_ReplayFromScratchReproduces(t *testing.T) {
	ctrl, _ := newStampFixture(t, notesSchema)
	a := &anyenc.Arena{}
	changes := []Change{
		stampChange(stampShared, "v1", 100, RecordChange{Id: stampObjectId, Upsert: true, Ops: []Op{setOp(a, "name", "n")}}),
		stampChange(stampNotes, "v2", 200, RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "text", "a")}}),
		stampChange(stampNotes, "v3", 300, RecordChange{Id: "r2", Upsert: true, Ops: []Op{setOp(a, "text", "b")}}),
		stampChange(stampShared, "v4", 400, RecordChange{Id: stampObjectId, Ops: []Op{setOp(a, "name", "m")}}),
		stampChange(stampNotes, "v5", 500, RecordChange{Id: "r1", Ops: []Op{{Type: OpDelete}}}),
	}
	for _, ch := range changes {
		require.NoError(t, ctrl.ApplyChange(ctx, ch))
	}
	first := ctrl.Get(ctx, stampShared, stampObjectId)
	require.EqualValues(t, 500, modifiedAtOf(t, ctrl))

	require.NoError(t, ctrl.Collection(ctx, stampShared).DeleteId(ctx, stampObjectId))
	for _, id := range []string{"r1", "r2"} {
		require.NoError(t, ctrl.Collection(ctx, stampNotes).DeleteId(ctx, id))
	}
	for _, ch := range changes {
		require.NoError(t, ctrl.ApplyChange(ctx, ch))
	}
	second := ctrl.Get(ctx, stampShared, stampObjectId)
	assert.Equal(t, first.String(), second.String(), "replayed row is byte-identical")
	assert.EqualValues(t, 500, modifiedAtOf(t, ctrl))
	assert.Equal(t, VersionId("v5"), GetRecordVersion(second, "modifiedAt"))
}
