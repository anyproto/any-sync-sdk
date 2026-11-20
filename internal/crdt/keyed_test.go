package crdt

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Keyed datasets: the records of every object live in one supplied
// collection, each under `<objectId>/<recordId>`.

const keyedSamples = "samples"

// idRecorder is a handler that records the record ids its hooks see.
type idRecorder struct {
	DefaultHandler
	created []string
}

func (h *idRecorder) BeforeCreate(_ *ChangeCtx, rec *RecordChange, _ *Sink) error {
	h.created = append(h.created, rec.Id)
	return nil
}

type keyedFixture struct {
	db   anystore.DB
	coll anystore.Collection
}

func newKeyedFixture(t *testing.T) *keyedFixture {
	t.Helper()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	coll, err := db.Collection(ctx, "space_"+keyedSamples)
	require.NoError(t, err)
	return &keyedFixture{db: db, coll: coll}
}

func (f *keyedFixture) controller(t *testing.T, objectId string, h Handler) *Controller {
	t.Helper()
	ctrl, err := NewControllerWithShared(ctx, objectId, f.db, SharedCollections{keyedSamples: f.coll},
		HandlerReg{Name: keyedSamples, Handler: h, Schema: dynSchema, Keyed: true},
		HandlerReg{Name: stampNotes, Handler: DefaultHandler{}, Schema: dynSchema},
	)
	require.NoError(t, err)
	return ctrl
}

func keyedChange(objectId string, ver VersionId, recs ...RecordChange) Change {
	return Change{
		ObjectId:    objectId,
		Dataset:     keyedSamples,
		ChangeId:    "ch-" + objectId + "-" + string(ver),
		VersionId:   ver,
		Timestamp:   1,
		DataVersion: keyedSamples + "-v1",
		Records:     recs,
	}
}

func (f *keyedFixture) ids(t *testing.T) []string {
	t.Helper()
	iter, err := f.coll.Find(nil).Sort(IdField).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var out []string
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		out = append(out, doc.Value().GetString(IdField))
	}
	return out
}

// Two objects write the same record id: each gets its own row, stamped
// with its object, and reads its own.
func TestKeyed_RowsPerObject(t *testing.T) {
	f := newKeyedFixture(t)
	a := &anyenc.Arena{}
	h1 := &idRecorder{}
	c1 := f.controller(t, "obj1", h1)
	c2 := f.controller(t, "obj2", &idRecorder{})

	require.NoError(t, c1.ApplyChange(ctx, keyedChange("obj1", "v1",
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "v", "one")}})))
	require.NoError(t, c2.ApplyChange(ctx, keyedChange("obj2", "v1",
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "v", "two")}})))

	assert.Equal(t, []string{"obj1/r1", "obj2/r1"}, f.ids(t))
	assert.Equal(t, []string{"r1"}, h1.created, "a handler sees the record id the change carries")

	row := c1.Get(ctx, keyedSamples, "r1")
	require.NotNil(t, row)
	assert.Equal(t, "obj1/r1", row.GetString(IdField))
	assert.Equal(t, "obj1", row.GetString(ObjectIdField))
	assert.Equal(t, "one", row.GetString("v"))
	assert.Equal(t, "two", c2.Get(ctx, keyedSamples, "r1").GetString("v"))

	// Get takes the id a change carries: the stored id names another
	// record, which does not exist.
	assert.Nil(t, c1.Get(ctx, keyedSamples, "obj1/r1"))
	assert.Nil(t, c1.Get(ctx, keyedSamples, "r2"))
	assert.Equal(t, "obj1/r1", c1.StoreId(keyedSamples, "r1"))
	assert.Equal(t, "obj1/obj1/r1", c1.StoreId(keyedSamples, "obj1/r1"), "the prefix is added whatever the id holds")
	ids := []string{"r1", "obj1/r1"}
	c1.StoreIds(keyedSamples, ids)
	assert.Equal(t, []string{"obj1/r1", "obj1/obj1/r1"}, ids)
	assert.Equal(t, "r1", c1.StoreId(stampNotes, "r1"), "a per-object dataset keeps the record id")
	// A caller may name a record by either form.
	assert.Equal(t, "obj1/r1", KeyedId("obj1", "r1"))
	assert.Equal(t, "obj1/r1", KeyedId("obj1", "obj1/r1"))

	// A strict update lands on the writer's row only.
	require.NoError(t, c1.ApplyChange(ctx, keyedChange("obj1", "v2",
		RecordChange{Id: "r1", Ops: []Op{setOp(a, "v", "uno")}})))
	assert.Equal(t, "uno", c1.Get(ctx, keyedSamples, "r1").GetString("v"))
	assert.Equal(t, "two", c2.Get(ctx, keyedSamples, "r1").GetString("v"))

	// Records lists the object's own rows.
	recs := c1.Records(ctx, keyedSamples)
	require.Len(t, recs, 1)
	assert.Equal(t, "obj1/r1", recs[0].GetString(IdField))

	assert.True(t, c1.IsKeyed(keyedSamples))
	assert.False(t, c1.IsShared(keyedSamples), "keyed is not the one-row-per-object mode")
	assert.False(t, c1.IsKeyed(stampNotes))
}

// The record id a change carries is stored under the object's prefix
// whatever it holds: `x` and `obj1/x` on obj1 are two records, each
// with its own fields and tombstone.
func TestKeyed_PrefixedRecordIdIsAnotherRecord(t *testing.T) {
	f := newKeyedFixture(t)
	a := &anyenc.Arena{}
	c := f.controller(t, "obj1", DefaultHandler{})
	res, err := c.ApplyChangeWithResult(ctx, keyedChange("obj1", "v1",
		RecordChange{Id: "x", Upsert: true, Ops: []Op{setOp(a, "v", "plain")}},
		RecordChange{Id: "obj1/x", Upsert: true, Ops: []Op{setOp(a, "v", "prefixed")}}))
	require.NoError(t, err)
	assert.Empty(t, res.Rejections)
	assert.Equal(t, []string{"obj1/obj1/x", "obj1/x"}, f.ids(t))
	assert.Equal(t, "plain", c.Get(ctx, keyedSamples, "x").GetString("v"))
	assert.Equal(t, "prefixed", c.Get(ctx, keyedSamples, "obj1/x").GetString("v"))
	assert.Equal(t, "obj1", c.Get(ctx, keyedSamples, "obj1/x").GetString(ObjectIdField))

	// An update of one leaves the other.
	require.NoError(t, c.ApplyChange(ctx, keyedChange("obj1", "v2",
		RecordChange{Id: "obj1/x", Ops: []Op{setOp(a, "w", "only-prefixed")}})))
	assert.Nil(t, c.Get(ctx, keyedSamples, "x").Get("w"))
	assert.Equal(t, "only-prefixed", c.Get(ctx, keyedSamples, "obj1/x").GetString("w"))

	// So does a delete.
	require.NoError(t, c.ApplyChange(ctx, keyedChange("obj1", "v3",
		RecordChange{Id: "x", Ops: []Op{{Type: OpDelete}}})))
	assert.NotNil(t, c.Get(ctx, keyedSamples, "x").Get(DeletedAtField))
	live := c.Get(ctx, keyedSamples, "obj1/x")
	assert.Nil(t, live.Get(DeletedAtField))
	assert.Equal(t, "prefixed", live.GetString("v"))
	recs := c.Records(ctx, keyedSamples)
	require.Len(t, recs, 1)
	assert.Equal(t, "obj1/obj1/x", recs[0].GetString(IdField))

	require.NoError(t, c.ApplyChange(ctx, keyedChange("obj1", "v4",
		RecordChange{Id: "obj1/x", Ops: []Op{{Type: OpDelete}}})))
	assert.Empty(t, c.Records(ctx, keyedSamples))
	assert.Equal(t, []string{"obj1/obj1/x", "obj1/x"}, f.ids(t), "each tombstone keeps its own row")
}

// derivedOp finds the op at path among a record's derived ops.
func derivedOp(ops []Op, path ...string) *Op {
	for i := range ops {
		if slices.Equal(ops[i].Path, path) {
			return &ops[i]
		}
	}
	return nil
}

// Creating a record derives its object stamp next to the creation
// version, so an event rebuilds the row whole; an update derives
// neither, and a per-object dataset derives no stamp.
func TestKeyed_CreateDerivesTheObjectStamp(t *testing.T) {
	f := newKeyedFixture(t)
	a := &anyenc.Arena{}
	c := f.controller(t, "obj1", DefaultHandler{})

	res, err := c.ApplyChangeWithResult(ctx, keyedChange("obj1", "v1",
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "v", "x")}}))
	require.NoError(t, err)
	require.Len(t, res.DerivedOps, 1)
	stamp := derivedOp(res.DerivedOps[0], ObjectIdField)
	require.NotNil(t, stamp, "the create derives _objectId")
	assert.Equal(t, OpSet, stamp.Type)
	assert.Equal(t, "obj1", string(stamp.Payload.GetStringBytes()))
	ver := derivedOp(res.DerivedOps[0], VersionsKey, IdField)
	require.NotNil(t, ver)
	assert.Equal(t, "v1", string(ver.Payload.GetStringBytes()))

	res, err = c.ApplyChangeWithResult(ctx, keyedChange("obj1", "v2",
		RecordChange{Id: "r1", Ops: []Op{setOp(a, "v", "y")}}))
	require.NoError(t, err)
	for _, ops := range res.DerivedOps {
		assert.Nil(t, derivedOp(ops, ObjectIdField), "an update derives no stamp")
	}

	res, err = c.ApplyChangeWithResult(ctx, Change{
		ObjectId: "obj1", Dataset: stampNotes, ChangeId: "ch-notes", VersionId: "v3",
		Timestamp: 1, DataVersion: "dv",
		Records: []RecordChange{{Id: "n1", Upsert: true, Ops: []Op{setOp(a, "v", "x")}}},
	})
	require.NoError(t, err)
	require.Len(t, res.DerivedOps, 1)
	assert.Nil(t, derivedOp(res.DerivedOps[0], ObjectIdField), "a per-object record carries no stamp")
	assert.NotNil(t, derivedOp(res.DerivedOps[0], VersionsKey, IdField))
}

// A controller whose collections were released opens a collection by
// name for each call; it leaves the declared indexes under its prune
// prefix as its successor set them, neither creating nor dropping one.
func TestController_ReleasedLeavesDeclaredIndexes(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	a := &anyenc.Arena{}
	open := func(indexes ...anystore.IndexInfo) *Controller {
		c, err := NewController(ctx, "obj1", db, HandlerReg{
			Name: stampNotes, Handler: DefaultHandler{}, Schema: dynSchema,
			Indexes: indexes, PruneIndexPrefix: "dx_",
		})
		require.NoError(t, err)
		return c
	}
	write := func(c *Controller, ver VersionId) {
		require.NoError(t, c.ApplyChange(ctx, Change{
			ObjectId: "obj1", Dataset: stampNotes, ChangeId: "ch-" + string(ver), VersionId: ver,
			Timestamp: 1, DataVersion: "dv",
			Records: []RecordChange{{Id: "r-" + string(ver), Upsert: true, Ops: []Op{setOp(a, "v", "x")}}},
		}))
	}
	collNames := func() []string {
		coll, err := db.OpenCollection(ctx, "obj1_"+stampNotes)
		require.NoError(t, err)
		return collIndexNames(coll)
	}

	stale := open(anystore.IndexInfo{Name: "dx_old", Fields: []string{"old"}})
	write(stale, "v1")
	assert.Equal(t, []string{"dx_old", "idx__addSeq"}, collNames())
	require.NoError(t, stale.CloseOwnedCollections())

	// The successor registers another set and reconciles the collection.
	next := open(anystore.IndexInfo{Name: "dx_new", Fields: []string{"new"}})
	write(next, "v2")
	assert.Equal(t, []string{"dx_new", "idx__addSeq"}, collNames())

	// The released one reads and writes without touching that set.
	require.NotNil(t, stale.Get(ctx, stampNotes, "r-v1"), "a read reopens the collection")
	assert.Equal(t, []string{"dx_new", "idx__addSeq"}, collNames())
	write(stale, "v3")
	assert.Equal(t, []string{"dx_new", "idx__addSeq"}, collNames())
	assert.NotNil(t, next.Get(ctx, stampNotes, "r-v3"))
}

// An empty record id resolves from the change id, then takes the
// object's prefix; the apply hook and rejections carry the stored id.
func TestKeyed_ResolvedAndReportedIds(t *testing.T) {
	f := newKeyedFixture(t)
	a := &anyenc.Arena{}
	c := f.controller(t, "obj1", DefaultHandler{})
	var hooked []string
	c.SetApplyHook(func(_ context.Context, _ *Change, recordIds []string, _ *ApplyResult) error {
		hooked = append([]string(nil), recordIds...)
		return nil
	})

	ch := keyedChange("obj1", "v1",
		RecordChange{Upsert: true, Ops: []Op{setOp(a, "v", "a")}},
		RecordChange{Upsert: true, Ops: []Op{setOp(a, "v", "b")}})
	require.NoError(t, c.ApplyChange(ctx, ch))
	derived := DeriveRecordId(ch.ChangeId)
	want := []string{"obj1/" + derived, "obj1/" + derived + ":1"}
	assert.Equal(t, want, hooked)
	assert.Equal(t, want, f.ids(t))

	// A strict update of an absent record reports the stored id.
	res, err := c.ApplyChangeWithResult(ctx, keyedChange("obj1", "v2",
		RecordChange{Id: "missing", Ops: []Op{setOp(a, "v", "x")}}))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.Equal(t, "obj1/missing", res.Rejections[0].RecordId)
	assert.ErrorIs(t, res.Rejections[0].Err, ErrStrictSkipAbsent)
}

// A tombstone keeps the row's id and its object, whether the record
// existed or not, and stays the writer's own.
func TestKeyed_Tombstones(t *testing.T) {
	f := newKeyedFixture(t)
	a := &anyenc.Arena{}
	c1 := f.controller(t, "obj1", DefaultHandler{})
	c2 := f.controller(t, "obj2", DefaultHandler{})

	for _, c := range []*Controller{c1, c2} {
		require.NoError(t, c.ApplyChange(ctx, keyedChange(c.objectId, "v1",
			RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "v", "x")}})))
	}
	require.NoError(t, c1.ApplyChange(ctx, keyedChange("obj1", "v2",
		RecordChange{Id: "r1", Ops: []Op{{Type: OpDelete}}})))
	require.NoError(t, c1.ApplyChange(ctx, keyedChange("obj1", "v3",
		RecordChange{Id: "never", Ops: []Op{{Type: OpDelete}}})))

	for _, id := range []string{"r1", "never"} {
		tomb := c1.Get(ctx, keyedSamples, id)
		require.NotNil(t, tomb, id)
		assert.NotNil(t, tomb.Get(DeletedAtField), id)
		assert.Equal(t, "obj1/"+id, tomb.GetString(IdField))
		assert.Equal(t, "obj1", tomb.GetString(ObjectIdField))
		assert.Nil(t, tomb.Get("v"))
	}
	assert.Empty(t, c1.Records(ctx, keyedSamples), "tombstones are not live records")
	assert.Equal(t, "x", c2.Get(ctx, keyedSamples, "r1").GetString("v"), "another object's record of the same id is untouched")
}

// The object stamp is the Controller's: an input op cannot write it.
func TestKeyed_ObjectStampIsReserved(t *testing.T) {
	f := newKeyedFixture(t)
	a := &anyenc.Arena{}
	c := f.controller(t, "obj1", DefaultHandler{})
	res, err := c.ApplyChangeWithResult(ctx, keyedChange("obj1", "v1",
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{
			setOp(a, "v", "x"),
			setOp(a, ObjectIdField, "obj2"),
		}}))
	require.NoError(t, err)
	require.NotEmpty(t, res.Rejections)
	row := c.Get(ctx, keyedSamples, "r1")
	require.NotNil(t, row)
	assert.Equal(t, "obj1", row.GetString(ObjectIdField))
}

// A keyed registration needs its collection supplied, and eviction
// leaves that collection open for the space's other controllers.
func TestKeyed_CollectionIsSupplied(t *testing.T) {
	f := newKeyedFixture(t)
	_, err := NewController(ctx, "obj1", f.db,
		HandlerReg{Name: keyedSamples, Handler: DefaultHandler{}, Schema: dynSchema, Keyed: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `keyed dataset "samples" registered without its collection`)

	a := &anyenc.Arena{}
	c1 := f.controller(t, "obj1", DefaultHandler{})
	require.NoError(t, c1.ApplyChange(ctx, keyedChange("obj1", "v1",
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "v", "x")}})))
	require.NoError(t, c1.CloseOwnedCollections())

	c2 := f.controller(t, "obj2", DefaultHandler{})
	require.NoError(t, c2.ApplyChange(ctx, keyedChange("obj2", "v1",
		RecordChange{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "v", "y")}})))
	assert.Equal(t, []string{"obj1/r1", "obj2/r1"}, f.ids(t))
}

func TestKeyedBounds(t *testing.T) {
	lo, hi := KeyedBounds("obj1")
	assert.Equal(t, "obj1/", lo)
	assert.Equal(t, "obj10", hi)
	for _, id := range []string{"obj1/", "obj1/a", "obj1/zzz", "obj1/a/b"} {
		assert.True(t, id >= lo && id < hi, id)
	}
	for _, id := range []string{"obj1", "obj10/a", "obj2/a", "obj/a"} {
		assert.False(t, id >= lo && id < hi, id)
	}
	assert.Equal(t, "obj1/a/b", KeyedId("obj1", "a/b"))
}
