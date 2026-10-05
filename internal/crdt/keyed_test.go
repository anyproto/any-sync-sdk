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

	// Either form of the id reads the row; another object's does not.
	assert.NotNil(t, c1.Get(ctx, keyedSamples, "obj1/r1"))
	assert.Nil(t, c1.Get(ctx, keyedSamples, "r2"))
	assert.Equal(t, "obj1/r1", c1.StoreId(keyedSamples, "r1"))
	assert.Equal(t, "obj1/r1", c1.StoreId(keyedSamples, "obj1/r1"))
	assert.Equal(t, "r1", c1.StoreId(stampNotes, "r1"), "a per-object dataset keeps the record id")

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
