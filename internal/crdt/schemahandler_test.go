package crdt

import (
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/schema"
)

const (
	shTestDS  = "docs"
	shTestDV  = "docs-v1"
	shAuthorA = "authorA"
	shAuthorB = "authorB"
)

func shDecl() schema.Dataset {
	return schema.Dataset{
		Fields: []schema.Field{
			{Id: "title", Schema: schema.Leaf(schema.KindString), Required: true},
			{Id: "body", Schema: schema.Leaf(schema.KindString), MutableBy: schema.MutableByAuthor},
			{Id: "note", Schema: schema.Leaf(schema.KindString), MutableBy: schema.MutableByAnyone},
			{Id: "tags", Schema: &schema.Schema{Kind: schema.KindArray, Items: schema.Leaf(schema.KindString)}, MutableBy: schema.MutableByAnyone},
			{Id: "count", Schema: schema.Leaf(schema.KindNumber), MutableBy: schema.MutableByAnyone},
			{Id: "creator", Stamp: schema.StampCreator, Scope: schema.ScopeDerived},
			{Id: "createdAt", Stamp: schema.StampCreateTime, Scope: schema.ScopeDerived},
			{Id: "modifiedAt", Stamp: schema.StampModifyTime, Scope: schema.ScopeDerived},
		},
		DeleteBy: schema.DeleteByAuthor,
		IdRule:   schema.IdUser,
	}
}

func newSchemaHandlerController(t *testing.T, ds schema.Dataset) *Controller {
	t.Helper()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "sh.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	h, err := NewSchemaHandler(ds)
	require.NoError(t, err)
	st, err := NewController(ctx, "obj1", db, HandlerReg{Name: shTestDS, Handler: h, Schema: ds})
	require.NoError(t, err)
	return st
}

// shChange builds a docs-dataset change with creator + timestamp.
func shChange(version, creator string, ts int64, recs ...RecordChange) Change {
	return Change{
		ObjectId:    "obj1",
		Dataset:     shTestDS,
		DataVersion: shTestDV,
		ChangeId:    "cid-" + version,
		VersionId:   VersionId(version),
		Creator:     creator,
		Timestamp:   ts,
		Records:     recs,
	}
}

func shCreatePayload(a *anyenc.Arena, fields map[string]string) *anyenc.Value {
	obj := a.NewObject()
	for k, v := range fields {
		obj.Set(k, a.NewString(v))
	}
	return obj
}

func shCreateRecord(a *anyenc.Arena, id string, fields map[string]string) RecordChange {
	return RecordChange{Id: id, Upsert: true, Ops: []Op{{Type: OpSet, Payload: shCreatePayload(a, fields)}}}
}

func shSeed(t *testing.T, st *Controller) {
	t.Helper()
	a := &anyenc.Arena{}
	res, err := st.ApplyChangeWithResult(ctx, shChange("v1", shAuthorA, 100,
		shCreateRecord(a, "row-1", map[string]string{"title": "t", "body": "b", "note": "n"})))
	require.NoError(t, err)
	require.Empty(t, res.Rejections)
}

func TestSchemaHandler_CreateStampsAndFields(t *testing.T) {
	st := newSchemaHandlerController(t, shDecl())
	shSeed(t, st)

	rec := st.Get(ctx, shTestDS, "row-1")
	require.NotNil(t, rec)
	assert.Equal(t, "t", string(rec.GetStringBytes("title")))
	assert.Equal(t, shAuthorA, string(rec.GetStringBytes("creator")))
	assert.Equal(t, float64(100), rec.GetFloat64("createdAt"))
	assert.Equal(t, float64(100), rec.GetFloat64("modifiedAt"))
}

func TestSchemaHandler_CreateMissingRequiredDropsRecord(t *testing.T) {
	st := newSchemaHandlerController(t, shDecl())
	a := &anyenc.Arena{}
	res, err := st.ApplyChangeWithResult(ctx, shChange("v1", shAuthorA, 100,
		shCreateRecord(a, "row-1", map[string]string{"body": "b"})))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.Equal(t, -1, res.Rejections[0].OpIndex)
	assert.ErrorIs(t, res.Rejections[0].Err, ErrValidation)
	assert.Nil(t, st.Get(ctx, shTestDS, "row-1"))
}

func TestSchemaHandler_IdUserRules(t *testing.T) {
	st := newSchemaHandlerController(t, shDecl())
	a := &anyenc.Arena{}

	// Bad characters rejected.
	res, err := st.ApplyChangeWithResult(ctx, shChange("v1", shAuthorA, 100,
		shCreateRecord(a, "has space", map[string]string{"title": "t"})))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.ErrorIs(t, res.Rejections[0].Err, ErrValidation)

	// Empty (auto-derived) id rejected on an IdUser dataset.
	res, err = st.ApplyChangeWithResult(ctx, shChange("v2", shAuthorA, 100,
		shCreateRecord(a, "", map[string]string{"title": "t"})))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.ErrorIs(t, res.Rejections[0].Err, ErrValidation)
}

func TestSchemaHandler_IdAutoRejectsExplicitId(t *testing.T) {
	ds := schema.Dataset{Fields: []schema.Field{
		{Id: "title", Schema: schema.Leaf(schema.KindString)},
	}}
	st := newSchemaHandlerController(t, ds)
	a := &anyenc.Arena{}

	res, err := st.ApplyChangeWithResult(ctx, shChange("v1", shAuthorA, 100,
		shCreateRecord(a, "explicit", map[string]string{"title": "t"})))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.ErrorIs(t, res.Rejections[0].Err, ErrValidation)

	// Empty id + upsert derives the id and creates.
	res, err = st.ApplyChangeWithResult(ctx, shChange("v2", shAuthorA, 100,
		shCreateRecord(a, "", map[string]string{"title": "t"})))
	require.NoError(t, err)
	require.Empty(t, res.Rejections)
}

func TestSchemaHandler_WriteOnceRejectsAnyModify(t *testing.T) {
	st := newSchemaHandlerController(t, shDecl())
	shSeed(t, st)
	a := &anyenc.Arena{}

	// Even the original author cannot rewrite a write-once field.
	res, err := st.ApplyChangeWithResult(ctx, shChange("v2", shAuthorA, 200, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpSet, Path: []string{"title"}, Payload: a.NewString("changed")}},
	}))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.ErrorIs(t, res.Rejections[0].Err, ErrValidation)
	assert.Equal(t, "t", string(st.Get(ctx, shTestDS, "row-1").GetStringBytes("title")))

	// $unset of a write-once field is a mutation too.
	res, err = st.ApplyChangeWithResult(ctx, shChange("v3", shAuthorA, 200, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpUnset, Path: []string{"title"}}},
	}))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
}

func TestSchemaHandler_AuthorMutable(t *testing.T) {
	st := newSchemaHandlerController(t, shDecl())
	shSeed(t, st)
	a := &anyenc.Arena{}

	// Non-author rejected.
	res, err := st.ApplyChangeWithResult(ctx, shChange("v2", shAuthorB, 200, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpSet, Path: []string{"body"}, Payload: a.NewString("hax")}},
	}))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.ErrorIs(t, res.Rejections[0].Err, ErrValidation)
	assert.Equal(t, "b", string(st.Get(ctx, shTestDS, "row-1").GetStringBytes("body")))

	// Author accepted; modifiedAt bumps to the change timestamp.
	res, err = st.ApplyChangeWithResult(ctx, shChange("v3", shAuthorA, 300, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpSet, Path: []string{"body"}, Payload: a.NewString("edited")}},
	}))
	require.NoError(t, err)
	require.Empty(t, res.Rejections)
	rec := st.Get(ctx, shTestDS, "row-1")
	assert.Equal(t, "edited", string(rec.GetStringBytes("body")))
	assert.Equal(t, float64(300), rec.GetFloat64("modifiedAt"))

	// A change with no creator cannot pass an author gate.
	res, err = st.ApplyChangeWithResult(ctx, shChange("v4", "", 400, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpSet, Path: []string{"body"}, Payload: a.NewString("anon")}},
	}))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
}

func TestSchemaHandler_AnyoneMutable(t *testing.T) {
	st := newSchemaHandlerController(t, shDecl())
	shSeed(t, st)
	a := &anyenc.Arena{}
	res, err := st.ApplyChangeWithResult(ctx, shChange("v2", shAuthorB, 200, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpSet, Path: []string{"note"}, Payload: a.NewString("other")}},
	}))
	require.NoError(t, err)
	require.Empty(t, res.Rejections)
	assert.Equal(t, "other", string(st.Get(ctx, shTestDS, "row-1").GetStringBytes("note")))
}

func TestSchemaHandler_ShapeEnforcement(t *testing.T) {
	st := newSchemaHandlerController(t, shDecl())
	shSeed(t, st)
	a := &anyenc.Arena{}

	// Kind mismatch on $set.
	res, err := st.ApplyChangeWithResult(ctx, shChange("v2", shAuthorB, 200, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpSet, Path: []string{"note"}, Payload: a.NewNumberInt(1)}},
	}))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)

	// $addToSet element type checked against Items.
	res, err = st.ApplyChangeWithResult(ctx, shChange("v3", shAuthorB, 200, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpAddToSet, Path: []string{"tags"}, Payload: a.NewNumberInt(1)}},
	}))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)

	res, err = st.ApplyChangeWithResult(ctx, shChange("v4", shAuthorB, 200, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpAddToSet, Path: []string{"tags"}, Payload: a.NewString("go")}},
	}))
	require.NoError(t, err)
	require.Empty(t, res.Rejections)

	// $inc allowed on number, rejected on string.
	res, err = st.ApplyChangeWithResult(ctx, shChange("v5", shAuthorB, 200, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpInc, Path: []string{"count"}, Payload: a.NewNumberInt(2)}},
	}))
	require.NoError(t, err)
	require.Empty(t, res.Rejections)

	res, err = st.ApplyChangeWithResult(ctx, shChange("v6", shAuthorB, 200, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpInc, Path: []string{"note"}, Payload: a.NewNumberInt(2)}},
	}))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
}

func TestSchemaHandler_MultiFieldSalvage(t *testing.T) {
	st := newSchemaHandlerController(t, shDecl())
	shSeed(t, st)
	a := &anyenc.Arena{}

	// Combined $set touching a write-once field sheds only that key.
	payload := a.NewObject()
	payload.Set("title", a.NewString("changed"))
	payload.Set("note", a.NewString("kept"))
	res, err := st.ApplyChangeWithResult(ctx, shChange("v2", shAuthorB, 200, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpSet, Payload: payload}},
	}))
	require.NoError(t, err)
	require.NotEmpty(t, res.Rejections)
	rec := st.Get(ctx, shTestDS, "row-1")
	assert.Equal(t, "t", string(rec.GetStringBytes("title")))
	assert.Equal(t, "kept", string(rec.GetStringBytes("note")))
	assert.Equal(t, float64(200), rec.GetFloat64("modifiedAt"))
}

func TestSchemaHandler_DeleteByAuthor(t *testing.T) {
	st := newSchemaHandlerController(t, shDecl())
	shSeed(t, st)

	// Non-author delete rejected; record stays live.
	res, err := st.ApplyChangeWithResult(ctx, shChange("v2", shAuthorB, 200, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpDelete}},
	}))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.ErrorIs(t, res.Rejections[0].Err, ErrValidation)
	rec := st.Get(ctx, shTestDS, "row-1")
	require.NotNil(t, rec)
	assert.Nil(t, rec.Get(DeletedAtField))

	// A delete racing ahead of its record's create (never-created id) is
	// rejected — only a non-author's delete can arrive in that order.
	res, err = st.ApplyChangeWithResult(ctx, shChange("v3", shAuthorB, 200, RecordChange{
		Id: "row-9", Ops: []Op{{Type: OpDelete}},
	}))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)

	// Author delete lands a tombstone.
	res, err = st.ApplyChangeWithResult(ctx, shChange("v4", shAuthorA, 300, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpDelete}},
	}))
	require.NoError(t, err)
	require.Empty(t, res.Rejections)
	rec = st.Get(ctx, shTestDS, "row-1")
	require.NotNil(t, rec)
	assert.NotNil(t, rec.Get(DeletedAtField))
}

func TestSchemaHandler_PreValidateMultiStrict(t *testing.T) {
	h, err := NewSchemaHandler(shDecl())
	require.NoError(t, err)

	a := &anyenc.Arena{}
	// Create missing required field: whole write rejected with a
	// readable error.
	ch := shChange("v1", shAuthorA, 100, shCreateRecord(a, "row-1", map[string]string{"body": "b"}))
	err = h.PreValidateMulti(&ch, func(string) *anyenc.Value { return nil })
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrValidation)

	// Write-once modify on an existing record rejected.
	existing := a.NewObject()
	existing.Set("title", a.NewString("t"))
	mod := shChange("v2", shAuthorA, 200, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpSet, Path: []string{"title"}, Payload: a.NewString("x")}},
	})
	err = h.PreValidateMulti(&mod, func(id string) *anyenc.Value { return existing })
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrValidation)

	// Valid create passes.
	ok := shChange("v3", shAuthorA, 100, shCreateRecord(a, "row-2", map[string]string{"title": "t"}))
	require.NoError(t, h.PreValidateMulti(&ok, func(string) *anyenc.Value { return nil }))
}

// Dotted keys must not satisfy required or slip past shape checks:
// {title.x: 1} doesn't put a valid string at `title`, and writing
// UNDER a declared scalar is invalid structure, not "unconstrained".
func TestSchemaHandler_DottedKeysNoBypass(t *testing.T) {
	st := newSchemaHandlerController(t, shDecl())
	a := &anyenc.Arena{}

	// Create whose only "title" write is a deep path: required fails,
	// whole record dropped.
	payload := a.NewObject()
	payload.Set("title.x", a.NewNumberInt(1))
	res, err := st.ApplyChangeWithResult(ctx, shChange("v1", shAuthorA, 100, RecordChange{
		Id: "row-1", Upsert: true, Ops: []Op{{Type: OpSet, Payload: payload}},
	}))
	require.NoError(t, err)
	require.NotEmpty(t, res.Rejections)
	assert.Nil(t, st.Get(ctx, shTestDS, "row-1"))

	// Existing record: deep write under a declared scalar rejected on
	// both the dotted-multi-field and the split-path forms.
	shSeed(t, st)
	res, err = st.ApplyChangeWithResult(ctx, shChange("v2", shAuthorB, 200, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpSet, Path: []string{"note", "x"}, Payload: a.NewNumberInt(1)}},
	}))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.ErrorIs(t, res.Rejections[0].Err, ErrValidation)

	deep := a.NewObject()
	deep.Set("note.x", a.NewNumberInt(1))
	res, err = st.ApplyChangeWithResult(ctx, shChange("v3", shAuthorB, 300, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpSet, Payload: deep}},
	}))
	require.NoError(t, err)
	require.NotEmpty(t, res.Rejections)
	rec := st.Get(ctx, shTestDS, "row-1")
	assert.Nil(t, rec.Get("note", "x"))
}

// Convergence: every SchemaHandler verdict must be arrival-order
// independent. The adversarial delete-races-create case (a non-author
// delete of a user-chosen id arriving before OR after the create) must
// end in the same state on both orders: the record lives, the delete is
// rejected — before the create because the record has no creation
// marker, after it because the author doesn't match.
func TestSchemaHandler_ConvergenceDeleteRacesCreate(t *testing.T) {
	a := &anyenc.Arena{}
	create := shChange("v1", shAuthorA, 100,
		shCreateRecord(a, "row-1", map[string]string{"title": "t"}))
	del := shChange("v2", shAuthorB, 200, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpDelete}},
	})

	apply := func(t *testing.T, order []Change) *anyenc.Value {
		st := newSchemaHandlerController(t, shDecl())
		for i := range order {
			_, err := st.ApplyChangeWithResult(ctx, order[i])
			require.NoError(t, err)
		}
		return st.Get(ctx, shTestDS, "row-1")
	}

	recAB := apply(t, []Change{create, del})
	recBA := apply(t, []Change{del, create})

	require.NotNil(t, recAB)
	require.NotNil(t, recBA)
	assert.Nil(t, recAB.Get(DeletedAtField), "create→delete: non-author delete rejected")
	assert.Nil(t, recBA.Get(DeletedAtField), "delete→create: seed rejected, create lands")
	assert.Equal(t, "t", string(recAB.GetStringBytes("title")))
	assert.Equal(t, "t", string(recBA.GetStringBytes("title")))
}

// Author-gated modify verdicts converge too: the creator stamp is
// min-convergent (every peer settles on the causally-earliest upsert's
// signer), so once delivery quiesces the verdict never depends on which
// of two independent edits applied first.
func TestSchemaHandler_ConvergenceAuthorEditOrders(t *testing.T) {
	a := &anyenc.Arena{}
	create := shChange("v1", shAuthorA, 100,
		shCreateRecord(a, "row-1", map[string]string{"title": "t", "body": "b"}))
	editByAuthor := shChange("v2", shAuthorA, 200, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpSet, Path: []string{"body"}, Payload: a.NewString("by-author")}},
	})
	editByOther := shChange("v3", shAuthorB, 300, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpSet, Path: []string{"body"}, Payload: a.NewString("by-other")}},
	})

	apply := func(t *testing.T, order []Change) string {
		st := newSchemaHandlerController(t, shDecl())
		for i := range order {
			_, err := st.ApplyChangeWithResult(ctx, order[i])
			require.NoError(t, err)
		}
		return string(st.Get(ctx, shTestDS, "row-1").GetStringBytes("body"))
	}

	// Both interleavings of the two edits end on the author's value:
	// the non-author op is rejected regardless of position.
	assert.Equal(t, "by-author", apply(t, []Change{create, editByAuthor, editByOther}))
	assert.Equal(t, "by-author", apply(t, []Change{create, editByOther, editByAuthor}))
}

// Concurrent upserts to the same id race on the creation stamps. The
// $setCreate min-rule (offers re-emitted by every accepted upsert)
// makes creator/createdAt settle on the causally-earliest upsert's
// envelope in either delivery order — the same change `_ver.id`
// elects. Before this rule each peer kept its local first-touch's
// stamp and diverged permanently.
func TestSchemaHandler_ConvergenceCreateStampsUpsertRace(t *testing.T) {
	a := &anyenc.Arena{}
	early := shChange("v1", shAuthorA, 100,
		shCreateRecord(a, "row-1", map[string]string{"title": "t", "note": "from-early"}))
	late := shChange("v2", shAuthorB, 200,
		shCreateRecord(a, "row-1", map[string]string{"title": "t", "note": "from-late"}))

	apply := func(t *testing.T, order []Change) *anyenc.Value {
		st := newSchemaHandlerController(t, shDecl())
		for i := range order {
			_, err := st.ApplyChangeWithResult(ctx, order[i])
			require.NoError(t, err)
		}
		return st.Get(ctx, shTestDS, "row-1")
	}

	recAB := apply(t, []Change{early, late})
	recBA := apply(t, []Change{late, early})
	require.NotNil(t, recAB)
	require.NotNil(t, recBA)
	for name, rec := range map[string]*anyenc.Value{"in-order": recAB, "reversed": recBA} {
		assert.Equal(t, shAuthorA, string(rec.GetStringBytes("creator")), "%s: earliest upsert's signer", name)
		assert.Equal(t, float64(100), rec.GetFloat64("createdAt"), "%s: earliest upsert's time", name)
		assert.Equal(t, "v1", string(rec.GetStringBytes(VersionsKey, IdField)), "%s: stamps match the creation marker", name)
	}
}

// The offers must ride the marker path, not the per-op accept path:
// create-path and modify-path validation are asymmetric, so a change
// can be accepted as a create on one peer and ALL-rejected as a modify
// on the other. Here `title` is write-once — v1's payload is valid at
// create but rejected wholesale on the modify path — yet the marker
// still lowers to v1 on the reversed peer, so the stamps must follow.
// (Acceptance-gated offers reproduced creator=authorA/createdAt=100 on
// one peer and authorB/200 on the other with _ver.id=v1 on both.)
func TestSchemaHandler_ConvergenceCreateStampsAllOpsRejectedModify(t *testing.T) {
	a := &anyenc.Arena{}
	early := shChange("v1", shAuthorA, 100,
		shCreateRecord(a, "row-1", map[string]string{"title": "t"}))
	late := shChange("v2", shAuthorB, 200,
		shCreateRecord(a, "row-1", map[string]string{"title": "t"}))

	apply := func(t *testing.T, order []Change) *anyenc.Value {
		st := newSchemaHandlerController(t, shDecl())
		for i := range order {
			_, err := st.ApplyChangeWithResult(ctx, order[i])
			require.NoError(t, err)
		}
		return st.Get(ctx, shTestDS, "row-1")
	}

	recAB := apply(t, []Change{early, late})
	recBA := apply(t, []Change{late, early})
	require.NotNil(t, recAB)
	require.NotNil(t, recBA)
	for name, rec := range map[string]*anyenc.Value{"in-order": recAB, "reversed": recBA} {
		assert.Equal(t, shAuthorA, string(rec.GetStringBytes("creator")), "%s", name)
		assert.Equal(t, float64(100), rec.GetFloat64("createdAt"), "%s", name)
		assert.Equal(t, "v1", string(rec.GetStringBytes(VersionsKey, IdField)), "%s", name)
	}
}

// modifyTime is a TouchStamper stamp — it bumps for EVERY change that
// touches the record, verdicts notwithstanding. The asymmetry probe:
// the MAX-version change is accepted as a create on one peer and
// all-rejected (write-once title) as a modify on the other; an
// acceptance-gated bump would leave the two on different values.
func TestSchemaHandler_ModifyTimeConvergesWhenMaxChangeAllRejected(t *testing.T) {
	a := &anyenc.Arena{}
	early := shChange("v1", shAuthorA, 100,
		shCreateRecord(a, "row-1", map[string]string{"title": "t"}))
	late := shChange("v2", shAuthorB, 200,
		shCreateRecord(a, "row-1", map[string]string{"title": "t"}))

	apply := func(t *testing.T, order []Change) *anyenc.Value {
		st := newSchemaHandlerController(t, shDecl())
		for i := range order {
			_, err := st.ApplyChangeWithResult(ctx, order[i])
			require.NoError(t, err)
		}
		return st.Get(ctx, shTestDS, "row-1")
	}

	recAB := apply(t, []Change{early, late})
	recBA := apply(t, []Change{late, early})
	require.NotNil(t, recAB)
	require.NotNil(t, recBA)
	assert.Equal(t, float64(200), recAB.GetFloat64("modifiedAt"), "max change bumps even when all-rejected")
	assert.Equal(t, float64(200), recBA.GetFloat64("modifiedAt"), "reversed order agrees")
}

// A timestamp-less change offers no createTime — pinning 0 under a low
// version would be irrecoverable (the min gate defends it against
// every later, higher-versioned real value). The next upsert that
// carries a timestamp backfills instead.
func TestSchemaHandler_CreateTimeZeroTimestampNotPinned(t *testing.T) {
	st := newSchemaHandlerController(t, shDecl())
	a := &anyenc.Arena{}
	_, err := st.ApplyChangeWithResult(ctx, shChange("v1", shAuthorA, 0,
		shCreateRecord(a, "row-1", map[string]string{"title": "t"})))
	require.NoError(t, err)
	rec := st.Get(ctx, shTestDS, "row-1")
	require.NotNil(t, rec)
	require.Nil(t, rec.Get("createdAt"), "no offer from a timestamp-less change")

	_, err = st.ApplyChangeWithResult(ctx, shChange("v2", shAuthorA, 200,
		shCreateRecord(a, "row-1", map[string]string{"title": "t"}))) // write-once: all-rejected
	require.NoError(t, err)
	rec = st.Get(ctx, shTestDS, "row-1")
	require.NotNil(t, rec)
	assert.Equal(t, float64(200), rec.GetFloat64("createdAt"), "next stamped upsert backfills")
}

// The stamps survive even a change whose every op is shed by the
// CONTROLLER's field-class filter — no handler hook ever sees such a
// change, so hook-emitted stamps would mint the row bare on the peer
// where it arrives first and split from peers where it arrives as a
// modify. The modifier-site derivation covers it: both orders agree on
// all three stamps and the marker.
func TestSchemaHandler_StampsSurviveFullyFilteredChange(t *testing.T) {
	// No Required fields: a zero-op create must mint the row (a required
	// check would drop the whole record and re-open the pre-existing
	// marker-divergence hole this test is not about).
	ds := schema.Dataset{
		Fields: []schema.Field{
			{Id: "note", Schema: schema.Leaf(schema.KindString), MutableBy: schema.MutableByAnyone},
			{Id: "creator", Stamp: schema.StampCreator, Scope: schema.ScopeDerived},
			{Id: "createdAt", Stamp: schema.StampCreateTime, Scope: schema.ScopeDerived},
			{Id: "modifiedAt", Stamp: schema.StampModifyTime, Scope: schema.ScopeDerived},
		},
		IdRule: schema.IdUser,
	}
	a := &anyenc.Arena{}
	// Undeclared head on a non-Dynamic dataset: dropped by the field-
	// class filter before any handler hook fires — a zero-op upsert.
	filtered := shChange("v1", shAuthorA, 100, RecordChange{
		Id: "row-1", Upsert: true,
		Ops: []Op{{Type: OpSet, Path: []string{"junk"}, Payload: a.NewString("x")}},
	})
	valid := shChange("v2", shAuthorB, 200, RecordChange{
		Id: "row-1", Upsert: true,
		Ops: []Op{{Type: OpSet, Path: []string{"note"}, Payload: a.NewString("n")}},
	})

	apply := func(t *testing.T, order []Change) *anyenc.Value {
		st := newSchemaHandlerController(t, ds)
		for i := range order {
			_, err := st.ApplyChangeWithResult(ctx, order[i])
			require.NoError(t, err)
		}
		return st.Get(ctx, shTestDS, "row-1")
	}

	recAB := apply(t, []Change{filtered, valid})
	recBA := apply(t, []Change{valid, filtered})
	require.NotNil(t, recAB)
	require.NotNil(t, recBA)
	for name, rec := range map[string]*anyenc.Value{"in-order": recAB, "reversed": recBA} {
		assert.Equal(t, shAuthorA, string(rec.GetStringBytes("creator")), "%s", name)
		assert.Equal(t, float64(100), rec.GetFloat64("createdAt"), "%s", name)
		assert.Equal(t, float64(200), rec.GetFloat64("modifiedAt"), "%s", name)
		assert.Equal(t, "v1", string(rec.GetStringBytes(VersionsKey, IdField)), "%s", name)
	}
}

// Strict (non-upsert) modifies leave the creation stamps alone — the
// offer rule mirrors the `_ver.id` marker's upsert-only update rule.
func TestSchemaHandler_CreateStampsIgnoreStrictModify(t *testing.T) {
	st := newSchemaHandlerController(t, shDecl())
	a := &anyenc.Arena{}
	_, err := st.ApplyChangeWithResult(ctx, shChange("v2", shAuthorB, 200,
		shCreateRecord(a, "row-1", map[string]string{"title": "t"})))
	require.NoError(t, err)

	// Contrived lower-versioned strict modify: accepted, but it must not
	// re-elect the creation stamps (nor lower _ver.id).
	_, err = st.ApplyChangeWithResult(ctx, shChange("v1", shAuthorA, 100, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpSet, Path: []string{"note"}, Payload: a.NewString("n")}},
	}))
	require.NoError(t, err)

	rec := st.Get(ctx, shTestDS, "row-1")
	require.NotNil(t, rec)
	assert.Equal(t, shAuthorB, string(rec.GetStringBytes("creator")))
	assert.Equal(t, float64(200), rec.GetFloat64("createdAt"))
	assert.Equal(t, "v2", string(rec.GetStringBytes(VersionsKey, IdField)))
}

// The accept path of BeforeModify must not allocate: cold restore replays
// millions of ops through this hook.
func BenchmarkSchemaHandler_BeforeModifyAccept(b *testing.B) {
	ds := schema.Dataset{Fields: []schema.Field{
		{Id: "note", Schema: schema.Leaf(schema.KindString), MutableBy: schema.MutableByAnyone},
		{Id: "body", Schema: schema.Leaf(schema.KindString), MutableBy: schema.MutableByAuthor},
		{Id: "creator", Stamp: schema.StampCreator, Scope: schema.ScopeDerived},
	}}
	h, err := NewSchemaHandler(ds)
	if err != nil {
		b.Fatal(err)
	}
	a := &anyenc.Arena{}
	before := a.NewObject()
	before.Set("creator", a.NewString(shAuthorA))
	chg := shChange("v1", shAuthorA, 100)
	cctx := &ChangeCtx{Change: &chg, Before: before}
	rec := &RecordChange{Id: "row-1"}
	opNote := &Op{Type: OpSet, Path: []string{"note"}, Payload: a.NewString("x")}
	opBody := &Op{Type: OpSet, Path: []string{"body"}, Payload: a.NewString("x")}
	sink := &Sink{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := h.BeforeModify(cctx, rec, opNote, sink); err != nil {
			b.Fatal(err)
		}
		if err := h.BeforeModify(cctx, rec, opBody, sink); err != nil {
			b.Fatal(err)
		}
	}
	if allocs := testing.AllocsPerRun(100, func() {
		_ = h.BeforeModify(cctx, rec, opNote, sink)
	}); allocs != 0 {
		b.Fatalf("accept path allocates: %v allocs/op", allocs)
	}
}
