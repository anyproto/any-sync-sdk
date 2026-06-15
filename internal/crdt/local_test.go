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

// newClassController returns a controller whose testDS dataset declares
// one field of each class (non-Dynamic), for field-class enforcement tests.
func newClassController(t *testing.T) *Controller {
	t.Helper()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ds := schema.Dataset{Fields: []schema.Field{
		{Id: "name", Schema: schema.Leaf(schema.KindString), Scope: schema.ScopeSynced},
		{Id: "status", Schema: schema.Leaf(schema.KindString), Scope: schema.ScopeLocal},
		{Id: "createdAt", Schema: schema.Leaf(schema.KindNumber), Scope: schema.ScopeDerived},
	}}
	st, err := NewController(ctx, "obj1", db, HandlerReg{Name: testDS, Handler: DefaultHandler{}, Schema: ds})
	require.NoError(t, err)
	return st
}

func TestNextVersion_Monotonic(t *testing.T) {
	a := NextVersion("")
	b := NextVersion(a)
	c := NextVersion(b)
	assert.Less(t, string(a), string(b))
	assert.Less(t, string(b), string(c))
	assert.Greater(t, string(NextVersion("zzzz")), "zzzz")
}

// assertFieldClassDropped applies ch through the remote/replay primitive
// and asserts the single offending op was DROPPED with a validation
// rejection rather than aborting the whole change — the "a remote change
// that violates field-class is ignored, never fatal" contract that keeps
// one bad historical change from wedging cold restore.
func assertFieldClassDropped(t *testing.T, st *Controller, ch Change) {
	t.Helper()
	res, err := st.ApplyChangeWithResult(ctx, ch)
	require.NoError(t, err, "remote field-class violation must be ignored, not fatal")
	require.Len(t, res.Rejections, 1)
	assert.ErrorIs(t, res.Rejections[0].Err, ErrValidation)
}

// A synced change may not write a Local-class field: the client path
// fails fast (ValidateChange), the remote path drops the op.
func TestApply_SyncedRejectsLocalField(t *testing.T) {
	st := newClassController(t)
	arena := &anyenc.Arena{}
	ch := makeUpsert("v1", "r1",
		Op{Type: OpSet, Path: []string{"status"}, Payload: arena.NewString("offloaded")})
	require.Error(t, st.ValidateChange(ch))
	assertFieldClassDropped(t, st, ch)
	// The Local-class value never landed via the synced route.
	if rec := st.Get(ctx, testDS, "r1"); rec != nil {
		assert.Empty(t, rec.GetString("status"))
	}
}

// A Change.Local may only write Local-class fields.
func TestApply_LocalRejectsSyncedField(t *testing.T) {
	st := newClassController(t)
	arena := &anyenc.Arena{}
	ch := makeUpsert("v1", "r1", Op{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("x")})
	ch.Local = true
	assertFieldClassDropped(t, st, ch)
}

// Derived fields are handler-only — no input op may write them, synced or local.
func TestApply_RejectsDerivedInputOp(t *testing.T) {
	st := newClassController(t)
	arena := &anyenc.Arena{}
	ch := makeUpsert("v1", "r1",
		Op{Type: OpSet, Path: []string{"createdAt"}, Payload: arena.NewNumberInt(5)})
	require.Error(t, st.ValidateChange(ch))
	assertFieldClassDropped(t, st, ch)
}

// On a non-Dynamic dataset, an undeclared field is rejected.
func TestApply_RejectsUnknownFieldNonDynamic(t *testing.T) {
	st := newClassController(t)
	arena := &anyenc.Arena{}
	ch := makeUpsert("v1", "r1",
		Op{Type: OpSet, Path: []string{"bogus"}, Payload: arena.NewString("x")})
	require.Error(t, st.ValidateChange(ch))
	assertFieldClassDropped(t, st, ch)
}

// A Dynamic dataset accepts undeclared fields as synced.
func TestApply_DynamicAcceptsUndeclared(t *testing.T) {
	st := newTestController(t) // testDS is Dynamic (dynSchema)
	arena := &anyenc.Arena{}
	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1",
		Op{Type: OpSet, Path: []string{"anything"}, Payload: arena.NewString("ok")})))
	assert.Equal(t, "ok", st.Get(ctx, testDS, "r1").GetString("anything"))
}

// A local write lands on a Local field, versioned locally, and a later
// synced write to a sibling synced field with a LOW version still applies
// (per-field _ver isolation).
func TestApply_LocalSetIsolatedFromSynced(t *testing.T) {
	st := newClassController(t)
	arena := &anyenc.Arena{}
	const id = "r1"

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", id,
		Op{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("Alpha")})))

	lch := Change{
		ObjectId: "obj1", Dataset: testDS, DataVersion: testDataVersion, Local: true,
		Records: []RecordChange{{Id: id, Ops: []Op{
			{Type: OpSet, Path: []string{"status"}, Payload: arena.NewString("offloaded")},
		}}},
	}
	lch.VersionId = st.NextLocalVersion(ctx, &lch)
	require.NoError(t, st.ApplyChange(ctx, lch))

	rec := st.Get(ctx, testDS, id)
	require.NotNil(t, rec)
	assert.Equal(t, "offloaded", rec.GetString("status"))
	assert.Equal(t, "Alpha", rec.GetString("name"))

	require.NoError(t, st.ApplyChange(ctx, makeChange("v2", id,
		Op{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("Beta")})))
	rec = st.Get(ctx, testDS, id)
	assert.Equal(t, "Beta", rec.GetString("name"))
	assert.Equal(t, "offloaded", rec.GetString("status"))
}

// ----------------------------------------------------------------------------
// DynamicScopeByKey — per-key-scoped dynamic datasets (the objects dataset)
// ----------------------------------------------------------------------------

// newScopeByKeyController declares a Dynamic dataset with
// DynamicScopeByKey plus one declared derived field — the objects
// dataset's shape: undeclared heads (`any`, typeIds) carry per-property
// scopes the controller can't see; declared fields stay enforced.
func newScopeByKeyController(t *testing.T) *Controller {
	t.Helper()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ds := schema.Dataset{Dynamic: true, Fields: []schema.Field{
		{Id: "createdAt", Schema: schema.Leaf(schema.KindNumber), Scope: schema.ScopeDerived},
	}}
	st, err := NewController(ctx, "obj1", db,
		HandlerReg{Name: testDS, Handler: DefaultHandler{}, Schema: ds, DynamicScopeByKey: true})
	require.NoError(t, err)
	return st
}

// A Local change may write UNDECLARED heads on a DynamicScopeByKey
// dataset — the per-key scope is the dataset layer's responsibility
// (Properties.Set routing + handler registry validation), not the
// controller's head-level check.
func TestApply_ScopeByKeyAllowsLocalOnUndeclared(t *testing.T) {
	st := newScopeByKeyController(t)
	arena := &anyenc.Arena{}

	// Seed the record with a synced-route write so the local one
	// modifies an existing row (mirrors the bootstrap-then-Set flow).
	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1",
		Op{Type: OpSet, Path: []string{"any", "name"}, Payload: arena.NewString("shared")})))

	ch := makeUpsert("v2", "r1",
		Op{Type: OpSet, Path: []string{"someType", "pinProp"}, Payload: arena.NewTrue()})
	ch.Local = true
	require.NoError(t, st.ApplyChange(ctx, ch))

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	assert.Equal(t, "shared", rec.GetString("any", "name"))
	assert.True(t, rec.GetBool("someType", "pinProp"))
}

// Declared fields keep full enforcement even with DynamicScopeByKey.
func TestApply_ScopeByKeyKeepsDeclaredEnforcement(t *testing.T) {
	st := newScopeByKeyController(t)
	arena := &anyenc.Arena{}

	ch := makeUpsert("v1", "r1",
		Op{Type: OpSet, Path: []string{"createdAt"}, Payload: arena.NewNumberFloat64(1)})
	require.Error(t, st.ValidateChange(ch), "derived field stays handler-only")
	assertFieldClassDropped(t, st, ch)
}

// ----------------------------------------------------------------------------
// Injected route — the account mirror's apply kind
// ----------------------------------------------------------------------------

// An injected change writes declared account fields with its
// caller-supplied version; synced/local/derived declared fields and
// every other route stay rejected.
func TestApply_InjectedRouteClassification(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ds := schema.Dataset{Fields: []schema.Field{
		{Id: "name", Schema: schema.Leaf(schema.KindString), Scope: schema.ScopeSynced},
		{Id: "read", Schema: schema.Leaf(schema.KindBoolean), Scope: schema.ScopeAccount},
	}}
	st, err := NewController(ctx, "obj1", db, HandlerReg{Name: testDS, Handler: DefaultHandler{}, Schema: ds})
	require.NoError(t, err)
	arena := &anyenc.Arena{}

	// Synced change cannot write the account field — op dropped on apply.
	assertFieldClassDropped(t, st, makeUpsert("v1", "r1",
		Op{Type: OpSet, Path: []string{"read"}, Payload: arena.NewTrue()}))

	// Injected change writes it, at the supplied (tech) version.
	ch := makeUpsert("tech-v7", "r1", Op{Type: OpSet, Path: []string{"read"}, Payload: arena.NewTrue()})
	ch.Injected = true
	require.NoError(t, st.ApplyChange(ctx, ch))
	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	assert.True(t, rec.GetBool("read"))
	assert.Equal(t, VersionId("tech-v7"), GetRecordVersion(rec, "read"))

	// Injected change cannot write the synced field — op dropped on apply.
	ch2 := makeUpsert("tech-v8", "r1", Op{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("x")})
	ch2.Injected = true
	assertFieldClassDropped(t, st, ch2)

	// A stale injected replay gates to a no-op (idempotent mirror).
	ch3 := makeUpsert("tech-v3", "r1", Op{Type: OpSet, Path: []string{"read"}, Payload: arena.NewFalse()})
	ch3.Injected = true
	require.NoError(t, st.ApplyChange(ctx, ch3))
	rec = st.Get(ctx, testDS, "r1")
	assert.True(t, rec.GetBool("read"), "older injected version gated out")

	// Local+Injected is a contradiction.
	ch4 := makeUpsert("v9", "r1", Op{Type: OpSet, Path: []string{"read"}, Payload: arena.NewTrue()})
	ch4.Local = true
	ch4.Injected = true
	require.Error(t, st.ApplyChange(ctx, ch4))
}

// Injected changes on a DynamicScopeByKey dataset may write undeclared
// heads — the mirror resolved the per-key scope itself.
func TestApply_InjectedOnScopeByKeyDataset(t *testing.T) {
	st := newScopeByKeyController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1",
		Op{Type: OpSet, Path: []string{"any", "name"}, Payload: arena.NewString("shared")})))

	ch := makeUpsert("tech-v2", "r1",
		Op{Type: OpSet, Path: []string{"movie", "read"}, Payload: arena.NewTrue()})
	ch.Injected = true
	require.NoError(t, st.ApplyChange(ctx, ch))

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	assert.True(t, rec.GetBool("movie", "read"))
	assert.Equal(t, VersionId("tech-v2"), GetRecordVersion(rec, "movie", "read"))
	assert.Equal(t, "shared", rec.GetString("any", "name"), "synced path untouched")
}
