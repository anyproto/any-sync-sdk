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

// A synced change may not write a Local-class field.
func TestApply_SyncedRejectsLocalField(t *testing.T) {
	st := newClassController(t)
	arena := &anyenc.Arena{}
	err := st.ApplyChange(ctx, makeUpsert("v1", "r1",
		Op{Type: OpSet, Path: []string{"status"}, Payload: arena.NewString("offloaded")}))
	require.Error(t, err)
	assert.Nil(t, st.Get(ctx, testDS, "r1"))
}

// A Change.Local may only write Local-class fields.
func TestApply_LocalRejectsSyncedField(t *testing.T) {
	st := newClassController(t)
	arena := &anyenc.Arena{}
	ch := makeUpsert("v1", "r1", Op{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("x")})
	ch.Local = true
	require.Error(t, st.ApplyChange(ctx, ch))
}

// Derived fields are handler-only — no input op may write them, synced or local.
func TestApply_RejectsDerivedInputOp(t *testing.T) {
	st := newClassController(t)
	arena := &anyenc.Arena{}
	require.Error(t, st.ApplyChange(ctx, makeUpsert("v1", "r1",
		Op{Type: OpSet, Path: []string{"createdAt"}, Payload: arena.NewNumberInt(5)})))
}

// On a non-Dynamic dataset, an undeclared field is rejected.
func TestApply_RejectsUnknownFieldNonDynamic(t *testing.T) {
	st := newClassController(t)
	arena := &anyenc.Arena{}
	require.Error(t, st.ApplyChange(ctx, makeUpsert("v1", "r1",
		Op{Type: OpSet, Path: []string{"bogus"}, Payload: arena.NewString("x")})))
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
