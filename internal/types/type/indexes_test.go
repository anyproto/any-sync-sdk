package typetype_test

import (
	"context"
	"fmt"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
)

func stringsValue(arena *anyenc.Arena, items ...string) *anyenc.Value {
	arr := arena.NewArray()
	for i, s := range items {
		arr.SetArrayItem(i, arena.NewString(s))
	}
	return arr
}

func indexPayload(arena *anyenc.Arena, headId, key string, sparse bool, fields ...string) crdt.Op {
	payload := map[string]any{
		typetype.DefFieldDef:         typetype.DefKindIndex,
		typetype.DefFieldDataset:     headId,
		typetype.FieldKey:            key,
		typetype.DefFieldIndexFields: stringsValue(arena, fields...),
	}
	if sparse {
		payload[typetype.DefFieldIndexSparse] = true
	}
	return setMulti(arena, payload)
}

// defsSeq applies definition records in order, each in its own change.
type defsSeq struct {
	t    *testing.T
	ctrl *crdt.Controller
	n    int
}

func (s *defsSeq) put(recId string, op crdt.Op) {
	s.t.Helper()
	s.n++
	require.NoError(s.t, s.ctrl.ApplyChange(context.Background(),
		defsChange(crdt.VersionId(fmt.Sprintf("v%04d", s.n)), fmt.Sprintf("c%04d", s.n), recId, true, op)))
}

func (s *defsSeq) del(recId string) {
	s.t.Helper()
	s.n++
	require.NoError(s.t, s.ctrl.ApplyChange(context.Background(),
		defsChange(crdt.VersionId(fmt.Sprintf("v%04d", s.n)), fmt.Sprintf("c%04d", s.n), recId, false, crdt.Op{Type: crdt.OpDelete})))
}

func compileOne(t *testing.T, db anystore.DB, key string) types.CompiledDataset {
	t.Helper()
	compiled, err := types.CompileDatasetDefs(context.Background(), db, testObjectId, nil)
	require.NoError(t, err)
	for _, ds := range compiled {
		if ds.Key == key {
			return ds
		}
	}
	t.Fatalf("dataset %q not compiled", key)
	return types.CompiledDataset{}
}

// An index record's creation projects no shortId row — an index changes
// no apply verdict, so data changes are not gated on it. Its removal
// projects one like every removal.
func TestDatasetDefs_IndexCreateProjectsNoShortId(t *testing.T) {
	ctrl, _ := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	const createChange = "change-create-index"
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v1", createChange, "idx-1", true,
		indexPayload(arena, "head-1", "by_ts", false, "ts"))))
	rec := ctrl.Get(ctx, typetype.DatasetDefs, "idx-1")
	require.NotNil(t, rec, "the index record is stored")
	assert.Equal(t, typetype.DefKindIndex, rec.GetString(typetype.DefFieldDef))
	assert.Nil(t, ctrl.Get(ctx, typetype.ShortIdsDataset, crdt.DeriveRecordId(createChange)))

	const removeChange = "change-remove-index"
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v2", removeChange, "idx-1", false, crdt.Op{Type: crdt.OpDelete})))
	assert.NotNil(t, ctrl.Get(ctx, typetype.ShortIdsDataset, crdt.DeriveRecordId(removeChange)))
}

func TestDatasetDefs_IndexCreateRejections(t *testing.T) {
	ctrl, _ := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	cases := []struct {
		name string
		op   crdt.Op
	}{
		{"no dataset ref", setMulti(arena, map[string]any{
			typetype.DefFieldDef:         typetype.DefKindIndex,
			typetype.FieldKey:            "x",
			typetype.DefFieldIndexFields: stringsValue(arena, "ts"),
		})},
		{"no key", indexPayload(arena, "head-1", "", false, "ts")},
		{"key is not a slug", indexPayload(arena, "head-1", "By Ts", false, "ts")},
		{"no fields", indexPayload(arena, "head-1", "x", false)},
		{"fields is not an array", setMulti(arena, map[string]any{
			typetype.DefFieldDef:         typetype.DefKindIndex,
			typetype.DefFieldDataset:     "head-1",
			typetype.FieldKey:            "x",
			typetype.DefFieldIndexFields: "ts",
		})},
		{"five fields", indexPayload(arena, "head-1", "x", false, "a", "b", "c", "d", "e")},
		{"empty field", indexPayload(arena, "head-1", "x", false, "")},
		{"one path twice", indexPayload(arena, "head-1", "x", false, "ts", "-ts")},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := ctrl.ApplyChangeWithResult(ctx,
				defsChange(crdt.VersionId(string(rune('A'+i))), "change-"+tc.name, "rec-"+tc.name, true, tc.op))
			require.NoError(t, err)
			require.Len(t, res.Rejections, 1)
			assert.ErrorIs(t, res.Rejections[0].Err, crdt.ErrValidation)
		})
	}
}

// An index definition is pinned whole.
func TestDatasetDefs_IndexIsPinned(t *testing.T) {
	ctrl, _ := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v1", "c1", "idx-1", true,
		indexPayload(arena, "head-1", "by_ts", false, "ts"))))

	for i, op := range []crdt.Op{
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldIndexFields}, Payload: stringsValue(arena, "value")},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldIndexSparse}, Payload: arena.NewTrue()},
		{Type: crdt.OpSet, Path: []string{typetype.FieldKey}, Payload: arena.NewString("other")},
	} {
		res, err := ctrl.ApplyChangeWithResult(ctx, defsChange(crdt.VersionId(fmt.Sprintf("w%d", i)), fmt.Sprintf("m%d", i), "idx-1", false, op))
		require.NoError(t, err)
		require.Len(t, res.Rejections, 1)
	}
	rec := ctrl.Get(ctx, typetype.DatasetDefs, "idx-1")
	assert.Equal(t, "by_ts", rec.GetString(typetype.FieldKey))
	assert.Nil(t, rec.Get(typetype.DefFieldIndexSparse))
}

// Index records fold under their dataset: by key, validated against the
// folded declaration, in creation order.
func TestCompile_Indexes(t *testing.T) {
	ctrl, db := newDefsController(t)
	arena := &anyenc.Arena{}
	seedPart(t, ctrl, arena)
	s := &defsSeq{t: t, ctrl: ctrl}

	s.put("head-1", headPayload(arena, "samples", nil))
	s.put("f-ts", fieldPayload(arena, "head-1", "ts", "datetime", nil))
	s.put("f-value", fieldPayload(arena, "head-1", "value", "number", nil))
	s.put("f-tags", fieldPayload(arena, "head-1", "tags", "array", nil))
	base := compileOne(t, db, "samples")
	require.False(t, base.Invalid, base.InvalidReason)
	assert.Empty(t, base.Indexes)
	assert.Empty(t, base.StoreIndexes())

	s.put("x-ts", indexPayload(arena, "head-1", "by_ts", false, "ts"))
	s.put("x-wide", indexPayload(arena, "head-1", "wide", true, "-value", "ts", schema.IndexPathCreated))
	s.put("x-dup", indexPayload(arena, "head-1", "by_ts", false, "value"))      // later duplicate key: hidden
	s.put("x-same", indexPayload(arena, "head-1", "again", false, "ts"))        // another key, the same shape
	s.put("x-tags", indexPayload(arena, "head-1", "by_tags", false, "tags"))    // not a scalar field
	s.put("x-gone", indexPayload(arena, "head-1", "by_gone", false, "gone"))    // undeclared field
	s.put("x-obj", indexPayload(arena, "head-1", "by_obj", false, "_objectId")) // per-object dataset
	s.put("x-orphan", indexPayload(arena, "head-nope", "lost", false, "ts"))

	ds := compileOne(t, db, "samples")
	require.False(t, ds.Invalid, "an invalid index leaves its dataset valid")
	byKey := map[string]types.CompiledIndex{}
	var order []string
	for _, x := range ds.Indexes {
		byKey[x.Key] = x
		order = append(order, x.Key)
	}
	assert.Equal(t, []string{"by_ts", "wide", "again", "by_tags", "by_gone", "by_obj"}, order)
	assert.Equal(t, "x-ts", byKey["by_ts"].DefId, "the first declaration of a key wins")
	assert.Equal(t, []string{"ts"}, byKey["by_ts"].Fields)
	assert.True(t, byKey["wide"].Sparse)
	for _, key := range []string{"by_ts", "wide", "again"} {
		assert.False(t, byKey[key].Invalid, key)
	}
	for _, key := range []string{"by_tags", "by_gone", "by_obj"} {
		assert.True(t, byKey[key].Invalid, key)
		assert.NotEmpty(t, byKey[key].InvalidReason, key)
	}

	store := ds.StoreIndexes()
	require.Len(t, store, 2, "one store index per shape")
	assert.Equal(t, anystore.IndexInfo{Name: "dx_ts", Fields: []string{"ts"}}, store[0])
	assert.Equal(t, anystore.IndexInfo{Name: "dx_-value,ts,_ver.id~sparse", Fields: []string{"-value", "ts", "_ver.id"}, Sparse: true}, store[1])
	assert.NotEqual(t, base.SchemaRev, ds.SchemaRev, "a per-object dataset re-registers for its indexes")

	// Removing an index, then the field another one names.
	rev := ds.SchemaRev
	s.del("x-wide")
	s.del("f-ts")
	ds = compileOne(t, db, "samples")
	assert.NotEqual(t, rev, ds.SchemaRev)
	assert.Empty(t, ds.StoreIndexes(), "an index over a removed field is not built")
	for _, x := range ds.Indexes {
		assert.NotEqual(t, "wide", x.Key)
		assert.True(t, x.Invalid, x.Key)
	}
}

// A shared dataset takes `_objectId`, and its registration does not
// follow its indexes: the store indexes its collection.
func TestCompile_IndexesOfASharedDataset(t *testing.T) {
	ctrl, db := newDefsController(t)
	arena := &anyenc.Arena{}
	seedPart(t, ctrl, arena)
	s := &defsSeq{t: t, ctrl: ctrl}

	s.put("head-1", headPayload(arena, "samples", map[string]any{typetype.DefFieldPerSpace: true}))
	s.put("f-ts", fieldPayload(arena, "head-1", "ts", "datetime", nil))
	base := compileOne(t, db, "samples")
	require.True(t, base.Shared)

	s.put("x-1", indexPayload(arena, "head-1", "by_ts", false, "ts", schema.IndexPathObject))
	ds := compileOne(t, db, "samples")
	require.Len(t, ds.Indexes, 1)
	assert.False(t, ds.Indexes[0].Invalid, ds.Indexes[0].InvalidReason)
	assert.Equal(t, "dx_ts,_objectId", ds.StoreIndexes()[0].Name)
	assert.Equal(t, base.SchemaRev, ds.SchemaRev)
}

// Past the per-dataset limit an index is listed and not built.
func TestCompile_IndexLimit(t *testing.T) {
	ctrl, db := newDefsController(t)
	arena := &anyenc.Arena{}
	seedPart(t, ctrl, arena)
	s := &defsSeq{t: t, ctrl: ctrl}

	s.put("head-1", headPayload(arena, "samples", nil))
	fields := []string{"a", "b", "c"}
	for _, f := range fields {
		s.put("f-"+f, fieldPayload(arena, "head-1", f, "string", nil))
	}
	// Distinct shapes: every ordered pair plus the singles.
	var shapes [][]string
	for _, f := range fields {
		shapes = append(shapes, []string{f})
		for _, g := range fields {
			if f != g {
				shapes = append(shapes, []string{f, g})
			}
		}
	}
	require.Greater(t, len(shapes), schema.MaxDatasetIndexes)
	for i, shape := range shapes {
		s.put(fmt.Sprintf("x-%d", i), indexPayload(arena, "head-1", fmt.Sprintf("k%d", i), false, shape...))
	}
	ds := compileOne(t, db, "samples")
	require.Len(t, ds.Indexes, len(shapes))
	for i, x := range ds.Indexes {
		assert.Equal(t, i >= schema.MaxDatasetIndexes, x.Invalid, x.Key)
	}
	assert.Len(t, ds.StoreIndexes(), schema.MaxDatasetIndexes)
}

// A module dataset owns its schema: an index record under it is dropped.
func TestCompile_IndexUnderAModuleDataset(t *testing.T) {
	ctrl, db := newDefsController(t)
	arena := &anyenc.Arena{}
	seedPart(t, ctrl, arena)
	s := &defsSeq{t: t, ctrl: ctrl}
	s.put("head-1", headPayload(arena, "body", map[string]any{typetype.DefFieldModule: "editor"}))
	s.put("x-1", indexPayload(arena, "head-1", "by_ts", false, "ts"))

	compiled, err := types.CompileDatasetDefs(context.Background(), db, testObjectId,
		types.NewModules(types.ModuleInfo{Name: "editor", Canonical: "editor_blocks"}))
	require.NoError(t, err)
	require.Len(t, compiled, 1)
	assert.Empty(t, compiled[0].Indexes)
}
