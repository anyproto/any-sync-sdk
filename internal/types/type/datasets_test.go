package typetype_test

import (
	"context"
	"path/filepath"
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

const defsDataVer = "typeDatasetHandler-v1" // matches typetype.DatasetDefsHandlerVersion

func newDefsController(t *testing.T) (*crdt.Controller, anystore.DB) {
	t.Helper()
	db, err := anystore.Open(context.Background(), filepath.Join(t.TempDir(), "defs.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ctrl, err := crdt.NewController(context.Background(), testObjectId, db,
		crdt.HandlerReg{Name: typetype.DatasetDefs, Handler: typetype.DatasetDefsHandler{}, Schema: schema.Dataset{Dynamic: true}},
		crdt.HandlerReg{Name: typetype.ShortIdsDataset, Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}},
	)
	require.NoError(t, err)
	return ctrl, db
}

func defsChange(versionId crdt.VersionId, changeId, recId string, upsert bool, ops ...crdt.Op) crdt.Change {
	return crdt.Change{
		ObjectId:    testObjectId,
		Dataset:     typetype.DatasetDefs,
		ChangeId:    changeId,
		VersionId:   versionId,
		DataVersion: defsDataVer,
		Records: []crdt.RecordChange{
			{Id: recId, Upsert: upsert, Ops: ops},
		},
	}
}

func headPayload(arena *anyenc.Arena, name string, extra map[string]any) crdt.Op {
	fields := map[string]any{
		typetype.DefFieldDef:  typetype.DefKindDataset,
		typetype.DefFieldName: name,
	}
	for k, v := range extra {
		fields[k] = v
	}
	return setMulti(arena, fields)
}

func fieldPayload(arena *anyenc.Arena, headId, key, kind string, extra map[string]any) crdt.Op {
	fields := map[string]any{
		typetype.DefFieldDef:     typetype.DefKindField,
		typetype.DefFieldDataset: headId,
		typetype.FieldKey:        key,
		typetype.FieldKind:       kind,
	}
	for k, v := range extra {
		fields[k] = v
	}
	return setMulti(arena, fields)
}

func TestDatasetDefs_CreateProjectsShortId(t *testing.T) {
	ctrl, _ := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	const changeId = "change-create-notes"
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v1", changeId, "head-1", true,
		headPayload(arena, "notes", nil))))

	row := ctrl.Get(ctx, typetype.ShortIdsDataset, crdt.DeriveRecordId(changeId))
	require.NotNil(t, row, "shortId row must be projected")
	assert.Equal(t, changeId, string(row.GetStringBytes(typetype.ShortIdFieldChangeId)))
	assert.Equal(t, "head-1", string(row.GetStringBytes(typetype.ShortIdFieldDefId)))
	assert.Equal(t, typetype.ShortIdSrcDatasets, string(row.GetStringBytes(typetype.ShortIdFieldSrc)))
}

func TestDatasetDefs_CreateRejections(t *testing.T) {
	ctrl, _ := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	cases := []struct {
		name string
		op   crdt.Op
	}{
		{"missing def", setMulti(arena, map[string]any{typetype.DefFieldName: "x"})},
		{"unknown def", setMulti(arena, map[string]any{typetype.DefFieldDef: "bogus"})},
		{"reserved name", headPayload(arena, "objects", nil)},
		{"underscore name", headPayload(arena, "_sneaky", nil)},
		{"bad idRule", headPayload(arena, "ok1", map[string]any{typetype.DefFieldIdRule: "bogus"})},
		{"bad deleteBy", headPayload(arena, "ok2", map[string]any{typetype.DefFieldDeleteBy: "bogus"})},
		{"field missing dataset ref", setMulti(arena, map[string]any{
			typetype.DefFieldDef: typetype.DefKindField,
			typetype.FieldKey:    "a",
			typetype.FieldKind:   "string",
		})},
		{"field reserved key", fieldPayload(arena, "head-1", "id", "string", nil)},
		{"field dotted key", fieldPayload(arena, "head-1", "a.b", "string", nil)},
		{"field bad kind", fieldPayload(arena, "head-1", "a", "bogus", nil)},
		{"field bad stamp", fieldPayload(arena, "head-1", "a", "string", map[string]any{typetype.DefFieldStamp: "bogus"})},
		{"field bad mutableBy", fieldPayload(arena, "head-1", "a", "string", map[string]any{typetype.DefFieldMutableBy: "bogus"})},
		{"field stamped and mutable", fieldPayload(arena, "head-1", "a", "string", map[string]any{
			typetype.DefFieldStamp:     "creator",
			typetype.DefFieldMutableBy: "any",
		})},
		{"field derived scope without stamp", fieldPayload(arena, "head-1", "a", "string", map[string]any{typetype.FieldScope: "derived"})},
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

func TestDatasetDefs_PinningMatrix(t *testing.T) {
	ctrl, _ := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v1", "c1", "head-1", true,
		headPayload(arena, "notes", map[string]any{typetype.DefFieldDeleteBy: "author"}))))

	pinned := []crdt.Op{
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldName}, Payload: arena.NewString("renamed")},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldDef}, Payload: arena.NewString("field")},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldIdRule}, Payload: arena.NewString("user")},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldDeleteBy}, Payload: arena.NewString("anyone")},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldSkipHistory}, Payload: arena.NewTrue()},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldSearch}, Payload: arena.NewObject()},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldSearch, "bogus"}, Payload: arena.NewString("x")},
		{Type: crdt.OpUnset, Path: []string{typetype.DefFieldName}},
	}
	for i, op := range pinned {
		res, err := ctrl.ApplyChangeWithResult(ctx,
			defsChange(crdt.VersionId("p"+string(rune('a'+i))), "cp"+string(rune('a'+i)), "head-1", false, op))
		require.NoError(t, err)
		require.Len(t, res.Rejections, 1, "op %d must be pinned", i)
		assert.ErrorIs(t, res.Rejections[0].Err, crdt.ErrValidation)
	}

	multiText := arena.NewArray()
	multiText.SetArrayItem(0, arena.NewString("body"))
	multiText.SetArrayItem(1, arena.NewString("summary"))
	mutable := []crdt.Op{
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldDisplayName}, Payload: arena.NewString("Notes")},
		{Type: crdt.OpSet, Path: []string{typetype.FieldDescription}, Payload: arena.NewString("descr")},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldSearch, typetype.SearchKeyTitle}, Payload: arena.NewString("title")},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldSearch, typetype.SearchKeyText}, Payload: arena.NewString("body")},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldSearch, typetype.SearchKeyText}, Payload: multiText},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldSearch, typetype.SearchKeyScope}, Payload: arena.NewString("notes")},
		{Type: crdt.OpUnset, Path: []string{typetype.DefFieldSearch, typetype.SearchKeyText}},
	}
	for i, op := range mutable {
		res, err := ctrl.ApplyChangeWithResult(ctx,
			defsChange(crdt.VersionId("m"+string(rune('a'+i))), "cm"+string(rune('a'+i)), "head-1", false, op))
		require.NoError(t, err)
		require.Empty(t, res.Rejections, "op %d must be mutable", i)
	}

	// title/scope leaves must stay scalar strings; text must be a
	// string or a well-formed key array.
	emptyArr := arena.NewArray()
	dupArr := arena.NewArray()
	dupArr.SetArrayItem(0, arena.NewString("body"))
	dupArr.SetArrayItem(1, arena.NewString("body"))
	blankArr := arena.NewArray()
	blankArr.SetArrayItem(0, arena.NewString(""))
	numArr := arena.NewArray()
	numArr.SetArrayItem(0, arena.NewNumberInt(1))
	badLeaves := []crdt.Op{
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldSearch, typetype.SearchKeyTitle}, Payload: arena.NewNumberInt(1)},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldSearch, typetype.SearchKeyText}, Payload: arena.NewNumberInt(1)},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldSearch, typetype.SearchKeyText}, Payload: emptyArr},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldSearch, typetype.SearchKeyText}, Payload: dupArr},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldSearch, typetype.SearchKeyText}, Payload: blankArr},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldSearch, typetype.SearchKeyText}, Payload: numArr},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldSearch, typetype.SearchKeyScope}, Payload: multiText},
	}
	for i, op := range badLeaves {
		res, err := ctrl.ApplyChangeWithResult(ctx,
			defsChange(crdt.VersionId("z"+string(rune('a'+i))), "cz"+string(rune('a'+i)), "head-1", false, op))
		require.NoError(t, err)
		require.Len(t, res.Rejections, 1, "bad search leaf %d must reject", i)
		assert.ErrorIs(t, res.Rejections[0].Err, crdt.ErrValidation)
	}

	// Multi-field $set may carry the array text leaf as a dotted key.
	payload := arena.NewObject()
	payload.Set(typetype.DefFieldSearch+"."+typetype.SearchKeyText, multiText)
	res, err := ctrl.ApplyChangeWithResult(ctx, defsChange("vx", "cx", "head-1", false,
		crdt.Op{Type: crdt.OpSet, Payload: payload}))
	require.NoError(t, err)
	require.Empty(t, res.Rejections)

	// Multi-field $set touching pinned state rejects (probed per key: the
	// mutable key survives).
	payload = arena.NewObject()
	payload.Set(typetype.DefFieldName, arena.NewString("hax"))
	payload.Set(typetype.DefFieldDisplayName, arena.NewString("kept"))
	res, err = ctrl.ApplyChangeWithResult(ctx, defsChange("vy", "cy", "head-1", false,
		crdt.Op{Type: crdt.OpSet, Payload: payload}))
	require.NoError(t, err)
	require.NotEmpty(t, res.Rejections)
	head := ctrl.Get(ctx, typetype.DatasetDefs, "head-1")
	assert.Equal(t, "notes", string(head.GetStringBytes(typetype.DefFieldName)))
	assert.Equal(t, "kept", string(head.GetStringBytes(typetype.DefFieldDisplayName)))
}

func TestDatasetDefs_DeleteProjectsRemovalRow(t *testing.T) {
	ctrl, _ := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v1", "c1", "head-1", true,
		headPayload(arena, "notes", nil))))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v2", "c2", "head-1", false,
		crdt.Op{Type: crdt.OpDelete})))

	row := ctrl.Get(ctx, typetype.ShortIdsDataset, crdt.DeriveRecordId("c2"))
	require.NotNil(t, row)
	assert.True(t, row.GetBool(typetype.ShortIdFieldRemoved))
	assert.Equal(t, "head-1", string(row.GetStringBytes(typetype.ShortIdFieldDefId)))
	assert.Equal(t, typetype.ShortIdSrcDatasets, string(row.GetStringBytes(typetype.ShortIdFieldSrc)))
}

func TestCompileDatasetDefs_FoldsRecords(t *testing.T) {
	ctrl, db := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	// Head + three fields (one stamped, one author-mutable, one shaped).
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v1", "c1", "head-1", true,
		headPayload(arena, "notes", map[string]any{
			typetype.DefFieldIdRule:   "user",
			typetype.DefFieldDeleteBy: "author",
		}))))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v2", "c2", "f-title", true,
		fieldPayload(arena, "head-1", "title", "string", map[string]any{typetype.DefFieldRequired: true}))))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v3", "c3", "f-body", true,
		fieldPayload(arena, "head-1", "body", "string", map[string]any{typetype.DefFieldMutableBy: "author"}))))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v4", "c4", "f-creator", true,
		fieldPayload(arena, "head-1", "creator", "string", map[string]any{typetype.DefFieldStamp: "creator"}))))
	// Orphan field (unknown head) — folded out.
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v5", "c5", "f-orphan", true,
		fieldPayload(arena, "head-nope", "ghost", "string", nil))))
	// Duplicate key — the earlier creation (_ver.id v2) wins.
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v6", "c6", "f-title-dup", true,
		fieldPayload(arena, "head-1", "title", "number", nil))))

	compiled, err := types.CompileDatasetDefs(ctx, db, testObjectId)
	require.NoError(t, err)
	require.Len(t, compiled, 1)
	ds := compiled[0]
	assert.Equal(t, "notes", ds.Name)
	assert.Equal(t, "head-1", ds.DefId)
	assert.Equal(t, schema.IdUser, ds.Schema.IdRule)
	assert.Equal(t, schema.DeleteByAuthor, ds.Schema.DeleteBy)
	require.Len(t, ds.Schema.Fields, 3)

	byId := map[string]schema.Field{}
	for _, f := range ds.Schema.Fields {
		byId[f.Id] = f
	}
	assert.Equal(t, schema.KindString, byId["title"].Schema.Kind, "first writer wins the duplicate key")
	assert.True(t, byId["title"].Required)
	assert.Equal(t, schema.MutableByAuthor, byId["body"].MutableBy)
	assert.Equal(t, schema.StampCreator, byId["creator"].Stamp)
	assert.Equal(t, schema.ScopeDerived, byId["creator"].Scope, "stamp normalizes to derived scope")
}

func TestCompileDatasetDefs_SearchTextForms(t *testing.T) {
	ctrl, db := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	// String wire form parses to a one-key mapping.
	single := arena.NewObject()
	single.Set(typetype.SearchKeyTitle, arena.NewString("title"))
	single.Set(typetype.SearchKeyText, arena.NewString("body"))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v1", "c1", "head-single", true,
		headPayload(arena, "articles", map[string]any{typetype.DefFieldSearch: single}))))

	// Array wire form keeps declaration order.
	multiArr := arena.NewArray()
	multiArr.SetArrayItem(0, arena.NewString("body"))
	multiArr.SetArrayItem(1, arena.NewString("notes"))
	multi := arena.NewObject()
	multi.Set(typetype.SearchKeyText, multiArr)
	multi.Set(typetype.SearchKeyScope, arena.NewString("email"))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v2", "c2", "head-multi", true,
		headPayload(arena, "emails", map[string]any{typetype.DefFieldSearch: multi}))))

	// A duplicate-key array (a raw peer write — the create hook doesn't
	// inspect search) folds Invalid via ValidateDatasetDecl: visible for
	// repair, never registered.
	dupArr := arena.NewArray()
	dupArr.SetArrayItem(0, arena.NewString("body"))
	dupArr.SetArrayItem(1, arena.NewString("body"))
	dup := arena.NewObject()
	dup.Set(typetype.SearchKeyText, dupArr)
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v3", "c3", "head-dup", true,
		headPayload(arena, "dups", map[string]any{typetype.DefFieldSearch: dup}))))

	compiled, err := types.CompileDatasetDefs(ctx, db, testObjectId)
	require.NoError(t, err)
	require.Len(t, compiled, 3)
	byName := map[string]types.CompiledDataset{}
	for _, c := range compiled {
		byName[c.Name] = c
	}

	require.NotNil(t, byName["articles"].Search)
	assert.Equal(t, []string{"body"}, byName["articles"].Search.Text)
	require.NotNil(t, byName["emails"].Search)
	assert.Equal(t, []string{"body", "notes"}, byName["emails"].Search.Text)
	assert.Equal(t, "email", byName["emails"].Search.Scope)
	assert.True(t, byName["dups"].Invalid)
	assert.NotEmpty(t, byName["dups"].InvalidReason)
}

func TestCompileDatasetDefs_AuthorRuleWithoutCreatorStampMarksInvalid(t *testing.T) {
	ctrl, db := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	// deleteBy author but no creator-stamped field: the folded decl
	// fails validation. The dataset stays VISIBLE (so it can be
	// repaired or removed) but marked Invalid — the catalog layer
	// never registers it.
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v1", "c1", "head-1", true,
		headPayload(arena, "notes", map[string]any{typetype.DefFieldDeleteBy: "author"}))))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v2", "c2", "f-a", true,
		fieldPayload(arena, "head-1", "a", "string", nil))))

	compiled, err := types.CompileDatasetDefs(ctx, db, testObjectId)
	require.NoError(t, err)
	require.Len(t, compiled, 1)
	assert.True(t, compiled[0].Invalid)
	assert.NotEmpty(t, compiled[0].InvalidReason)
	assert.Empty(t, compiled[0].SchemaRev)

	// Adding the creator stamp repairs the declaration.
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v3", "c3", "f-creator", true,
		fieldPayload(arena, "head-1", "creator", "string", map[string]any{typetype.DefFieldStamp: "creator"}))))
	compiled, err = types.CompileDatasetDefs(ctx, db, testObjectId)
	require.NoError(t, err)
	require.Len(t, compiled, 1)
	assert.False(t, compiled[0].Invalid)
	assert.NotEmpty(t, compiled[0].SchemaRev)
}

func TestCompileDatasetDefs_DuplicateNameFirstWriterWins(t *testing.T) {
	ctrl, db := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v1", "c1", "head-1", true,
		headPayload(arena, "notes", nil))))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v2", "c2", "head-2", true,
		headPayload(arena, "notes", map[string]any{typetype.DefFieldDynamic: true}))))

	compiled, err := types.CompileDatasetDefs(ctx, db, testObjectId)
	require.NoError(t, err)
	require.Len(t, compiled, 1)
	assert.Equal(t, "head-1", compiled[0].DefId)
	assert.False(t, compiled[0].Schema.Dynamic)
}

func TestCompileDatasetDefs_TombstonedHeadDropsDataset(t *testing.T) {
	ctrl, db := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v1", "c1", "head-1", true,
		headPayload(arena, "notes", nil))))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v2", "c2", "head-1", false,
		crdt.Op{Type: crdt.OpDelete})))

	compiled, err := types.CompileDatasetDefs(ctx, db, testObjectId)
	require.NoError(t, err)
	assert.Empty(t, compiled)
}
