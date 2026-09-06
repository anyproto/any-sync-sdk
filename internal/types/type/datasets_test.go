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

// testPartId is the part every head in these tests hangs off; seedPart
// creates it.
const testPartId = "part-1"

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

func partPayload(arena *anyenc.Arena, key string, extra map[string]any) crdt.Op {
	fields := map[string]any{
		typetype.DefFieldDef: typetype.DefKindPart,
		typetype.FieldKey:    key,
	}
	for k, v := range extra {
		fields[k] = v
	}
	return setMulti(arena, fields)
}

// seedPart creates the part the test heads reference.
func seedPart(t *testing.T, ctrl *crdt.Controller, arena *anyenc.Arena) {
	t.Helper()
	require.NoError(t, ctrl.ApplyChange(context.Background(), defsChange("p0", "cp0", testPartId, true,
		partPayload(arena, "body", nil))))
}

func headPayload(arena *anyenc.Arena, key string, extra map[string]any) crdt.Op {
	fields := map[string]any{
		typetype.DefFieldDef:    typetype.DefKindDataset,
		typetype.FieldKey:       key,
		typetype.DefFieldModule: types.RecordsModule,
		typetype.DefFieldPart:   testPartId,
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

	// A part creation projects one too — parts are schema state.
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v2", "change-create-part", "part-x", true,
		partPayload(arena, "body", nil))))
	row = ctrl.Get(ctx, typetype.ShortIdsDataset, crdt.DeriveRecordId("change-create-part"))
	require.NotNil(t, row)
	assert.Equal(t, "part-x", string(row.GetStringBytes(typetype.ShortIdFieldDefId)))
}

func TestDatasetDefs_CreateRejections(t *testing.T) {
	ctrl, _ := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	noModule := map[string]any{
		typetype.DefFieldDef:  typetype.DefKindDataset,
		typetype.FieldKey:     "ok",
		typetype.DefFieldPart: testPartId,
	}
	noPart := map[string]any{
		typetype.DefFieldDef:    typetype.DefKindDataset,
		typetype.FieldKey:       "ok",
		typetype.DefFieldModule: types.RecordsModule,
	}
	cases := []struct {
		name string
		op   crdt.Op
	}{
		{"missing def", setMulti(arena, map[string]any{typetype.FieldKey: "x"})},
		{"unknown def", setMulti(arena, map[string]any{typetype.DefFieldDef: "bogus"})},
		{"underscore key", headPayload(arena, "_sneaky", nil)},
		{"uppercase key", headPayload(arena, "Notes", nil)},
		{"dotted key", headPayload(arena, "a.b", nil)},
		{"missing module", setMulti(arena, noModule)},
		{"bad module slug", headPayload(arena, "ok0", map[string]any{typetype.DefFieldModule: "Bad Module"})},
		{"missing part", setMulti(arena, noPart)},
		{"bad idRule", headPayload(arena, "ok1", map[string]any{typetype.DefFieldIdRule: "bogus"})},
		{"bad deleteBy", headPayload(arena, "ok2", map[string]any{typetype.DefFieldDeleteBy: "bogus"})},
		{"part bad key", partPayload(arena, "9lives", nil)},
		{"part ui not an object", partPayload(arena, "ok3", map[string]any{typetype.PartFieldUI: "table"})},
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
		{Type: crdt.OpSet, Path: []string{typetype.FieldKey}, Payload: arena.NewString("renamed")},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldDef}, Payload: arena.NewString("field")},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldModule}, Payload: arena.NewString("editor")},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldShared}, Payload: arena.NewTrue()},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldPart}, Payload: arena.NewString("part-2")},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldIdRule}, Payload: arena.NewString("user")},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldDeleteBy}, Payload: arena.NewString("anyone")},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldSkipHistory}, Payload: arena.NewTrue()},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldSearch}, Payload: arena.NewObject()},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldSearch, "bogus"}, Payload: arena.NewString("x")},
		{Type: crdt.OpUnset, Path: []string{typetype.FieldKey}},
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

	// A part record: the key is pinned, the display slice mutates, `ui`
	// is written whole as an object.
	ui := arena.NewObject()
	ui.Set("type", arena.NewString("table"))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("q1", "cq1", "part-1", true,
		partPayload(arena, "body", map[string]any{typetype.PartFieldUI: ui}))))
	uses := arena.NewArray()
	uses.SetArrayItem(0, arena.NewString("notes"))
	ui2 := arena.NewObject()
	ui2.Set("type", arena.NewString("list"))
	partMutable := []crdt.Op{
		{Type: crdt.OpSet, Path: []string{typetype.FieldName}, Payload: arena.NewString("Body")},
		{Type: crdt.OpSet, Path: []string{typetype.PartFieldIcon}, Payload: arena.NewString("doc")},
		{Type: crdt.OpSet, Path: []string{typetype.PartFieldPos}, Payload: arena.NewString("a1")},
		{Type: crdt.OpSet, Path: []string{typetype.PartFieldHidden}, Payload: arena.NewTrue()},
		{Type: crdt.OpSet, Path: []string{typetype.PartFieldUI}, Payload: ui2},
		{Type: crdt.OpSet, Path: []string{typetype.PartFieldUses}, Payload: uses},
		{Type: crdt.OpUnset, Path: []string{typetype.PartFieldUI}},
	}
	for i, op := range partMutable {
		res, err := ctrl.ApplyChangeWithResult(ctx,
			defsChange(crdt.VersionId("qm"+string(rune('a'+i))), "cqm"+string(rune('a'+i)), "part-1", false, op))
		require.NoError(t, err)
		require.Empty(t, res.Rejections, "part op %d must be mutable", i)
	}
	partPinned := []crdt.Op{
		{Type: crdt.OpSet, Path: []string{typetype.FieldKey}, Payload: arena.NewString("other")},
		{Type: crdt.OpSet, Path: []string{typetype.PartFieldUI}, Payload: arena.NewString("table")},
	}
	for i, op := range partPinned {
		res, err := ctrl.ApplyChangeWithResult(ctx,
			defsChange(crdt.VersionId("qp"+string(rune('a'+i))), "cqp"+string(rune('a'+i)), "part-1", false, op))
		require.NoError(t, err)
		require.Len(t, res.Rejections, 1, "part op %d must reject", i)
	}
	assert.True(t, typetype.IsPartPinnedPath([]string{typetype.FieldKey}))
	assert.True(t, typetype.IsPartPinnedPath([]string{typetype.PartFieldUI, "type"}), "ui is written whole")
	assert.False(t, typetype.IsPartPinnedPath([]string{typetype.PartFieldUI}))
	assert.False(t, typetype.IsPartPinnedPath([]string{typetype.PartFieldPos}))

	// A field record: the descriptor bag is created whole and then every
	// path under it mutates with any value type; the behavioral
	// declaration stays pinned.
	xf := arena.NewObject()
	xf.Set("type", arena.NewString("choice"))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("f1", "cf1", "field-1", true,
		fieldPayload(arena, "head-1", "stage", "array", map[string]any{typetype.FieldXFormat: xf}))))
	fieldMutable := []crdt.Op{
		{Type: crdt.OpSet, Path: []string{typetype.FieldXFormat, "type"}, Payload: arena.NewString("relation")},
		{Type: crdt.OpSet, Path: []string{typetype.FieldXFormat, "config", "multiple"}, Payload: arena.NewTrue()},
		{Type: crdt.OpSet, Path: []string{typetype.FieldXFormat, "options", "won", "name"}, Payload: arena.NewString("Won")},
		{Type: crdt.OpUnset, Path: []string{typetype.FieldXFormat, "options", "won"}},
		{Type: crdt.OpSet, Path: []string{typetype.FieldDescription}, Payload: arena.NewString("descr")},
	}
	for i, op := range fieldMutable {
		res, err := ctrl.ApplyChangeWithResult(ctx,
			defsChange(crdt.VersionId("fm"+string(rune('a'+i))), "cfm"+string(rune('a'+i)), "field-1", false, op))
		require.NoError(t, err)
		require.Empty(t, res.Rejections, "field op %d must be mutable", i)
	}
	fieldPinned := []crdt.Op{
		{Type: crdt.OpSet, Path: []string{typetype.FieldKind}, Payload: arena.NewString("string")},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldRequired}, Payload: arena.NewTrue()},
		{Type: crdt.OpSet, Path: []string{typetype.DefFieldMutableBy}, Payload: arena.NewString("any")},
	}
	for i, op := range fieldPinned {
		res, err := ctrl.ApplyChangeWithResult(ctx,
			defsChange(crdt.VersionId("fp"+string(rune('a'+i))), "cfp"+string(rune('a'+i)), "field-1", false, op))
		require.NoError(t, err)
		require.Len(t, res.Rejections, 1, "field op %d must be pinned", i)
	}
	field := ctrl.Get(ctx, typetype.DatasetDefs, "field-1")
	assert.Equal(t, "relation", field.GetString(typetype.FieldXFormat, "type"))
	assert.True(t, field.GetBool(typetype.FieldXFormat, "config", "multiple"))
	assert.Nil(t, field.Get(typetype.FieldXFormat, "options", "won"))
	assert.Equal(t, "array", field.GetString(typetype.FieldKind))

	// The one creation rule on the bag: an object, written whole.
	bad := arena.NewObject()
	bad.Set(typetype.DefFieldDef, arena.NewString(typetype.DefKindField))
	bad.Set(typetype.DefFieldDataset, arena.NewString("head-1"))
	bad.Set(typetype.FieldKey, arena.NewString("broken"))
	bad.Set(typetype.FieldKind, arena.NewString("string"))
	bad.Set(typetype.FieldXFormat, arena.NewString("email"))
	badRes, badErr := ctrl.ApplyChangeWithResult(ctx, defsChange("fb", "cfb", "field-bad", true,
		crdt.Op{Type: crdt.OpSet, Payload: bad}))
	require.NoError(t, badErr)
	require.Len(t, badRes.Rejections, 1, "a non-object x-format must reject the field create")

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
	payload.Set(typetype.FieldKey, arena.NewString("hax"))
	payload.Set(typetype.DefFieldDisplayName, arena.NewString("kept"))
	res, err = ctrl.ApplyChangeWithResult(ctx, defsChange("vy", "cy", "head-1", false,
		crdt.Op{Type: crdt.OpSet, Payload: payload}))
	require.NoError(t, err)
	require.NotEmpty(t, res.Rejections)
	head := ctrl.Get(ctx, typetype.DatasetDefs, "head-1")
	assert.Equal(t, "notes", string(head.GetStringBytes(typetype.FieldKey)))
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
	seedPart(t, ctrl, arena)

	// Head + three fields (one stamped, one author-mutable, one shaped).
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v1", "c1", "head-1", true,
		headPayload(arena, "notes", map[string]any{
			typetype.DefFieldIdRule:   "user",
			typetype.DefFieldDeleteBy: "author",
		}))))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v2", "c2", "f-title", true,
		fieldPayload(arena, "head-1", "title", "string", map[string]any{typetype.DefFieldRequired: true}))))
	bodyXF := arena.NewObject()
	bodyXF.Set("type", arena.NewString("longtext"))
	bodyXF.Set("icon", arena.NewString("text"))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v3", "c3", "f-body", true,
		fieldPayload(arena, "head-1", "body", "string", map[string]any{
			typetype.DefFieldMutableBy: "author",
			typetype.FieldDescription:  "The article body",
			typetype.FieldXFormat:      bodyXF,
		}))))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v4", "c4", "f-creator", true,
		fieldPayload(arena, "head-1", "creator", "string", map[string]any{typetype.DefFieldStamp: "creator"}))))
	// Orphan field (unknown head) — folded out.
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v5", "c5", "f-orphan", true,
		fieldPayload(arena, "head-nope", "ghost", "string", nil))))
	// Duplicate key — the earlier creation (_ver.id v2) wins.
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v6", "c6", "f-title-dup", true,
		fieldPayload(arena, "head-1", "title", "number", nil))))
	// Orphan head (unknown part) — folded out.
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v7", "c7", "head-orphan", true,
		headPayload(arena, "ghosts", map[string]any{typetype.DefFieldPart: "part-nope"}))))

	compiled, err := types.CompileDatasetDefs(ctx, db, testObjectId, nil)
	require.NoError(t, err)
	require.Len(t, compiled, 1)
	ds := compiled[0]
	assert.Equal(t, "notes", ds.Key)
	assert.Equal(t, testObjectId+"_notes", ds.Name, "a records dataset is namespaced under its type")
	assert.Equal(t, types.RecordsModule, ds.Module)
	assert.Equal(t, testPartId, ds.PartId)
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
	assert.Equal(t, "The article body", byId["body"].Description)
	assert.Equal(t, map[string]any{"type": "longtext", "icon": "text"}, byId["body"].XFormat)
	assert.Equal(t, schema.StampCreator, byId["creator"].Stamp)
	assert.Equal(t, schema.ScopeDerived, byId["creator"].Scope, "stamp normalizes to derived scope")

	// The descriptive slice is outside the schema revision: editing it
	// must not re-register the dataset.
	rev := ds.SchemaRev
	require.NotEmpty(t, rev)
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v8", "c8", "f-body", false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldXFormat, "type"}, Payload: arena.NewString("text")})))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v9", "c9", "f-body", false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldDescription}, Payload: arena.NewString("edited")})))
	compiled, err = types.CompileDatasetDefs(ctx, db, testObjectId, nil)
	require.NoError(t, err)
	require.Len(t, compiled, 1)
	assert.Equal(t, rev, compiled[0].SchemaRev, "descriptive edits leave the schema revision alone")
	for _, f := range compiled[0].Schema.Fields {
		if f.Id == "body" {
			assert.Equal(t, "text", f.XFormat["type"])
			assert.Equal(t, "edited", f.Description)
		}
	}
	// Discovery renders both.
	raw, err := compiled[0].Schema.MarshalJSON()
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"x-format":{"icon":"text","type":"text"}`)
	assert.Contains(t, string(raw), `"description":"edited"`)

	// The part view carries the same dataset.
	ct, err := types.CompileTypeParts(ctx, db, testObjectId, nil)
	require.NoError(t, err)
	require.Len(t, ct.Parts, 1)
	assert.Equal(t, "body", ct.Parts[0].Key)
	assert.Equal(t, testPartId, ct.Parts[0].Id)
	require.Len(t, ct.Parts[0].Datasets, 1)
	assert.Equal(t, "notes", ct.Parts[0].Datasets[0].Key)
}

func TestCompileDatasetDefs_SearchTextForms(t *testing.T) {
	ctrl, db := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}
	seedPart(t, ctrl, arena)

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

	compiled, err := types.CompileDatasetDefs(ctx, db, testObjectId, nil)
	require.NoError(t, err)
	require.Len(t, compiled, 3)
	byKey := map[string]types.CompiledDataset{}
	for _, c := range compiled {
		byKey[c.Key] = c
	}

	require.NotNil(t, byKey["articles"].Search)
	assert.Equal(t, []string{"body"}, byKey["articles"].Search.Text)
	require.NotNil(t, byKey["emails"].Search)
	assert.Equal(t, []string{"body", "notes"}, byKey["emails"].Search.Text)
	assert.Equal(t, "email", byKey["emails"].Search.Scope)
	assert.True(t, byKey["dups"].Invalid)
	assert.NotEmpty(t, byKey["dups"].InvalidReason)
}

func TestCompileDatasetDefs_AuthorRuleWithoutCreatorStampMarksInvalid(t *testing.T) {
	ctrl, db := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}
	seedPart(t, ctrl, arena)

	// deleteBy author but no creator-stamped field: the folded decl
	// fails validation. The dataset stays VISIBLE (so it can be
	// repaired or removed) but marked Invalid — the catalog layer
	// never registers it.
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v1", "c1", "head-1", true,
		headPayload(arena, "notes", map[string]any{typetype.DefFieldDeleteBy: "author"}))))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v2", "c2", "f-a", true,
		fieldPayload(arena, "head-1", "a", "string", nil))))

	compiled, err := types.CompileDatasetDefs(ctx, db, testObjectId, nil)
	require.NoError(t, err)
	require.Len(t, compiled, 1)
	assert.True(t, compiled[0].Invalid)
	assert.NotEmpty(t, compiled[0].InvalidReason)
	assert.Empty(t, compiled[0].SchemaRev)

	// Adding the creator stamp repairs the declaration.
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v3", "c3", "f-creator", true,
		fieldPayload(arena, "head-1", "creator", "string", map[string]any{typetype.DefFieldStamp: "creator"}))))
	compiled, err = types.CompileDatasetDefs(ctx, db, testObjectId, nil)
	require.NoError(t, err)
	require.Len(t, compiled, 1)
	assert.False(t, compiled[0].Invalid)
	assert.NotEmpty(t, compiled[0].SchemaRev)
}

// Two concurrent declarations of one key name the same collection by
// construction: the earlier creation is the definition's identity,
// fields union across both heads, and a disagreement on a pinned leaf
// marks the definition invalid.
func TestCompileDatasetDefs_DuplicateKeyUnionsFields(t *testing.T) {
	ctrl, db := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}
	seedPart(t, ctrl, arena)

	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v1", "c1", "head-1", true,
		headPayload(arena, "notes", nil))))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v2", "c2", "head-2", true,
		headPayload(arena, "notes", nil))))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v3", "c3", "f-a", true,
		fieldPayload(arena, "head-1", "a", "string", nil))))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v4", "c4", "f-b", true,
		fieldPayload(arena, "head-2", "b", "string", nil))))

	compiled, err := types.CompileDatasetDefs(ctx, db, testObjectId, nil)
	require.NoError(t, err)
	require.Len(t, compiled, 1)
	assert.Equal(t, "head-1", compiled[0].DefId)
	assert.False(t, compiled[0].Invalid)
	require.Len(t, compiled[0].Schema.Fields, 2, "fields union across duplicate heads")

	// A third head disagreeing on a pinned leaf invalidates the fold.
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v5", "c5", "head-3", true,
		headPayload(arena, "notes", map[string]any{typetype.DefFieldDynamic: true}))))
	compiled, err = types.CompileDatasetDefs(ctx, db, testObjectId, nil)
	require.NoError(t, err)
	require.Len(t, compiled, 1)
	assert.True(t, compiled[0].Invalid)
	assert.Contains(t, compiled[0].InvalidReason, "pinned")
}

func TestCompileDatasetDefs_TombstonesDrop(t *testing.T) {
	ctrl, db := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}
	seedPart(t, ctrl, arena)

	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v1", "c1", "head-1", true,
		headPayload(arena, "notes", nil))))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v2", "c2", "head-1", false,
		crdt.Op{Type: crdt.OpDelete})))

	compiled, err := types.CompileDatasetDefs(ctx, db, testObjectId, nil)
	require.NoError(t, err)
	assert.Empty(t, compiled)

	// A tombstoned part takes its datasets with it: the heads go orphan.
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v3", "c3", "head-2", true,
		headPayload(arena, "tasks", nil))))
	compiled, err = types.CompileDatasetDefs(ctx, db, testObjectId, nil)
	require.NoError(t, err)
	require.Len(t, compiled, 1)
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v4", "c4", testPartId, false,
		crdt.Op{Type: crdt.OpDelete})))
	compiled, err = types.CompileDatasetDefs(ctx, db, testObjectId, nil)
	require.NoError(t, err)
	assert.Empty(t, compiled)
}

// Parts fold by key (display from the earlier creation, datasets and
// uses unioned) and the collection rule places every dataset: a shared
// dataset on the module's canonical collection, a namespaced one under
// the type; module-served datasets carry no fields; violations of the
// rule compile invalid.
func TestCompileTypeParts_ModulesAndCollections(t *testing.T) {
	ctrl, db := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}
	modules := types.NewModules(
		types.ModuleInfo{Name: "editor", Canonical: "editor_blocks"},
		types.ModuleInfo{Name: "chat", Canonical: "chat_messages", SharedOnly: true},
	)

	usesA := arena.NewArray()
	usesA.SetArrayItem(0, arena.NewString("summary"))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("p1", "cp1", "part-a", true,
		partPayload(arena, "body", map[string]any{typetype.FieldName: "Body", typetype.PartFieldUses: usesA}))))
	// Concurrent duplicate of the same part: later creation, its
	// display loses, its datasets and uses join.
	usesB := arena.NewArray()
	usesB.SetArrayItem(0, arena.NewString("nope"))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("p2", "cp2", "part-b", true,
		partPayload(arena, "body", map[string]any{typetype.FieldName: "Loser", typetype.PartFieldUses: usesB}))))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("p3", "cp3", "part-c", true,
		partPayload(arena, "chat", map[string]any{typetype.FieldName: "Chat"}))))

	heads := []struct {
		id, key, module, part string
		shared                bool
	}{
		{"h-shared", "editor_blocks", "editor", "part-a", true},
		{"h-summary", "summary", "editor", "part-b", false},
		{"h-badkey", "notes", "editor", "part-a", true}, // shared key must be the canonical
		{"h-thread", "thread", "chat", "part-c", false}, // chat admits shared only
		{"h-chat", "chat_messages", "chat", "part-c", true},
		{"h-unknown", "x", "sketch", "part-a", false}, // no such module
		{"h-records", "segments", types.RecordsModule, "part-a", false},
	}
	for i, h := range heads {
		extra := map[string]any{typetype.DefFieldModule: h.module, typetype.DefFieldPart: h.part}
		if h.shared {
			extra[typetype.DefFieldShared] = true
		}
		require.NoError(t, ctrl.ApplyChange(ctx, defsChange(crdt.VersionId("h"+string(rune('a'+i))), "ch"+h.id, h.id, true,
			headPayload(arena, h.key, extra))))
	}
	// A field on a module-served dataset is an orphan: the module owns
	// the schema.
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("f1", "cf1", "f-editor", true,
		fieldPayload(arena, "h-shared", "text", "string", nil))))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("f2", "cf2", "f-seg", true,
		fieldPayload(arena, "h-records", "speaker", "string", nil))))

	ct, err := types.CompileTypeParts(ctx, db, testObjectId, modules)
	require.NoError(t, err)
	require.Len(t, ct.Parts, 2, "duplicate parts fold")
	body := ct.Parts[0]
	assert.Equal(t, "body", body.Key)
	assert.Equal(t, "part-a", body.Id, "the earlier creation is the part's identity")
	assert.Equal(t, "Body", body.Name)
	assert.Equal(t, []string{"summary"}, body.Uses, "uses union, unknown keys dropped")
	assert.Equal(t, "chat", ct.Parts[1].Key)

	byKey := map[string]types.CompiledDataset{}
	for _, ds := range ct.Datasets {
		byKey[ds.Key] = ds
	}
	shared := byKey["editor_blocks"]
	assert.Equal(t, "editor_blocks", shared.Name, "a shared dataset is the canonical collection")
	assert.True(t, shared.Shared)
	assert.False(t, shared.Invalid)
	assert.NotEmpty(t, shared.SchemaRev)
	assert.Empty(t, shared.Schema.Fields, "module-served: no fields")
	assert.Equal(t, "part-a", shared.PartId)

	summary := byKey["summary"]
	assert.Equal(t, testObjectId+"_summary", summary.Name)
	assert.Equal(t, "editor", summary.Module)
	assert.False(t, summary.Invalid)
	assert.Equal(t, "part-a", summary.PartId, "a head under the losing duplicate attaches to the winner")

	assert.True(t, byKey["notes"].Invalid, "shared editor keyed other than the canonical")
	assert.True(t, byKey["thread"].Invalid, "namespaced chat is refused")
	assert.False(t, byKey["chat_messages"].Invalid)
	assert.Equal(t, "chat_messages", byKey["chat_messages"].Name)
	assert.True(t, byKey["x"].Invalid)
	assert.Contains(t, byKey["x"].InvalidReason, "unknown module")

	seg := byKey["segments"]
	assert.False(t, seg.Invalid)
	assert.Equal(t, testObjectId+"_segments", seg.Name)
	require.Len(t, seg.Schema.Fields, 1, "records datasets keep their fields")

	// Parts carry their datasets in key order, invalid ones included.
	var bodyKeys []string
	for _, ds := range body.Datasets {
		bodyKeys = append(bodyKeys, ds.Key)
	}
	assert.Equal(t, []string{"editor_blocks", "notes", "segments", "summary", "x"}, bodyKeys)

	// A second shared editor dataset (a different key cannot be the
	// canonical, so re-declare the canonical under another part): the
	// earlier creation keeps it.
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("h9", "ch9", "h-shared-2", true,
		headPayload(arena, "editor_blocks", map[string]any{
			typetype.DefFieldModule: "editor", typetype.DefFieldPart: "part-c", typetype.DefFieldShared: true,
		}))))
	ct, err = types.CompileTypeParts(ctx, db, testObjectId, modules)
	require.NoError(t, err)
	byKey = map[string]types.CompiledDataset{}
	for _, ds := range ct.Datasets {
		byKey[ds.Key] = ds
	}
	assert.True(t, byKey["editor_blocks"].Invalid, "two heads of one key disagreeing on the part fold invalid")
}

// A head record carries no descriptor: the create rule refuses one and
// the head preflight pins the path, while the (kind-blind) apply-time
// handler keeps admitting it for field records.
func TestDatasetDefs_HeadCarriesNoXFormat(t *testing.T) {
	ctrl, _ := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	xf := arena.NewObject()
	xf.Set("icon", arena.NewString("book"))
	res, err := ctrl.ApplyChangeWithResult(ctx, defsChange("h1", "ch1", "head-xf", true,
		headPayload(arena, "notes", map[string]any{typetype.FieldXFormat: xf})))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1, "a head create carrying x-format must reject")

	assert.True(t, typetype.IsDatasetDefPinnedPath([]string{typetype.FieldXFormat, "icon"}))
	assert.True(t, typetype.IsDatasetDefPinnedPath([]string{typetype.FieldXFormat}))
	assert.False(t, typetype.IsDatasetFieldPinnedPath([]string{typetype.FieldXFormat, "icon"}))

	// On a field record the whole-bag set must be an object.
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("h2", "ch2", "head-1", true, headPayload(arena, "notes", nil))))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("f1", "cf1", "field-1", true,
		fieldPayload(arena, "head-1", "title", "string", nil))))
	res, err = ctrl.ApplyChangeWithResult(ctx, defsChange("f2", "cf2", "field-1", false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldXFormat}, Payload: arena.NewString("text")}))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1, "scalar over a field's bag must drop")
	bag := arena.NewObject()
	bag.Set("type", arena.NewString("text"))
	res, err = ctrl.ApplyChangeWithResult(ctx, defsChange("f3", "cf3", "field-1", false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldXFormat}, Payload: bag}))
	require.NoError(t, err)
	require.Empty(t, res.Rejections)
}

// Whatever a client-side preflight admits, the apply-time handler
// admits too — otherwise a multi-op patch would half-apply. Pins the
// containment for all three record kinds over the discriminating paths.
func TestDatasetDefs_PreflightSubsetOfHandler(t *testing.T) {
	ctrl, _ := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v0", "c0", "part-1", true, partPayload(arena, "body", nil))))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v1", "c1", "head-1", true, headPayload(arena, "notes", nil))))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v2", "c2", "field-1", true,
		fieldPayload(arena, "head-1", "title", "string", nil))))

	paths := [][]string{
		{typetype.FieldName}, {typetype.FieldDescription}, {typetype.DefFieldDisplayName},
		{typetype.DefFieldSearch, typetype.SearchKeyTitle}, {typetype.DefFieldSearch, typetype.SearchKeyText},
		{typetype.DefFieldSearch, typetype.SearchKeyScope},
		{typetype.FieldXFormat, "icon"}, {typetype.FieldXFormat, "options", "a", "name"},
		{typetype.PartFieldIcon}, {typetype.PartFieldPos},
		{typetype.FieldKey}, {typetype.FieldKind}, {typetype.DefFieldRequired}, {typetype.DefFieldModule},
	}
	for i, p := range paths {
		op := crdt.Op{Type: crdt.OpSet, Path: p, Payload: arena.NewString("x")}
		for _, rec := range []struct {
			id      string
			pinned  bool
			preflit string
		}{
			{"part-1", typetype.IsPartPinnedPath(p), "part"},
			{"head-1", typetype.IsDatasetDefPinnedPath(p), "head"},
			{"field-1", typetype.IsDatasetFieldPinnedPath(p), "field"},
		} {
			res, err := ctrl.ApplyChangeWithResult(ctx, defsChange(
				crdt.VersionId("s"+rec.preflit+string(rune('a'+i))), "cs"+rec.preflit+string(rune('a'+i)), rec.id, false, op))
			require.NoError(t, err)
			if !rec.pinned {
				assert.Empty(t, res.Rejections, "%s preflight admits %v, the handler must too", rec.preflit, p)
			}
		}
	}
}
