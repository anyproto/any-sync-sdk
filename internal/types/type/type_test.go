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
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
)

// Test setup: a Controller wired with PropertyHandler on the
// "properties" dataset and a DefaultHandler on the "shortIds" sibling
// dataset so projections from BeforeCreate / BeforeDelete have somewhere
// to land. Real wiring lives in internal/object once type-object trees
// are loaded; tests do it inline.

const (
	testObjectId = "type-1"
	testDataVer  = "typePropertyHandler-v1" // matches typetype.HandlerVersion
)

func newTypeController(t *testing.T) *crdt.Controller {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := anystore.Open(context.Background(), dbPath, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctrl, err := crdt.NewController(context.Background(), testObjectId, db,
		crdt.HandlerReg{Name: typetype.DatasetPropertyDefs, Handler: typetype.PropertyHandler{}, Schema: schema.Dataset{Dynamic: true}},
		crdt.HandlerReg{Name: typetype.ShortIdsDataset, Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}},
	)
	require.NoError(t, err)
	return ctrl
}

// makeChange builds a Change targeting the properties dataset.
func makeChange(versionId crdt.VersionId, changeId string, recId string, upsert bool, ops ...crdt.Op) crdt.Change {
	return crdt.Change{
		ObjectId:    testObjectId,
		Dataset:     typetype.DatasetPropertyDefs,
		ChangeId:    changeId,
		VersionId:   versionId,
		DataVersion: testDataVer,
		Records: []crdt.RecordChange{
			{Id: recId, Upsert: upsert, Ops: ops},
		},
	}
}

func setMulti(arena *anyenc.Arena, fields map[string]any) crdt.Op {
	obj := arena.NewObject()
	for k, v := range fields {
		switch x := v.(type) {
		case string:
			obj.Set(k, arena.NewString(x))
		case bool:
			if x {
				obj.Set(k, arena.NewTrue())
			} else {
				obj.Set(k, arena.NewFalse())
			}
		default:
			panic("setMulti: unsupported type")
		}
	}
	return crdt.Op{Type: crdt.OpSet, Payload: obj}
}

// ----------------------------------------------------------------------------
// BeforeCreate — kind extraction + shortId projection
// ----------------------------------------------------------------------------

func TestPropertyHandler_CreateProjectsShortId(t *testing.T) {
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	const propId = "prop-actors"
	const changeId = "change-create-actors"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", changeId, propId, true,
		setMulti(arena, map[string]any{
			typetype.FieldKey:  "actors",
			typetype.FieldKind: "array",
			typetype.FieldName: "Actors",
		}),
	)))

	// Property record landed.
	rec := ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
	require.NotNil(t, rec)
	assert.Equal(t, "actors", rec.GetString(typetype.FieldKey))
	assert.Equal(t, "array", rec.GetString(typetype.FieldKind))

	// ShortIds row exists keyed by base58(xxh3-64(changeId)).
	shortId := crdt.DeriveRecordId(changeId)
	row := ctrl.Get(context.Background(), typetype.ShortIdsDataset, shortId)
	require.NotNil(t, row, "shortIds projection missing")
	assert.Equal(t, changeId, row.GetString(typetype.ShortIdFieldChangeId))
	assert.Equal(t, propId, row.GetString(typetype.ShortIdFieldPropId))
	assert.Equal(t, "array", row.GetString(typetype.ShortIdFieldKind))
	assert.Nil(t, row.Get(typetype.ShortIdFieldRemoved), "create row must not carry removed flag")

	// Both records carry the same versionId — atomic projection.
	assert.Equal(t, "v1", rec.GetString("_ver", "id"))
	assert.Equal(t, "v1", row.GetString("_ver", "id"))
}

func TestPropertyHandler_CreateRejectedWithoutKind(t *testing.T) {
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	const propId = "prop-bare"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", "ch-no-kind", propId, true,
		setMulti(arena, map[string]any{
			typetype.FieldName: "missing-kind",
		}),
	)))

	// BeforeCreate dropped the record; nothing landed.
	assert.Nil(t, ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId))
	// And no shortIds row was projected.
	shortId := crdt.DeriveRecordId("ch-no-kind")
	assert.Nil(t, ctrl.Get(context.Background(), typetype.ShortIdsDataset, shortId))
}

func TestPropertyHandler_CreateRejectedOnUnknownKind(t *testing.T) {
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	const propId = "prop-bad"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", "ch-bad-kind", propId, true,
		setMulti(arena, map[string]any{
			typetype.FieldKey:  "bad",
			typetype.FieldKind: "complex-number",
		}),
	)))

	assert.Nil(t, ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId))
}

// ----------------------------------------------------------------------------
// BeforeModify — schema-bearing rejection vs display-only pass-through
// ----------------------------------------------------------------------------

func TestPropertyHandler_DisplayOnlyEditPasses(t *testing.T) {
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	const propId = "prop-year"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", "ch-create", propId, true,
		setMulti(arena, map[string]any{
			typetype.FieldKey:  "year",
			typetype.FieldKind: "number",
			typetype.FieldName: "Year",
		}),
	)))

	// Modify the human label — passes.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", "ch-rename", propId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldName}, Payload: arena.NewString("Release Year")},
	)))

	rec := ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
	require.NotNil(t, rec)
	assert.Equal(t, "Release Year", rec.GetString(typetype.FieldName))
	assert.Equal(t, "number", rec.GetString(typetype.FieldKind), "kind must be untouched")
}

func TestPropertyHandler_KindEditDropped(t *testing.T) {
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	const propId = "prop-pinned"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", "ch-create", propId, true,
		setMulti(arena, map[string]any{
			typetype.FieldKey:  "pinned",
			typetype.FieldKind: "string",
		}),
	)))

	// Try to flip the kind. Op is dropped silently; record unchanged.
	// Bundle a display-only edit with it to verify per-op rejection
	// (other ops in the same change still apply).
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", "ch-attempt-flip", propId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldKind}, Payload: arena.NewString("number")},
		crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldName}, Payload: arena.NewString("Renamed")},
	)))

	rec := ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
	require.NotNil(t, rec)
	assert.Equal(t, "string", rec.GetString(typetype.FieldKind), "kind survives the edit attempt")
	assert.Equal(t, "Renamed", rec.GetString(typetype.FieldName), "non-schema edit landed")
}

func TestPropertyHandler_MultiFieldSchemaEditDropped(t *testing.T) {
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	const propId = "prop-multi"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", "ch-create", propId, true,
		setMulti(arena, map[string]any{
			typetype.FieldKey:  "multi",
			typetype.FieldKind: "string",
		}),
	)))

	// Multi-field $set bundling pinned `kind` with an unconstrained
	// `name`: per-key salvage sheds only the pinned key, the name lands.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", "ch-multi", propId, false,
		setMulti(arena, map[string]any{
			typetype.FieldKind: "number",
			typetype.FieldName: "AlsoChanged",
		}),
	)))

	rec := ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
	require.NotNil(t, rec)
	assert.Equal(t, "string", rec.GetString(typetype.FieldKind), "kind stayed pinned")
	assert.Equal(t, "AlsoChanged", rec.GetString(typetype.FieldName), "bundled non-pinned key landed")
}

// ----------------------------------------------------------------------------
// BeforeDelete — removal projects a shortId row, original tombstoned
// ----------------------------------------------------------------------------

func TestPropertyHandler_DeleteProjectsRemovalShortId(t *testing.T) {
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	const propId = "prop-doomed"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", "ch-create", propId, true,
		setMulti(arena, map[string]any{
			typetype.FieldKey:  "doomed",
			typetype.FieldKind: "string",
		}),
	)))

	const removeChangeId = "ch-remove"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", removeChangeId, propId, false,
		crdt.Op{Type: crdt.OpDelete},
	)))

	// Original record is tombstoned (Get returns the tombstone shape;
	// Records() filters tombstones out).
	live := ctrl.Records(context.Background(), typetype.DatasetPropertyDefs)
	for _, r := range live {
		assert.NotEqual(t, propId, r.GetString("id"), "deleted property should not be among live records")
	}

	// Removal shortId row landed — different shortId from the create
	// (different changeId), with `removed=true` and no kind.
	shortId := crdt.DeriveRecordId(removeChangeId)
	row := ctrl.Get(context.Background(), typetype.ShortIdsDataset, shortId)
	require.NotNil(t, row, "removal shortId projection missing")
	assert.Equal(t, removeChangeId, row.GetString(typetype.ShortIdFieldChangeId))
	assert.Equal(t, propId, row.GetString(typetype.ShortIdFieldPropId))
	assert.True(t, row.GetBool(typetype.ShortIdFieldRemoved))
	assert.Equal(t, "v2", row.GetString("_ver", "id"))
}

// ----------------------------------------------------------------------------
// Scope — create-time validation + post-create pin
// ----------------------------------------------------------------------------

func TestPropertyHandler_CreateWithScope(t *testing.T) {
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	const propId = "prop-read"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", "change-create-read", propId, true,
		setMulti(arena, map[string]any{
			typetype.FieldKind:  "boolean",
			typetype.FieldScope: "account",
		}),
	)))

	rec := ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
	require.NotNil(t, rec)
	assert.Equal(t, "account", rec.GetString(typetype.FieldScope))
}

func TestPropertyHandler_CreateRejectedOnBadScope(t *testing.T) {
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	for _, bad := range []string{"derived", "global", "Account"} {
		propId := "prop-" + bad
		require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
			"v1", "change-"+bad, propId, true,
			setMulti(arena, map[string]any{
				typetype.FieldKind:  "string",
				typetype.FieldScope: bad,
			}),
		)), "apply commits; the record drops via BeforeCreate rejection")
		rec := ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
		if rec != nil {
			assert.Nil(t, rec.Get(typetype.FieldKind), "record with scope %q must not materialize fields", bad)
		}
	}
}

// ----------------------------------------------------------------------------
// Format — create-time structure validation + sub-path pinning
// ----------------------------------------------------------------------------

// setMultiFormat builds a multi-field creation $set carrying kind and a
// `format` object with the given sub-keys (nil-valued keys are skipped).
func setMultiFormat(arena *anyenc.Arena, kind string, formatFields map[string]*anyenc.Value) crdt.Op {
	obj := arena.NewObject()
	obj.Set(typetype.FieldKind, arena.NewString(kind))
	format := arena.NewObject()
	for k, v := range formatFields {
		if v != nil {
			format.Set(k, v)
		}
	}
	obj.Set(typetype.FieldFormat, format)
	return crdt.Op{Type: crdt.OpSet, Payload: obj}
}

func TestPropertyHandler_CreateWithFormat(t *testing.T) {
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	const propId = "prop-related"
	const changeId = "ch-create-related"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", changeId, propId, true,
		setMultiFormat(arena, "array", map[string]*anyenc.Value{
			typetype.FormatKeyType:   arena.NewString("links"),
			typetype.FormatKeyUi:     arena.NewString("multiselect"),
			typetype.FormatKeyFilter: arena.NewString(`{"type":{"$in":["page"]}}`),
		}),
	)))

	rec := ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
	require.NotNil(t, rec)
	assert.Equal(t, "links", rec.GetString(typetype.FieldFormat, typetype.FormatKeyType))
	assert.Equal(t, "multiselect", rec.GetString(typetype.FieldFormat, typetype.FormatKeyUi))
	assert.Equal(t, `{"type":{"$in":["page"]}}`, rec.GetString(typetype.FieldFormat, typetype.FormatKeyFilter))

	// Format creation is not "important" beyond the add itself — the
	// usual create shortId row is still minted.
	row := ctrl.Get(context.Background(), typetype.ShortIdsDataset, crdt.DeriveRecordId(changeId))
	require.NotNil(t, row, "create with format must still project a shortId")
}

func TestPropertyHandler_CreateWithFormatRejections(t *testing.T) {
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	cases := []struct {
		name string
		op   crdt.Op
	}{
		{"unknown format.type", setMultiFormat(arena, "array", map[string]*anyenc.Value{
			typetype.FormatKeyType: arena.NewString("rainbow"),
		})},
		{"missing format.type", setMultiFormat(arena, "array", map[string]*anyenc.Value{
			typetype.FormatKeyUi: arena.NewString("select"),
		})},
		{"kind mismatch links/string", setMultiFormat(arena, "string", map[string]*anyenc.Value{
			typetype.FormatKeyType: arena.NewString("links"),
		})},
		{"kind mismatch datetime/array", setMultiFormat(arena, "array", map[string]*anyenc.Value{
			typetype.FormatKeyType: arena.NewString("datetime"),
		})},
		{"non-string ui", setMultiFormat(arena, "array", map[string]*anyenc.Value{
			typetype.FormatKeyType: arena.NewString("links"),
			typetype.FormatKeyUi:   arena.NewNumberFloat64(7),
		})},
		{"non-string filter", setMultiFormat(arena, "array", map[string]*anyenc.Value{
			typetype.FormatKeyType:   arena.NewString("links"),
			typetype.FormatKeyFilter: arena.NewObject(),
		})},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			propId := "prop-bad-format-" + tc.name
			require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
				crdt.VersionId("v"+string(rune('1'+i))), "ch-"+tc.name, propId, true, tc.op,
			)))
			rec := ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
			if rec != nil {
				assert.Nil(t, rec.Get(typetype.FieldKind), "record %q must not materialize", tc.name)
			}
		})
	}
}

func TestPropertyHandler_CreateTagsToleratedInbound(t *testing.T) {
	// `tags` is rejected by the local AddProperty pre-flight until the
	// tag table lands, but the handler admits it so definitions from
	// newer SDKs replicate.
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	const propId = "prop-tags"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", "ch-create-tags", propId, true,
		setMultiFormat(arena, "array", map[string]*anyenc.Value{
			typetype.FormatKeyType: arena.NewString("tags"),
		}),
	)))

	rec := ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
	require.NotNil(t, rec)
	assert.Equal(t, "tags", rec.GetString(typetype.FieldFormat, typetype.FormatKeyType))
}

func TestPropertyHandler_CreateRejectsDottedFormatKeys(t *testing.T) {
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	const propId = "prop-dotted"
	obj := arena.NewObject()
	obj.Set(typetype.FieldKind, arena.NewString("array"))
	obj.Set("format.type", arena.NewString("links"))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", "ch-dotted", propId, true,
		crdt.Op{Type: crdt.OpSet, Payload: obj},
	)))

	rec := ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
	if rec != nil {
		assert.Nil(t, rec.Get(typetype.FieldKind), "dotted format.* creation must drop the record")
	}
}

func TestPropertyHandler_FormatLeafEditsPassTypeEditsDrop(t *testing.T) {
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	const propId = "prop-format-edit"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", "ch-create", propId, true,
		setMultiFormat(arena, "array", map[string]*anyenc.Value{
			typetype.FormatKeyType: arena.NewString("links"),
			typetype.FormatKeyUi:   arena.NewString("select"),
		}),
	)))

	// Leaf edit via single dotted path — passes.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", "ch-ui-edit", propId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldFormat, typetype.FormatKeyUi}, Payload: arena.NewString("multiselect")},
	)))
	// Leaf edit via multi-field dotted key — passes.
	filterEdit := arena.NewObject()
	filterEdit.Set("format.filter", arena.NewString(`{"type":"person"}`))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v3", "ch-filter-edit", propId, false,
		crdt.Op{Type: crdt.OpSet, Payload: filterEdit},
	)))

	// format.type edit — dropped in every shape.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v4", "ch-type-edit-path", propId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldFormat, typetype.FormatKeyType}, Payload: arena.NewString("tags")},
	)))
	typeEdit := arena.NewObject()
	typeEdit.Set("format.type", arena.NewString("tags"))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v5", "ch-type-edit-multi", propId, false,
		crdt.Op{Type: crdt.OpSet, Payload: typeEdit},
	)))
	// Broad `format` replace — dropped (could smuggle a type change).
	broad := arena.NewObject()
	broad.Set(typetype.FormatKeyType, arena.NewString("tags"))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v6", "ch-broad-replace", propId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldFormat}, Payload: broad},
	)))
	// Non-string leaf value — dropped.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v7", "ch-ui-nonstring", propId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldFormat, typetype.FormatKeyUi}, Payload: arena.NewNumberFloat64(1)},
	)))

	rec := ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
	require.NotNil(t, rec)
	assert.Equal(t, "links", rec.GetString(typetype.FieldFormat, typetype.FormatKeyType), "type pinned through all edit shapes")
	assert.Equal(t, "multiselect", rec.GetString(typetype.FieldFormat, typetype.FormatKeyUi), "ui leaf edit landed")
	assert.Equal(t, `{"type":"person"}`, rec.GetString(typetype.FieldFormat, typetype.FormatKeyFilter), "filter leaf edit landed")
}

func TestPropertyHandler_ConcurrentFormatLeafEditsMerge(t *testing.T) {
	// Two writers touching different format leaves: per-leaf `_ver`
	// tracking must let both land regardless of arrival order.
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	const propId = "prop-concurrent"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", "ch-create", propId, true,
		setMultiFormat(arena, "array", map[string]*anyenc.Value{
			typetype.FormatKeyType: arena.NewString("links"),
		}),
	)))

	// "Concurrent" edits: the ui edit carries the LATER versionId but is
	// applied FIRST; the filter edit arrives after with an earlier
	// versionId. Distinct leaves — both must survive.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v3", "ch-ui", propId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldFormat, typetype.FormatKeyUi}, Payload: arena.NewString("select")},
	)))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", "ch-filter", propId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldFormat, typetype.FormatKeyFilter}, Payload: arena.NewString(`{"a":1}`)},
	)))

	rec := ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
	require.NotNil(t, rec)
	assert.Equal(t, "select", rec.GetString(typetype.FieldFormat, typetype.FormatKeyUi))
	assert.Equal(t, `{"a":1}`, rec.GetString(typetype.FieldFormat, typetype.FormatKeyFilter))
}

func TestPropertyHandler_ScopeEditDropped(t *testing.T) {
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	const propId = "prop-pinned-scope"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", "change-create-pinned", propId, true,
		setMulti(arena, map[string]any{
			typetype.FieldKind:  "string",
			typetype.FieldScope: "local",
		}),
	)))

	// Single-path edit drops.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", "change-edit-scope", propId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldScope}, Payload: arena.NewString("synced")},
	)))
	// Multi-field edit bundling the pinned scope with a legal name change:
	// per-key salvage sheds only the scope key, the name lands.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v3", "change-edit-scope-multi", propId, false,
		setMulti(arena, map[string]any{
			typetype.FieldScope: "synced",
			typetype.FieldName:  "Renamed",
		}),
	)))

	rec := ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
	require.NotNil(t, rec)
	assert.Equal(t, "local", rec.GetString(typetype.FieldScope), "scope is pinned after first write")
	assert.Equal(t, "Renamed", rec.GetString(typetype.FieldName), "bundled non-pinned key still landed")
}
