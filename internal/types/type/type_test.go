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
		case *anyenc.Value:
			obj.Set(k, x)
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
// x-format — creation shape; everything under it is opaque and mutable
// ----------------------------------------------------------------------------

// setMultiXFormat builds a creation $set with `kind` and a whole
// `x-format` object.
func setMultiXFormat(arena *anyenc.Arena, kind string, xformat map[string]*anyenc.Value) crdt.Op {
	obj := arena.NewObject()
	obj.Set(typetype.FieldKind, arena.NewString(kind))
	xf := arena.NewObject()
	for k, v := range xformat {
		if v != nil {
			xf.Set(k, v)
		}
	}
	obj.Set(typetype.FieldXFormat, xf)
	return crdt.Op{Type: crdt.OpSet, Payload: obj}
}

func TestPropertyHandler_CreateWithXFormat(t *testing.T) {
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	const propId = "prop-related"
	const changeId = "ch-create-related"
	opts := arena.NewObject()
	lead := arena.NewObject()
	lead.Set("name", arena.NewString("Lead"))
	opts.Set("lead", lead)
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", changeId, propId, true,
		setMultiXFormat(arena, "array", map[string]*anyenc.Value{
			"type":    arena.NewString("choice"),
			"pos":     arena.NewString("a0"),
			"options": opts,
			"config":  arena.NewObject(),
		}),
	)))

	rec := ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
	require.NotNil(t, rec)
	assert.Equal(t, "choice", rec.GetString(typetype.FieldXFormat, "type"))
	assert.Equal(t, "Lead", rec.GetString(typetype.FieldXFormat, "options", "lead", "name"))

	// The descriptor is not "important" beyond the add itself — the
	// usual create shortId row is still minted.
	row := ctrl.Get(context.Background(), typetype.ShortIdsDataset, crdt.DeriveRecordId(changeId))
	require.NotNil(t, row, "create with x-format must still project a shortId")
}

func TestPropertyHandler_CreateXFormatIsOpaque(t *testing.T) {
	// Nothing inside the bag is inspected: an unknown slug, a slug the
	// consumer would reject for this kind, non-string leaves and vendor
	// keys all land. The consumer is the semantics boundary.
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	const propId = "prop-opaque"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", "ch-opaque", propId, true,
		setMultiXFormat(arena, "string", map[string]*anyenc.Value{
			"type":     arena.NewString("rainbow"),
			"decimals": arena.NewNumberFloat64(2),
			"acme":     arena.NewObject(),
		}),
	)))
	rec := ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
	require.NotNil(t, rec)
	assert.Equal(t, "rainbow", rec.GetString(typetype.FieldXFormat, "type"))
	assert.Equal(t, float64(2), rec.GetFloat64(typetype.FieldXFormat, "decimals"))
}

func TestPropertyHandler_CreateXFormatRejections(t *testing.T) {
	// The one structural rule: an object, created whole.
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	nonObject := arena.NewObject()
	nonObject.Set(typetype.FieldKind, arena.NewString("string"))
	nonObject.Set(typetype.FieldXFormat, arena.NewString("email"))

	dotted := arena.NewObject()
	dotted.Set(typetype.FieldKind, arena.NewString("string"))
	dotted.Set("x-format.type", arena.NewString("email"))

	cases := []struct {
		name string
		op   crdt.Op
	}{
		{"non-object", crdt.Op{Type: crdt.OpSet, Payload: nonObject}},
		{"dotted-key", crdt.Op{Type: crdt.OpSet, Payload: dotted}},
		{"deep-path", crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldXFormat, "type"}, Payload: arena.NewString("email")}},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			propId := "prop-bad-xformat-" + tc.name
			kindOp := crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldKind}, Payload: arena.NewString("string")}
			ops := []crdt.Op{tc.op}
			if len(tc.op.Path) > 0 {
				ops = []crdt.Op{kindOp, tc.op}
			}
			require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
				crdt.VersionId("v"+string(rune('1'+i))), "ch-"+tc.name, propId, true, ops...,
			)))
			rec := ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
			if rec != nil {
				assert.Nil(t, rec.Get(typetype.FieldKind), "record %q must not materialize", tc.name)
			}
		})
	}
}

func TestPropertyHandler_XFormatEditsPass(t *testing.T) {
	// Every path under x-format is mutable — the slug included — in
	// every op shape, with any value type. Pinned state stays pinned.
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	const propId = "prop-xformat-edit"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", "ch-create", propId, true,
		setMultiXFormat(arena, "string", map[string]*anyenc.Value{
			"type": arena.NewString("text"),
		}),
	)))

	// Slug edit via single dotted path — passes.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", "ch-type-edit", propId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldXFormat, "type"}, Payload: arena.NewString("email")},
	)))
	// Nested leaves via multi-field dotted keys, non-string values — pass.
	multi := arena.NewObject()
	multi.Set("x-format.config.multiple", arena.NewTrue())
	multi.Set("x-format.options.high.name", arena.NewString("High"))
	targets := arena.NewArray()
	targets.SetArrayItem(0, arena.NewString("companies"))
	multi.Set("x-format.relation.targetTypes", targets)
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v3", "ch-multi-edit", propId, false,
		crdt.Op{Type: crdt.OpSet, Payload: multi},
	)))
	// Whole-bag replace passes at this layer (the leaf-only rule is the
	// consumer's); a pinned field bundled with it still drops per key.
	bag := arena.NewObject()
	bag.Set("type", arena.NewString("url"))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v4", "ch-bag-replace", propId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldXFormat}, Payload: bag},
	)))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v5", "ch-kind-edit", propId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldKind}, Payload: arena.NewString("number")},
	)))

	rec := ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
	require.NotNil(t, rec)
	assert.Equal(t, "string", rec.GetString(typetype.FieldKind), "kind pinned")
	assert.Equal(t, "url", rec.GetString(typetype.FieldXFormat, "type"), "whole-bag replace landed")
	assert.Nil(t, rec.Get(typetype.FieldXFormat, "options", "high"), "whole-bag replace dropped the old option subtree")
}

func TestPropertyHandler_XFormatOptionLeafEdits(t *testing.T) {
	// Option leaves are ordinary per-path members: a multi-field $set
	// adds an option, an $unset of the option path removes the subtree.
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	const propId = "prop-select"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", "ch-create", propId, true,
		setMultiXFormat(arena, "array", map[string]*anyenc.Value{
			"type": arena.NewString("choice"),
		}),
	)))

	add := arena.NewObject()
	add.Set("x-format.options.high.name", arena.NewString("High"))
	add.Set("x-format.options.high.color", arena.NewString("red"))
	add.Set("x-format.options.high.pos", arena.NewString("a0"))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", "ch-add-opt", propId, false,
		crdt.Op{Type: crdt.OpSet, Payload: add},
	)))

	rec := ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
	require.NotNil(t, rec)
	assert.Equal(t, "High", rec.GetString(typetype.FieldXFormat, "options", "high", "name"))
	assert.Equal(t, "red", rec.GetString(typetype.FieldXFormat, "options", "high", "color"))
	assert.Equal(t, "a0", rec.GetString(typetype.FieldXFormat, "options", "high", "pos"))

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v3", "ch-del-opt", propId, false,
		crdt.Op{Type: crdt.OpUnset, Path: []string{typetype.FieldXFormat, "options", "high"}},
	)))
	rec = ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
	require.NotNil(t, rec)
	assert.Nil(t, rec.Get(typetype.FieldXFormat, "options", "high"), "option subtree removed")
}

func TestPropertyHandler_ConcurrentXFormatLeafEditsMerge(t *testing.T) {
	// Two writers touching different x-format leaves: per-leaf `_ver`
	// tracking must let both land regardless of arrival order.
	ctrl := newTypeController(t)
	arena := &anyenc.Arena{}

	const propId = "prop-concurrent"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", "ch-create", propId, true,
		setMultiXFormat(arena, "array", map[string]*anyenc.Value{
			"type": arena.NewString("relation"),
		}),
	)))

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v3", "ch-icon", propId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldXFormat, "icon"}, Payload: arena.NewString("building")},
	)))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", "ch-filter", propId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldXFormat, "relation", "filter"}, Payload: arena.NewString(`{"a":1}`)},
	)))

	rec := ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
	require.NotNil(t, rec)
	assert.Equal(t, "building", rec.GetString(typetype.FieldXFormat, "icon"))
	assert.Equal(t, `{"a":1}`, rec.GetString(typetype.FieldXFormat, "relation", "filter"))
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
