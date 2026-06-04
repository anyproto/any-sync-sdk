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

	// Multi-field $set whose payload includes `kind` → whole op
	// dropped, since schemaBearingFields are pinned. (We could
	// later split per-key, but pinning a whole multi-field op is
	// the safer Phase-1 default.)
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", "ch-multi", propId, false,
		setMulti(arena, map[string]any{
			typetype.FieldKind: "number",
			typetype.FieldName: "AlsoChanged",
		}),
	)))

	rec := ctrl.Get(context.Background(), typetype.DatasetPropertyDefs, propId)
	require.NotNil(t, rec)
	assert.Equal(t, "string", rec.GetString(typetype.FieldKind))
	// Per the rejection-is-whole-op rule, the bundled name change
	// also doesn't land. Callers wanting display edits must avoid
	// bundling them with schema-bearing keys.
	assert.NotEqual(t, "AlsoChanged", rec.GetString(typetype.FieldName))
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
