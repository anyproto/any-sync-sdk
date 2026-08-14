package spaceobjects

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

const catTypeId = "type-notes"

// seedTypeObject writes a `__type__` row for typeId into the shared
// objects collection so the catalog's boot scan finds it.
func seedTypeObject(t *testing.T, ctx context.Context, db anystore.DB, spaceId, typeId string) {
	t.Helper()
	coll, err := db.Collection(ctx, spaceId+"_"+SpaceObjectsCollection)
	require.NoError(t, err)
	a := &anyenc.Arena{}
	row := a.NewObject()
	row.Set("id", a.NewString(typeId))
	anyObj := a.NewObject()
	typesArr := a.NewArray()
	typesArr.SetArrayItem(0, a.NewString("__type__"))
	anyObj.Set("types", typesArr)
	row.Set("any", anyObj)
	require.NoError(t, coll.UpsertOne(ctx, row))
}

// seedDatasetDefs applies head+field records through a real defs
// controller so records carry proper `_ver` state. verPrefix keeps
// creation `_ver.id`s distinct across seeded type objects (real trees
// never share orderIds; equal ids here would make the cross-type
// name-conflict tiebreak ambiguous by test artifact).
func seedDatasetDefs(t *testing.T, ctx context.Context, db anystore.DB, typeId, dsName, verPrefix string) {
	t.Helper()
	ctrl, err := crdt.NewController(ctx, typeId, db,
		crdt.HandlerReg{Name: typetype.DatasetDefs, Handler: typetype.DatasetDefsHandler{}, Schema: schema.Dataset{Dynamic: true}},
		crdt.HandlerReg{Name: typetype.ShortIdsDataset, Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}},
	)
	require.NoError(t, err)
	a := &anyenc.Arena{}
	head := a.NewObject()
	head.Set(typetype.DefFieldDef, a.NewString(typetype.DefKindDataset))
	head.Set(typetype.DefFieldName, a.NewString(dsName))
	head.Set(typetype.DefFieldIdRule, a.NewString("user"))
	require.NoError(t, ctrl.ApplyChange(ctx, crdt.Change{
		ObjectId: typeId, Dataset: typetype.DatasetDefs, ChangeId: "cd-head-" + dsName,
		VersionId: crdt.VersionId(verPrefix + "1"), DataVersion: typetype.DatasetDefsHandlerVersion,
		Records: []crdt.RecordChange{{Id: "head-" + dsName, Upsert: true,
			Ops: []crdt.Op{{Type: crdt.OpSet, Payload: head}}}},
	}))
	field := a.NewObject()
	field.Set(typetype.DefFieldDef, a.NewString(typetype.DefKindField))
	field.Set(typetype.DefFieldDataset, a.NewString("head-"+dsName))
	field.Set(typetype.FieldKey, a.NewString("title"))
	field.Set(typetype.FieldKind, a.NewString("string"))
	require.NoError(t, ctrl.ApplyChange(ctx, crdt.Change{
		ObjectId: typeId, Dataset: typetype.DatasetDefs, ChangeId: "cd-field-" + dsName,
		VersionId: crdt.VersionId(verPrefix + "2"), DataVersion: typetype.DatasetDefsHandlerVersion,
		Records: []crdt.RecordChange{{Id: "field-" + dsName, Upsert: true,
			Ops: []crdt.Op{{Type: crdt.OpSet, Payload: field}}}},
	}))
	require.NoError(t, ctrl.CloseOwnedCollections())
}

func TestCatalog_BootScanAndBuildRegs(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "cat.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	seedTypeObject(t, ctx, db, "spaceA", catTypeId)
	seedDatasetDefs(t, ctx, db, catTypeId, "notes", "a")

	store := NewStore(nil, db, nil, "spaceA", nil, nil)
	t.Cleanup(func() { _ = store.Close() })

	ds, ok := store.RuntimeDataset("notes")
	require.True(t, ok, "boot scan must pick the runtime dataset up")
	assert.Equal(t, catTypeId, ds.TypeId)
	assert.Equal(t, schema.IdUser, ds.Schema.IdRule)

	owner, ok := store.DatasetOwner("notes")
	require.True(t, ok)
	assert.Equal(t, catTypeId, owner)

	regs, _, err := store.buildRegs()
	require.NoError(t, err)
	var found bool
	for _, reg := range regs {
		if reg.Name == "notes" {
			found = true
			_, isSchemaHandler := reg.Handler.(*crdt.SchemaHandler)
			assert.True(t, isSchemaHandler)
		}
	}
	assert.True(t, found, "buildRegs must include the runtime dataset")

	// DataVersionFor stamps the owning type's latest shortId.
	dv, err := store.DataVersionFor(ctx, "notes")
	require.NoError(t, err)
	assert.Contains(t, dv, catTypeId+":")
}

func TestCatalog_RefreshTypeAddsAndRemoves(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "cat.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	store := NewStore(nil, db, nil, "spaceA", nil, nil)
	t.Cleanup(func() { _ = store.Close() })
	_, ok := store.RuntimeDataset("notes")
	require.False(t, ok)

	// Defs land after store open (the inbound-sync order): refreshType
	// picks them up without a store reopen.
	seedDatasetDefs(t, ctx, db, catTypeId, "notes", "a")
	store.refreshType(ctx, catTypeId)
	_, ok = store.RuntimeDataset("notes")
	require.True(t, ok)

	// A reserved/static name never enters the catalog.
	seedDatasetDefs(t, ctx, db, "type-other", "notes", "b")
	store.refreshType(ctx, "type-other")
	ds, ok := store.RuntimeDataset("notes")
	require.True(t, ok)
	assert.Equal(t, catTypeId, ds.TypeId, "cross-type conflict resolves deterministically")
}

func TestGate_ParksUnknownDatasetChange(t *testing.T) {
	ctx, store := gateStore(t)

	// Controller without the `notes` dataset (the stale-reg case).
	ctrl, err := crdt.NewController(ctx, "obj-X", store.db,
		crdt.HandlerReg{Name: "known", Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}})
	require.NoError(t, err)

	gate := store.gateFor("obj-X", ctrl)
	ch := gateChange("legacy-version-string")
	ch.Dataset = "notes"
	ok, err := gate(ctx, ch, []byte("payload"))
	require.NoError(t, err)
	assert.False(t, ok, "unknown-dataset change must park, not stall")
	rows := countDetached(t, ctx, store)
	require.Len(t, rows, 1)

	// A change for a registered dataset still passes.
	ch2 := gateChange("legacy-version-string2")
	ch2.Dataset = "known"
	ok, err = gate(ctx, ch2, []byte("payload"))
	require.NoError(t, err)
	assert.True(t, ok)
}
