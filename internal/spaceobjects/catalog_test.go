package spaceobjects

import (
	"context"
	"path/filepath"
	"sort"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
	collectiontype "github.com/anyproto/any-sync-sdk/internal/types/collection"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
)

const catTypeId = "type-notes"

// seedTypeObject writes a `__type__` row for typeId into the shared
// objects collection so the catalog's boot scan finds it.
func seedTypeObject(t *testing.T, ctx context.Context, db anystore.DB, spaceId, typeId string) {
	t.Helper()
	seedDefinitionObject(t, ctx, db, spaceId, typeId, typetype.MetaTypeMarker)
}

// seedDefinitionObject writes a definition row carrying `marker` in
// `any.type` — the one slot that tells a type object from a collection
// object.
func seedDefinitionObject(t *testing.T, ctx context.Context, db anystore.DB, spaceId, id, marker string) {
	t.Helper()
	coll, err := db.Collection(ctx, spaceId+"_"+SpaceObjectsCollection)
	require.NoError(t, err)
	a := &anyenc.Arena{}
	row := a.NewObject()
	row.Set("id", a.NewString(id))
	anyObj := a.NewObject()
	anyObj.Set(anytype.FieldType, a.NewString(marker))
	row.Set(anytype.TypeId, anyObj)
	require.NoError(t, coll.UpsertOne(ctx, row))
}

// seedDatasetDefs applies part+head+field records through a real defs
// controller so records carry proper `_ver` state. verPrefix keeps
// creation `_ver.id`s distinct across seeded type objects (real trees
// never share orderIds). The dataset is a records one keyed dsKey.
func seedDatasetDefs(t *testing.T, ctx context.Context, db anystore.DB, typeId, dsKey, verPrefix string) {
	t.Helper()
	seedModuleDefs(t, ctx, db, typeId, dsKey, types.RecordsModule, false, verPrefix)
}

// seedModuleDefs declares one part keyed dsKey carrying one dataset
// keyed dsKey of the given module (a records dataset also gets a
// `title` field).
func seedModuleDefs(t *testing.T, ctx context.Context, db anystore.DB, typeId, dsKey, module string, shared bool, verPrefix string) {
	t.Helper()
	ctrl, err := crdt.NewController(ctx, typeId, db,
		crdt.HandlerReg{Name: typetype.DatasetDefs, Handler: typetype.DatasetDefsHandler{}, Schema: schema.Dataset{Dynamic: true}},
		crdt.HandlerReg{Name: typetype.ShortIdsDataset, Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}},
	)
	require.NoError(t, err)
	a := &anyenc.Arena{}
	partId := typeId + "-part-" + dsKey
	part := a.NewObject()
	part.Set(typetype.DefFieldDef, a.NewString(typetype.DefKindPart))
	part.Set(typetype.FieldKey, a.NewString(dsKey))
	require.NoError(t, ctrl.ApplyChange(ctx, crdt.Change{
		ObjectId: typeId, Dataset: typetype.DatasetDefs, ChangeId: "cd-part-" + typeId + dsKey,
		VersionId: crdt.VersionId(verPrefix + "0"), DataVersion: typetype.DatasetDefsHandlerVersion,
		Records: []crdt.RecordChange{{Id: partId, Upsert: true,
			Ops: []crdt.Op{{Type: crdt.OpSet, Payload: part}}}},
	}))
	head := a.NewObject()
	head.Set(typetype.DefFieldDef, a.NewString(typetype.DefKindDataset))
	head.Set(typetype.FieldKey, a.NewString(dsKey))
	head.Set(typetype.DefFieldModule, a.NewString(module))
	head.Set(typetype.DefFieldPart, a.NewString(partId))
	if shared {
		head.Set(typetype.DefFieldShared, a.NewTrue())
	}
	if module == types.RecordsModule {
		head.Set(typetype.DefFieldIdRule, a.NewString("user"))
	}
	require.NoError(t, ctrl.ApplyChange(ctx, crdt.Change{
		ObjectId: typeId, Dataset: typetype.DatasetDefs, ChangeId: "cd-head-" + typeId + dsKey,
		VersionId: crdt.VersionId(verPrefix + "1"), DataVersion: typetype.DatasetDefsHandlerVersion,
		Records: []crdt.RecordChange{{Id: typeId + "-head-" + dsKey, Upsert: true,
			Ops: []crdt.Op{{Type: crdt.OpSet, Payload: head}}}},
	}))
	if module == types.RecordsModule {
		field := a.NewObject()
		field.Set(typetype.DefFieldDef, a.NewString(typetype.DefKindField))
		field.Set(typetype.DefFieldDataset, a.NewString(typeId+"-head-"+dsKey))
		field.Set(typetype.FieldKey, a.NewString("title"))
		field.Set(typetype.FieldKind, a.NewString("string"))
		require.NoError(t, ctrl.ApplyChange(ctx, crdt.Change{
			ObjectId: typeId, Dataset: typetype.DatasetDefs, ChangeId: "cd-field-" + typeId + dsKey,
			VersionId: crdt.VersionId(verPrefix + "2"), DataVersion: typetype.DatasetDefsHandlerVersion,
			Records: []crdt.RecordChange{{Id: "field-" + typeId + dsKey, Upsert: true,
				Ops: []crdt.Op{{Type: crdt.OpSet, Payload: field}}}},
		}))
	}
	require.NoError(t, ctrl.CloseOwnedCollections())
}

func TestCatalog_BootScanAndBuildRegs(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "cat.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	seedTypeObject(t, ctx, db, "spaceA", catTypeId)
	seedDatasetDefs(t, ctx, db, catTypeId, "notes", "a")
	coll := types.CollectionName(catTypeId, "notes")

	store := NewStore(nil, db, nil, "spaceA", nil, nil, nil, nil)
	t.Cleanup(func() { _ = store.Close() })

	ds, ok := store.RuntimeDataset(coll)
	require.True(t, ok, "boot scan must pick the runtime dataset up")
	assert.Equal(t, catTypeId, ds.TypeId)
	assert.Equal(t, "notes", ds.Key)
	assert.Equal(t, schema.IdUser, ds.Schema.IdRule)
	_, bare := store.RuntimeDataset("notes")
	assert.False(t, bare, "the bare key is not a collection")

	owners, ok := store.DatasetOwners(coll)
	require.True(t, ok)
	assert.Equal(t, []string{catTypeId}, owners)

	regs, _, err := store.buildRegs()
	require.NoError(t, err)
	var found bool
	for _, reg := range regs {
		if reg.Name == coll {
			found = true
			_, isSchemaHandler := reg.Handler.(*crdt.SchemaHandler)
			assert.True(t, isSchemaHandler)
		}
	}
	assert.True(t, found, "buildRegs must include the runtime dataset")

	// DataVersionFor stamps the owning type's latest shortId.
	dv, err := store.DataVersionFor(ctx, coll)
	require.NoError(t, err)
	assert.Contains(t, dv, catTypeId+":")

	// Discovery lists it under its owner and module.
	var listed bool
	for _, ns := range store.Schemas() {
		if ns.Name == coll {
			listed = true
			assert.Equal(t, []string{catTypeId}, ns.Owners)
			assert.Equal(t, types.RecordsModule, ns.Module)
			assert.False(t, ns.Shared)
		}
	}
	assert.True(t, listed)

	// The compiled parts view is reachable through the store.
	ct, err := store.TypeParts(ctx, catTypeId)
	require.NoError(t, err)
	require.Len(t, ct.Parts, 1)
	assert.Equal(t, "notes", ct.Parts[0].Key)
}

// markerRowIds lists the ids a live-rows filter selects from the shared
// objects collection.
func markerRowIds(t *testing.T, ctx context.Context, db anystore.DB, spaceId string, f query.Filter) []string {
	t.Helper()
	coll, err := db.Collection(ctx, spaceId+"_"+SpaceObjectsCollection)
	require.NoError(t, err)
	iter, err := coll.Find(f).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var out []string
	for iter.Next() {
		doc, derr := iter.Doc()
		require.NoError(t, derr)
		out = append(out, doc.Value().GetString("id"))
	}
	require.NoError(t, iter.Err())
	sort.Strings(out)
	return out
}

// A collection carries properties only: its `__collection__` row is not
// a type, so the boot scan skips it and nothing it declares reaches the
// parts catalog. LiveCollectionRowsFilter is the mirror selector, and
// both filters exclude tombstones.
func TestCatalog_CollectionRowIsNotAType(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "coll.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	const collId = "coll-shelf"
	seedTypeObject(t, ctx, db, "spaceA", catTypeId)
	seedDefinitionObject(t, ctx, db, "spaceA", collId, collectiontype.MetaMarker)
	seedDatasetDefs(t, ctx, db, catTypeId, "notes", "a")
	// Defs written under a collection id are never compiled — the id is
	// not scanned as a type.
	seedDatasetDefs(t, ctx, db, collId, "notes", "b")

	store := NewStore(nil, db, nil, "spaceA", nil, nil, nil, nil)
	t.Cleanup(func() { _ = store.Close() })

	ids, err := store.catalogTypeIds(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{catTypeId}, ids, "only `__type__` rows are types")

	_, ok := store.RuntimeDataset(types.CollectionName(catTypeId, "notes"))
	assert.True(t, ok)
	_, ok = store.RuntimeDataset(types.CollectionName(collId, "notes"))
	assert.False(t, ok, "a collection row contributes no runtime dataset")
	assert.False(t, store.catalogHasType(collId))

	// The two filters partition the definition rows.
	assert.Equal(t, []string{catTypeId}, markerRowIds(t, ctx, db, "spaceA", LiveTypeRowsFilter))
	assert.Equal(t, []string{collId}, markerRowIds(t, ctx, db, "spaceA", LiveCollectionRowsFilter))

	// A tombstoned collection row leaves the live set.
	objs, err := db.Collection(ctx, "spaceA_"+SpaceObjectsCollection)
	require.NoError(t, err)
	a := &anyenc.Arena{}
	row := a.NewObject()
	row.Set("id", a.NewString(collId))
	anyObj := a.NewObject()
	anyObj.Set(anytype.FieldType, a.NewString(collectiontype.MetaMarker))
	row.Set(anytype.TypeId, anyObj)
	row.Set(crdt.DeletedAtField, a.NewNumberInt(1))
	require.NoError(t, objs.UpsertOne(ctx, row))
	assert.Empty(t, markerRowIds(t, ctx, db, "spaceA", LiveCollectionRowsFilter))
}

// Classify is what tells the local write pre-flight which slot an id
// belongs in: registered and reserved ids, then the live definition
// rows. A tombstone, a plain object and an unknown id are all unknown —
// the definition may simply not have synced yet.
func TestStore_Classify(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "classify.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	seedDefinitionObject(t, ctx, db, "spaceA", "live-type", typetype.MetaTypeMarker)
	seedDefinitionObject(t, ctx, db, "spaceA", "live-coll", collectiontype.MetaMarker)
	seedDefinitionObject(t, ctx, db, "spaceA", "plain", "live-type")

	store := NewStore(nil, db, nil, "spaceA", nil,
		[]handler.Type{{Id: "reg-type"}}, []handler.Collection{{Id: "reg-coll"}}, nil)
	t.Cleanup(func() { _ = store.Close() })

	for id, want := range map[string]properties.OwnerKind{
		anytype.TypeId:        properties.OwnerType,
		typetype.TypeId:       properties.OwnerType,
		collectiontype.TypeId: properties.OwnerType,
		"reg-type":            properties.OwnerType,
		"reg-coll":            properties.OwnerCollection,
		"live-type":           properties.OwnerType,
		"live-coll":           properties.OwnerCollection,
		"plain":               properties.OwnerUnknown,
		"not-synced-yet":      properties.OwnerUnknown,
		"":                    properties.OwnerUnknown,
	} {
		assert.Equal(t, want, store.Classify(ctx, id), id)
	}

	// A tombstoned definition stops resolving.
	a := &anyenc.Arena{}
	objs, err := db.Collection(ctx, "spaceA_"+SpaceObjectsCollection)
	require.NoError(t, err)
	row := a.NewObject()
	row.Set("id", a.NewString("live-coll"))
	anyObj := a.NewObject()
	anyObj.Set(anytype.FieldType, a.NewString(collectiontype.MetaMarker))
	row.Set(anytype.TypeId, anyObj)
	row.Set(crdt.DeletedAtField, a.NewNumberInt(1))
	require.NoError(t, objs.UpsertOne(ctx, row))
	assert.Equal(t, properties.OwnerUnknown, store.Classify(ctx, "live-coll"))
}

func TestCatalog_RefreshTypeAddsAndRemoves(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "cat.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	store := NewStore(nil, db, nil, "spaceA", nil, nil, nil, nil)
	t.Cleanup(func() { _ = store.Close() })
	coll := types.CollectionName(catTypeId, "notes")
	_, ok := store.RuntimeDataset(coll)
	require.False(t, ok)

	// Defs land after store open (the inbound-sync order): refreshType
	// picks them up without a store reopen.
	seedDatasetDefs(t, ctx, db, catTypeId, "notes", "a")
	store.refreshType(ctx, catTypeId)
	_, ok = store.RuntimeDataset(coll)
	require.True(t, ok)

	// Another type declaring the same key gets its own collection —
	// namespacing makes cross-type conflicts impossible.
	seedDatasetDefs(t, ctx, db, "type-other", "notes", "b")
	store.refreshType(ctx, "type-other")
	other, ok := store.RuntimeDataset(types.CollectionName("type-other", "notes"))
	require.True(t, ok)
	assert.Equal(t, "type-other", other.TypeId)
	ds, ok := store.RuntimeDataset(coll)
	require.True(t, ok)
	assert.Equal(t, catTypeId, ds.TypeId)
}

func TestControllerStaleFor_RemovedDataset(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "stale.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	seedTypeObject(t, ctx, db, "spaceA", catTypeId)
	seedDatasetDefs(t, ctx, db, catTypeId, "notes", "a")
	store := NewStore(nil, db, nil, "spaceA", nil, nil, nil, nil)
	t.Cleanup(func() { _ = store.Close() })
	coll := types.CollectionName(catTypeId, "notes")

	regs, _, err := store.buildRegs()
	require.NoError(t, err)
	ctrl, err := crdt.NewController(ctx, "obj-X", db, regs...)
	require.NoError(t, err)

	require.False(t, store.controllerStaleFor(ctrl, coll), "fresh reg matches catalog rev")

	// Definition removed: the resident controller (still carrying the
	// reg) must go stale so it stops applying what fresh peers park.
	headColl, err := db.Collection(ctx, catTypeId+"_datasets")
	require.NoError(t, err)
	require.NoError(t, headColl.DeleteId(ctx, catTypeId+"-head-notes"))
	store.refreshType(ctx, catTypeId)
	_, known := store.RuntimeDataset(coll)
	require.False(t, known)
	assert.True(t, store.controllerStaleFor(ctrl, coll))
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

// blocksHandler is a stateless module handler whose identity the tests
// can recognise on a registration.
type blocksHandler struct {
	crdt.DefaultHandler
	inst handler.ModuleInstance
}

func (blocksHandler) Init(context.Context) error { return nil }

func testModule() handler.Module {
	return handler.Module{
		Name:        "blocks",
		Canonical:   "blocks_shared",
		DataVersion: "blocks-v1",
		Properties: []handler.PropertyDecl{
			{Id: "count", Name: "Count", Kind: handler.PropertyKindNumber, Scope: handler.ScopeLocal},
		},
		New: func(inst handler.ModuleInstance) handler.Dataset {
			return handler.Dataset{
				Handler: blocksHandler{inst: inst},
				Schema:  handler.Schema{Dynamic: true},
			}
		},
	}
}

// A module registers its canonical collection on every controller
// before any type declares it, owner sets follow the catalog, and a
// namespaced instance is served by the module's handler rather than
// the generic schema handler.
func TestCatalog_ModuleCanonicalAndInstances(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "mod.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	store := NewStore(nil, db, nil, "spaceA", nil, nil, nil, []handler.Module{testModule()})
	t.Cleanup(func() { _ = store.Close() })

	regs, _, err := store.buildRegs()
	require.NoError(t, err)
	var canonical *crdt.HandlerReg
	for i := range regs {
		if regs[i].Name == "blocks_shared" {
			canonical = &regs[i]
		}
	}
	require.NotNil(t, canonical, "the canonical collection registers statically")
	bh, ok := canonical.Handler.(blocksHandler)
	require.True(t, ok)
	assert.True(t, bh.inst.Shared)
	assert.Equal(t, "blocks_shared", bh.inst.Collection)
	dv, err := store.DataVersionFor(ctx, "blocks_shared")
	require.NoError(t, err)
	assert.Equal(t, "blocks-v1", dv)

	owners, gated := store.DatasetOwners("blocks_shared")
	require.True(t, gated, "a canonical collection is always gated")
	assert.Empty(t, owners, "nothing declares it yet")
	assert.Empty(t, store.ModuleGrants(map[string]struct{}{"type-a": {}}))

	// Type A shares the module; type B declares a namespaced instance.
	seedTypeObject(t, ctx, db, "spaceA", "type-a")
	seedTypeObject(t, ctx, db, "spaceA", "type-b")
	seedModuleDefs(t, ctx, db, "type-a", "blocks_shared", "blocks", true, "a")
	seedModuleDefs(t, ctx, db, "type-b", "notes", "blocks", false, "b")
	store.refreshType(ctx, "type-a")
	store.refreshType(ctx, "type-b")

	owners, _ = store.DatasetOwners("blocks_shared")
	assert.Equal(t, []string{"type-a"}, owners)
	inst := types.CollectionName("type-b", "notes")
	owners, gated = store.DatasetOwners(inst)
	require.True(t, gated)
	assert.Equal(t, []string{"type-b"}, owners)

	regs, _, err = store.buildRegs()
	require.NoError(t, err)
	var instReg *crdt.HandlerReg
	for i := range regs {
		if regs[i].Name == inst {
			instReg = &regs[i]
		}
	}
	require.NotNil(t, instReg, "the namespaced instance registers")
	bh, ok = instReg.Handler.(blocksHandler)
	require.True(t, ok, "served by the module, not the schema handler")
	assert.Equal(t, "type-b", bh.inst.TypeId)
	assert.Equal(t, "notes", bh.inst.Key)
	assert.False(t, bh.inst.Shared)
	assert.NotEmpty(t, instReg.SchemaRev)

	// The module namespace is granted off either declaration kind.
	assert.Equal(t, []string{"blocks"}, store.ModuleGrants(map[string]struct{}{"type-a": {}}))
	assert.Equal(t, []string{"blocks"}, store.ModuleGrants(map[string]struct{}{"type-b": {}}))
	assert.Empty(t, store.ModuleGrants(map[string]struct{}{"type-c": {}}))
	assert.Equal(t, "blocks", store.counterNamespace("blocks_shared"))
	assert.Equal(t, "blocks", store.counterNamespace(inst))
	props, ok := store.Registry().PropsOf("blocks")
	require.True(t, ok, "the module's properties resolve as a namespace")
	require.Len(t, props, 1)
	assert.Equal(t, "count", props[0].Id)

	// Discovery: the canonical carries its owner set, the instance its
	// module.
	var sawCanonical, sawInst bool
	for _, ns := range store.Schemas() {
		switch ns.Name {
		case "blocks_shared":
			sawCanonical = true
			assert.Equal(t, []string{"type-a"}, ns.Owners)
			assert.Equal(t, "blocks", ns.Module)
			assert.True(t, ns.Shared)
		case inst:
			sawInst = true
			assert.Equal(t, []string{"type-b"}, ns.Owners)
			assert.Equal(t, "blocks", ns.Module)
			assert.False(t, ns.Shared)
		}
	}
	assert.True(t, sawCanonical && sawInst)

	// A withdrawn shared declaration drops the owner; the canonical
	// registration stays.
	headColl, err := db.Collection(ctx, "type-a_datasets")
	require.NoError(t, err)
	require.NoError(t, headColl.DeleteId(ctx, "type-a-head-blocks_shared"))
	store.refreshType(ctx, "type-a")
	owners, gated = store.DatasetOwners("blocks_shared")
	assert.True(t, gated)
	assert.Empty(t, owners)
}

func TestValidateExternalModules(t *testing.T) {
	good := testModule()
	sharedOnly := testModule()
	sharedOnly.Name = "chatty"
	sharedOnly.Canonical = "chatty_shared"
	sharedOnly.SharedOnly = true
	noCanonical := testModule()
	noCanonical.Name = "plain"
	noCanonical.Canonical = ""
	noCanonical.DataVersion = ""
	require.NoError(t, ValidateExternalModules(nil, []handler.Module{good, sharedOnly, noCanonical}))

	bad := func(mut func(m *handler.Module)) handler.Module {
		m := testModule()
		mut(&m)
		return m
	}
	cases := []struct {
		name  string
		types []handler.Type
		mods  []handler.Module
	}{
		{"empty name", nil, []handler.Module{bad(func(m *handler.Module) { m.Name = "" })}},
		{"bad slug", nil, []handler.Module{bad(func(m *handler.Module) { m.Name = "Blocks" })}},
		{"records reserved", nil, []handler.Module{bad(func(m *handler.Module) { m.Name = "records" })}},
		{"reserved type id", nil, []handler.Module{bad(func(m *handler.Module) { m.Name = "any" })}},
		{"type id collision", []handler.Type{{Id: "blocks"}}, []handler.Module{good}},
		{"duplicate", nil, []handler.Module{good, good}},
		{"nil New", nil, []handler.Module{bad(func(m *handler.Module) { m.New = nil })}},
		{"shared-only without canonical", nil, []handler.Module{bad(func(m *handler.Module) { m.Canonical = ""; m.SharedOnly = true })}},
		{"canonical reserved", nil, []handler.Module{bad(func(m *handler.Module) { m.Canonical = "objects" })}},
		{"canonical collides with a type dataset", []handler.Type{{Id: "t", Datasets: []handler.Dataset{{Name: "blocks_shared", DataVersion: "v", Handler: crdt.DefaultHandler{}}}}}, []handler.Module{good}},
		{"empty data version", nil, []handler.Module{bad(func(m *handler.Module) { m.DataVersion = "" })}},
		{"bad property", nil, []handler.Module{bad(func(m *handler.Module) {
			m.Properties = []handler.PropertyDecl{{Id: "_x", Kind: handler.PropertyKindString}}
		})}},
		{"derived property scope", nil, []handler.Module{bad(func(m *handler.Module) {
			m.Properties = []handler.PropertyDecl{{Id: "x", Kind: handler.PropertyKindString, Scope: handler.ScopeDerived}}
		})}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, ValidateExternalModules(tc.types, tc.mods))
		})
	}
}
