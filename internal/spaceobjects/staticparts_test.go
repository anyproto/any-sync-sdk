package spaceobjects

import (
	"context"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/types"
)

// docType is a registered type with static parts: a shared module
// body, a namespaced module instance and a static records dataset
// under a hidden part.
func docType() handler.Type {
	return handler.Type{
		Id:     "doc",
		Name:   "Document",
		Hidden: true,
		Datasets: []handler.Dataset{{
			Name: "doc_meta", DataVersion: "doc_meta-v1",
			Schema: handler.Schema{Fields: []handler.Field{
				{Id: "title", Schema: handler.Leaf(handler.PropertyKindString), MutableBy: handler.MutableByAnyone},
			}},
		}},
		Parts: []handler.Part{
			{Key: "body", Name: "Body", Pos: "a0", UI: map[string]any{"type": "document"},
				Datasets: []handler.PartDataset{{Module: "blocks", Shared: true}, {Module: "blocks", Key: "summary"}}},
			{Key: "meta", Hidden: true, Uses: []string{"summary"},
				Datasets: []handler.PartDataset{{Name: "doc_meta"}}},
		},
	}
}

func TestValidateExternalTypes_StaticParts(t *testing.T) {
	require.NoError(t, ValidateExternalTypes([]handler.Type{docType()}))

	bad := func(mut func(t *handler.Type)) []handler.Type {
		d := docType()
		mut(&d)
		return []handler.Type{d}
	}
	cases := []struct {
		name  string
		types []handler.Type
		err   string
	}{
		{"duplicate part key", bad(func(d *handler.Type) { d.Parts[1].Key = "body" }), "duplicate part key"},
		{"bad part key", bad(func(d *handler.Type) { d.Parts[0].Key = "Body" }), "part"},
		{"empty part", bad(func(d *handler.Type) { d.Parts[1].Datasets = nil }), "at least one dataset"},
		{"unnamed static dataset", bad(func(d *handler.Type) { d.Parts = d.Parts[:1] }), "named by no part"},
		{"unknown static name", bad(func(d *handler.Type) { d.Parts[1].Datasets[0].Name = "nope" }), "not among the type's Datasets"},
		{"static named twice", bad(func(d *handler.Type) {
			d.Parts[0].Datasets = append(d.Parts[0].Datasets, handler.PartDataset{Name: "doc_meta"})
		}), "already owned by part"},
		{"both forms", bad(func(d *handler.Type) { d.Parts[1].Datasets[0].Module = "blocks" }), "exclusive"},
		{"neither form", bad(func(d *handler.Type) { d.Parts[1].Datasets[0] = handler.PartDataset{} }), "Name or Module required"},
		{"shared on a static name", bad(func(d *handler.Type) { d.Parts[1].Datasets[0].Shared = true }), "module datasets only"},
		{"records module", bad(func(d *handler.Type) { d.Parts[0].Datasets[1].Module = "records" }), "static records dataset"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateExternalTypes(tc.types)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.err)
		})
	}
}

func TestValidateExternalModules_StaticPartsAndReserved(t *testing.T) {
	require.NoError(t, ValidateExternalModules([]handler.Type{docType()}, []handler.Module{testModule()}))

	reserved := testModule()
	reserved.Name = "secret"
	reserved.Canonical = "secret_shared"
	reserved.Reserved = true
	reserved.SharedOnly = true
	require.NoError(t, ValidateExternalModules(nil, []handler.Module{reserved}))
	reserved.SharedOnly = false
	err := ValidateExternalModules(nil, []handler.Module{reserved})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Reserved requires SharedOnly")

	// A static part naming a module the config lacks, a shared dataset
	// of a module without a canonical, a namespaced instance of a
	// module without a DataVersion, a collision on the minted name.
	noCanonical := testModule()
	noCanonical.Name = "plain"
	noCanonical.Canonical = ""
	noCanonical.DataVersion = ""
	cases := []struct {
		name  string
		types []handler.Type
		mods  []handler.Module
		err   string
	}{
		{"unknown module", []handler.Type{docType()}, nil, "unknown module"},
		{"shared without canonical", []handler.Type{{Id: "t", Parts: []handler.Part{{Key: "p",
			Datasets: []handler.PartDataset{{Module: "plain", Shared: true}}}}}}, []handler.Module{noCanonical}, "no shared collection"},
		{"instance without data version", []handler.Type{{Id: "t", Parts: []handler.Part{{Key: "p",
			Datasets: []handler.PartDataset{{Module: "plain", Key: "notes"}}}}}}, []handler.Module{noCanonical}, "needs a DataVersion"},
		{"instance collides with a registered dataset", []handler.Type{
			{Id: "t", Parts: []handler.Part{{Key: "p", Datasets: []handler.PartDataset{{Module: "blocks", Key: "notes"}}}}},
			{Id: "u", Datasets: []handler.Dataset{{Name: "t_notes", DataVersion: "v", Handler: crdt.DefaultHandler{}}}},
		}, []handler.Module{testModule()}, "already registered"},
		// A second shared dataset of one module collides on the
		// canonical key — the same verdict a runtime declaration gets.
		{"two shared of one module", []handler.Type{{Id: "t", Parts: []handler.Part{
			{Key: "p", Datasets: []handler.PartDataset{{Module: "blocks", Shared: true}}},
			{Key: "q", Datasets: []handler.PartDataset{{Module: "blocks", Shared: true}}},
		}}}, []handler.Module{testModule()}, "duplicate dataset key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateExternalModules(tc.types, tc.mods)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.err)
		})
	}
}

// Static parts enter the catalog as ownership and registrations, on
// the same footing as a runtime declaration, and read back through
// the compiled view.
func TestCatalog_StaticParts(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "static.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	extTypes := []handler.Type{docType()}
	mods := []handler.Module{testModule()}
	require.NoError(t, ValidateExternalTypes(extTypes))
	require.NoError(t, ValidateExternalModules(extTypes, mods))
	store := NewStore(nil, db, nil, "spaceA", nil, extTypes, mods)
	t.Cleanup(func() { _ = store.Close() })

	// Ownership before any runtime type exists.
	owners, gated := store.DatasetOwners("blocks_shared")
	require.True(t, gated)
	assert.Equal(t, []string{"doc"}, owners, "a static shared declaration owns the canonical collection")
	owners, gated = store.DatasetOwners("doc_summary")
	require.True(t, gated)
	assert.Equal(t, []string{"doc"}, owners)
	owners, gated = store.DatasetOwners("doc_meta")
	require.True(t, gated)
	assert.Equal(t, []string{"doc"}, owners)
	assert.Equal(t, []string{"blocks"}, store.ModuleGrants(map[string]struct{}{"doc": {}}))
	assert.Equal(t, "blocks", store.counterNamespace("doc_summary"))
	dv, err := store.DataVersion("doc_summary")
	require.NoError(t, err)
	assert.Equal(t, "blocks-v1", dv, "a static instance stamps the module's version")

	// The instance registers on every controller, served by the module.
	regs, _, err := store.buildRegs()
	require.NoError(t, err)
	var inst *crdt.HandlerReg
	for i := range regs {
		if regs[i].Name == "doc_summary" {
			inst = &regs[i]
		}
	}
	require.NotNil(t, inst)
	bh, ok := inst.Handler.(blocksHandler)
	require.True(t, ok)
	assert.Equal(t, "doc", bh.inst.TypeId)
	assert.Equal(t, "summary", bh.inst.Key)

	// Discovery.
	var sawCanonical, sawInst bool
	for _, ns := range store.Schemas() {
		switch ns.Name {
		case "blocks_shared":
			sawCanonical = true
			assert.Equal(t, []string{"doc"}, ns.Owners)
		case "doc_summary":
			sawInst = true
			assert.Equal(t, []string{"doc"}, ns.Owners)
			assert.Equal(t, "blocks", ns.Module)
			assert.False(t, ns.Shared)
		}
	}
	assert.True(t, sawCanonical && sawInst)

	// A runtime type sharing the module joins the static owner; a
	// refresh never drops the static one.
	seedTypeObject(t, ctx, db, "spaceA", "type-a")
	seedModuleDefs(t, ctx, db, "type-a", "blocks_shared", "blocks", true, "a")
	store.refreshType(ctx, "type-a")
	owners, _ = store.DatasetOwners("blocks_shared")
	assert.Equal(t, []string{"doc", "type-a"}, owners)
	headColl, err := db.Collection(ctx, "type-a_datasets")
	require.NoError(t, err)
	require.NoError(t, headColl.DeleteId(ctx, "type-a-head-blocks_shared"))
	store.refreshType(ctx, "type-a")
	owners, _ = store.DatasetOwners("blocks_shared")
	assert.Equal(t, []string{"doc"}, owners)

	// The compiled view: declared parts in order, keys as ids, the
	// static dataset under records with its schema.
	ct, err := StaticTypeParts(docType(), store.Modules())
	require.NoError(t, err)
	require.Len(t, ct.Parts, 2)
	body, meta := ct.Parts[0], ct.Parts[1]
	assert.Equal(t, "body", body.Id)
	assert.Equal(t, map[string]any{"type": "document"}, body.UI)
	require.Len(t, body.Datasets, 2)
	assert.Equal(t, "blocks_shared", body.Datasets[0].Name)
	assert.True(t, body.Datasets[0].Shared)
	assert.Equal(t, "doc_summary", body.Datasets[1].Name)
	assert.Equal(t, "summary", body.Datasets[1].Key)
	assert.Equal(t, "blocks", body.Datasets[1].Module)
	assert.True(t, meta.Hidden)
	assert.Equal(t, []string{"summary"}, meta.Uses)
	require.Len(t, meta.Datasets, 1)
	assert.Equal(t, "doc_meta", meta.Datasets[0].Name)
	assert.Equal(t, types.RecordsModule, meta.Datasets[0].Module)
	assert.Equal(t, "meta", meta.Datasets[0].PartId)
	require.Len(t, meta.Datasets[0].Schema.Fields, 1)
	assert.Equal(t, []string{"title"}, meta.Datasets[0].FieldDefIds)
	assert.Len(t, ct.Datasets, 3)
}

// A type without declared parts reads back one implicit part per
// dataset, keyed by the dataset name; a bespoke handler reports no
// module.
func TestStaticTypeParts_ImplicitParts(t *testing.T) {
	ct, err := StaticTypeParts(handler.Type{Id: "movie", Datasets: []handler.Dataset{
		{Name: "scenes", DataVersion: "v1", Handler: stubHandler{}},
		{Name: "credits", DataVersion: "v1", Schema: handler.Schema{Dynamic: true}},
	}}, types.NewModules())
	require.NoError(t, err)
	require.Len(t, ct.Parts, 2)
	assert.Equal(t, "scenes", ct.Parts[0].Key)
	assert.Equal(t, "scenes", ct.Parts[0].Id)
	assert.Equal(t, "", ct.Parts[0].Datasets[0].Module, "bespoke handler: no module")
	assert.Equal(t, "credits", ct.Parts[1].Key)
	assert.Equal(t, types.RecordsModule, ct.Parts[1].Datasets[0].Module)
	assert.True(t, ct.Parts[1].Datasets[0].Schema.Dynamic)
}
