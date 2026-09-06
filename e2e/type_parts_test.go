package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/space"
)

// notesModule is a stand-in for a compiled-in dataset module (the
// editor, the chat): a shared canonical collection plus namespaced
// instances, one free-form record shape, a per-object counter in the
// module's namespace.
func notesModule() handler.Module {
	return handler.Module{
		Name:        "notes",
		Canonical:   "notes_shared",
		DataVersion: "notes-v1",
		Properties: []handler.PropertyDecl{
			{Id: "pinned", Name: "Pinned", Kind: handler.PropertyKindBoolean},
		},
		New: func(handler.ModuleInstance) handler.Dataset {
			return handler.Dataset{
				Schema: handler.Schema{Dynamic: true, Fields: []handler.Field{
					{Id: "text", Schema: handler.Leaf(handler.PropertyKindString), MutableBy: handler.MutableByAnyone},
				}},
			}
		},
	}
}

// TestE2E_TypeParts_ModulesSharedAndNamespaced covers the module
// surface on one device: two types sharing the module's canonical
// collection give an object carrying either a single body, a
// namespaced instance is its own collection served by the module, a
// write to a collection none of the object's types declare is refused
// without attaching anything, and the module's namespace on the
// objects row opens with the declaration.
func TestE2E_TypeParts_ModulesSharedAndNamespaced(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Modules: []handler.Module{notesModule()},
	}, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "PartsLab"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatal(err)
	}

	// The canonical collection is discoverable before any type
	// declares it, with no owners.
	var canonical *space.DatasetSchema
	for _, ds := range sp.Datasets() {
		if ds.Name == "notes_shared" {
			c := ds
			canonical = &c
		}
	}
	require.NotNil(t, canonical, "the module's canonical collection registers statically")
	assert.Equal(t, "notes", canonical.Module)
	assert.True(t, canonical.Shared)
	assert.Empty(t, canonical.Owners)

	// Page and Meeting both share the module; Meeting adds a namespaced
	// summary instance under a second part.
	pageId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Page", Weight: 1})
	require.NoError(t, err)
	_, err = sp.Types().AddPart(ctx, pageId, space.PartDraft{
		Key: "body", Datasets: []space.DatasetDraft{{Module: "notes", Shared: true}},
	})
	require.NoError(t, err, "a shared dataset defaults its key to the canonical")
	meetingId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Meeting", Weight: 50})
	require.NoError(t, err)
	bodyPart, err := sp.Types().AddPart(ctx, meetingId, space.PartDraft{
		Key: "description", Name: "Description",
		Datasets: []space.DatasetDraft{{Key: "notes_shared", Module: "notes", Shared: true}},
	})
	require.NoError(t, err)
	summaryPart, err := sp.Types().AddPart(ctx, meetingId, space.PartDraft{
		Key: "summary", Name: "Summary", Uses: []string{"notes_shared"},
		Datasets: []space.DatasetDraft{{Key: "summary", Module: "notes"}},
	})
	require.NoError(t, err)

	defs, err := sp.Types().Datasets(ctx, meetingId)
	require.NoError(t, err)
	require.Len(t, defs, 2)
	byKey := map[string]space.DatasetDef{}
	for _, d := range defs {
		byKey[d.Key] = d
	}
	assert.Equal(t, "notes_shared", byKey["notes_shared"].Collection)
	assert.True(t, byKey["notes_shared"].Shared)
	assert.Equal(t, bodyPart, byKey["notes_shared"].PartId)
	assert.Equal(t, meetingId+"_summary", byKey["summary"].Collection)
	assert.Equal(t, "notes", byKey["summary"].Module)
	assert.Equal(t, summaryPart, byKey["summary"].PartId)
	parts, err := sp.Types().Parts(ctx, meetingId)
	require.NoError(t, err)
	require.Len(t, parts, 2)
	assert.Equal(t, []string{"notes_shared"}, parts[1].Uses)

	// Rule violations are refused at declaration time.
	_, err = sp.Types().AddPart(ctx, meetingId, space.PartDraft{
		Key: "second", Datasets: []space.DatasetDraft{{Module: "notes", Shared: true}},
	})
	require.Error(t, err, "at most one shared dataset per module per type")
	_, err = sp.Types().AddPart(ctx, meetingId, space.PartDraft{
		Key: "fields", Datasets: []space.DatasetDraft{{Key: "extra", Module: "notes",
			Fields: []space.DatasetFieldDraft{{Key: "x", Kind: space.PropertyKindString}}}},
	})
	require.ErrorIs(t, err, space.ErrModuleOwned, "a module-served dataset declares no fields")
	_, err = sp.Types().AddPart(ctx, meetingId, space.PartDraft{
		Key: "sketch", Datasets: []space.DatasetDraft{{Key: "x", Module: "sketch"}},
	})
	require.Error(t, err, "unknown module")
	_, err = sp.Types().AddDatasetField(ctx, meetingId, byKey["summary"].Id, space.DatasetFieldDraft{
		Key: "x", Kind: space.PropertyKindString,
	})
	require.ErrorIs(t, err, space.ErrModuleOwned)

	// Discovery: the canonical carries both owners now.
	for _, ds := range sp.Datasets() {
		if ds.Name == "notes_shared" {
			assert.ElementsMatch(t, []string{pageId, meetingId}, ds.Owners)
		}
		if ds.Name == meetingId+"_summary" {
			assert.Equal(t, []string{meetingId}, ds.Owners)
			assert.Equal(t, "notes", ds.Module)
			assert.False(t, ds.Shared)
		}
	}

	// An object carrying only Page writes the shared body.
	obj, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{pageId}})
	require.NoError(t, err)
	write := func(dataset, text string) (space.ModifyResult, error) {
		return sp.Modify(ctx, space.ModifyBatch{
			ObjectId: obj, Dataset: dataset,
			Records: []space.RecordModify{{Upsert: true, Ops: []space.Op{{Type: space.OpSet, Path: "text", Value: text}}}},
		})
	}
	res, err := write("notes_shared", "hello")
	require.NoError(t, err)
	require.Empty(t, res.Rejections)
	require.Len(t, res.RecordIds, 1)

	// Its summary collection is not declared by Page: refused, and
	// nothing is attached on the way.
	_, err = write(meetingId+"_summary", "nope")
	require.ErrorIs(t, err, space.ErrDatasetNotDeclared)
	row, err := sp.Objects().Get(ctx, obj)
	require.NoError(t, err)
	require.Len(t, row.GetArray("any", "types"), 1, "no type attaches on write")

	// The module namespace on the objects row opens with the
	// declaration: a Page carries `notes.*`, a bare object does not.
	_, err = sp.Properties().Set(ctx, obj, "notes", map[string]any{"pinned": true})
	require.NoError(t, err, "module namespace granted through the declaring type")
	row, err = sp.Objects().Get(ctx, obj)
	require.NoError(t, err)
	assert.True(t, row.GetBool("notes", "pinned"))
	bare, err := sp.Objects().Create(ctx, space.CreateObjectOpts{})
	require.NoError(t, err)
	_, err = sp.Properties().Set(ctx, bare, "notes", map[string]any{"pinned": true})
	require.Error(t, err, "no declaring type, no namespace")

	// Retyping Page → Meeting keeps the body (same canonical
	// collection) and opens the summary.
	_, err = sp.Properties().AttachType(ctx, obj, meetingId)
	require.NoError(t, err)
	_, err = sp.Properties().DetachType(ctx, obj, pageId)
	require.NoError(t, err)
	rows, err := sp.Query(obj, "notes_shared").All(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "hello", string(rows[0].GetStringBytes("text")))
	res, err = write(meetingId+"_summary", "tl;dr")
	require.NoError(t, err)
	require.Empty(t, res.Rejections)
	rows, err = sp.Query(obj, meetingId+"_summary").All(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "tl;dr", string(rows[0].GetStringBytes("text")))

	// Removing Meeting's shared part withdraws only its ownership: the
	// canonical collection stays registered, Page still owns it.
	require.NoError(t, sp.Types().RemovePart(ctx, meetingId, bodyPart))
	for _, ds := range sp.Datasets() {
		if ds.Name == "notes_shared" {
			assert.Equal(t, []string{pageId}, ds.Owners)
		}
	}
	_, err = write("notes_shared", "again")
	require.ErrorIs(t, err, space.ErrDatasetNotDeclared, "the object now carries only Meeting, which no longer declares the body")
	_, err = write(meetingId+"_summary", "still mine")
	require.NoError(t, err)
}
