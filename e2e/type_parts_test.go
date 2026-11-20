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
// editor, the chat): a canonical collection plus namespaced instances,
// one free-form record shape, a per-object counter in the module's
// namespace.
func notesModule() handler.Module {
	return handler.Module{
		Name:        "notes",
		Canonical:   "notes_body",
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

// TestE2E_TypeParts_ModulesCanonicalAndNamespaced covers the module
// surface on one device. The key decides the collection: no key or the
// canonical name is the module's canonical collection, so two types
// declaring it give an object carrying either a single body; any other
// key is a namespaced instance served by the module. A write to a
// collection none of the object's types declare is refused without
// attaching anything, and the module's namespace on the objects row
// opens with the declaration.
func TestE2E_TypeParts_ModulesCanonicalAndNamespaced(t *testing.T) {
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
		if ds.Name == "notes_body" {
			c := ds
			canonical = &c
		}
	}
	require.NotNil(t, canonical, "the module's canonical collection registers statically")
	assert.Equal(t, "notes", canonical.Module)
	assert.Empty(t, canonical.Owners)

	// Page declares the module with no key, Meeting with the canonical
	// name spelled out: both are the canonical collection. Meeting adds
	// a namespaced summary instance under a second part.
	pageId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Page"})
	require.NoError(t, err)
	_, err = sp.Types().AddPart(ctx, pageId, space.PartDraft{
		Key: "body", Datasets: []space.DatasetDraft{{Module: "notes"}},
	})
	require.NoError(t, err)
	pageDefs, err := sp.Types().Datasets(ctx, pageId)
	require.NoError(t, err)
	require.Len(t, pageDefs, 1)
	assert.Equal(t, "notes_body", pageDefs[0].Key, "a module dataset with no key takes the canonical name as its key")
	assert.Equal(t, "notes_body", pageDefs[0].Collection, "a module dataset with no key is the canonical collection")
	assert.Equal(t, "notes", pageDefs[0].Module)
	meetingId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Meeting"})
	require.NoError(t, err)
	bodyPart, err := sp.Types().AddPart(ctx, meetingId, space.PartDraft{
		Key: "description", Name: "Description",
		Datasets: []space.DatasetDraft{{Key: "notes_body", Module: "notes"}},
	})
	require.NoError(t, err)
	summaryPart, err := sp.Types().AddPart(ctx, meetingId, space.PartDraft{
		Key: "summary", Name: "Summary", Uses: []string{"notes_body"},
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
	assert.Equal(t, "notes_body", byKey["notes_body"].Collection, "a module dataset keyed by the canonical name is the canonical collection")
	assert.Equal(t, "notes", byKey["notes_body"].Module)
	assert.Equal(t, bodyPart, byKey["notes_body"].PartId)
	assert.Equal(t, meetingId+"_summary", byKey["summary"].Collection, "a module dataset with any other key is namespaced under its type")
	assert.Equal(t, "notes", byKey["summary"].Module)
	assert.Equal(t, summaryPart, byKey["summary"].PartId)
	parts, err := sp.Types().Parts(ctx, meetingId)
	require.NoError(t, err)
	require.Len(t, parts, 2)
	assert.Equal(t, []string{"notes_body"}, parts[1].Uses)

	// Rule violations are refused at declaration time. The canonical
	// dataset is one key on its type: declaring it again, keyed or not,
	// is a duplicate.
	_, err = sp.Types().AddPart(ctx, meetingId, space.PartDraft{
		Key: "second", Datasets: []space.DatasetDraft{{Module: "notes"}},
	})
	require.ErrorContains(t, err, "is already declared on type", "a second part declaring the canonical dataset is a duplicate key")
	_, err = sp.Types().AddDataset(ctx, meetingId, summaryPart, space.DatasetDraft{Key: "notes_body", Module: "notes"})
	require.ErrorContains(t, err, "is already declared on type", "AddDataset of the canonical dataset again is a duplicate key")
	defs, err = sp.Types().Datasets(ctx, meetingId)
	require.NoError(t, err)
	require.Len(t, defs, 2, "a refused declaration writes nothing")
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

	// Discovery: one canonical entry listing both declaring types, and
	// the namespaced instance owned by Meeting alone. Neither spelling
	// of the canonical key mints a per-type collection.
	seen := map[string]space.DatasetSchema{}
	for _, ds := range sp.Datasets() {
		seen[ds.Name] = ds
	}
	require.Contains(t, seen, "notes_body")
	assert.ElementsMatch(t, []string{pageId, meetingId}, seen["notes_body"].Owners, "every type declaring the canonical dataset owns the canonical collection")
	assert.Equal(t, "notes", seen["notes_body"].Module)
	require.Contains(t, seen, meetingId+"_summary")
	assert.Equal(t, []string{meetingId}, seen[meetingId+"_summary"].Owners)
	assert.Equal(t, "notes", seen[meetingId+"_summary"].Module)
	assert.NotContains(t, seen, pageId+"_notes_body", "an unkeyed canonical declaration is not namespaced")
	assert.NotContains(t, seen, meetingId+"_notes_body", "the canonical key spelled out is not namespaced")

	// An object carrying only Page writes the canonical body.
	obj, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: pageId})
	require.NoError(t, err)
	write := func(dataset, text string) (space.ModifyResult, error) {
		return sp.Modify(ctx, space.ModifyBatch{
			ObjectId: obj, Dataset: dataset,
			Records: []space.RecordModify{{Upsert: true, Ops: []space.Op{{Type: space.OpSet, Path: "text", Value: text}}}},
		})
	}
	res, err := write("notes_body", "hello")
	require.NoError(t, err)
	require.Empty(t, res.Rejections)
	require.Len(t, res.RecordIds, 1)

	// Its summary collection is not declared by Page: refused, and
	// nothing is attached on the way.
	_, err = write(meetingId+"_summary", "nope")
	require.ErrorIs(t, err, space.ErrDatasetNotDeclared)
	row, err := sp.Objects().Get(ctx, obj)
	require.NoError(t, err)
	require.Equal(t, pageId, row.GetString("any", "type"), "no type attaches on write")
	require.Empty(t, row.GetArray("any", "collections"), "no collection attaches on write")

	// The module namespace on the objects row opens with the
	// declaration: a Page carries `notes.*`, an object whose type
	// declares nothing does not.
	_, err = sp.Properties().Set(ctx, obj, "notes", map[string]any{"pinned": true})
	require.NoError(t, err, "module namespace granted through the declaring type")
	row, err = sp.Objects().Get(ctx, obj)
	require.NoError(t, err)
	assert.True(t, row.GetBool("notes", "pinned"))
	bare, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
		Type: markerTypeId(t, ctx, sp, "Bare"),
	})
	require.NoError(t, err)
	_, err = sp.Properties().Set(ctx, bare, "notes", map[string]any{"pinned": true})
	require.Error(t, err, "no declaring type, no namespace")

	// The slots are enforced for ids this device resolves: a collection
	// is not a type and a type is not a collection.
	tagColl, tagTitle := setupTagCollection(t, ctx, sp)
	_, err = sp.Properties().SetType(ctx, bare, tagColl)
	require.ErrorIs(t, err, space.ErrWrongSlot)
	_, err = sp.Properties().AttachCollection(ctx, bare, pageId)
	require.ErrorIs(t, err, space.ErrWrongSlot)
	_, err = sp.Types().Get(ctx, tagColl)
	require.ErrorIs(t, err, space.ErrNotAType)
	_, err = sp.Collections().Get(ctx, pageId)
	require.ErrorIs(t, err, space.ErrNotACollection)

	// A collection's namespace opens once the object is filed under it,
	// alongside its type's.
	_, err = sp.Properties().AttachCollection(ctx, obj, tagColl)
	require.NoError(t, err)
	_, err = sp.Properties().Set(ctx, obj, tagColl, map[string]any{tagTitle: "filed"})
	require.NoError(t, err)
	row, err = sp.Objects().Get(ctx, obj)
	require.NoError(t, err)
	assert.Equal(t, "filed", row.GetString(tagColl, tagTitle))
	assert.Equal(t, pageId, row.GetString("any", "type"), "filing changes no type")

	// Retyping Page → Meeting keeps the body (same canonical
	// collection) and opens the summary.
	_, err = sp.Properties().SetType(ctx, obj, meetingId)
	require.NoError(t, err)
	rows, err := sp.Query(obj, "notes_body").All(ctx)
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

	// Removing Meeting's canonical part withdraws only its ownership:
	// the canonical collection stays registered, Page still owns it.
	require.NoError(t, sp.Types().RemovePart(ctx, meetingId, bodyPart))
	seen = map[string]space.DatasetSchema{}
	for _, ds := range sp.Datasets() {
		seen[ds.Name] = ds
	}
	require.Contains(t, seen, "notes_body", "the canonical collection stays registered")
	assert.Equal(t, []string{pageId}, seen["notes_body"].Owners)
	_, err = write("notes_body", "again")
	require.ErrorIs(t, err, space.ErrDatasetNotDeclared, "the object now carries only Meeting, which no longer declares the body")
	_, err = write(meetingId+"_summary", "still mine")
	require.NoError(t, err)
}
