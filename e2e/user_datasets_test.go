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

// articleDatasetDraft is the SYN-147 acceptance schema: required title,
// author-mutable body, freely-mutable summary, full stamp set,
// author-gated deletes, caller-supplied ids.
func articleDatasetDraft() space.DatasetDraft {
	stageShape := handler.Leaf(handler.PropertyKindArray)
	stageShape.Items = handler.Leaf(handler.PropertyKindString)
	return space.DatasetDraft{
		Key:         "articles",
		DisplayName: "Articles",
		IdRule:      space.IdUser,
		DeleteBy:    space.DeleteByAuthor,
		Search:      &space.SearchFields{Title: "title", Text: []string{"body"}, Scope: "articles"},
		Fields: []space.DatasetFieldDraft{
			{Key: "title", Kind: space.PropertyKindString, Required: true,
				Description: "Headline", XFormat: map[string]any{"type": "text", "icon": "heading"}},
			{Key: "body", Kind: space.PropertyKindString, MutableBy: space.MutableByAuthor},
			{Key: "summary", Kind: space.PropertyKindString, MutableBy: space.MutableByAnyone},
			{Key: "stage", Kind: space.PropertyKindArray, MutableBy: space.MutableByAnyone,
				Shape:   stageShape,
				XFormat: map[string]any{"type": "choice", "config": map[string]any{"multiple": true}}},
			{Key: "creator", Stamp: space.StampCreator},
			{Key: "createdAt", Stamp: space.StampCreateTime},
			{Key: "modifiedAt", Stamp: space.StampModifyTime},
		},
	}
}

// articlesPart wraps the articles dataset in the part a type declares
// it under.
func articlesPart() space.PartDraft {
	return space.PartDraft{
		Key: "articles", Name: "Articles", Pos: "a0",
		UI:       map[string]any{"type": "table"},
		Datasets: []space.DatasetDraft{articleDatasetDraft()},
	}
}

// TestE2E_UserDatasets_DefineAndUpsert is the single-device SYN-147
// lifecycle: declare a part with a records dataset on a runtime type,
// batch-upsert records through the generic path, verify stamps /
// enforcement / idempotency / discovery — all local reads, no network
// round-trip required.
func TestE2E_UserDatasets_DefineAndUpsert(t *testing.T) {
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
	}, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "DatasetLab"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatal(err)
	}

	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Article",
		Layout: map[string]any{"type": "page"}})
	require.NoError(t, err)
	ti, err := sp.Types().Get(ctx, typeId)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"type": "page"}, ti.Layout)

	partId, err := sp.Types().AddPart(ctx, typeId, articlesPart())
	require.NoError(t, err)
	require.NotEmpty(t, partId)

	// The part and its dataset read back compiled.
	parts, err := sp.Types().Parts(ctx, typeId)
	require.NoError(t, err)
	require.Len(t, parts, 1)
	assert.Equal(t, partId, parts[0].Id)
	assert.Equal(t, "articles", parts[0].Key)
	assert.Equal(t, "Articles", parts[0].Name)
	assert.Equal(t, map[string]any{"type": "table"}, parts[0].UI)
	require.Len(t, parts[0].Datasets, 1)

	defs, err := sp.Types().Datasets(ctx, typeId)
	require.NoError(t, err)
	require.Len(t, defs, 1)
	def := defs[0]
	defId := def.Id
	coll := def.Collection
	assert.Equal(t, "articles", def.Key)
	assert.Equal(t, typeId+"_articles", coll, "a records dataset is namespaced under its type")
	assert.Equal(t, space.RecordsModule, def.Module)
	assert.False(t, def.Shared)
	assert.Equal(t, partId, def.PartId)
	assert.Equal(t, space.IdUser, def.IdRule)
	assert.Equal(t, space.DeleteByAuthor, def.DeleteBy)
	require.NotNil(t, def.Search)
	assert.Equal(t, "articles", def.Search.Scope)
	assert.Equal(t, []string{"body"}, def.Search.Text)
	require.Len(t, def.Fields, 7)

	// The descriptive slice and the full shape round-trip.
	fieldByKey := func(defs []space.DatasetDef, key string) space.DatasetFieldDef {
		for _, f := range defs[0].Fields {
			if f.Key == key {
				return f
			}
		}
		t.Fatalf("field %q not found", key)
		return space.DatasetFieldDef{}
	}
	title := fieldByKey(defs, "title")
	assert.Equal(t, "Headline", title.Description)
	assert.Equal(t, map[string]any{"type": "text", "icon": "heading"}, title.XFormat)
	stage := fieldByKey(defs, "stage")
	assert.Equal(t, space.PropertyKindArray, stage.Kind)
	require.NotNil(t, stage.Shape)
	require.NotNil(t, stage.Shape.Items, "the sub-shape reads back")
	assert.Equal(t, handler.Leaf(handler.PropertyKindString).Kind, stage.Shape.Items.Kind)
	assert.Equal(t, "choice", stage.XFormat["type"])
	assert.Nil(t, fieldByKey(defs, "body").XFormat, "a field without a descriptor reads back nil")

	// Discovery includes the runtime dataset under its collection with
	// its owning type and module. A single-key text mapping surfaces as
	// the bare string — discovery output is unchanged for consumers of
	// the single-field form.
	var discovered bool
	for _, ds := range sp.Datasets() {
		if ds.Name == coll {
			discovered = true
			assert.Equal(t, []string{typeId}, ds.Owners)
			assert.Equal(t, space.RecordsModule, ds.Module)
			assert.Contains(t, string(ds.JSONSchema), `"x-search"`)
			assert.Contains(t, string(ds.JSONSchema), `"scope":"articles"`)
			assert.Contains(t, string(ds.JSONSchema), `"text":"body"`)
			assert.Contains(t, string(ds.JSONSchema), `"x-format":{"icon":"heading","type":"text"}`)
			assert.Contains(t, string(ds.JSONSchema), `"description":"Headline"`)
		}
	}
	assert.True(t, discovered, "Datasets() must list the runtime dataset")

	// Part patch: the display slice mutates, the key is pinned.
	require.NoError(t, sp.Types().PatchPart(ctx, typeId, partId, space.DatasetDefPatch{
		Set:   map[string]any{"name": "Pieces", "hidden": true, "ui": map[string]any{"type": "list"}},
		Unset: []string{"pos"},
	}))
	parts, err = sp.Types().Parts(ctx, typeId)
	require.NoError(t, err)
	assert.Equal(t, "Pieces", parts[0].Name)
	assert.True(t, parts[0].Hidden)
	assert.Equal(t, map[string]any{"type": "list"}, parts[0].UI)
	assert.Empty(t, parts[0].Pos)
	require.ErrorIs(t, sp.Types().PatchPart(ctx, typeId, partId, space.DatasetDefPatch{
		Set: map[string]any{"key": "other"},
	}), space.ErrPinnedField)
	require.ErrorIs(t, sp.Types().PatchPart(ctx, typeId, partId, space.DatasetDefPatch{
		Set: map[string]any{"ui": "table"},
	}), space.ErrInvalidFieldValue)

	// Type patch: display and rendering metadata.
	require.NoError(t, sp.Types().Patch(ctx, typeId, space.TypePatch{Name: strPtr("Articles"),
		Layout: map[string]any{"type": "tabs", "config": map[string]any{"header": true}}}))
	ti, err = sp.Types().Get(ctx, typeId)
	require.NoError(t, err)
	assert.Equal(t, "Articles", ti.Name)
	assert.Equal(t, map[string]any{"type": "tabs", "config": map[string]any{"header": true}}, ti.Layout)
	require.NoError(t, sp.Types().Patch(ctx, typeId, space.TypePatch{ClearLayout: true}))
	ti, err = sp.Types().Get(ctx, typeId)
	require.NoError(t, err)
	assert.Nil(t, ti.Layout)

	// Field patch: the display pair and any path under x-format mutate;
	// the behavioral declaration is pinned; an unknown id is not found.
	require.NoError(t, sp.Types().PatchDatasetField(ctx, typeId, title.Id, space.DatasetDefPatch{
		Set:   map[string]any{"description": "The headline", "x-format.icon": "title", "x-format.config.maxLen": 120},
		Unset: []string{"x-format.type"},
	}))
	defs, err = sp.Types().Datasets(ctx, typeId)
	require.NoError(t, err)
	title = fieldByKey(defs, "title")
	assert.Equal(t, "The headline", title.Description)
	assert.Equal(t, map[string]any{"icon": "title", "config": map[string]any{"maxLen": float64(120)}}, title.XFormat)
	require.ErrorIs(t, sp.Types().PatchDatasetField(ctx, typeId, title.Id, space.DatasetDefPatch{
		Set: map[string]any{"required": false},
	}), space.ErrPinnedField)
	require.ErrorIs(t, sp.Types().PatchDatasetField(ctx, typeId, "no-such-field", space.DatasetDefPatch{
		Set: map[string]any{"name": "x"},
	}), space.ErrNotFound)

	// The text mapping patches to a key array (the ensure-drift path);
	// malformed values are rejected up-front with ErrInvalidFieldValue
	// (not the pinned-path sentinel).
	err = sp.Types().PatchDataset(ctx, typeId, defId, space.DatasetDefPatch{
		Set: map[string]any{"search.text": []string{"body", "body"}},
	})
	require.ErrorIs(t, err, space.ErrInvalidFieldValue, "duplicate keys must be rejected")
	require.ErrorIs(t, sp.Types().PatchDataset(ctx, typeId, defId, space.DatasetDefPatch{
		Set: map[string]any{"search.text": []string{}},
	}), space.ErrInvalidFieldValue, "an empty array must be rejected")
	require.ErrorIs(t, sp.Types().PatchDataset(ctx, typeId, defId, space.DatasetDefPatch{
		Set: map[string]any{"search.text": ""},
	}), space.ErrInvalidFieldValue, "the empty string must be rejected — Unset clears")
	require.NoError(t, sp.Types().PatchDataset(ctx, typeId, defId, space.DatasetDefPatch{
		Set: map[string]any{"search.text": []string{"body", "summary"}},
	}))
	defs, err = sp.Types().Datasets(ctx, typeId)
	require.NoError(t, err)
	require.NotNil(t, defs[0].Search)
	assert.Equal(t, []string{"body", "summary"}, defs[0].Search.Text)

	// A single-element array patch canonicalizes to the bare-string
	// wire form and reads back as the one-key mapping.
	require.NoError(t, sp.Types().PatchDataset(ctx, typeId, defId, space.DatasetDefPatch{
		Set: map[string]any{"search.text": []string{"body"}},
	}))
	defs, err = sp.Types().Datasets(ctx, typeId)
	require.NoError(t, err)
	require.NotNil(t, defs[0].Search)
	assert.Equal(t, []string{"body"}, defs[0].Search.Text)

	// An object that does not implement the type cannot hold its
	// dataset — no type attaches on write.
	stray, err := sp.Objects().Create(ctx, space.CreateObjectOpts{})
	require.NoError(t, err)
	_, err = sp.Upsert(ctx, space.UpsertBatch{
		ObjectId: stray, Dataset: coll,
		Records: []space.UpsertRecord{{Id: "s-1", Fields: map[string]any{"title": "Stray"}}},
	})
	require.ErrorIs(t, err, space.ErrDatasetNotDeclared)

	// An instance object implementing the type hosts the records.
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)

	batch := space.UpsertBatch{
		ObjectId: objId,
		Dataset:  coll,
		Records: []space.UpsertRecord{
			{Id: "a-1", Fields: map[string]any{"title": "One", "body": "b1", "summary": "s1"}},
			{Id: "a-2", Fields: map[string]any{"title": "Two", "body": "b2"}},
			{Id: "a-3", Fields: map[string]any{"title": "Three"}},
		},
	}
	res, err := sp.Upsert(ctx, batch)
	require.NoError(t, err)
	assert.Equal(t, 3, res.Created)
	assert.Empty(t, res.Rejections)

	// Stamps landed; values readable.
	row, err := sp.Query(objId, coll).Filter(map[string]any{"id": "a-1"}).One(ctx)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, "One", string(row.GetStringBytes("title")))
	assert.Equal(t, sdk.Account().Id(), string(row.GetStringBytes("creator")))
	for _, stamp := range []string{"createdAt", "modifiedAt"} {
		leaf := row.Get(stamp)
		require.NotNil(t, leaf, "%s stamped", stamp)
		ms, mserr := leaf.DateTimeMillis()
		require.NoError(t, mserr, "%s is a datetime instant, not an epoch number", stamp)
		assert.NotZero(t, ms)
	}

	// Idempotent re-run: nothing to write.
	res, err = sp.Upsert(ctx, batch)
	require.NoError(t, err)
	assert.Equal(t, 3, res.Skipped)
	assert.Zero(t, res.Created+res.Updated)
	assert.Empty(t, res.Pages)

	// Mutable-field change updates; immutable change rejects the record.
	res, err = sp.Upsert(ctx, space.UpsertBatch{
		ObjectId: objId, Dataset: coll,
		Records: []space.UpsertRecord{
			{Id: "a-1", Fields: map[string]any{"title": "One", "summary": "s1-edited"}},
			{Id: "a-2", Fields: map[string]any{"title": "TWO-CHANGED"}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, res.Updated)
	require.Len(t, res.Rejections, 1)
	assert.Equal(t, "a-2", res.Rejections[0].Id)
	assert.ErrorIs(t, res.Rejections[0].Err, space.ErrImmutableFieldChanged)

	row, err = sp.Query(objId, coll).Filter(map[string]any{"id": "a-1"}).One(ctx)
	require.NoError(t, err)
	assert.Equal(t, "s1-edited", string(row.GetStringBytes("summary")))
	assert.Equal(t, "One", string(row.GetStringBytes("title")))

	// Direct Modify enforcement: a write-once field can't be rewritten.
	mres, err := sp.Modify(ctx, space.ModifyBatch{
		ObjectId: objId, Dataset: coll,
		Records: []space.RecordModify{{Id: "a-1", Ops: []space.Op{
			{Type: space.OpSet, Path: "title", Value: "hax"},
		}}},
	})
	if err == nil {
		require.NotEmpty(t, mres.Rejections, "write-once title must be rejected")
	}

	// Author delete works (same identity); the record tombstones and
	// its id never reuses.
	dres, err := sp.Delete(ctx, space.DeleteBatch{
		ObjectId: objId, Dataset: coll, RecordIds: []string{"a-3"},
	})
	require.NoError(t, err)
	assert.Empty(t, dres.Rejections)
	res, err = sp.Upsert(ctx, space.UpsertBatch{
		ObjectId: objId, Dataset: coll,
		Records: []space.UpsertRecord{{Id: "a-3", Fields: map[string]any{"title": "Back"}}},
	})
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.ErrorIs(t, res.Rejections[0].Err, space.ErrRecordDeleted)

	// Evolution guards: additive fields cannot be required (fresh
	// devices would replay history against the stricter schema), and
	// removing the creator stamp of an author-gated dataset is refused
	// (it would invalidate the fold on every peer).
	_, err = sp.Types().AddDatasetField(ctx, typeId, defId, space.DatasetFieldDraft{
		Key: "mandatory", Kind: space.PropertyKindString, Required: true,
	})
	require.Error(t, err)
	defs, err = sp.Types().Datasets(ctx, typeId)
	require.NoError(t, err)
	var creatorFieldId string
	for _, f := range defs[0].Fields {
		if f.Stamp == space.StampCreator {
			creatorFieldId = f.Id
		}
	}
	require.NotEmpty(t, creatorFieldId, "field def ids must surface")
	require.Error(t, sp.Types().RemoveDatasetField(ctx, typeId, creatorFieldId))

	// Additive evolution: a new field lands and accepts writes.
	_, err = sp.Types().AddDatasetField(ctx, typeId, defId, space.DatasetFieldDraft{
		Key: "tags", Kind: space.PropertyKindArray, MutableBy: space.MutableByAnyone,
	})
	require.NoError(t, err)
	res, err = sp.Upsert(ctx, space.UpsertBatch{
		ObjectId: objId, Dataset: coll,
		Records: []space.UpsertRecord{{Id: "a-1", Fields: map[string]any{
			"title": "One", "tags": []any{"go", "crdt"},
		}}},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, res.Updated)
	assert.Empty(t, res.Rejections)

	// A second dataset joins the same part; a duplicate key is refused.
	_, err = sp.Types().AddDataset(ctx, typeId, partId, space.DatasetDraft{
		Key: "notes", Fields: []space.DatasetFieldDraft{{Key: "text", Kind: space.PropertyKindString, MutableBy: space.MutableByAnyone}},
	})
	require.NoError(t, err)
	_, err = sp.Types().AddDataset(ctx, typeId, partId, space.DatasetDraft{Key: "notes"})
	require.Error(t, err, "duplicate key within the type")
	_, err = sp.Types().AddDataset(ctx, typeId, "no-such-part", space.DatasetDraft{Key: "more"})
	require.ErrorIs(t, err, space.ErrNotFound)
	parts, err = sp.Types().Parts(ctx, typeId)
	require.NoError(t, err)
	require.Len(t, parts[0].Datasets, 2)

	// Removing the part removes its datasets.
	require.NoError(t, sp.Types().RemovePart(ctx, typeId, partId))
	parts, err = sp.Types().Parts(ctx, typeId)
	require.NoError(t, err)
	assert.Empty(t, parts)
	defs, err = sp.Types().Datasets(ctx, typeId)
	require.NoError(t, err)
	assert.Empty(t, defs)
}

// TestE2E_UserDatasets_ColdSync: device A declares a part with a
// records dataset and upserts records; device B (same account, fresh
// DataDir) cold-syncs and must converge on the schema
// (Types().Datasets), the registered dataset, and the records — the
// schema-then-data ordering runs through the park/drain gate on B.
func TestE2E_UserDatasets_ColdSync(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("cold-sync e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	provider := newFixedSeedProvider(t)

	sdkA, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}, provider)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdkA.Close() })

	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "DatasetSync"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatal(err)
	}
	typeId, err := spA.Types().Create(ctx, space.TypeCreateParams{Name: "Article"})
	require.NoError(t, err)
	_, err = spA.Types().AddPart(ctx, typeId, articlesPart())
	require.NoError(t, err)
	coll := typeId + "_articles"
	objId, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)
	res, err := spA.Upsert(ctx, space.UpsertBatch{
		ObjectId: objId, Dataset: coll,
		Records: []space.UpsertRecord{
			{Id: "a-1", Fields: map[string]any{"title": "One", "body": "b1"}},
			{Id: "a-2", Fields: map[string]any{"title": "Two"}},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 2, res.Created)

	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = spA.SyncHeads(ctx)

	sdkB, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}, provider)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdkB.Close() })

	// Space appears on B.
	var spB space.Space
	require.True(t, waitFor(ctx, 90*time.Second, 250*time.Millisecond, func() bool {
		_ = sdkB.Spaces().SyncSpaceList(ctx)
		got, gerr := sdkB.Spaces().Get(ctx, spA.Id())
		if gerr != nil {
			return false
		}
		spB = got
		return true
	}), "device B must see the space")

	// Schema converges: the part and its dataset definition read back on B.
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		defs, derr := spB.Types().Datasets(ctx, typeId)
		return derr == nil && len(defs) == 1 && len(defs[0].Fields) == 7 && defs[0].Collection == coll
	}), "device B must converge on the dataset definition")
	parts, err := spB.Types().Parts(ctx, typeId)
	require.NoError(t, err)
	require.Len(t, parts, 1)
	assert.Equal(t, "articles", parts[0].Key)

	// Data converges: both records with their derived stamps.
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		rows, qerr := spB.Query(objId, coll).All(ctx)
		return qerr == nil && len(rows) == 2
	}), "device B must converge on the upserted records")

	row, err := spB.Query(objId, coll).Filter(map[string]any{"id": "a-1"}).One(ctx)
	require.NoError(t, err)
	assert.Equal(t, "One", string(row.GetStringBytes("title")))
	assert.Equal(t, sdkA.Account().Id(), string(row.GetStringBytes("creator")))

	// B can write through the generic path too (same account = author).
	bres, err := spB.Upsert(ctx, space.UpsertBatch{
		ObjectId: objId, Dataset: coll,
		Records: []space.UpsertRecord{{Id: "a-1", Fields: map[string]any{"title": "One", "body": "edited-on-B"}}},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, bres.Updated)
	assert.Empty(t, bres.Rejections)
}
