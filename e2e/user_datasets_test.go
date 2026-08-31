package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// articleDatasetDraft is the SYN-147 acceptance schema: required title,
// author-mutable body, freely-mutable summary, full stamp set,
// author-gated deletes, caller-supplied ids.
func articleDatasetDraft() space.DatasetDraft {
	return space.DatasetDraft{
		Name:        "articles",
		DisplayName: "Articles",
		IdRule:      space.IdUser,
		DeleteBy:    space.DeleteByAuthor,
		Search:      &space.SearchFields{Title: "title", Text: []string{"body"}, Scope: "articles"},
		Fields: []space.DatasetFieldDraft{
			{Key: "title", Kind: space.PropertyKindString, Required: true},
			{Key: "body", Kind: space.PropertyKindString, MutableBy: space.MutableByAuthor},
			{Key: "summary", Kind: space.PropertyKindString, MutableBy: space.MutableByAnyone},
			{Key: "creator", Stamp: space.StampCreator},
			{Key: "createdAt", Stamp: space.StampCreateTime},
			{Key: "modifiedAt", Stamp: space.StampModifyTime},
		},
	}
}

// TestE2E_UserDatasets_DefineAndUpsert is the single-device SYN-147
// lifecycle: define a dataset on a runtime type, batch-upsert records
// through the generic path, verify stamps / enforcement / idempotency /
// discovery — all local reads, no network round-trip required.
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

	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Article"})
	require.NoError(t, err)

	defId, err := sp.Types().AddDataset(ctx, typeId, articleDatasetDraft())
	require.NoError(t, err)
	require.NotEmpty(t, defId)

	// Compiled definition reads back.
	defs, err := sp.Types().Datasets(ctx, typeId)
	require.NoError(t, err)
	require.Len(t, defs, 1)
	def := defs[0]
	assert.Equal(t, "articles", def.Name)
	assert.Equal(t, space.IdUser, def.IdRule)
	assert.Equal(t, space.DeleteByAuthor, def.DeleteBy)
	require.NotNil(t, def.Search)
	assert.Equal(t, "articles", def.Search.Scope)
	assert.Equal(t, []string{"body"}, def.Search.Text)
	require.Len(t, def.Fields, 6)

	// Discovery includes the runtime dataset with its owning type. A
	// single-key text mapping surfaces as the bare string — discovery
	// output is unchanged for consumers of the single-field form.
	var discovered bool
	for _, ds := range sp.Datasets() {
		if ds.Name == "articles" {
			discovered = true
			assert.Equal(t, typeId, ds.TypeId)
			assert.Contains(t, string(ds.JSONSchema), `"x-search"`)
			assert.Contains(t, string(ds.JSONSchema), `"scope":"articles"`)
			assert.Contains(t, string(ds.JSONSchema), `"text":"body"`)
		}
	}
	assert.True(t, discovered, "Datasets() must list the runtime dataset")

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

	// An instance object implementing the type hosts the records.
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)

	batch := space.UpsertBatch{
		ObjectId: objId,
		Dataset:  "articles",
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
	row, err := sp.Query(objId, "articles").Filter(map[string]any{"id": "a-1"}).One(ctx)
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
		ObjectId: objId, Dataset: "articles",
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

	row, err = sp.Query(objId, "articles").Filter(map[string]any{"id": "a-1"}).One(ctx)
	require.NoError(t, err)
	assert.Equal(t, "s1-edited", string(row.GetStringBytes("summary")))
	assert.Equal(t, "One", string(row.GetStringBytes("title")))

	// Direct Modify enforcement: a write-once field can't be rewritten.
	mres, err := sp.Modify(ctx, space.ModifyBatch{
		ObjectId: objId, Dataset: "articles",
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
		ObjectId: objId, Dataset: "articles", RecordIds: []string{"a-3"},
	})
	require.NoError(t, err)
	assert.Empty(t, dres.Rejections)
	res, err = sp.Upsert(ctx, space.UpsertBatch{
		ObjectId: objId, Dataset: "articles",
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
		ObjectId: objId, Dataset: "articles",
		Records: []space.UpsertRecord{{Id: "a-1", Fields: map[string]any{
			"title": "One", "tags": []any{"go", "crdt"},
		}}},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, res.Updated)
	assert.Empty(t, res.Rejections)
}

// TestE2E_UserDatasets_ColdSync: device A defines a runtime dataset and
// upserts records; device B (same account, fresh DataDir) cold-syncs
// and must converge on the schema (Types().Datasets), the registered
// dataset, and the records — the schema-then-data ordering runs through
// the park/drain gate on B.
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
	_, err = spA.Types().AddDataset(ctx, typeId, articleDatasetDraft())
	require.NoError(t, err)
	objId, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	res, err := spA.Upsert(ctx, space.UpsertBatch{
		ObjectId: objId, Dataset: "articles",
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

	// Schema converges: the runtime dataset definition reads back on B.
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		defs, derr := spB.Types().Datasets(ctx, typeId)
		return derr == nil && len(defs) == 1 && len(defs[0].Fields) == 6
	}), "device B must converge on the dataset definition")

	// Data converges: both records with their derived stamps.
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		rows, qerr := spB.Query(objId, "articles").All(ctx)
		return qerr == nil && len(rows) == 2
	}), "device B must converge on the upserted records")

	row, err := spB.Query(objId, "articles").Filter(map[string]any{"id": "a-1"}).One(ctx)
	require.NoError(t, err)
	assert.Equal(t, "One", string(row.GetStringBytes("title")))
	assert.Equal(t, sdkA.Account().Id(), string(row.GetStringBytes("creator")))

	// B can write through the generic path too (same account = author).
	bres, err := spB.Upsert(ctx, space.UpsertBatch{
		ObjectId: objId, Dataset: "articles",
		Records: []space.UpsertRecord{{Id: "a-1", Fields: map[string]any{"title": "One", "body": "edited-on-B"}}},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, bres.Updated)
	assert.Empty(t, bres.Rejections)
}
