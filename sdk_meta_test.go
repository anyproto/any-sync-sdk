package anysyncsdk

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/space"
)

// Objects().Create with Meta, offline: the row-root creation bag, its
// filter and listing, what no later write may do to it, and what a
// copy does with it.
func TestMeta_Bag(t *testing.T) {
	ctx, sp := cfOpen(t)
	note := cfSeed(t, ctx, sp, nil, nil)

	task, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
		Type: cfType, Meta: map[string]any{"createdIn": note, "via": "/task"},
		InitialProperties: map[string]map[string]any{"any": {"name": "Buy milk"}},
	})
	require.NoError(t, err)

	row, err := sp.Objects().Get(ctx, task)
	require.NoError(t, err)
	assert.Equal(t, note, row.GetString("meta", "createdIn"), "the bag sits at the row root")
	assert.Equal(t, "/task", row.GetString("meta", "via"), "keys are the consumer's")
	assert.Nil(t, row.Get("any", "meta"), "not under any.*")
	assert.Equal(t, "Buy milk", row.GetString("any", "name"))
	noteRow, err := sp.Objects().Get(ctx, note)
	require.NoError(t, err)
	assert.Nil(t, noteRow.Get("meta"), "absent on an object created with none")

	// A key inside the bag filters.
	rows, err := sp.QueryObjects().Filter(map[string]any{"meta.createdIn": note}).All(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, task, rows[0].GetString("id"))

	// Listed as a derived object property of `any`, like the other stamps.
	defs, err := sp.Types().Properties(ctx, "any")
	require.NoError(t, err)
	var listed bool
	for _, d := range defs {
		if d.Id == "meta" {
			listed = true
			assert.Equal(t, space.ScopeDerived, d.Scope)
			assert.Equal(t, space.PropertyKindObject, d.Kind)
		}
	}
	assert.True(t, listed, "any lists meta")

	// No later write moves it: the raw write surface refuses a $set of
	// the bag or of a key inside it and an $unset in any shape, the
	// property surface `any.meta` as every derived field.
	other := cfSeed(t, ctx, sp, nil, nil)
	for _, ops := range [][]space.Op{
		{{Type: space.OpSet, Value: map[string]any{"meta": map[string]any{"createdIn": other}}}},
		{{Type: space.OpSet, Value: map[string]any{"meta.createdIn": other}}},
		{{Type: space.OpSet, Path: "meta", Value: map[string]any{"createdIn": other}}},
		{{Type: space.OpSet, Path: "meta.createdIn", Value: other}},
		{{Type: space.OpUnset, Path: "meta"}},
		{{Type: space.OpUnset, Path: "meta.createdIn"}},
	} {
		_, err = sp.Modify(ctx, space.ModifyBatch{ObjectId: task, Dataset: properties.Dataset, Records: []space.RecordModify{
			{Id: task, Upsert: true, Ops: ops},
		}})
		assert.ErrorIs(t, err, properties.ErrScopeMismatch, "%+v", ops)
	}
	_, err = sp.Properties().Set(ctx, task, "any", map[string]any{"meta": map[string]any{"createdIn": other}})
	assert.ErrorIs(t, err, properties.ErrScopeMismatch)
	row, err = sp.Objects().Get(ctx, task)
	require.NoError(t, err)
	assert.Equal(t, note, row.GetString("meta", "createdIn"))

	// A copy does not carry the source's bag; its own request may.
	dup, err := sp.Objects().Create(ctx, space.CreateObjectOpts{From: task})
	require.NoError(t, err)
	row, err = sp.Objects().Get(ctx, dup)
	require.NoError(t, err)
	assert.Nil(t, row.Get("meta"), "From does not copy meta")
	assert.Equal(t, "Buy milk", row.GetString("any", "name"))
	dup2, err := sp.Objects().Create(ctx, space.CreateObjectOpts{From: task, Meta: map[string]any{"createdIn": other}})
	require.NoError(t, err)
	row, err = sp.Objects().Get(ctx, dup2)
	require.NoError(t, err)
	assert.Equal(t, other, row.GetString("meta", "createdIn"), "a copy made in something names it")
	assert.Nil(t, row.Get("meta", "via"))

	// A value the write rules refuse fails the create, and nothing is minted.
	_, err = sp.Objects().Create(ctx, space.CreateObjectOpts{Type: cfType, Meta: map[string]any{"bad": make(chan int)}})
	require.Error(t, err)
	rows, err = sp.QueryObjects().Filter(map[string]any{"any.type": cfType}).All(ctx)
	require.NoError(t, err)
	assert.Len(t, rows, 5, "note, task, other, dup, dup2")
}

// config.ObjectIndexes puts the consumer's index on every space's
// objects collection; a bad declaration fails the open.
func TestMeta_ObjectIndexes(t *testing.T) {
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	cfg := cfConfig(t, dataDir)
	cfg.ObjectIndexes = []anystore.IndexInfo{{Name: "idx_ext_meta_created_in", Fields: []string{"meta.createdIn"}, Sparse: true}}
	sdk, err := Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Indexed"})
	require.NoError(t, err)
	note := cfSeed(t, ctx, sp, nil, nil)
	task, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: cfType, Meta: map[string]any{"createdIn": note}})
	require.NoError(t, err)
	rows, err := sp.QueryObjects().Filter(map[string]any{"meta.createdIn": note}).All(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, task, rows[0].GetString("id"))
	spaceId := sp.Id()
	require.NoError(t, sdk.Close())

	var dbPath string
	require.NoError(t, filepath.WalkDir(filepath.Dir(dataDir), func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && d.Name() == "sdk.db" {
			dbPath = path
		}
		return nil
	}))
	require.NotEmpty(t, dbPath, "sdk.db under %s", filepath.Dir(dataDir))
	db, err := anystore.Open(ctx, dbPath, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	collNames, err := db.GetCollectionNames(ctx)
	require.NoError(t, err)
	var objects string
	for _, n := range collNames {
		if strings.HasSuffix(n, "_objects") && strings.Contains(n, spaceId) {
			objects = n
		}
	}
	require.NotEmpty(t, objects, "objects collection of %s among %v", spaceId, collNames)
	coll, err := db.OpenCollection(ctx, objects)
	require.NoError(t, err)
	var names []string
	for _, idx := range coll.GetIndexes() {
		names = append(names, idx.Info().Name)
	}
	assert.Contains(t, names, "idx_ext_meta_created_in")
	require.NoError(t, db.Close())

	for _, bad := range [][]anystore.IndexInfo{
		{{Fields: []string{"meta.createdIn"}}},
		{{Name: "idx_any_type", Fields: []string{"meta.createdIn"}}},
		{{Name: "any.types", Fields: []string{"meta.createdIn"}}},
		{{Name: strings.Repeat("n", 65), Fields: []string{"meta.createdIn"}}},
		{{Name: "idx_ext_x", Fields: nil}},
		{{Name: "idx_ext_x", Fields: []string{"meta..x"}}},
		{{Name: "idx_ext_x", Fields: []string{"meta."}}},
		{{Name: "idx_ext_x", Fields: []string{"$x"}}},
		{{Name: "idx_ext_x", Fields: []string{"-"}}},
		{{Name: "idx_ext_x", Fields: []string{"meta.createdIn"}, Unique: true}},
		{{Name: "idx_ext_x", Fields: []string{"meta.createdIn"}, Kind: anystore.IndexKindVector}},
		{{Name: "idx_ext_x", Fields: []string{"a"}}, {Name: "idx_ext_x", Fields: []string{"b"}}},
	} {
		cfg := cfConfig(t, t.TempDir())
		cfg.ObjectIndexes = bad
		_, err := Open(ctx, cfg, wmProvider(t, cfg.Storage.DataDir))
		assert.Error(t, err, "%+v", bad)
	}
}

// A name redefined between releases replaces the stored index at the
// next open of the space.
func TestMeta_ObjectIndexRedefined(t *testing.T) {
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	open := func(t *testing.T, fields ...string) *SDK {
		t.Helper()
		cfg := cfConfig(t, dataDir)
		cfg.ObjectIndexes = []anystore.IndexInfo{{Name: "idx_ext_meta", Fields: fields, Sparse: true}}
		sdk, err := Open(ctx, cfg, wmProvider(t, dataDir))
		require.NoError(t, err)
		return sdk
	}
	sdk := open(t, "meta.createdIn")
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Redefined"})
	require.NoError(t, err)
	spaceId := sp.Id()
	_, err = sp.Objects().Create(ctx, space.CreateObjectOpts{Type: cfType, Meta: map[string]any{"createdIn": "x", "via": "/task"}})
	require.NoError(t, err)
	require.NoError(t, sdk.Close())

	sdk = open(t, "meta.via")
	sp, err = sdk.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)
	rows, err := sp.QueryObjects().Filter(map[string]any{"meta.via": "/task"}).All(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.NoError(t, sdk.Close())

	var dbPath string
	require.NoError(t, filepath.WalkDir(filepath.Dir(dataDir), func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && d.Name() == "sdk.db" {
			dbPath = path
		}
		return nil
	}))
	db, err := anystore.Open(ctx, dbPath, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	names, err := db.GetCollectionNames(ctx)
	require.NoError(t, err)
	var checked bool
	for _, n := range names {
		if !strings.HasSuffix(n, "_objects") || !strings.Contains(n, spaceId) {
			continue
		}
		coll, err := db.OpenCollection(ctx, n)
		require.NoError(t, err)
		for _, idx := range coll.GetIndexes() {
			if idx.Info().Name == "idx_ext_meta" {
				checked = true
				assert.Equal(t, []string{"meta.via"}, idx.Info().Fields, "the redefined index replaced the stored one")
			}
		}
	}
	assert.True(t, checked, "idx_ext_meta on the objects collection")
}
