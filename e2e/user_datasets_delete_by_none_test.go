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

// TestE2E_UserDatasets_DeleteByNone declares the multi-writer shape
// (caller ids, every field mutable by anyone, no stamps, no record
// deletes), checks the policy survives the definition round trip, and
// that Delete is refused before anything is written: the record stays
// live and its id stays writable.
func TestE2E_UserDatasets_DeleteByNone(t *testing.T) {
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

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "NoDeleteLab"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatal(err)
	}

	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Overview"})
	require.NoError(t, err)
	_, err = sp.Types().AddPart(ctx, typeId, space.PartDraft{
		Key: "entries", Name: "Entries",
		Datasets: []space.DatasetDraft{{
			Key:      "entries",
			IdRule:   space.IdUser,
			DeleteBy: space.DeleteByNone,
			Fields: []space.DatasetFieldDraft{
				{Key: "pos", Kind: space.PropertyKindString, MutableBy: space.MutableByAnyone},
				{Key: "removed", Kind: space.PropertyKindBoolean, MutableBy: space.MutableByAnyone},
			},
		}},
	})
	require.NoError(t, err)

	defs, err := sp.Types().Datasets(ctx, typeId)
	require.NoError(t, err)
	require.Len(t, defs, 1)
	assert.Equal(t, space.DeleteByNone, defs[0].DeleteBy, "the policy is on the head record")
	coll := defs[0].Collection

	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)
	res, err := sp.Upsert(ctx, space.UpsertBatch{
		ObjectId: objId, Dataset: coll,
		Records: []space.UpsertRecord{{Id: "w:chat", Fields: map[string]any{"pos": "a0"}}},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, res.Created)

	// Refused before a change exists: the sentinel, nothing written.
	_, err = sp.Delete(ctx, space.DeleteBatch{ObjectId: objId, Dataset: coll, RecordIds: []string{"w:chat", "never"}})
	require.ErrorIs(t, err, space.ErrDatasetUndeletable)

	row, err := sp.Query(objId, coll).Filter(map[string]any{"id": "w:chat"}).One(ctx)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, "a0", string(row.GetStringBytes("pos")))

	// The id was never burned: the next write lands.
	res, err = sp.Upsert(ctx, space.UpsertBatch{
		ObjectId: objId, Dataset: coll,
		Records: []space.UpsertRecord{
			{Id: "w:chat", Fields: map[string]any{"removed": true}},
			{Id: "never", Fields: map[string]any{"pos": "b0"}},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, res.Rejections)
	assert.Equal(t, 1, res.Updated)
	assert.Equal(t, 1, res.Created)
}
