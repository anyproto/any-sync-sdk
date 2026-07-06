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

// TestSDK_Aggregate exercises the Space.Aggregate / AggregateObjects
// surface end-to-end:
//
//   - AggregateObjects runs a $match + $group pipeline over the
//     per-space objects collection; the group key comes back as `id`
//     (not `_id`).
//   - Tombstoned rows are excluded (the prepended _deletedAt $match).
//   - Aggregate(objectId, dataset) works over a per-object dataset.
//   - Bad pipelines surface space.ErrBadPipeline; GroupLimit overruns
//     surface space.ErrAggGroupLimitExceeded.
//   - A non-materialised dataset aggregates to empty, not an error.
func TestSDK_Aggregate(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Types:   []handler.Type{newBlocksType()},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Agg"})
	require.NoError(t, err)

	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Movie"})
	require.NoError(t, err)
	titleProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Title", XKey: "title", Kind: space.PropertyKindString,
	})
	require.NoError(t, err)
	yearProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Year", XKey: "year", Kind: space.PropertyKindNumber,
	})
	require.NoError(t, err)

	yearPath := typeId + "." + yearProp
	titlePath := typeId + "." + titleProp

	seed := func(title string, year int) string {
		t.Helper()
		id, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
		require.NoError(t, err)
		_, err = sp.Properties().Set(ctx, id, typeId, map[string]any{
			titleProp: title, yearProp: year,
		})
		require.NoError(t, err)
		return id
	}
	seed("Brazil", 1985)
	aliensId := seed("Aliens", 1986)
	seed("Platoon", 1986)

	groupByYear := `[
		{"$match": {"` + yearPath + `": {"$gte": 1980}}},
		{"$group": {"_id": "$` + yearPath + `", "n": {"$count": {}}, "titles": {"$push": "$` + titlePath + `"}}},
		{"$sort": {"id": 1}}
	]`

	// Group output key is `id`, never `_id`.
	rows, err := sp.AggregateObjects(groupByYear).All(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Nil(t, rows[0].Get("_id"))
	assert.Equal(t, float64(1985), rows[0].GetFloat64("id"))
	assert.Equal(t, 1, rows[0].GetInt("n"))
	assert.Equal(t, float64(1986), rows[1].GetFloat64("id"))
	assert.Equal(t, 2, rows[1].GetInt("n"))
	assert.Len(t, rows[1].GetArray("titles"), 2)

	// Count counts result documents (groups), not source rows.
	n, err := sp.AggregateObjects(groupByYear).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	// Explain reports the pushed prefix and the in-pipeline stages.
	plan, err := sp.AggregateObjects(groupByYear).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, plan, "Stages:")

	// Tombstone exclusion: delete one 1986 movie, the group shrinks.
	require.NoError(t, sp.Objects().Delete(ctx, aliensId))
	rows, err = sp.AggregateObjects(groupByYear).All(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, 1, rows[1].GetInt("n"), "deleted movie still aggregated")

	// GroupLimit overrun surfaces the sentinel mid-iteration.
	_, err = sp.AggregateObjects(groupByYear).GroupLimit(1).All(ctx)
	assert.ErrorIs(t, err, space.ErrAggGroupLimitExceeded)

	// Bad pipelines: non-array and unknown stage.
	_, err = sp.AggregateObjects(`{"$match": {}}`).All(ctx)
	assert.ErrorIs(t, err, space.ErrBadPipeline)
	_, err = sp.AggregateObjects(`[{"$bogus": {}}]`).All(ctx)
	assert.ErrorIs(t, err, space.ErrBadPipeline)

	// Per-object dataset aggregation over the blocks handler type.
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{"blocks-type"}})
	require.NoError(t, err)
	for recId, kind := range map[string]string{
		"b1": "paragraph", "b2": "heading", "b3": "paragraph",
	} {
		_, err = sp.Modify(ctx, space.ModifyBatch{
			ObjectId: objId,
			Dataset:  blocksDataset,
			Records: []space.RecordModify{{
				Id:     recId,
				Upsert: true,
				Ops: []space.Op{{
					Type:  space.OpSet,
					Path:  "kind",
					Value: kind,
				}},
			}},
		})
		require.NoError(t, err)
	}
	rows, err = sp.Aggregate(objId, blocksDataset, `[
		{"$group": {"_id": "$kind", "n": {"$count": {}}}},
		{"$sort": {"id": 1}}
	]`).All(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, "heading", rows[0].GetString("id"))
	assert.Equal(t, 1, rows[0].GetInt("n"))
	assert.Equal(t, "paragraph", rows[1].GetString("id"))
	assert.Equal(t, 2, rows[1].GetInt("n"))

	// Non-materialised dataset → empty result, zero count, no error.
	empty, err := sp.Aggregate(objId, "no-such-dataset", nil).All(ctx)
	require.NoError(t, err)
	assert.Empty(t, empty)
	n, err = sp.Aggregate(objId, "no-such-dataset", nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}
