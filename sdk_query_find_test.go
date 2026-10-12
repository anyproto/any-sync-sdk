package anysyncsdk

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// TestQuery_FindWindowExcludesTombstones: the find terminals cut their
// limit/offset window over live rows. A tombstone keeps no user field,
// so under a user-field sort it sorts first; a window cut before the
// tombstone skip came back short, paged wrong, or found nothing.
func TestQuery_FindWindowExcludesTombstones(t *testing.T) {
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sdk, err := Open(ctx, wmConfig(t, dataDir), wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Find"})
	require.NoError(t, err)
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: "wm-blocks-type"})
	require.NoError(t, err)

	// Six records n=1..6; the two highest are then deleted and sort
	// first as tombstones.
	var records []space.RecordModify
	for i := 1; i <= 6; i++ {
		records = append(records, space.RecordModify{
			Id: fmt.Sprintf("r%d", i), Upsert: true,
			Ops: []space.Op{{Type: space.OpSet, Path: "n", Value: i}},
		})
	}
	_, err = sp.Modify(ctx, space.ModifyBatch{ObjectId: objId, Dataset: wmDataset, Records: records})
	require.NoError(t, err)
	_, err = sp.Modify(ctx, space.ModifyBatch{ObjectId: objId, Dataset: wmDataset, Records: []space.RecordModify{
		{Id: "r5", Ops: []space.Op{{Type: space.OpDelete}}},
		{Id: "r6", Ops: []space.Op{{Type: space.OpDelete}}},
	}})
	require.NoError(t, err)

	ids := func(docs []*anyenc.Value) []string {
		out := make([]string, 0, len(docs))
		for _, d := range docs {
			out = append(out, string(d.GetStringBytes("id")))
		}
		return out
	}
	sorted := func() space.Query { return sp.Query(objId, wmDataset).Sort("n") }

	first, err := sorted().Limit(2).All(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"r1", "r2"}, ids(first), "the window holds live rows only")

	second, err := sorted().Limit(2).Offset(2).All(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"r3", "r4"}, ids(second), "the offset counts live rows only")

	one, err := sorted().One(ctx)
	require.NoError(t, err)
	assert.Equal(t, "r1", string(one.GetStringBytes("id")))

	it, err := sorted().Limit(3).Offset(1).Iter(ctx)
	require.NoError(t, err)
	var streamed []*anyenc.Value
	for it.Next() {
		d, err := it.Doc()
		require.NoError(t, err)
		streamed = append(streamed, d)
		assert.Nil(t, d.Get("_deletedAt"))
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	assert.Len(t, streamed, 3)

	n, err := sorted().Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, n)

	// IncludeDeleted keeps the tombstones in the same window arithmetic.
	withDeleted, err := sorted().Projection(space.ProjectionOpts{IncludeDeleted: true}).Limit(2).All(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"r5", "r6"}, ids(withDeleted), "tombstones sort first without n")
	n, err = sorted().Projection(space.ProjectionOpts{IncludeDeleted: true}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 6, n)
}
