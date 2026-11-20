package anysyncsdk

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// A subscribe's total is counted in the snapshot's own read: with
// writes landing throughout, Total and HasNext describe Initial on
// every subscribe. Rows are numbered in write order, one change each,
// so the highest id a snapshot holds names how many rows it saw.
func TestSDK_SubscribeTotalDescribesSnapshot(t *testing.T) {
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sdk, err := Open(ctx, wmConfig(t, dataDir), wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Total"})
	require.NoError(t, err)
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: "wm-blocks-type"})
	require.NoError(t, err)

	var written atomic.Int64
	var writeErr atomic.Value
	writerCtx, stopWriter := context.WithCancel(ctx)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 1; writerCtx.Err() == nil; i++ {
			_, err := sp.Modify(writerCtx, space.ModifyBatch{ObjectId: objId, Dataset: wmDataset, Records: []space.RecordModify{{
				Id: fmt.Sprintf("r%06d", i), Upsert: true,
				Ops: []space.Op{{Type: space.OpSet, Path: "n", Value: i}},
			}}})
			if err != nil {
				if writerCtx.Err() == nil {
					writeErr.Store(err)
				}
				return
			}
			written.Store(int64(i))
		}
	}()
	defer func() {
		stopWriter()
		<-writerDone
		if err, _ := writeErr.Load().(error); err != nil {
			t.Errorf("writer: %v", err)
		}
	}()
	require.Eventually(t, func() bool { return written.Load() >= 4 }, 20*time.Second, 10*time.Millisecond)

	rowNumber := func(id string) int {
		n, err := strconv.Atoi(id[1:])
		require.NoError(t, err, id)
		return n
	}
	for i := 0; i < 50 && writeErr.Load() == nil; i++ {
		// A window smaller than the row set: the count runs.
		res, err := sp.Query(objId, wmDataset).Sort("-id").Limit(3).Subscribe(ctx, space.QueryOpts{IncludeTotal: true})
		require.NoError(t, err)
		require.NotEmpty(t, res.Initial)
		rows := rowNumber(res.Initial[0].GetString("id"))
		assert.Equal(t, rows, res.Total, "total counts the rows the snapshot saw")
		assert.Equal(t, len(res.Initial) < res.Total, res.HasNext)
		require.NoError(t, res.Sub.Close())

		// A window that holds every match: the total is the row count.
		all, err := sp.Query(objId, wmDataset).Sort("-id").Limit(1_000_000).Subscribe(ctx, space.QueryOpts{IncludeTotal: true})
		require.NoError(t, err)
		require.NotEmpty(t, all.Initial)
		assert.Equal(t, rowNumber(all.Initial[0].GetString("id")), len(all.Initial))
		assert.Equal(t, len(all.Initial), all.Total)
		assert.False(t, all.HasNext)
		require.NoError(t, all.Sub.Close())

		// Unbounded: the same.
		unb, err := sp.Query(objId, wmDataset).Subscribe(ctx, space.QueryOpts{IncludeTotal: true})
		require.NoError(t, err)
		assert.Equal(t, len(unb.Initial), unb.Total)
		assert.False(t, unb.HasNext)
		require.NoError(t, unb.Sub.Close())
	}
}
