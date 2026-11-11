package anysyncsdk

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// Rows that share a sort key across the window's edge: the row Initial
// leaves out is the row the live window hides, in either sort
// direction. A change to it reaches no consumer, and when a shown row
// leaves, the window ends equal to a fresh query.
func TestSDK_SubscribeTiedWindowEdge(t *testing.T) {
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sdk, err := Open(ctx, wmConfig(t, dataDir), wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Ties"})
	require.NoError(t, err)

	for _, sortKey := range []string{"n", "-n"} {
		t.Run(sortKey, func(t *testing.T) {
			objId, err := sp.Objects().Derive(ctx, space.DeriveObjectOpts{Seed: []byte("ties" + sortKey), Type: "wm-blocks-type"})
			require.NoError(t, err)
			set := func(id, path string, v int) {
				_, err := sp.Modify(ctx, space.ModifyBatch{ObjectId: objId, Dataset: wmDataset, Records: []space.RecordModify{{
					Id: id, Upsert: true, Ops: []space.Op{{Type: space.OpSet, Path: path, Value: v}},
				}}})
				require.NoError(t, err, id)
			}
			// Six rows on one sort key: the edge of a window of 2 falls
			// among ties whichever way it is sorted.
			for i := 1; i <= 6; i++ {
				set(fmt.Sprintf("r%d", i), "n", 7)
			}
			q := func(limit int) space.Query { return sp.Query(objId, wmDataset).Sort(sortKey).Limit(limit) }
			fresh := func(limit int) []string {
				rows, err := q(limit).All(ctx)
				require.NoError(t, err)
				return idsOfRows(rows)
			}

			res, err := q(2).Subscribe(ctx, space.QueryOpts{})
			require.NoError(t, err)
			defer func() { _ = res.Sub.Close() }()
			window := fresh(3)
			require.Equal(t, window[:2], idsOfRows(res.Initial))
			shown, hidden := window[0], window[2]

			// The hidden row changes, then a shown row: only the second
			// is the consumer's.
			set(hidden, "m", 1)
			set(shown, "m", 1)
			evCtx, evCancel := context.WithTimeout(ctx, 15*time.Second)
			defer evCancel()
			ev, err := res.Sub.Events().WaitOne(evCtx)
			require.NoError(t, err)
			assert.Empty(t, ev.Added)
			assert.Empty(t, ev.Removed)
			require.Len(t, ev.Updated, 1)
			assert.Equal(t, shown, ev.Updated[0].Id)

			// A shown row leaves: the hidden one replaces it.
			_, err = sp.Modify(ctx, space.ModifyBatch{ObjectId: objId, Dataset: wmDataset, Records: []space.RecordModify{{
				Id: shown, Ops: []space.Op{{Type: space.OpDelete}},
			}}})
			require.NoError(t, err)
			view := map[string]struct{}{window[0]: {}, window[1]: {}}
			want := fresh(2)
			for !equalStrings(sortedIds(view), sortedStrings(want)) {
				evs, err := res.Sub.Events().Wait(evCtx)
				require.NoError(t, err, "want %v, view %v, sub err %v", want, sortedIds(view), res.Sub.Err())
				for _, ev := range evs {
					for _, r := range ev.Added {
						_, dup := view[r.Id]
						require.False(t, dup, "added %s is already shown", r.Id)
						view[r.Id] = struct{}{}
					}
					for _, r := range ev.Removed {
						_, ok := view[r.Id]
						require.True(t, ok, "removed %s was never shown", r.Id)
						delete(view, r.Id)
					}
				}
			}
			assert.Contains(t, view, hidden)
			assert.NoError(t, res.Sub.Err())
		})
	}
}

func sortedIds(view map[string]struct{}) []string {
	ids := make([]string, 0, len(view))
	for id := range view {
		ids = append(ids, id)
	}
	return sortedStrings(ids)
}

func sortedStrings(ids []string) []string {
	out := append([]string(nil), ids...)
	sort.Strings(out)
	return out
}
