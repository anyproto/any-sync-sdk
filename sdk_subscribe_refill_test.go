package anysyncsdk

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// A per-object window refills from storage however it came to hold its
// rows: one subscribed before its object was local resolves the object
// at refill time, and one whose builder is chained on after Subscribe
// reads the query as it was at Subscribe.
func TestSDK_SubscribeRefillResolvesObjectAndQuery(t *testing.T) {
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sdk, err := Open(ctx, wmConfig(t, dataDir), wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Refill"})
	require.NoError(t, err)

	// The derived id is a pure function of the root payload, so the
	// subscription opens on it before the object exists.
	seed := []byte("refill-before-local")
	root, err := objecttree.DeriveObjectTreeRoot(objecttree.ObjectTreeDerivePayload{
		ChangeType: "object", ChangePayload: seed, SpaceId: sp.Id(), IsEncrypted: true,
	}, nil)
	require.NoError(t, err)
	objId := root.Id

	res, err := sp.Query(objId, wmDataset).Sort("n").Limit(3).Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	defer func() { _ = res.Sub.Close() }()
	require.Empty(t, res.Initial, "the object is not local yet")

	derived, err := sp.Objects().Derive(ctx, space.DeriveObjectOpts{Seed: seed, Type: "wm-blocks-type"})
	require.NoError(t, err)
	require.Equal(t, objId, derived)

	write := func(id string, n int) {
		_, err := sp.Modify(ctx, space.ModifyBatch{ObjectId: objId, Dataset: wmDataset, Records: []space.RecordModify{{
			Id: id, Upsert: true, Ops: []space.Op{{Type: space.OpSet, Path: "n", Value: n}},
		}}})
		require.NoError(t, err, id)
	}
	del := func(id string) {
		_, err := sp.Modify(ctx, space.ModifyBatch{ObjectId: objId, Dataset: wmDataset, Records: []space.RecordModify{{
			Id: id, Ops: []space.Op{{Type: space.OpDelete}},
		}}})
		require.NoError(t, err, id)
	}
	fresh := func() []string {
		rows, err := sp.Query(objId, wmDataset).Sort("n").Limit(3).All(ctx)
		require.NoError(t, err)
		return idsOfRows(rows)
	}
	for i := 1; i <= 8; i++ {
		write(fmt.Sprintf("r%d", i), i)
	}

	view := map[string]*anyenc.Value{}
	for _, v := range res.Initial {
		view[v.GetString("id")] = v
	}
	waitView := func(sub space.QuerySubscription, want []string) {
		t.Helper()
		waitCtx, waitCancel := context.WithTimeout(ctx, 15*time.Second)
		defer waitCancel()
		for {
			if got := idsOfView(view); sort.StringsAreSorted(got) && equalStrings(got, want) {
				return
			}
			evs, err := sub.Events().Wait(waitCtx)
			require.NoError(t, err, "want %v, view %v, sub err %v", want, idsOfView(view), sub.Err())
			for _, ev := range evs {
				for _, r := range ev.Added {
					view[r.Id] = r.Doc
				}
				for _, r := range ev.Updated {
					view[r.Id] = r.Doc
				}
				for _, r := range ev.Removed {
					delete(view, r.Id)
				}
			}
		}
	}
	waitView(res.Sub, []string{"r1", "r2", "r3"})

	// The window has rows past its edge; a departure refills through
	// the object the subscribe call never saw.
	del("r1")
	waitView(res.Sub, []string{"r2", "r3", "r4"})
	assert.Equal(t, fresh(), idsOfView(view))
	assert.NoError(t, res.Sub.Err())

	// A builder chained on after Subscribe does not reach the live
	// window: the refill keeps the limit of 3 where a leaked limit of
	// 6 would show six of the seven rows left.
	base := sp.Query(objId, wmDataset).Sort("n").Limit(3)
	res2, err := base.Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	defer func() { _ = res2.Sub.Close() }()
	require.Equal(t, []string{"r2", "r3", "r4"}, idsOfRows(res2.Initial))
	wide, err := base.Limit(6).All(ctx)
	require.NoError(t, err)
	require.Len(t, wide, 6)
	view = map[string]*anyenc.Value{}
	for _, v := range res2.Initial {
		view[v.GetString("id")] = v
	}
	del("r2")
	waitView(res2.Sub, []string{"r3", "r4", "r5"})
	// Let a leaked limit show: the refill's event has landed when the
	// view reached three rows, so a wider one would be visible now.
	assert.Equal(t, []string{"r3", "r4", "r5"}, idsOfView(view))
	assert.NoError(t, res2.Sub.Err())
}

func idsOfRows(rows []*anyenc.Value) []string {
	ids := make([]string, 0, len(rows))
	for _, v := range rows {
		ids = append(ids, v.GetString("id"))
	}
	return ids
}

func idsOfView(view map[string]*anyenc.Value) []string {
	ids := make([]string, 0, len(view))
	for id := range view {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
