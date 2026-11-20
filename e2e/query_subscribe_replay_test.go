package e2e

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_QuerySubscribe_ReplayRefillsThroughLoadingObject: a windowed
// per-object Subscribe opened before the object exists anywhere, on a
// device that then receives the object's whole history in one piece.
// The load replays every change through the apply hook while the
// object is still loading; five of the changes delete rows inside the
// window, which the window refills through the loading instance. A
// refill that could not reach it would count each deletion as a loss
// and close the window as drifted after the second.
//
// Skips when no any-sync network is reachable.
func TestE2E_QuerySubscribe_ReplayRefillsThroughLoadingObject(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("replay e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	providerA := newFixedSeedProvider(t)
	providerB := sameAccountFreshDevice(t, providerA)
	// LAN p2p is off: device A would push each change to B as it is
	// written, and B would apply them live instead of replaying the
	// tree it pulls from the node.
	p2pOff := false
	open := func(name string, provider *fixedSeedProvider) *anysyncsdk.SDK {
		t.Helper()
		sdk, err := anysyncsdk.Open(ctx, config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
			P2P:     config.P2P{Enabled: &p2pOff},
			Types:   []handler.Type{newNotesType()},
		}, provider)
		require.NoError(t, err, "%s: Open", name)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk
	}

	sdkA := open("device A", providerA)
	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "ReplayRefill"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("device A: Spaces().Create: %v", err)
	}
	spaceId := spA.Id()
	_ = sdkA.Spaces().SyncSpaceList(ctx)

	// The derived id is a pure function of the root payload: device B
	// subscribes on it before device A creates the object.
	seed := []byte("replay-refill")
	root, err := objecttree.DeriveObjectTreeRoot(objecttree.ObjectTreeDerivePayload{
		ChangeType: "object", ChangePayload: seed, SpaceId: spaceId, IsEncrypted: true,
	}, nil)
	require.NoError(t, err)
	objId := root.Id

	sdkB := open("device B", providerB)
	var spB space.Space
	polls := 0
	require.True(t, waitFor(ctx, 90*time.Second, 50*time.Millisecond, func() bool {
		spB, err = sdkB.Spaces().Get(ctx, spaceId)
		if err != nil && polls%20 == 0 {
			_ = sdkB.Spaces().SyncSpaceList(ctx)
		}
		polls++
		return err == nil
	}), "device B: Spaces().Get(%s) never succeeded: %v", spaceId, err)

	res, err := spB.Query(objId, notesDataset).Sort("text").Limit(5).Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err, "device B: Subscribe on an object that exists nowhere yet")
	t.Cleanup(func() { _ = res.Sub.Close() })
	require.Empty(t, res.Initial)

	// ---- Device A: the object's whole history, one change each ----
	derived, err := spA.Objects().Derive(ctx, space.DeriveObjectOpts{Seed: seed, Type: "notes-type"})
	require.NoError(t, err, "device A: Objects().Derive")
	require.Equal(t, objId, derived)
	const records = 15
	texts := make([]string, 0, records)
	for i := 0; i < records; i++ {
		text := fmt.Sprintf("note %02d", i)
		texts = append(texts, text)
		_, err = spA.Modify(ctx, space.ModifyBatch{
			ObjectId: objId,
			Dataset:  notesDataset,
			Records: []space.RecordModify{{
				Id:     fmt.Sprintf("rec-%02d", i),
				Upsert: true,
				Ops:    []space.Op{{Type: space.OpSet, Path: "text", Value: text}},
			}},
		})
		require.NoError(t, err, "device A: seed record %d", i)
	}
	// The five rows the window shows first, deleted one change each:
	// well past the default budget of 30% of the limit.
	for i := 0; i < 5; i++ {
		_, err = spA.Modify(ctx, space.ModifyBatch{
			ObjectId: objId,
			Dataset:  notesDataset,
			Records:  []space.RecordModify{{Id: fmt.Sprintf("rec-%02d", i), Ops: []space.Op{{Type: space.OpDelete}}}},
		})
		require.NoError(t, err, "device A: delete record %d", i)
	}
	require.NoError(t, spA.SyncHeads(ctx), "device A: push the tree to the node")

	// ---- Device B: fold events until the window shows the survivors ----
	want := texts[5:10]
	view := map[string]*anyenc.Value{}
	textsOf := func() []string {
		out := make([]string, 0, len(view))
		for _, v := range view {
			out = append(out, v.GetString("text"))
		}
		sort.Strings(out)
		return out
	}
	foldCtx, foldCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer foldCancel()
	for !slices.Equal(textsOf(), want) {
		evs, err := res.Sub.Events().Wait(foldCtx)
		require.NoError(t, err, "the window must stay open while the replay refills it; view %v, sub err %v", textsOf(), res.Sub.Err())
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
	assert.NoError(t, res.Sub.Err())

	// A fresh query of the same body agrees once the object has landed.
	require.True(t, waitFor(ctx, 30*time.Second, 100*time.Millisecond, func() bool {
		n, err := spB.Query(objId, notesDataset).Count(ctx)
		return err == nil && n == records-5
	}), "device B never converged to %d records", records-5)
	fresh, err := spB.Query(objId, notesDataset).Sort("text").Limit(5).All(ctx)
	require.NoError(t, err)
	got := make([]string, 0, len(fresh))
	for _, v := range fresh {
		got = append(got, v.GetString("text"))
	}
	assert.Equal(t, want, got)
	assert.Equal(t, want, textsOf())
}
