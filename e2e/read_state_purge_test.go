package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestSDK_ReadState_PurgeSurvivesReconcile: a purged object's read
// state stays gone across a restart. The device published its frontier
// for the object before the delete; the boot reconcile replays every
// published frontier, and merging this one must not park its heads on
// a fresh state row for an object whose tree is deleted.
func TestSDK_ReadState_PurgeSurvivesReconcile(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available at %s: %v", confPath, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	provider := newFixedSeedProvider(t)
	dataDir := t.TempDir()
	open := func() *anysyncsdk.SDK {
		sdk, err := anysyncsdk.Open(ctx, config.Config{
			Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
			Types:   []handler.Type{newReadTrackedType()},
		}, provider)
		require.NoError(t, err)
		return sdk
	}
	sdk := open()
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "ReadStatePurge"})
	if err != nil {
		_ = sdk.Close()
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: rtTypeId})
	require.NoError(t, err)
	res, err := sp.Modify(ctx, space.ModifyBatch{
		ObjectId: objId,
		Dataset:  rtDataset,
		Records: []space.RecordModify{{
			Id: "m1", Upsert: true,
			Ops: []space.Op{{Type: space.OpSet, Path: "text", Value: "hello"}},
		}},
	})
	require.NoError(t, err)
	// The mark publishes this device's frontier for the object.
	require.NoError(t, sp.ReadState().MarkRead(ctx, objId, []string{res.ChangeId}))

	listed := func(sp space.Space) bool {
		dirty, err := sp.ReadState().ChangedSince(ctx, 0, 0)
		require.NoError(t, err)
		for _, d := range dirty {
			if d.ObjectId == objId {
				return true
			}
		}
		return false
	}
	require.True(t, listed(sp), "the tracked object has read state")

	require.NoError(t, sp.Objects().Delete(ctx, objId))
	require.False(t, listed(sp), "the purge removed the read state")

	require.NoError(t, sdk.Close())
	sdk = open()
	t.Cleanup(func() { _ = sdk.Close() })
	sp, err = sdk.Spaces().Get(ctx, sp.Id())
	require.NoError(t, err)
	require.Never(t, func() bool { return listed(sp) }, 3*time.Second, 100*time.Millisecond,
		"the boot reconcile must not re-create the purged object's read state")
}
