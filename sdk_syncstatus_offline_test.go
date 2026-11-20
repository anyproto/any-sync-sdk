package anysyncsdk

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// With no node reachable and no direct peer, a space reads Offline:
// empty, after a write, and after a restart with no writes. Nothing a
// peer never confirmed reads Synced.
func TestSDK_SyncStatusOfflineWithoutNodes(t *testing.T) {
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cfg := wmConfig(t, dataDir)
	// Node addresses moved to closed ports, so every dial is refused,
	// and no LAN peer to find.
	cfg.Network.NodeConfYAML = bytes.ReplaceAll(cfg.Network.NodeConfYAML, []byte("127.0.0.1:"), []byte("127.0.0.1:1"))
	p2pOff := false
	cfg.P2P.Enabled = &p2pOff

	sdk, err := Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Offline"})
	require.NoError(t, err)
	spaceId := sp.Id()

	got := sdk.Spaces().Status(spaceId)
	assert.Equal(t, space.SyncStateOffline, got.State, "empty space: %+v", got)
	assert.Zero(t, got.NetworkPeers+got.LocalPeers+got.GlobalPeers)

	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: "wm-blocks-type"})
	require.NoError(t, err)
	_, err = sp.Modify(ctx, space.ModifyBatch{ObjectId: objId, Dataset: wmDataset, Records: []space.RecordModify{{
		Id: "r1", Upsert: true, Ops: []space.Op{{Type: space.OpSet, Path: "n", Value: 1}},
	}}})
	require.NoError(t, err)
	got = sdk.Spaces().Status(spaceId)
	assert.Equal(t, space.SyncStateOffline, got.State, "after a write: %+v", got)
	assert.GreaterOrEqual(t, got.Total, 1)
	total := got.Total
	require.NoError(t, sdk.Close())

	sdk, err = Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()
	_, err = sdk.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)
	got = sdk.Spaces().Status(spaceId)
	assert.Equal(t, space.SyncStateOffline, got.State, "after a restart with no writes: %+v", got)
	assert.Equal(t, total, got.Total)
	assert.True(t, got.LastSyncedAt.IsZero(), "no peer confirmed anything in this run")
}
