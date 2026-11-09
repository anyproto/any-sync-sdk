package anysyncsdk

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// After a responsible peer's zero-diff round, an object with no
// per-tree sync history reads synced only when its tree is in local
// storage: an id this device never stored, and a deleted object, read
// unknown.
func TestSDK_SyncStatusInheritsSyncedOnlyForStoredTrees(t *testing.T) {
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	sdk, err := Open(ctx, wmConfig(t, dataDir), wmProvider(t, dataDir))
	require.NoError(t, err)
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Status"})
	require.NoError(t, err)
	spaceId := sp.Id()
	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Note"})
	require.NoError(t, err)
	kept, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)
	deleted, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)
	require.NoError(t, sp.Objects().Delete(ctx, deleted))
	require.NoError(t, sdk.Close())

	// A reopened SDK has no per-tree rows.
	sdk, err = Open(ctx, wmConfig(t, dataDir), wmProvider(t, dataDir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })
	sp, err = sdk.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)

	const node = "responsible-node"
	svc := sdk.app.SyncStatus()
	svc.SetNodeIdsFn(func(string) []string { return []string{node} })
	svc.For(spaceId).BulkSyncedFromPeer(node)

	assert.Equal(t, space.SyncStateSynced, sp.SyncStatus().Object(kept).State, "stored tree")
	assert.Equal(t, space.SyncStateUnknown, sp.SyncStatus().Object(deleted).State, "deleted tree")
	assert.Equal(t, space.SyncStateUnknown, sp.SyncStatus().Object("bafyreinotstoredonthisdevice").State, "id never stored")
}
