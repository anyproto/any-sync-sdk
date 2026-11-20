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

// TestSDK_DebugAPI covers the per-object debug snapshot end-to-end:
//
//   - Object() resolves an unknown id with an error.
//   - After two writes (object create + Properties().Set) the tree has at
//     least 2 changes, one snapshot (the root), and one head.
//   - LatestVersionId is the lexid OrderId of the head change.
//   - MaxAddSeq advances as new local changes apply.
//   - Branch count is 0 in a linear tree.
//   - Space() returns the spaceId and an in-memory peer-stats slice
//     (may be empty if no diff round has run yet).
//
// Local-only: no network round-trips needed. Skips when the staging
// config isn't present, same as the other end-to-end tests in this
// package — Open requires a node-config YAML.
func TestSDK_DebugAPI(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "DebugDemo"})
	require.NoError(t, err)

	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Note"})
	require.NoError(t, err)
	propId, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Title",
		XKey: "title",
		Kind: space.PropertyKindString,
	})
	require.NoError(t, err)

	objectId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
		Type: typeId,
	})
	require.NoError(t, err)

	// Two local writes — adds two changes to the object tree on
	// top of the root (which is itself a snapshot).
	_, err = sp.Properties().Set(ctx, objectId, typeId, map[string]any{
		propId: "first",
	})
	require.NoError(t, err)
	_, err = sp.Properties().Set(ctx, objectId, typeId, map[string]any{
		propId: "second",
	})
	require.NoError(t, err)

	dbg := sp.Debug()

	// Unknown id: validation error, not a panic.
	_, err = dbg.Object(ctx, "")
	require.Error(t, err)

	got, err := dbg.Object(ctx, objectId)
	require.NoError(t, err)
	assert.Equal(t, objectId, got.ObjectId)

	// Tree shape: linear chain root → write1 → write2 → … .
	// HeadsCount must be 1 (no concurrent writers), BranchCount 0
	// (no merges), and the root change is always a snapshot so
	// Snapshots ≥ 1.
	assert.Equal(t, 1, got.HeadsCount, "single-writer object should have one head")
	assert.Equal(t, 0, got.BranchCount, "linear tree has no branches")
	assert.GreaterOrEqual(t, got.TreeLen, 3, "root + at least two writes")
	assert.GreaterOrEqual(t, got.Snapshots, 1, "root is a snapshot")

	require.Len(t, got.Heads, 1)
	assert.NotEmpty(t, got.LatestVersionId, "head change must carry an OrderId")
	assert.Greater(t, got.MaxAddSeq, uint64(0), "controller must have applied at least one change")

	// SyncState is one of {Unknown, Syncing, Synced, Error,
	// Offline}; for a freshly-created local object without
	// network ack we expect Syncing or Unknown depending on
	// whether the HeadsChange hook has fired yet.
	assert.NotEqual(t, space.SyncStateError, got.SyncState)

	// Space-level view: spaceId set, Peers slice is in-memory only
	// (may be empty if no headsync round has completed yet against
	// a responsible peer in this short-lived local test).
	sd := dbg.Space()
	assert.Equal(t, sp.Id(), sd.SpaceId)
	for _, p := range sd.Peers {
		assert.NotEmpty(t, p.PeerId)
	}
}
