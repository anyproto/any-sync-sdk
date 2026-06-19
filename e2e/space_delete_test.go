package e2e

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestSDK_SpaceDelete_OffloadsLocally is the offline-first deletion
// assertion: Delete reclaims the space's local storage immediately,
// without requiring the network. We don't depend on coordinator
// reachability — Create writes locally first, and the offload is purely
// local. (The signed coordinator SpaceDelete is driven asynchronously by
// the deletion reconciler; its classification logic is unit-tested.)
func TestSDK_SpaceDelete_OffloadsLocally(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	dataDir := t.TempDir()
	cfg := config.Config{
		Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Types:   []handler.Type{newBlocksType()},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "ToDelete"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}

	// Seed an object + record so the space has real per-object state on
	// disk to offload.
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{"blocks-type"}})
	require.NoError(t, err)
	_, err = sp.Modify(ctx, space.ModifyBatch{
		ObjectId: objId,
		Dataset:  blocksDataset,
		Records: []space.RecordModify{{
			Id:     "rec-1",
			Upsert: true,
			Ops:    []space.Op{{Type: space.OpSet, Path: "text", Value: "hi"}},
		}},
	})
	require.NoError(t, err)

	// The any-sync per-space DB file exists before deletion.
	dbPath := filepath.Join(dataDir, "anysync", sp.Id()+".db")
	require.FileExists(t, dbPath, "space DB should exist before delete")

	// Delete — offline-first, returns after the local offload.
	require.NoError(t, sdk.Spaces().Delete(ctx, sp.Id()))

	// Local storage reclaimed: the any-sync DB file is gone.
	_, statErr := os.Stat(dbPath)
	assert.True(t, os.IsNotExist(statErr), "space DB should be removed after delete, stat err=%v", statErr)

	// The row stays in List as a sticky tombstone with Status=Deleted.
	list, err := sdk.Spaces().List(ctx)
	require.NoError(t, err)
	got := findSpace(list, sp.Id())
	require.NotNil(t, got, "deleted space should remain in List as a tombstone")
	assert.Equal(t, space.StatusDeleted, got.Status)
}

// TestSDK_SpaceDelete_NotReloadedAfterRestart guards the boot-path
// eager-loader against resurrecting deletion tombstones. Deletion is
// recorded only on the SYNCED RemoteStatus field; an earlier loader bug
// skipped solely on LocalStatus (which is never "deleted"), so every
// restart re-loaded the deleted space, rebuilt its any-sync storage, and
// started periodic headsync that the node rejects with "space is
// deleted". After Delete + reopen, the space's DB file must stay gone
// and the row must remain a tombstone.
func TestSDK_SpaceDelete_NotReloadedAfterRestart(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	dataDir := t.TempDir()
	cfg := config.Config{
		Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Types:   []handler.Type{newBlocksType()},
	}
	// Same seed across both boots so the reopened SDK derives the same
	// account → same tech space → same space-index tombstone.
	seed := newFixedSeedProvider(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, seed)
	require.NoError(t, err)

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "ToDelete"})
	if err != nil {
		_ = sdk.Close()
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}
	spaceId := sp.Id()
	dbPath := filepath.Join(dataDir, "anysync", spaceId+".db")

	require.NoError(t, sdk.Spaces().Delete(ctx, spaceId))
	_, statErr := os.Stat(dbPath)
	require.True(t, os.IsNotExist(statErr), "space DB should be gone after delete")
	require.NoError(t, sdk.Close())

	// Reboot the same account on the same data dir.
	sdk2, err := anysyncsdk.Open(ctx, cfg, seed)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk2.Close() })

	// The eager-loader must NOT have reloaded the tombstone — no DB file.
	_, statErr = os.Stat(dbPath)
	assert.True(t, os.IsNotExist(statErr),
		"deleted space DB must not be re-created on restart, stat err=%v", statErr)

	// And it's still a sticky tombstone in the index.
	list, err := sdk2.Spaces().List(ctx)
	require.NoError(t, err)
	got := findSpace(list, spaceId)
	require.NotNil(t, got, "deleted space should remain in List after restart")
	assert.Equal(t, space.StatusDeleted, got.Status)
}
