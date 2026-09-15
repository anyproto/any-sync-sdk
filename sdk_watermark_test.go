package anysyncsdk

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/auth"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/space"
)

// The watermark-persist discipline tests. All offline: local.yml is
// only a valid nodeconf shape; creates/writes are local-first. Not
// parallel — they use the package-level test seams.

const wmDataset = "wm_blocks"

func wmConfig(t *testing.T, dataDir string) config.Config {
	t.Helper()
	yaml, err := os.ReadFile("e2e/local.yml")
	if err != nil {
		t.Skipf("nodeconf not available: %v", err)
	}
	return config.Config{
		Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Types: []handler.Type{{
			Id:   "wm-blocks-type",
			Name: "WMBlocks",
			Datasets: []handler.Dataset{{
				Name:        wmDataset,
				DataVersion: "wm-v1",
				Handler:     handler.DefaultHandler{},
			}},
		}},
	}
}

func wmProvider(t *testing.T, dataDir string) auth.Provider {
	t.Helper()
	p, err := auth.NewFileProvider(auth.FileProviderConfig{
		Path: filepath.Join(dataDir, "wallet.key"),
	})
	require.NoError(t, err)
	return p
}

// wmRead reads the persisted space watermark straight from sdk.db —
// must be called with the SDK closed.
func wmRead(t *testing.T, dataDir, spaceId string) uint64 {
	t.Helper()
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(dataDir, "sdk.db"), nil)
	require.NoError(t, err)
	defer db.Close()
	coll, err := db.OpenCollection(ctx, crdt.MetaCollectionName)
	require.NoError(t, err)
	seq, err := crdt.LoadSpaceMaxAddSeq(ctx, coll, spaceId)
	require.NoError(t, err)
	return seq
}

func wmWrite(t *testing.T, ctx context.Context, sp space.Space, n int) {
	t.Helper()
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: "wm-blocks-type"})
	require.NoError(t, err)
	for k := 0; k < n; k++ {
		_, err := sp.Modify(ctx, space.ModifyBatch{
			ObjectId: objId, Dataset: wmDataset,
			Records: []space.RecordModify{{
				Id: "rec", Upsert: true,
				Ops: []space.Op{{Type: space.OpSet, Path: "n", Value: k}},
			}},
		})
		require.NoError(t, err)
	}
}

// TestClose_WatermarkAllowlist pins the allowlist rule of the
// close-time snapshot: a created-this-session space is born clean and
// persisted; a space that never entered the allowlist (its catch-up
// Run never ran — Close mid-bootstrap) keeps its old watermark so the
// next boot still replays; a normal session then advances it.
func TestClose_WatermarkAllowlist(t *testing.T) {
	dataDir := t.TempDir()
	provider := wmProvider(t, dataDir)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Session 1: create + write + clean Close. The created space is
	// born clean (allowlisted via the Create hook), so Close snapshots
	// its watermark.
	var spaceId string
	{
		sdk, err := Open(ctx, wmConfig(t, dataDir), provider)
		require.NoError(t, err)
		sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "WM"})
		require.NoError(t, err)
		spaceId = sp.Id()
		wmWrite(t, ctx, sp, 5)
		require.NoError(t, sdk.Close())
	}
	w1 := wmRead(t, dataDir, spaceId)
	require.Greater(t, w1, uint64(0), "a created-this-session space must be persisted (born clean)")

	// Session 2: hold the bootstrap pass open so Run never executes —
	// the space never enters the allowlist. Write more (advances the
	// head store past w1), then Close mid-bootstrap. The watermark must
	// NOT move: those writes must stay replayable.
	bootstrapTestHook = func(hctx context.Context) { <-hctx.Done() }
	t.Cleanup(func() { bootstrapTestHook = nil })
	{
		sdk, err := Open(ctx, wmConfig(t, dataDir), provider)
		require.NoError(t, err)
		sp, err := sdk.Spaces().Get(ctx, spaceId)
		require.NoError(t, err)
		wmWrite(t, ctx, sp, 5)
		require.NoError(t, sdk.Close())
	}
	assert.Equal(t, w1, wmRead(t, dataDir, spaceId),
		"Close must not snapshot a space that never entered the allowlist")
	bootstrapTestHook = nil

	// Session 3 (control): a full bootstrap replays the session-2
	// residue and the watermark advances past w1.
	{
		sdk, err := Open(ctx, wmConfig(t, dataDir), provider)
		require.NoError(t, err)
		select {
		case <-sdk.BootstrapDone():
		case <-ctx.Done():
			t.Fatal("bootstrap did not complete")
		}
		require.NoError(t, sdk.Close())
	}
	assert.Greater(t, wmRead(t, dataDir, spaceId), w1,
		"a completed catch-up must advance the watermark past the un-snapshotted writes")
}

// TestClose_WatermarkSkipsParkedTrees pins exclusion three: a space
// whose treesyncer parked set is non-empty at Close keeps its old
// watermark — parked trees are storage-committed but never
// materialized, and the boot replay is their only cross-restart
// recovery.
func TestClose_WatermarkSkipsParkedTrees(t *testing.T) {
	dataDir := t.TempDir()
	provider := wmProvider(t, dataDir)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var spaceId string
	{
		sdk, err := Open(ctx, wmConfig(t, dataDir), provider)
		require.NoError(t, err)
		sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "WMPark"})
		require.NoError(t, err)
		spaceId = sp.Id()
		wmWrite(t, ctx, sp, 5)
		require.NoError(t, sdk.Close())
	}
	w1 := wmRead(t, dataDir, spaceId)
	require.Greater(t, w1, uint64(0))

	// Session 2: bootstrap completes (the space is allowlisted via its
	// Run), new writes advance the head store, but the parked-tree gate
	// must still block the snapshot.
	parkedCountHook = func(string) int { return 1 }
	t.Cleanup(func() { parkedCountHook = nil })
	{
		sdk, err := Open(ctx, wmConfig(t, dataDir), provider)
		require.NoError(t, err)
		select {
		case <-sdk.BootstrapDone():
		case <-ctx.Done():
			t.Fatal("bootstrap did not complete")
		}
		sp, err := sdk.Spaces().Get(ctx, spaceId)
		require.NoError(t, err)
		wmWrite(t, ctx, sp, 5)
		require.NoError(t, sdk.Close())
	}
	assert.Equal(t, w1, wmRead(t, dataDir, spaceId),
		"Close must not snapshot a space with parked trees")
	parkedCountHook = nil

	// Session 3 (control): parked set empty again — boot Run replays
	// the residue and advances the watermark.
	{
		sdk, err := Open(ctx, wmConfig(t, dataDir), provider)
		require.NoError(t, err)
		select {
		case <-sdk.BootstrapDone():
		case <-ctx.Done():
			t.Fatal("bootstrap did not complete")
		}
		require.NoError(t, sdk.Close())
	}
	assert.Greater(t, wmRead(t, dataDir, spaceId), w1)
}
