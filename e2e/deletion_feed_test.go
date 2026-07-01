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
	"github.com/anyproto/any-sync-sdk/space"
)

// TestSDK_DeletionFeed_AndRebuild exercises the durable object-deletion
// surface added for SYN-20:
//
//   - Object deletion is reported through the change-index feed as
//     ObjectChange{Deleted:true} at a fresh applySeq > the object's last
//     content change (both the live Subscribe and the durable ChangedSince),
//     and the object leaves QueryObjects.
//   - After an sdk.db REBUILD (wipe the SDK projection, keep any-sync's
//     per-space storage) the deleted object stays absent, and Generation()
//     changes so a consumer knows to reset its cursor.
func TestSDK_DeletionFeed_AndRebuild(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	dataDir := t.TempDir()
	cfg := config.Config{
		Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	// One seed → same account/tech-space across both boots.
	seed := newFixedSeedProvider(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, seed)
	require.NoError(t, err)

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "DelFeed"})
	if err != nil {
		_ = sdk.Close()
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}
	spaceId := sp.Id()

	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Note"})
	require.NoError(t, err)
	titleProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Title", XKey: "title", Kind: space.PropertyKindString,
	})
	require.NoError(t, err)

	// A live object with a content write, so it has a real content applySeq.
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	_, err = sp.Properties().Set(ctx, objId, typeId, map[string]any{titleProp: "doomed"})
	require.NoError(t, err)

	contentMax, err := sp.Changes().MaxApplySeq(ctx)
	require.NoError(t, err)

	dels := make(chan space.ObjectChange, 16)
	cancelSub := sp.Changes().Subscribe(func(e space.ObjectChange) {
		if e.Deleted {
			dels <- e
		}
	})
	defer cancelSub()

	// Delete the object.
	require.NoError(t, sp.Objects().Delete(ctx, objId))

	// Live feed announced the deletion at a fresh seq above the content max.
	select {
	case e := <-dels:
		assert.Equal(t, objId, e.ObjectId)
		assert.True(t, e.Deleted)
		assert.Greater(t, e.ApplySeq, contentMax)
	case <-time.After(5 * time.Second):
		t.Fatal("no live deletion event on the change feed")
	}

	// Durable feed: a from-zero scan surfaces the deletion exactly once.
	changed, err := sp.Changes().ChangedSince(ctx, 0, 0)
	require.NoError(t, err)
	var seen *space.ObjectChange
	for i := range changed {
		if changed[i].ObjectId == objId {
			c := changed[i]
			seen = &c
		}
	}
	require.NotNil(t, seen, "deleted object must appear in ChangedSince")
	assert.True(t, seen.Deleted)
	assert.Greater(t, seen.ApplySeq, contentMax)

	// And it left QueryObjects.
	n, err := sp.QueryObjects().Filter(map[string]any{"id": objId}).Count(ctx)
	require.NoError(t, err)
	assert.Zero(t, n, "deleted object must be absent from QueryObjects")

	gen0, err := sp.Changes().Generation(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, gen0)
	require.NoError(t, sdk.Close())

	// ---- Rebuild: wipe the SDK projection (sdk.db), keep any-sync storage.
	matches, err := filepath.Glob(filepath.Join(dataDir, "sdk.db*"))
	require.NoError(t, err)
	require.NotEmpty(t, matches, "sdk.db should exist before the wipe")
	for _, p := range matches {
		require.NoError(t, os.Remove(p))
	}
	// any-sync per-space storage must survive.
	_, statErr := os.Stat(filepath.Join(dataDir, "anysync", spaceId+".db"))
	require.NoError(t, statErr, "any-sync per-space DB must remain")

	sdk2, err := anysyncsdk.Open(ctx, cfg, seed)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk2.Close() })

	sp2, err := sdk2.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)

	// The deleted object does not re-materialise after the rebuild.
	n2, err := sp2.QueryObjects().Filter(map[string]any{"id": objId}).Count(ctx)
	require.NoError(t, err)
	assert.Zero(t, n2, "deleted object must stay absent after rebuild")

	// Generation changed — the consumer's cursor is stale, reset + reindex.
	gen1, err := sp2.Changes().Generation(ctx)
	require.NoError(t, err)
	assert.NotEqual(t, gen0, gen1, "generation must change after an sdk.db rebuild")
}
