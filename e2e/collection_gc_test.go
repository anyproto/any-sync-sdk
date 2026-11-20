package e2e

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestSDK_OrphanCollectionGC pins the boot-time sweep's integration
// contract: a purge-leaked per-object collection is dropped on the next
// Open, while everything live (the tech space's own collections, the
// created space's roster, its object's dataset rows) survives the same
// sweep untouched — as does an `_objects` shell for a space with no
// tech-space row (no positive tombstone, so the sweep must not touch
// it).
func TestSDK_OrphanCollectionGC(t *testing.T) {
	t.Parallel()
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
	seed := newFixedSeedProvider(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, seed)
	require.NoError(t, err)

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "GCKeep"})
	if err != nil {
		_ = sdk.Close()
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}
	spaceId := sp.Id()
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: "blocks-type"})
	require.NoError(t, err)
	_, err = sp.Modify(ctx, space.ModifyBatch{
		ObjectId: objId,
		Dataset:  blocksDataset,
		Records: []space.RecordModify{{
			Id:     "rec-1",
			Upsert: true,
			Ops:    []space.Op{{Type: space.OpSet, Path: "text", Value: "keep me"}},
		}},
	})
	require.NoError(t, err)
	require.NoError(t, sdk.Close())

	// Inject straight into sdk.db while the SDK is down: a per-object
	// collection for an object no space knows (swept), and an
	// `_objects` shell for a space with no tech-space row (kept —
	// absence of a row is not a tombstone).
	orphanObjHash, err := mh.Sum([]byte("gc-orphan-object"), mh.SHA2_256, -1)
	require.NoError(t, err)
	orphanObj := cid.NewCidV1(cid.Raw, orphanObjHash).String()
	ghostHash, err := mh.Sum([]byte("gc-ghost-space"), mh.SHA2_256, -1)
	require.NoError(t, err)
	ghostSpace := cid.NewCidV1(cid.Raw, ghostHash).String() + ".2f"

	dbPath := filepath.Join(dataDir, "sdk.db")
	db, err := anystore.Open(ctx, dbPath, nil)
	require.NoError(t, err)
	seedLeak := func(collName string) {
		coll, cerr := db.Collection(ctx, collName)
		require.NoError(t, cerr)
		a := &anyenc.Arena{}
		doc := a.NewObject()
		doc.Set("id", a.NewString("leak"))
		require.NoError(t, coll.UpsertOne(ctx, doc))
	}
	seedLeak(orphanObj + "_blocks")
	seedLeak(orphanObj + "__history")
	seedLeak(ghostSpace + "_objects")
	// Consumer-tagged collections (SDK.Store contract) sit next to the
	// CRDT ones and are never swept, whatever their embedded space id.
	seedLeak("l_a_keep")
	seedLeak("l_s_" + spaceId + "_keep")
	seedLeak("l_s_" + ghostSpace + "_keep")
	require.NoError(t, db.Close())

	// Reboot: the startup sweep runs before any space loads.
	sdk2, err := anysyncsdk.Open(ctx, cfg, seed)
	require.NoError(t, err)

	// Live state intact: the space is listed and the object's record
	// still reads back.
	list, err := sdk2.Spaces().List(ctx)
	require.NoError(t, err)
	require.NotNil(t, findSpace(list, spaceId), "created space must survive the sweep")
	sp2, err := sdk2.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)
	recs, err := sp2.Query(objId, blocksDataset).Snapshot(ctx, space.QueryOpts{})
	require.NoError(t, err)
	require.Len(t, recs.Initial, 1, "object record must survive the sweep")

	// SDK.Store hands out the same sdk.db the sweep ran on: the
	// consumer-tagged collections are reachable through it, untouched.
	for _, n := range []string{"l_a_keep", "l_s_" + spaceId + "_keep", "l_s_" + ghostSpace + "_keep"} {
		coll, err := sdk2.Store().OpenCollection(ctx, n)
		require.NoError(t, err, "consumer collection %s must survive the sweep", n)
		cnt, err := coll.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, cnt, n)
	}
	require.NoError(t, sdk2.Close())

	// The injected leaks are gone from sdk.db.
	db, err = anystore.Open(ctx, dbPath, nil)
	require.NoError(t, err)
	defer db.Close()
	names, err := db.GetCollectionNames(ctx)
	require.NoError(t, err)
	got := map[string]bool{}
	for _, n := range names {
		got[n] = true
	}
	for _, n := range []string{orphanObj + "_blocks", orphanObj + "__history"} {
		assert.False(t, got[n], "expected leaked %s swept", n)
	}
	assert.True(t, got[ghostSpace+"_objects"], "unknown-space shell must be kept (no tombstone)")
	assert.True(t, got[spaceId+"_objects"], "live space roster must survive")
	assert.True(t, got[objId+"_"+blocksDataset], "live object dataset must survive")
}
