package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

const replayOrderRecords = 24

// writeUpdatedArticles installs a bundle whose root declares the
// articles part, upserts replayOrderRecords records on the root and
// then modifies each one with a strict update. It returns the root and
// the dataset.
func writeUpdatedArticles(ctx context.Context, t *testing.T, sp space.Space, bundleId, xKey string) (root, dataset string) {
	t.Helper()
	inst, _, err := sp.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: bundleId, Name: xKey, XKey: xKey, Hidden: true,
		Parts: []space.PartDraft{articlesPart()},
	})
	require.NoError(t, err)
	root = inst.RootId
	dataset = root + "_articles"
	for i := 0; i < replayOrderRecords; i++ {
		_, err := sp.Upsert(ctx, space.UpsertBatch{
			ObjectId: root, Dataset: dataset,
			Records: []space.UpsertRecord{{Id: fmt.Sprintf("a-%02d", i), Fields: map[string]any{"title": "t", "summary": "v0"}}},
		})
		require.NoError(t, err)
	}
	for i := 0; i < replayOrderRecords; i++ {
		_, err := sp.Modify(ctx, space.ModifyBatch{
			ObjectId: root, Dataset: dataset,
			Records: []space.RecordModify{{Id: fmt.Sprintf("a-%02d", i), Ops: []space.Op{{Type: space.OpSet, Path: "summary", Value: "v1"}}}},
		})
		require.NoError(t, err)
	}
	return root, dataset
}

// requireEveryUpdate waits until the root holds every record with its
// update applied, and returns the number of deliveries the records
// arrived in (their distinct add sequences).
func requireEveryUpdate(ctx context.Context, t *testing.T, label string, sp space.Space, root, dataset string) int {
	t.Helper()
	var (
		count      int
		stale      []string
		deliveries map[int]struct{}
	)
	ok := waitFor(ctx, 30*time.Second, 500*time.Millisecond, func() bool {
		rows, err := sp.Query(root, dataset).All(ctx)
		if err != nil {
			return false
		}
		count, stale, deliveries = len(rows), nil, map[int]struct{}{}
		for _, r := range rows {
			if r.GetString("summary") != "v1" {
				stale = append(stale, r.GetString("id"))
			}
			deliveries[r.GetInt("_addSeq")] = struct{}{}
		}
		return count == replayOrderRecords && len(stale) == 0
	})
	require.True(t, ok, "%s: %d of %d records, %d without their update: %v", label, count, replayOrderRecords, len(stale), stale)
	return len(deliveries)
}

// A device that cold-joins gets the records of a root's own parts with
// every update applied: the root's tree carries the definitions and the
// records together, and a strict update must land on the record its
// create made. The same holds when the device then loses its sdk.db and
// rebuilds the records from the tree it holds, which arrived in one
// delivery.
func TestE2E_SelfDeclaredRecordsKeepUpdatesOnColdJoinAndRebuild(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	provider := newFixedSeedProvider(t)
	open := func(label, dir string) *anysyncsdk.SDK {
		t.Helper()
		sdk, err := anysyncsdk.Open(ctx, config.Config{
			Storage: config.Storage{DataDir: dir, Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
		}, provider)
		require.NoError(t, err, "%s: Open", label)
		return sdk
	}

	sdkA := open("device A", t.TempDir())
	defer sdkA.Close()
	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "ColdJoinRecords"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("device A: Spaces().Create: %v", err)
	}
	root, dataset := writeUpdatedArticles(ctx, t, spA, "coldjoin/v1", "coldjoin")
	dbgA, err := spA.Debug().Object(ctx, root)
	require.NoError(t, err)

	_ = sdkA.Spaces().SyncSpaceList(ctx)
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = spA.SyncHeads(ctx)
		return sdkA.Spaces().Status(spA.Id()).State == space.SyncStateSynced
	}), "device A: the space never reached synced")

	dirB := t.TempDir()
	sdkB := open("device B", dirB)
	closeB := func() {
		if sdkB != nil {
			_ = sdkB.Close()
			sdkB = nil
		}
	}
	defer closeB()
	waitCtx, waitCancel := context.WithTimeout(ctx, 90*time.Second)
	require.NoError(t, sdkB.Spaces().WaitListSynced(waitCtx))
	waitCancel()
	var spB space.Space
	require.True(t, waitFor(ctx, 30*time.Second, 250*time.Millisecond, func() bool {
		spB, err = sdkB.Spaces().Get(ctx, spA.Id())
		return err == nil
	}), "device B: Spaces().Get: %v", err)
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		dbgB, err := spB.Debug().Object(ctx, root)
		return err == nil && dbgB.TreeLen == dbgA.TreeLen
	}), "device B: the root's tree never arrived in full")

	deliveries := requireEveryUpdate(ctx, t, "device B, after the cold join", spB, root, dataset)
	// Only then did every record change park and wait for the drain: a
	// tree that arrives in parts replays its later parts in tree order
	// without it.
	require.Equal(t, 1, deliveries, "device B: the root's tree must arrive in one delivery for the drain's order to decide the records")

	// Device B loses its sdk.db and keeps its trees.
	closeB()
	matches, err := filepath.Glob(filepath.Join(dirB, "sdk.db*"))
	require.NoError(t, err)
	require.NotEmpty(t, matches, "sdk.db exists before the removal")
	for _, p := range matches {
		require.NoError(t, os.Remove(p))
	}
	sdkB = open("device B, reopened", dirB)
	select {
	case <-sdkB.BootstrapDone():
	case <-ctx.Done():
		t.Fatal("device B: the rebuild after the removal never finished")
	}
	spB, err = sdkB.Spaces().Get(ctx, spA.Id())
	require.NoError(t, err)
	requireEveryUpdate(ctx, t, "device B, rebuilt without sdk.db", spB, root, dataset)
}
