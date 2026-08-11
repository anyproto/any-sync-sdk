package anysyncsdk

// SYN-104: read/<spaceId>/<objectId> frontier rows in the tech-space KV
// are pruned when their space is removed. The deletion reconciler
// issues a prefix deletion watermark (any-sync SYN-145) that physically
// drops the rows on every replica; joining re-seeds read state, so
// removed spaces' frontiers have no restore value.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonspace/object/keyvalue/keyvaluestorage"
	"github.com/anyproto/any-sync/commonspace/object/keyvalue/keyvaluestorage/innerstorage"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/auth"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

func pruneTechKV(t *testing.T, ctx context.Context, s *SDK) keyvaluestorage.Storage {
	t.Helper()
	store, err := s.app.KeyValueStore(ctx, s.tsp.SpaceId())
	require.NoError(t, err)
	return store
}

// prunePublish writes a frontier row the way readsync.publish does.
func prunePublish(t *testing.T, ctx context.Context, s *SDK, prefix, objId string) {
	t.Helper()
	raw, err := json.Marshal(struct {
		H []string `json:"h"`
	}{H: []string{"c1"}})
	require.NoError(t, err)
	require.NoError(t, pruneTechKV(t, ctx, s).Set(ctx, prefix+objId, raw))
}

// pruneVisible counts the rows readsync's merge/seed paths would see.
func pruneVisible(t *testing.T, ctx context.Context, s *SDK, prefix string) int {
	t.Helper()
	n := 0
	require.NoError(t, pruneTechKV(t, ctx, s).Iterate(ctx, func(_ keyvaluestorage.Decryptor, k string, values []innerstorage.KeyValue) (bool, error) {
		if strings.HasPrefix(k, prefix) {
			n += len(values)
		}
		return true, nil
	}))
	return n
}

// pruneRaw counts physical rows under the prefix, watermark included.
func pruneRaw(t *testing.T, ctx context.Context, s *SDK, prefix string) int {
	t.Helper()
	n := 0
	require.NoError(t, pruneTechKV(t, ctx, s).InnerStorage().IteratePrefix(ctx, prefix, func(innerstorage.KeyValue) error {
		n++
		return nil
	}))
	return n
}

// pruneWatermarkTs returns the newest watermark timestamp stored under
// the prefix (0 when none) — the proof a (re-)issued watermark reached
// this replica.
func pruneWatermarkTs(t *testing.T, ctx context.Context, s *SDK, prefix string) int64 {
	t.Helper()
	var ts int64
	require.NoError(t, pruneTechKV(t, ctx, s).InnerStorage().IteratePrefix(ctx, prefix, func(kv innerstorage.KeyValue) error {
		if kv.DeletePrefix != "" && kv.TimestampMicro > ts {
			ts = kv.TimestampMicro
		}
		return nil
	}))
	return ts
}

// pruneWait polls until cond returns true. nudge keeps the tech space
// loaded on live-network tests (idle spaces age out of the cache, and
// KV sync piggybacks on the space's headsync rounds).
func pruneWait(t *testing.T, ctx context.Context, s *SDK, why string, timeout time.Duration, nudge bool, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: condition never met", why)
		}
		if nudge {
			_ = s.Spaces().SyncSpaceList(ctx)
			time.Sleep(2 * time.Second)
		} else {
			time.Sleep(200 * time.Millisecond)
		}
	}
}

// TestReadPrune_SpaceDeletePrunesFrontiers drives the full loop through
// the real tech-space KV: publish frontiers (the exact Set call
// readsync.publish makes), delete the space, and let the deletion
// reconciler prune. The rows must vanish (watermark row only), stay
// gone across a reboot, and a row published later by a lagging device
// must be re-pruned by a subsequent pass.
func TestReadPrune_SpaceDeletePrunesFrontiers(t *testing.T) {
	yaml, err := os.ReadFile("e2e/local.yml")
	if err != nil {
		t.Skipf("nodeconf not available: %v", err)
	}
	restore := spaceimpl.SetDeletionReconcileIntervalForTest(500 * time.Millisecond)
	t.Cleanup(restore)

	dataDir := t.TempDir()
	provider, err := auth.NewFileProvider(auth.FileProviderConfig{
		Path: filepath.Join(dataDir, "wallet.key"),
	})
	require.NoError(t, err)
	cfg := config.Config{
		Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	sdk, err := Open(ctx, cfg, provider)
	require.NoError(t, err)
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "ReadPrune"})
	if err != nil {
		_ = sdk.Close()
		t.Skipf("space create failed (network?): %v", err)
	}
	spaceId := sp.Id()
	prefix := "read/" + spaceId + "/"

	prunePublish(t, ctx, sdk, prefix, "obj1")
	prunePublish(t, ctx, sdk, prefix, "obj2")
	require.Equal(t, 2, pruneVisible(t, ctx, sdk, prefix))

	require.NoError(t, sdk.Spaces().Delete(ctx, spaceId))
	pruneWait(t, ctx, sdk, "reconciler prunes after delete", 20*time.Second, false, func() bool {
		return pruneVisible(t, ctx, sdk, prefix) == 0
	})
	require.Equal(t, 1, pruneRaw(t, ctx, sdk, prefix), "only the watermark row remains physically")

	// A lagging device publishes after the prune: newer than the
	// watermark, so it lands — and the next pass re-prunes it.
	prunePublish(t, ctx, sdk, prefix, "obj-late")
	pruneWait(t, ctx, sdk, "late publish re-pruned", 20*time.Second, false, func() bool {
		return pruneVisible(t, ctx, sdk, prefix) == 0
	})

	require.NoError(t, sdk.Close())

	// Reboot: the prune must hold, and boot must not resurrect anything.
	sdk2, err := Open(ctx, cfg, provider)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk2.Close() })
	select {
	case <-sdk2.BootstrapDone():
	case <-ctx.Done():
		t.Fatal("bootstrap did not complete after reboot")
	}
	require.Equal(t, 0, pruneVisible(t, ctx, sdk2, prefix), "rows stay pruned across reboot")
	require.Equal(t, 1, pruneRaw(t, ctx, sdk2, prefix), "watermark survives reboot")
}

// TestReadPrune_TwoDevicesLive is the cross-device contract: device A
// deletes a space and every device of the account converges to zero
// visible read/ rows — through the real network, whose nodes may not
// understand watermarks (they relay them as opaque rows). The late-
// publish leg asserts REAL propagation: devA's stored watermark
// timestamp must advance, which requires either devB's late row or
// devB's re-issued watermark to have crossed the network. Needs a
// reachable network; skips otherwise.
func TestReadPrune_TwoDevicesLive(t *testing.T) {
	yaml, err := os.ReadFile("e2e/local.yml")
	if err != nil {
		t.Skipf("nodeconf not available: %v", err)
	}
	restore := spaceimpl.SetDeletionReconcileIntervalForTest(time.Second)
	t.Cleanup(restore)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	walletDir := t.TempDir()
	provider, err := auth.NewFileProvider(auth.FileProviderConfig{
		Path: filepath.Join(walletDir, "wallet.key"),
	})
	require.NoError(t, err)
	open := func(name string) *SDK {
		sdk, oerr := Open(ctx, config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
		}, provider)
		require.NoError(t, oerr, "%s: Open", name)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk
	}

	devA := open("devA")
	sp, err := devA.Spaces().Create(ctx, space.CreateRequest{Name: "ReadPrune2Dev"})
	if err != nil {
		t.Skipf("space create failed (network?): %v", err)
	}
	spaceId := sp.Id()
	prefix := "read/" + spaceId + "/"

	prunePublish(t, ctx, devA, prefix, "obj1")
	prunePublish(t, ctx, devA, prefix, "obj2")

	devB := open("devB")
	select {
	case <-devB.BootstrapDone():
	case <-ctx.Done():
		t.Fatal("devB bootstrap did not complete")
	}
	pruneWait(t, ctx, devB, "devB pulls published rows", 2*time.Minute, true, func() bool {
		return pruneVisible(t, ctx, devB, prefix) == 2
	})

	// A deletes the space; both devices converge — via their own
	// reconcilers and via the synced watermark, whichever lands first.
	require.NoError(t, devA.Spaces().Delete(ctx, spaceId))
	pruneWait(t, ctx, devA, "devA prunes after delete", 30*time.Second, true, func() bool {
		return pruneVisible(t, ctx, devA, prefix) == 0
	})
	pruneWait(t, ctx, devB, "devB converges after delete", 2*time.Minute, true, func() bool {
		return pruneVisible(t, ctx, devB, prefix) == 0
	})

	// Lagging publish on B after the prune. B re-prunes locally, and the
	// cross-replica proof is devA's watermark timestamp advancing: that
	// requires B's late row (making A re-issue) or B's re-issued
	// watermark to have propagated to A.
	wmA := pruneWatermarkTs(t, ctx, devA, prefix)
	require.NotZero(t, wmA, "devA holds a watermark after the prune")
	prunePublish(t, ctx, devB, prefix, "obj-late")
	pruneWait(t, ctx, devB, "late publish re-pruned on devB", 2*time.Minute, true, func() bool {
		return pruneVisible(t, ctx, devB, prefix) == 0
	})
	pruneWait(t, ctx, devA, "re-issued watermark reaches devA", 2*time.Minute, true, func() bool {
		return pruneWatermarkTs(t, ctx, devA, prefix) > wmA && pruneVisible(t, ctx, devA, prefix) == 0
	})
}
