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

	techKV := func(s *SDK) keyvaluestorage.Storage {
		store, kerr := s.app.KeyValueStore(ctx, s.tsp.SpaceId())
		require.NoError(t, kerr)
		return store
	}
	publish := func(s *SDK, objId string) {
		raw, merr := json.Marshal(struct {
			H []string `json:"h"`
		}{H: []string{"c1"}})
		require.NoError(t, merr)
		require.NoError(t, techKV(s).Set(ctx, prefix+objId, raw))
	}
	// visible = what readsync's merge/seed paths see; raw includes the
	// watermark row.
	visible := func(s *SDK) int {
		n := 0
		require.NoError(t, techKV(s).Iterate(ctx, func(_ keyvaluestorage.Decryptor, k string, values []innerstorage.KeyValue) (bool, error) {
			if strings.HasPrefix(k, prefix) {
				n += len(values)
			}
			return true, nil
		}))
		return n
	}
	raw := func(s *SDK) int {
		n := 0
		require.NoError(t, techKV(s).InnerStorage().IteratePrefix(ctx, prefix, func(innerstorage.KeyValue) error {
			n++
			return nil
		}))
		return n
	}
	waitVisible := func(s *SDK, want int, why string) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for {
			if got := visible(s); got == want {
				return
			} else if time.Now().After(deadline) {
				t.Fatalf("%s: visible rows never reached %d (last %d)", why, want, got)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	publish(sdk, "obj1")
	publish(sdk, "obj2")
	require.Equal(t, 2, visible(sdk))

	require.NoError(t, sdk.Spaces().Delete(ctx, spaceId))
	waitVisible(sdk, 0, "reconciler prunes after delete")
	require.Equal(t, 1, raw(sdk), "only the watermark row remains physically")

	// A lagging device publishes after the prune: newer than the
	// watermark, so it lands — and the next pass re-prunes it.
	publish(sdk, "obj-late")
	waitVisible(sdk, 0, "late publish re-pruned")

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
	require.Equal(t, 0, visible(sdk2), "rows stay pruned across reboot")
	require.Equal(t, 1, raw(sdk2), "watermark survives reboot")
}
