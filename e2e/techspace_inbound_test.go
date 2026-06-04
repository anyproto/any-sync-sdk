package e2e

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_TechSpaceLiveSubscribe proves the tech space's live
// subscription path now that the index object is Store-backed: a space
// created on device A surfaces on device B through Spaces().Subscribe
// — WITHOUT device B ever calling List — and a delete on A surfaces as
// a Removed event on B. This is the real-time guarantee the migration
// off the bespoke read-drain was about.
func TestE2E_TechSpaceLiveSubscribe(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("tech-space live-subscribe e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	provider := newFixedSeedProvider(t)

	// Device A: create one space, then stay open and push it to the node.
	cfgA := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	sdkA, err := anysyncsdk.Open(ctx, cfgA, provider)
	require.NoError(t, err, "device A: Open")
	t.Cleanup(func() { _ = sdkA.Close() })

	sp, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "Alpha"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("device A: Spaces().Create: %v", err)
	}
	spaceID := sp.Id()
	_ = sdkA.Spaces().SyncSpaceList(ctx)

	// Device B: fresh DB, same keys. Subscribe BEFORE the row has been
	// pulled so the live delta isn't missed (the sub is delta-only).
	cfgB := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	sdkB, err := anysyncsdk.Open(ctx, cfgB, provider)
	require.NoError(t, err, "device B: Open")
	t.Cleanup(func() { _ = sdkB.Close() })

	var mu sync.Mutex
	addedSeen := map[string]bool{}
	removedSeen := map[string]bool{}
	cancelSub := sdkB.Spaces().Subscribe(func(ev space.SpaceListEvent) {
		mu.Lock()
		defer mu.Unlock()
		for _, info := range ev.Added {
			addedSeen[info.Id] = true
		}
		for _, info := range ev.Updated {
			addedSeen[info.Id] = true
		}
		for _, id := range ev.Removed {
			removedSeen[id] = true
		}
	})
	defer cancelSub()

	seen := func(m map[string]bool, id string) bool {
		mu.Lock()
		defer mu.Unlock()
		return m[id]
	}

	// Drive the inbound diff round; the Subscribe callback must fire an
	// Added for the new space without us ever calling List.
	if !waitFor(ctx, 90*time.Second, 250*time.Millisecond, func() bool {
		_ = sdkB.Spaces().SyncSpaceList(ctx)
		return seen(addedSeen, spaceID)
	}) {
		t.Fatalf("device B never received an Added event for space %s via Subscribe", spaceID)
	}

	// Now delete on A; B's callback must surface the sticky-deleted row
	// as a Removed event.
	require.NoError(t, sdkA.Spaces().Delete(ctx, spaceID), "device A: Delete")
	_ = sdkA.Spaces().SyncSpaceList(ctx)

	if !waitFor(ctx, 90*time.Second, 250*time.Millisecond, func() bool {
		_ = sdkB.Spaces().SyncSpaceList(ctx)
		return seen(removedSeen, spaceID)
	}) {
		t.Fatalf("device B never received a Removed event for space %s via Subscribe", spaceID)
	}
}
