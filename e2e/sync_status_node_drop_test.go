package e2e

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_SyncStatusOfflineOnNodeDrop pins the space rollup against a
// live network: Synced while the tree nodes are reachable, Offline once
// their connections drop, Synced again when they return. The tree nodes
// sit behind local TCP proxies; stopping them cuts every node connection
// and refuses redials, starting them again on the same addresses lets
// the SDK reconnect.
func TestE2E_SyncStatusOfflineOnNodeDrop(t *testing.T) {
	if testing.Short() {
		t.Skip("needs a live local network; rerun without -short")
	}
	t.Parallel()
	netYAML := loadLocalNetworkYAML(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// The boot refresh replaces a nodeconf whose id differs from the
	// coordinator's, so the rewrite starts from the served one.
	proxYAML, routes := proxyNodeconf(t, capturedServedNodeconf(t, ctx, netYAML), freeBasePort(t, 3))
	stop := startTCPProxies(t, routes)
	defer func() { stop() }()

	// Direct peers off: only the tree nodes count toward presence.
	p2pOff := false
	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: proxYAML},
		P2P:     config.P2P{Enabled: &p2pOff},
	}
	openedAt := time.Now()
	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Status"})
	require.NoError(t, err)
	spaceId := sp.Id()
	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Note"})
	require.NoError(t, err)
	_, err = sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)

	type event struct {
		at time.Time
		st space.SpaceSyncStatus
	}
	var (
		mu     sync.Mutex
		events []event
	)
	unsub := sdk.Spaces().SubscribeStatus(func(st space.SpaceSyncStatus) {
		if st.SpaceId != spaceId {
			return
		}
		mu.Lock()
		events = append(events, event{at: time.Now(), st: st})
		mu.Unlock()
	})
	defer unsub()
	// eventAt is the first event in state want at or after since.
	eventAt := func(want space.SyncState, since time.Time) (time.Time, bool) {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range events {
			if e.st.State == want && !e.at.Before(since) {
				return e.at, true
			}
		}
		return time.Time{}, false
	}

	// await polls Status until ok holds and returns when it did.
	await := func(phase string, within time.Duration, ok func(space.SpaceSyncStatus) bool) time.Time {
		t.Helper()
		deadline := time.Now().Add(within)
		var last space.SpaceSyncStatus
		for time.Now().Before(deadline) {
			last = sdk.Spaces().Status(spaceId)
			if ok(last) {
				return time.Now()
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("%s: not reached within %s; last=%+v", phase, within, last)
		return time.Time{}
	}

	syncedAt := await("synced", 30*time.Second, func(st space.SpaceSyncStatus) bool {
		return st.State == space.SyncStateSynced && st.NetworkPeers >= 1
	})
	t.Logf("synced %.2fs after Open: %+v", syncedAt.Sub(openedAt).Seconds(), sdk.Spaces().Status(spaceId))
	// Events go out on the rollup tick; the drop must follow a Synced one.
	require.Eventually(t, func() bool {
		_, ok := eventAt(space.SyncStateSynced, openedAt)
		return ok
	}, 5*time.Second, 50*time.Millisecond, "no Synced event while the nodes are reachable")

	stoppedAt := time.Now()
	stop()
	offlineAt := await("offline", 60*time.Second, func(st space.SpaceSyncStatus) bool {
		return st.State == space.SyncStateOffline && st.NetworkPeers == 0
	})
	t.Logf("offline %.2fs after the proxies stopped", offlineAt.Sub(stoppedAt).Seconds())
	var offlineEvAt time.Time
	require.Eventually(t, func() bool {
		var ok bool
		offlineEvAt, ok = eventAt(space.SyncStateOffline, stoppedAt)
		return ok
	}, 5*time.Second, 50*time.Millisecond, "no Offline event after the proxies stopped")
	t.Logf("offline event %.2fs after the proxies stopped", offlineEvAt.Sub(stoppedAt).Seconds())

	restartedAt := time.Now()
	stop = startTCPProxies(t, routes)
	resyncedAt := await("synced again", 60*time.Second, func(st space.SpaceSyncStatus) bool {
		return st.State == space.SyncStateSynced && st.NetworkPeers >= 1
	})
	t.Logf("synced again %.2fs after the proxies restarted: %+v", resyncedAt.Sub(restartedAt).Seconds(), sdk.Spaces().Status(spaceId))
	var syncedEvAt time.Time
	require.Eventually(t, func() bool {
		var ok bool
		syncedEvAt, ok = eventAt(space.SyncStateSynced, offlineEvAt)
		return ok
	}, 5*time.Second, 50*time.Millisecond, "no Synced event after the Offline one")
	assert.True(t, syncedEvAt.After(offlineEvAt))
	t.Logf("synced event %.2fs after the proxies restarted", syncedEvAt.Sub(restartedAt).Seconds())

	mu.Lock()
	for _, e := range events {
		t.Logf("event +%.2fs %s nodes=%d", e.at.Sub(openedAt).Seconds(), e.st.State, e.st.NetworkPeers)
	}
	mu.Unlock()
}

// freeBasePort returns a port p such that p..p+n-1 are free to listen
// on right now, so two suites on one machine do not collide. The
// candidates sit below the ephemeral range, which outgoing connections
// draw from: a proxy port must stay free while the proxies are down.
func freeBasePort(t *testing.T, n int) int {
	t.Helper()
	for attempt := 0; attempt < 50; attempt++ {
		base := 20000 + rand.IntN(10000)
		var held []net.Listener
		ok := true
		for p := base; p < base+n; p++ {
			ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
			if err != nil {
				ok = false
				break
			}
			held = append(held, ln)
		}
		for _, ln := range held {
			_ = ln.Close()
		}
		if ok {
			return base
		}
	}
	t.Fatalf("no %d consecutive free ports found", n)
	return 0
}
