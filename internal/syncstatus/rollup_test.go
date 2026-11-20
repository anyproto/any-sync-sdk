package syncstatus

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// The rollup table. With no node and no direct peer connected a space
// reads Offline whatever its trees did; with a peer connected, pending
// heads read Syncing, a confirmation from this run Synced, nothing
// confirmed yet Unknown; the network refusing this client reads Error
// over everything.
func TestRollup_States(t *testing.T) {
	const spaceId, node, lan, global = "space-1", "node-1", "lan-1", "global-1"
	heads := []string{"h1"}
	newSvc := func(peers PeerCountsFn, total int) (*Service, *Tracker) {
		svc := NewService()
		t.Cleanup(svc.Close)
		svc.SetNodeIdsFn(func(string) []string { return []string{node} })
		svc.SetLocalPeerIdsFn(func(string) []string { return []string{lan, global} })
		svc.SetPeerCountsFn(peers)
		svc.SetTotalFn(func(string) int { return total })
		return svc, svc.For(spaceId)
	}
	none := func(string) (int, int, int) { return 0, 0, 0 }
	nodeOnly := func(string) (int, int, int) { return 1, 0, 0 }
	lanOnly := func(string) (int, int, int) { return 0, 1, 0 }
	globalOnly := func(string) (int, int, int) { return 0, 0, 1 }
	state := func(svc *Service) space.SyncState { return svc.Status(spaceId).State }

	t.Run("no node and no peer", func(t *testing.T) {
		svc, _ := newSvc(none, 0)
		assert.Equal(t, space.SyncStateOffline, state(svc), "empty space")

		svc, tr := newSvc(none, 3)
		got := svc.Status(spaceId)
		assert.Equal(t, space.SyncStateOffline, got.State, "after a restart with no writes")
		assert.Equal(t, 3, got.Synced, "stored trees have nothing pending")
		tr.HeadsChange("obj", heads)
		got = svc.Status(spaceId)
		assert.Equal(t, space.SyncStateOffline, got.State, "after a write in this run")
		assert.Equal(t, 2, got.Synced, "the written tree is pending")
	})

	t.Run("node connected", func(t *testing.T) {
		svc, tr := newSvc(nodeOnly, 0)
		assert.Equal(t, space.SyncStateUnknown, state(svc), "empty space, nothing confirmed")
		tr.HeadsChange("obj", heads)
		assert.Equal(t, space.SyncStateSyncing, state(svc), "after a write")
		tr.HeadsApply("stranger", "obj", heads, true)
		assert.Equal(t, space.SyncStateSyncing, state(svc), "a non-responsible sender confirms nothing")
		tr.HeadsApply(node, "obj", heads, true)
		assert.Equal(t, space.SyncStateSynced, state(svc), "the node applied the heads")

		svc, tr = newSvc(nodeOnly, 3)
		assert.Equal(t, space.SyncStateUnknown, state(svc), "after a restart, before any round")
		tr.BulkSyncedFromPeer(node)
		assert.Equal(t, space.SyncStateSynced, state(svc), "a zero-diff round confirms the space")
	})

	t.Run("direct peer only", func(t *testing.T) {
		svc, tr := newSvc(lanOnly, 1)
		assert.Equal(t, space.SyncStateUnknown, state(svc))
		tr.HeadsChange("obj", heads)
		assert.Equal(t, space.SyncStateSyncing, state(svc))
		tr.HeadsApply(lan, "obj", heads, true)
		assert.Equal(t, space.SyncStateSynced, state(svc), "a LAN peer confirms like a node")

		svc, tr = newSvc(globalOnly, 3)
		tr.BulkSyncedFromPeer(global)
		assert.Equal(t, space.SyncStateSynced, state(svc), "a global peer confirms like a node")
	})

	t.Run("network incompatible", func(t *testing.T) {
		svc, tr := newSvc(nodeOnly, 1)
		svc.SetNetworkIncompatibleFn(func() bool { return true })
		tr.HeadsChange("obj", heads)
		assert.Equal(t, space.SyncStateError, state(svc), "over Syncing")
		svc, _ = newSvc(none, 0)
		svc.SetNetworkIncompatibleFn(func() bool { return true })
		assert.Equal(t, space.SyncStateError, state(svc), "over Offline")
	})
}

// Presence moves with no tracker hook: a node connection dropping
// flips an emitted Synced rollup to Offline on the next tick and its
// return flips it back, one event each; a tick with nothing moved
// emits nothing.
func TestTick_PresenceMovesWithoutHook(t *testing.T) {
	svc := NewService()
	defer svc.Close()
	var nodes atomic.Int32
	nodes.Store(1)
	svc.SetPeerCountsFn(func(string) (int, int, int) { return int(nodes.Load()), 0, 0 })
	svc.SetTotalFn(func(string) int { return 1 })
	var incompatible atomic.Bool
	svc.SetNetworkIncompatibleFn(incompatible.Load)

	var mu sync.Mutex
	var seen []space.SyncState
	svc.SubscribeStatus(func(ev space.SpaceSyncStatus) {
		mu.Lock()
		seen = append(seen, ev.State)
		mu.Unlock()
	})
	states := func() []space.SyncState {
		mu.Lock()
		defer mu.Unlock()
		return append([]space.SyncState(nil), seen...)
	}

	tr := svc.For("space-1")
	tr.HeadsChange("obj", []string{"h"})
	tr.HeadsApply("node", "obj", []string{"h"}, true)
	svc.Tick()
	svc.Tick()
	require.Equal(t, []space.SyncState{space.SyncStateSynced}, states(), "one event for the confirmed space; a quiet tick adds none")

	nodes.Store(0)
	svc.Tick()
	assert.Equal(t, []space.SyncState{space.SyncStateSynced, space.SyncStateOffline}, states(), "the node dropped")
	assert.Equal(t, 0, svc.Status("space-1").NetworkPeers)

	nodes.Store(1)
	svc.Tick()
	assert.Equal(t, []space.SyncState{space.SyncStateSynced, space.SyncStateOffline, space.SyncStateSynced}, states(), "the node is back; this run's confirmation stands")

	incompatible.Store(true)
	svc.Tick()
	assert.Equal(t, space.SyncStateError, states()[len(states())-1], "the network refused the client")
	incompatible.Store(false)
	svc.Tick()
	assert.Equal(t, space.SyncStateSynced, states()[len(states())-1])
	assert.Len(t, states(), 5)
}

// A loaded space emits its first rollup on the next tick, so
// subscribers see it at all and the presence poll has a baseline.
func TestFor_EmitsBaselineRollup(t *testing.T) {
	svc := NewService()
	defer svc.Close()
	svc.SetPeerCountsFn(func(string) (int, int, int) { return 0, 0, 0 })
	var mu sync.Mutex
	var seen []space.SpaceSyncStatus
	svc.SubscribeStatus(func(ev space.SpaceSyncStatus) {
		mu.Lock()
		seen = append(seen, ev)
		mu.Unlock()
	})
	svc.For("space-1")
	svc.Tick()
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seen, 1)
	assert.Equal(t, "space-1", seen[0].SpaceId)
	assert.Equal(t, space.SyncStateOffline, seen[0].State)
}

// Only tracked spaces are recorded, emitted and polled: an id that was
// merely refreshed emits nothing and leaves no baseline behind.
func TestTick_PollsTrackedSpacesOnly(t *testing.T) {
	svc := NewService()
	defer svc.Close()
	var nodes atomic.Int32
	nodes.Store(1)
	svc.SetPeerCountsFn(func(string) (int, int, int) { return int(nodes.Load()), 0, 0 })
	var mu sync.Mutex
	seen := map[string][]space.SyncState{}
	svc.SubscribeStatus(func(ev space.SpaceSyncStatus) {
		mu.Lock()
		seen[ev.SpaceId] = append(seen[ev.SpaceId], ev.State)
		mu.Unlock()
	})

	svc.For("tracked")
	svc.Refresh("refreshed-only")
	svc.Tick()
	nodes.Store(0)
	svc.Tick()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []space.SyncState{space.SyncStateUnknown, space.SyncStateOffline}, seen["tracked"])
	assert.Empty(t, seen["refreshed-only"], "no tracker, no rollup")
	svc.dirtyMu.Lock()
	defer svc.dirtyMu.Unlock()
	assert.Len(t, svc.lastEvent, 1, "no baseline for an untracked id")
}

// Without a subscriber the poll is skipped; the first tick after one
// subscribes catches up on the presence that moved meanwhile.
func TestTick_PollsOnlyWithSubscribers(t *testing.T) {
	svc := NewService()
	defer svc.Close()
	var nodes atomic.Int32
	nodes.Store(1)
	var reads atomic.Int32
	svc.SetPeerCountsFn(func(string) (int, int, int) {
		reads.Add(1)
		return int(nodes.Load()), 0, 0
	})
	svc.For("space-1")
	svc.Tick()
	before := reads.Load()
	svc.Tick()
	assert.Equal(t, before, reads.Load(), "nobody listens: presence is not read")

	var mu sync.Mutex
	var seen []space.SyncState
	svc.SubscribeStatus(func(ev space.SpaceSyncStatus) {
		mu.Lock()
		seen = append(seen, ev.State)
		mu.Unlock()
	})
	nodes.Store(0)
	svc.Tick()
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []space.SyncState{space.SyncStateOffline}, seen)
}

// A forgotten space stops being polled and emitted and reads Unknown;
// For starts it afresh.
func TestForget_StopsPollingAndEmitting(t *testing.T) {
	svc := NewService()
	defer svc.Close()
	var nodes atomic.Int32
	nodes.Store(1)
	svc.SetPeerCountsFn(func(string) (int, int, int) { return int(nodes.Load()), 0, 0 })
	var mu sync.Mutex
	var seen []space.SyncState
	svc.SubscribeStatus(func(ev space.SpaceSyncStatus) {
		mu.Lock()
		seen = append(seen, ev.State)
		mu.Unlock()
	})
	states := func() []space.SyncState {
		mu.Lock()
		defer mu.Unlock()
		return append([]space.SyncState(nil), seen...)
	}

	tr := svc.For("space-1")
	tr.HeadsChange("obj", []string{"h"})
	tr.HeadsApply("node", "obj", []string{"h"}, true)
	svc.Tick()
	require.Equal(t, []space.SyncState{space.SyncStateSynced}, states())

	svc.Forget("space-1")
	nodes.Store(0)
	svc.Tick()
	assert.Equal(t, []space.SyncState{space.SyncStateSynced}, states(), "forgotten: nothing emitted")
	assert.Equal(t, space.SyncStateUnknown, svc.Status("space-1").State)

	svc.For("space-1")
	svc.Tick()
	assert.Equal(t, []space.SyncState{space.SyncStateSynced, space.SyncStateOffline}, states(), "a fresh tracker starts over")
}

// A space forgotten while the tick reads its status leaves no event
// and no baseline; one recreated in the same window is not stamped
// with the old tracker's rollup and reads on the next tick instead.
func TestTick_ForgetDuringStatusLeavesNothing(t *testing.T) {
	svc := NewService()
	defer svc.Close()
	var during atomic.Value
	during.Store(func() {})
	svc.SetPeerCountsFn(func(string) (int, int, int) {
		during.Swap(func() {}).(func())()
		return 1, 0, 0
	})
	var mu sync.Mutex
	var seen []space.SyncState
	svc.SubscribeStatus(func(ev space.SpaceSyncStatus) {
		mu.Lock()
		seen = append(seen, ev.State)
		mu.Unlock()
	})
	states := func() []space.SyncState {
		mu.Lock()
		defer mu.Unlock()
		return append([]space.SyncState(nil), seen...)
	}
	baselines := func() int {
		svc.dirtyMu.Lock()
		defer svc.dirtyMu.Unlock()
		return len(svc.lastEvent)
	}

	tr := svc.For("space-1")
	tr.HeadsChange("obj", []string{"h"})
	during.Store(func() { svc.Forget("space-1") })
	svc.Tick()
	assert.Empty(t, states(), "forgotten mid-read: nothing emitted")
	assert.Zero(t, baselines())

	tr = svc.For("space-1")
	tr.HeadsChange("obj", []string{"h"})
	during.Store(func() {
		svc.Forget("space-1")
		svc.For("space-1")
	})
	svc.Tick()
	assert.Empty(t, states(), "replaced mid-read: the old rollup is dropped")
	svc.Tick()
	assert.Equal(t, []space.SyncState{space.SyncStateUnknown}, states(), "the new tracker reads on the next tick")
	assert.Equal(t, 1, baselines())
}
