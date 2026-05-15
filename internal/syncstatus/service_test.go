package syncstatus

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anyproto/any-sync-sdk/space"
)

// TestStatus_UnknownSpace verifies that asking for a space the
// Service has never seen returns a zero SpaceSyncStatus with the
// spaceId stamped, not a panic.
func TestStatus_UnknownSpace(t *testing.T) {
	svc := NewService()
	defer svc.Close()

	got := svc.Status("never-touched")
	if got.SpaceId != "never-touched" {
		t.Fatalf("SpaceId = %q, want %q", got.SpaceId, "never-touched")
	}
	if got.State != space.SyncStateUnknown {
		t.Fatalf("State = %v, want Unknown", got.State)
	}
	if got.Total != 0 || got.Synced != 0 {
		t.Fatalf("counts = (%d, %d), want (0,0)", got.Total, got.Synced)
	}
}

// TestTracker_HeadsChangeThenApply walks one tree through the basic
// happy path: local write → Syncing → responsible-node Apply →
// Synced. Verifies both the per-object snapshot and the per-object
// subscribe firehose.
func TestTracker_HeadsChangeThenApply(t *testing.T) {
	svc := NewService()
	defer svc.Close()
	// Constant total = 1 so Synced math is well-defined when the
	// tree converges.
	svc.SetTotalFn(func(string) int { return 1 })

	tr := svc.For("space-1")

	const treeId = "obj-1"
	var seen []space.ObjectSyncStatus
	var mu sync.Mutex
	cancel := tr.SubscribeObject(treeId, func(ev space.ObjectSyncStatus) {
		mu.Lock()
		seen = append(seen, ev)
		mu.Unlock()
	})
	defer cancel()

	tr.HeadsChange(treeId, []string{"head-a"})
	if got := tr.Object(treeId).State; got != space.SyncStateSyncing {
		t.Fatalf("after HeadsChange State = %v, want Syncing", got)
	}
	if pending := tr.PendingCount(); pending != 1 {
		t.Fatalf("PendingCount = %d, want 1", pending)
	}

	// Responsible-sender Apply with allAdded=true must flip Synced.
	tr.HeadsApply("node-1", treeId, []string{"head-a"}, true)
	if got := tr.Object(treeId).State; got != space.SyncStateSynced {
		t.Fatalf("after HeadsApply State = %v, want Synced", got)
	}
	if pending := tr.PendingCount(); pending != 0 {
		t.Fatalf("PendingCount = %d, want 0", pending)
	}

	// Snapshot via Service.Status should agree.
	got := svc.Status("space-1")
	if got.State != space.SyncStateSynced {
		t.Fatalf("Service.Status.State = %v, want Synced", got.State)
	}
	if got.Synced != 1 || got.Total != 1 {
		t.Fatalf("Status (Synced, Total) = (%d, %d), want (1, 1)", got.Synced, got.Total)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("got %d events, want 2 (Syncing, Synced)", len(seen))
	}
	if seen[0].State != space.SyncStateSyncing || seen[1].State != space.SyncStateSynced {
		t.Fatalf("event states = (%v, %v), want (Syncing, Synced)", seen[0].State, seen[1].State)
	}
}

// TestSubscribeStatus_FanOut verifies the account-wide subscriber
// registry fires on tracker transitions and that cancel detaches
// the subscriber. Dispatch goes through the debounce loop; tests
// drive it via Tick().
func TestSubscribeStatus_FanOut(t *testing.T) {
	svc := NewService()
	defer svc.Close()
	svc.SetTotalFn(func(string) int { return 1 })

	var fired atomic.Int64
	cancel := svc.SubscribeStatus(func(ev space.SpaceSyncStatus) {
		if ev.SpaceId == "space-A" {
			fired.Add(1)
		}
	})

	tr := svc.For("space-A")
	tr.HeadsChange("obj-x", []string{"h1"})
	svc.Tick() // → Syncing event
	tr.HeadsApply("node", "obj-x", []string{"h1"}, true)
	svc.Tick() // → Synced event

	if got := fired.Load(); got != 2 {
		t.Fatalf("fired = %d, want 2 (Syncing, Synced)", got)
	}

	cancel()
	before := fired.Load()
	tr.HeadsChange("obj-y", []string{"h2"})
	svc.Tick()
	if got := fired.Load(); got != before {
		t.Fatalf("after cancel fired changed: %d → %d", before, got)
	}
}

// TestSubscribeStatus_Dedupe verifies the debounce loop suppresses
// repeated dispatches when the rollup didn't actually change.
func TestSubscribeStatus_Dedupe(t *testing.T) {
	svc := NewService()
	defer svc.Close()
	svc.SetTotalFn(func(string) int { return 1 })

	var fired atomic.Int64
	svc.SubscribeStatus(func(ev space.SpaceSyncStatus) {
		if ev.SpaceId == "space-X" {
			fired.Add(1)
		}
	})

	tr := svc.For("space-X")
	tr.HeadsChange("obj", []string{"h"})
	svc.Tick() // → Syncing event (1)
	if got := fired.Load(); got != 1 {
		t.Fatalf("after first tick fired = %d, want 1", got)
	}
	// Same head, same state — should not re-dispatch.
	tr.HeadsChange("obj", []string{"h"})
	svc.Tick()
	if got := fired.Load(); got != 1 {
		t.Fatalf("after dedupe tick fired = %d, want 1", got)
	}
	tr.HeadsApply("node", "obj", []string{"h"}, true)
	svc.Tick() // → Synced event (2)
	if got := fired.Load(); got != 2 {
		t.Fatalf("after Apply tick fired = %d, want 2", got)
	}
}

// TestTracker_NonResponsibleSenderIgnored exercises the v1 rule that
// only nodeconf-listed senders flip Synced.
func TestTracker_NonResponsibleSenderIgnored(t *testing.T) {
	svc := NewService()
	defer svc.Close()
	svc.SetNodeIdsFn(func(string) []string { return []string{"node-1"} })

	tr := svc.For("space-1")
	tr.HeadsChange("obj", []string{"h"})

	tr.HeadsApply("some-peer", "obj", []string{"h"}, true)
	if got := tr.Object("obj").State; got != space.SyncStateSyncing {
		t.Fatalf("non-responsible Apply flipped State to %v, want Syncing", got)
	}

	tr.HeadsApply("node-1", "obj", []string{"h"}, true)
	if got := tr.Object("obj").State; got != space.SyncStateSynced {
		t.Fatalf("responsible Apply: State = %v, want Synced", got)
	}
}

// TestTracker_ExcludedTrees verifies the tracker drops hooks for
// excluded tree ids (ACL, spaceIndex, etc).
func TestTracker_ExcludedTrees(t *testing.T) {
	svc := NewService()
	defer svc.Close()
	svc.SetExcludedTreesFn(func(string) []string { return []string{"acl-tree", "space-index"} })

	tr := svc.For("space-1")
	tr.HeadsChange("acl-tree", []string{"x"})
	tr.HeadsChange("space-index", []string{"y"})
	tr.HeadsChange("user-obj", []string{"z"})

	if pending := tr.PendingCount(); pending != 1 {
		t.Fatalf("PendingCount = %d, want 1 (only user-obj)", pending)
	}
	if got := tr.Object("acl-tree").State; got != space.SyncStateUnknown {
		t.Fatalf("acl-tree State = %v, want Unknown (excluded)", got)
	}
}

// TestTracker_AddExcludedAtRuntime verifies AddExcluded drops a tree
// that the tracker had previously recorded state for. Mirrors the
// production flow where spaceIndex's id is only derived after a few
// hooks may have already fired for it.
func TestTracker_AddExcludedAtRuntime(t *testing.T) {
	svc := NewService()
	defer svc.Close()

	tr := svc.For("space-1")
	tr.HeadsChange("late-system-tree", []string{"h"})
	if pending := tr.PendingCount(); pending != 1 {
		t.Fatalf("before exclude PendingCount = %d, want 1", pending)
	}
	tr.AddExcluded("late-system-tree")
	if pending := tr.PendingCount(); pending != 0 {
		t.Fatalf("after exclude PendingCount = %d, want 0", pending)
	}
	// Future hooks for this id are dropped too.
	tr.HeadsChange("late-system-tree", []string{"h2"})
	if pending := tr.PendingCount(); pending != 0 {
		t.Fatalf("after re-hook PendingCount = %d, want 0", pending)
	}
}

// TestTracker_AppComponent_NameMatchesAnysync verifies the Tracker's
// component name matches any-sync's expected CName so the per-space
// commonspace graph wires us in.
func TestTracker_AppComponent_NameMatchesAnysync(t *testing.T) {
	svc := NewService()
	defer svc.Close()
	tr := svc.For("space-1")
	if got := tr.Name(); got != "common.commonspace.syncstatus" {
		t.Fatalf("Name() = %q, want common.commonspace.syncstatus", got)
	}
	if err := tr.Init(nil); err != nil {
		t.Fatalf("Init returned err: %v", err)
	}
}

// TestService_RunStartsTicker confirms Run launches the goroutine and
// SubscribeStatus deliveries land without a manual Tick. Uses a
// generous timeout to keep CI flake-free.
func TestService_RunStartsTicker(t *testing.T) {
	svc := NewService()
	defer svc.Close()
	svc.SetTotalFn(func(string) int { return 1 })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc.Run(ctx)

	done := make(chan struct{}, 1)
	svc.SubscribeStatus(func(ev space.SpaceSyncStatus) {
		select {
		case done <- struct{}{}:
		default:
		}
	})

	tr := svc.For("space-1")
	tr.HeadsChange("obj", []string{"h"})

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("ticker did not dispatch within 3s")
	}
}
