package anysyncx

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/stretchr/testify/require"
)

// scriptedFetcher answers a fetch with the tree's changeType from types
// ("object" when unnamed), or the error in fail; the ids in local are
// reported as in storage already. Every call is recorded.
type scriptedFetcher struct {
	types map[string]string
	fail  map[string]error
	local map[string]bool
	// onFetch runs inside every fetch, outside the lock.
	onFetch func(ctx context.Context, treeId string)

	mu    sync.Mutex
	calls []string
}

func (f *scriptedFetcher) fetch(ctx context.Context, _, treeId string) (string, bool, error) {
	f.mu.Lock()
	f.calls = append(f.calls, treeId)
	f.mu.Unlock()
	if f.onFetch != nil {
		f.onFetch(ctx, treeId)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.fail[treeId]; ok {
		return "", false, err
	}
	ct := "object"
	if named, ok := f.types[treeId]; ok {
		ct = named
	}
	return ct, f.local[treeId], nil
}

func (f *scriptedFetcher) got() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// jobSyncer returns a syncer whose rounds queue on the sync job: the
// registry names pull-first types and the fetcher is set.
func jobSyncer(t *testing.T, reg *scriptedRegistry, f *scriptedFetcher) *treeSyncerAdapter {
	t.Helper()
	if reg.firstTypes == nil {
		reg.firstTypes = []string{"type", "collection"}
	}
	ts := newTreeSyncer("space1", reg, nil)
	ts.fetcher = f
	t.Cleanup(func() { _ = ts.Close(context.Background()) })
	return ts
}

// events records what the job did, in order: "f:" a fetch, "g:" a load
// through the registry.
type events struct {
	mu  sync.Mutex
	log []string
}

func (e *events) add(kind, id string) {
	e.mu.Lock()
	e.log = append(e.log, kind+":"+id)
	e.mu.Unlock()
}

func (e *events) all() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.log)
}

// A pass handles the trees the registry names first, fetches every
// other missing tree into storage, then materializes the fetched types
// and collections, then the fetched objects, then handles the changed
// trees and the retries — each group done before the next starts.
func TestSyncJobHandlesGroupsInOrder(t *testing.T) {
	ev := &events{}
	defs := map[string]string{"m03": "type", "m07": "collection", "m11": "type"}
	reg := &scriptedRegistry{first: []string{"index"}, fail: map[string]error{}}
	reg.onGet = func(_ context.Context, id string) { ev.add("g", id) }
	f := &scriptedFetcher{types: defs, onFetch: func(_ context.Context, id string) { ev.add("f", id) }}
	ts := jobSyncer(t, reg, f)
	ts.markPending("p1", false)
	ts.markPending("p2", false)

	missing := append(backlog(12), "index")
	existing := []string{"e1", "e2"}
	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, existing, missing))

	group := func(e string) int {
		kind, id, _ := strings.Cut(e, ":")
		switch {
		case id == "index":
			return 0
		case kind == "f":
			return 1
		case defs[id] != "":
			return 2
		case strings.HasPrefix(id, "m"):
			return 3
		case strings.HasPrefix(id, "e"):
			return 4
		default:
			return 5
		}
	}
	got := ev.all()
	require.Len(t, got, 1+12+12+2+2)
	last := -1
	for _, e := range got {
		g := group(e)
		require.GreaterOrEqual(t, g, last, "%v", got)
		last = g
	}
	require.Equal(t, backlog(12), f.got(), "fetched in diff order; the index is fetched through the registry")
	require.Equal(t, []string{"m03", "m07", "m11"}, reg.got()[1:4], "definitions materialize first, in diff order")
	require.Zero(t, ts.pendingCount())
	require.Zero(t, ts.syncingCount())
}

// The trees the registry names first are asked for again after they
// are handled: a fetched spaceIndex lists bundle roots, which then go
// ahead of the fetched trees too.
func TestSyncJobAsksPullFirstAgain(t *testing.T) {
	reg := &scriptedRegistry{first: []string{"index"}, grow: map[string][]string{"index": {"root1"}}}
	f := &scriptedFetcher{}
	ts := jobSyncer(t, reg, f)
	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, nil, []string{"m1", "root1", "index", "m2"}))
	require.Equal(t, []string{"index", "root1", "m1", "m2"}, reg.got())
	require.Equal(t, []string{"m1", "m2"}, f.got())
}

// A tree the registry names first more than once — a bundle root as
// the register's and as a claimed one — is handled once.
func TestSyncJobHandlesATreeNamedTwiceOnce(t *testing.T) {
	reg := &scriptedRegistry{first: []string{"root1", "index", "root1"}}
	f := &scriptedFetcher{}
	ts := jobSyncer(t, reg, f)
	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, nil, []string{"m1", "root1", "index"}))
	require.Equal(t, []string{"root1", "index", "m1"}, reg.got())
}

// Each group runs at its own width: the fetches treeFetchWorkers at a
// time, the fetched objects treeSyncWorkers at a time, the fetched
// definitions one at a time.
func TestSyncJobGroupsRunAtTheirWidth(t *testing.T) {
	withWorkers(t, 4, 3)
	defs := map[string]string{}
	for _, id := range backlog(12)[:5] {
		defs[id] = "type"
	}
	fetches := newFlight(4, 5*time.Second)
	defLoads := newFlight(2, 20*time.Millisecond) // two never overlap
	objLoads := newFlight(3, 5*time.Second)
	reg := &scriptedRegistry{}
	reg.onGet = func(ctx context.Context, id string) {
		if defs[id] != "" {
			defLoads.hold(ctx, id)
		} else {
			objLoads.hold(ctx, id)
		}
	}
	ts := jobSyncer(t, reg, &scriptedFetcher{types: defs, onFetch: fetches.hold})
	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, nil, backlog(12)))
	require.Equal(t, int64(4), fetches.peak.Load())
	require.Equal(t, int64(1), defLoads.peak.Load())
	require.Equal(t, int64(3), objLoads.peak.Load())
	require.ElementsMatch(t, backlog(12), reg.got())
}

// The fetches stay within the peer's request budget, however many
// workers the pass has.
func TestSyncJobFetchesStayWithinThePeerBudget(t *testing.T) {
	withWorkers(t, 6, 1)
	fetches := newFlight(6, 300*time.Millisecond) // six never overlap: the budget is two
	reg := &scriptedRegistry{}
	ts := jobSyncer(t, reg, &scriptedFetcher{onFetch: fetches.hold})
	ts.limits.of("peer1").width = 2
	// Four trees: too few answered requests for the budget to widen
	// before the last fetch starts.
	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, nil, backlog(4)))
	require.Equal(t, int64(2), fetches.peak.Load())
}

// The trees the registry names first are fetched on the budget's
// reserved slot: a small space's spaceIndex does not wait behind a
// large space's fetch stage holding the whole budget.
func TestSyncJobFirstTreesBypassASpentBudget(t *testing.T) {
	reg := &scriptedRegistry{first: []string{"index"}}
	f := &scriptedFetcher{}
	ts := jobSyncer(t, reg, f)
	limit := ts.limits.of("peer1")
	var held []peerSlot
	for i := 0; i < peerLimitMax; i++ {
		slot, err := limit.acquire(context.Background(), false)
		require.NoError(t, err)
		held = append(held, slot)
	}
	done := make(chan error, 1)
	go func() {
		done <- ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, nil, []string{"m1", "index", "m2"})
	}()
	require.Eventually(t, func() bool { return slices.Equal(reg.got(), []string{"index"}) }, 5*time.Second, time.Millisecond,
		"the index is fetched on the reserved slot")
	time.Sleep(50 * time.Millisecond)
	require.Empty(t, f.got(), "the other trees wait for the budget")
	for _, slot := range held {
		limit.release(slot, false)
	}
	require.NoError(t, <-done)
	require.ElementsMatch(t, []string{"m1", "m2"}, f.got())
}

// A round returns with its caller's error once the caller's context
// ends, and the job goes on: nothing is parked, the fetch in flight
// completes and the tree is materialized.
func TestSyncJobOutlivesTheRound(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	reg := &scriptedRegistry{}
	f := &scriptedFetcher{onFetch: func(context.Context, string) {
		close(started)
		<-release
	}}
	ts := jobSyncer(t, reg, f)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ts.SyncAll(ctx, fakePeer{id: "peer1"}, nil, []string{"m1"}) }()
	<-started
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Zero(t, ts.pendingCount(), "a round its caller left parks nothing")
	require.Equal(t, 1, ts.syncingCount())
	require.Empty(t, reg.got())

	close(release)
	require.Eventually(t, func() bool { return ts.syncingCount() == 0 }, 5*time.Second, time.Millisecond)
	require.Equal(t, []string{"m1"}, reg.got())
	require.Zero(t, ts.pendingCount())
}

// A round waits for the whole queue, not only for the trees its own
// diff named: a round with an empty diff that arrives while fetched
// trees await their replay returns only once they are in.
func TestSyncJobRoundWaitsForTheWholeQueue(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	reg := &scriptedRegistry{}
	reg.onGet = func(context.Context, string) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
	}
	ts := jobSyncer(t, reg, &scriptedFetcher{})
	first := make(chan error, 1)
	go func() { first <- ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, nil, backlog(3)) }()
	<-started // the trees are fetched; the first replay is held

	empty := make(chan error, 1)
	go func() { empty <- ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, nil, nil) }()
	select {
	case err := <-empty:
		t.Fatalf("an empty-diff round returned (%v) while fetched trees await their replay", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-first)
	require.NoError(t, <-empty)
	require.Equal(t, backlog(3), reg.got())
}

// A tree is reported fetched from the peer it was fetched from, however
// many rounds named it since; a local tree is pinged on every peer
// whose round named it.
func TestSyncJobKeepsTheFetchingPeerAndPingsEveryNamingPeer(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	changed := &pingTree{}
	reg := &scriptedRegistry{trees: map[string]objecttree.ObjectTree{"m1": headsTree{heads: []string{"h"}}, "c1": changed}, has: map[string]bool{"c1": true}}
	f := &scriptedFetcher{onFetch: func(context.Context, string) {
		close(started)
		<-release
	}}
	ts := jobSyncer(t, reg, f)
	var reported []string
	ts.onFetched = func(peerId, treeId string, _ []string) { reported = append(reported, peerId+"/"+treeId) }

	done := make(chan error, 2)
	go func() {
		done <- ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, []string{"c1"}, []string{"m1"})
	}()
	<-started // m1 is being fetched from peer1
	go func() {
		done <- ts.SyncAll(context.Background(), fakePeer{id: "peer2"}, []string{"c1"}, []string{"m1"})
	}()
	require.Eventually(t, func() bool {
		ts.job.mu.Lock()
		defer ts.job.mu.Unlock()
		return len(ts.job.items["c1"].peers) == 2
	}, 5*time.Second, time.Millisecond)
	close(release)
	require.NoError(t, <-done)
	require.NoError(t, <-done)
	require.Equal(t, []string{"peer1/m1"}, reported)
	require.Equal(t, int64(2), changed.pings.Load())
}

// A tree deleted while it was queued is finished without a park,
// whether the fetch or the replay finds it gone.
func TestSyncJobDeletedTreeIsNotParked(t *testing.T) {
	gone := spacestorage.ErrTreeStorageAlreadyDeleted
	reg := &scriptedRegistry{fail: map[string]error{"m2": fmt.Errorf("spaceobjects: BuildTree m2: %w", gone)}}
	f := &scriptedFetcher{fail: map[string]error{"m1": gone}}
	ts := jobSyncer(t, reg, f)
	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, nil, backlog(3)))
	require.Equal(t, []string{"m02", "m03"}, reg.got()[len(reg.got())-2:])
	require.Zero(t, ts.pendingCount())
	require.Zero(t, ts.syncingCount())
}

// Rounds that overlap queue the same trees once and both wait for them.
func TestSyncJobSharesTreesBetweenRounds(t *testing.T) {
	withWorkers(t, 2, 2)
	hold := newFlight(2, 5*time.Second)
	reg := &scriptedRegistry{}
	f := &scriptedFetcher{onFetch: hold.hold}
	ts := jobSyncer(t, reg, f)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, nil, backlog(4))
		}()
	}
	wg.Wait()
	require.NoError(t, errors.Join(errs...))
	require.ElementsMatch(t, backlog(4), f.got(), "each tree fetched once")
	require.ElementsMatch(t, backlog(4), reg.got())
}

// A fetch that fails parks the tree; the next round's diff offers it
// again, and it is fetched and materialized then.
func TestSyncJobFetchFailureParks(t *testing.T) {
	reg := &scriptedRegistry{}
	f := &scriptedFetcher{fail: map[string]error{"m02": errors.New("boom")}}
	ts := jobSyncer(t, reg, f)
	p := fakePeer{id: "peer1"}

	require.NoError(t, ts.SyncAll(context.Background(), p, nil, backlog(3)))
	require.Equal(t, []string{"m01", "m03"}, reg.got())
	require.Equal(t, 1, ts.pendingCount())

	f.mu.Lock()
	f.fail = nil
	f.mu.Unlock()
	reg.reset(nil)
	require.NoError(t, ts.SyncAll(context.Background(), p, nil, []string{"m02"}))
	require.Equal(t, []string{"m02"}, reg.got())
	require.Equal(t, []string{"m01", "m02", "m03", "m02"}, f.got())
	require.Zero(t, ts.pendingCount())
}

// A fetched tree whose replay fails parks as local: the retry loads it
// again without a fetch, on a round with an empty diff.
func TestSyncJobReplayFailureParks(t *testing.T) {
	reg := &scriptedRegistry{fail: map[string]error{"m02": errors.New("boom")}}
	f := &scriptedFetcher{}
	ts := jobSyncer(t, reg, f)
	p := fakePeer{id: "peer1"}

	require.NoError(t, ts.SyncAll(context.Background(), p, nil, backlog(3)))
	require.Equal(t, backlog(3), reg.got())
	require.Equal(t, 1, ts.pendingCount())
	require.False(t, ts.parkedMissing("m02"))

	reg.reset(nil)
	require.NoError(t, ts.SyncAll(context.Background(), p, nil, nil))
	require.Equal(t, []string{"m02"}, reg.got())
	require.Equal(t, backlog(3), f.got(), "a tree in storage is not fetched again")
	require.Zero(t, ts.pendingCount())
}

// Close during a replay: the replay's context ends, the tree is not
// parked, and the round waiting returns.
func TestSyncJobCloseDuringReplay(t *testing.T) {
	started := make(chan struct{})
	reg := &scriptedRegistry{fail: map[string]error{"m01": context.Canceled}}
	reg.onGet = func(ctx context.Context, _ string) {
		close(started)
		<-ctx.Done()
	}
	ts := jobSyncer(t, reg, &scriptedFetcher{})
	done := make(chan error, 1)
	go func() { done <- ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, nil, backlog(2)) }()
	<-started
	require.NoError(t, ts.Close(context.Background()))
	require.NoError(t, <-done)
	require.Zero(t, ts.pendingCount())
	require.Equal(t, 2, ts.syncingCount(), "both trees are in storage and not in the projection")
}

// Close ends the job: the fetch in flight is cut, nothing more runs,
// the round waiting returns, and what was queued stays counted for the
// close-time watermark gate.
func TestSyncJobCloseStopsTheWork(t *testing.T) {
	started := make(chan struct{})
	reg := &scriptedRegistry{}
	f := &scriptedFetcher{fail: map[string]error{"m01": context.Canceled}}
	f.onFetch = func(ctx context.Context, _ string) {
		close(started)
		<-ctx.Done()
	}
	ts := jobSyncer(t, reg, f)

	done := make(chan error, 1)
	go func() { done <- ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, nil, backlog(3)) }()
	<-started
	require.NoError(t, ts.Close(context.Background()))
	require.NoError(t, <-done)
	require.Equal(t, []string{"m01"}, f.got())
	require.Empty(t, reg.got())
	require.Zero(t, ts.pendingCount(), "a closing job parks nothing")
	require.Equal(t, 3, ts.syncingCount())
}

// A tree the diff names as missing that is in storage already — held
// as its root alone — is pinged with a full-sync request once it is
// materialized, like a changed tree; a fetched tree is reported as in
// sync instead.
func TestSyncJobPingsTreesThatWereLocal(t *testing.T) {
	fetched, local := &pingTree{}, &pingTree{}
	reg := &scriptedRegistry{trees: map[string]objecttree.ObjectTree{"m1": fetched, "m2": local}}
	f := &scriptedFetcher{local: map[string]bool{"m2": true}}
	ts := jobSyncer(t, reg, f)
	var reported []string
	ts.onFetched = func(_, treeId string, _ []string) { reported = append(reported, treeId) }
	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, nil, []string{"m1", "m2"}))
	require.Zero(t, fetched.pings.Load())
	require.Equal(t, int64(1), local.pings.Load())
	require.Equal(t, []string{"m1"}, reported)
}

// A fetched tree is released once it is materialized, definitions and
// objects alike: the replay was all the job loaded it for. A tree that
// was local is pinged and stays loaded for the answer; so do the trees
// handled in one step (pull-first, changed) and a tree whose replay
// failed.
func TestSyncJobReleasesMaterializedTrees(t *testing.T) {
	reg := &scriptedRegistry{first: []string{"index"}, fail: map[string]error{"m3": errors.New("boom")}}
	f := &scriptedFetcher{types: map[string]string{"d1": "type"}, local: map[string]bool{"m2": true}}
	ts := jobSyncer(t, reg, f)
	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, []string{"e1"}, []string{"index", "d1", "m1", "m2", "m3"}))
	require.Equal(t, []string{"index", "d1", "m1", "m2", "m3", "e1"}, reg.got())
	require.Equal(t, []string{"d1", "m1"}, reg.releasedIds())
}

// A space the registry names no pull-first types for syncs within the
// round, one tree at a time, fetcher or not.
func TestSyncJobIsNotUsedWithoutPullFirstTypes(t *testing.T) {
	reg := &scriptedRegistry{}
	f := &scriptedFetcher{}
	ts := newTreeSyncer("space1", reg, nil)
	ts.fetcher = f
	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, nil, backlog(3)))
	require.Equal(t, backlog(3), reg.got())
	require.Empty(t, f.got())
	require.Zero(t, ts.job.count())
}
