package anysyncx

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// scriptedRegistry fails GetTree for the ids in fail and returns the
// trees in trees; every call is recorded so tests can assert retry
// behavior. has is what HasTree reports. first is what PullFirst
// reports; a fetched id adds its grow entry to it, the way a fetched
// spaceIndex makes its bundle roots known. firstTypes is what
// PullFirstTypes reports: nil syncs inline, set uses the sync job.
// ReleaseTree calls are recorded in released.
type scriptedRegistry struct {
	fail       map[string]error
	trees      map[string]objecttree.ObjectTree
	has        map[string]bool
	first      []string
	grow       map[string][]string
	firstTypes []string
	// onGet runs inside every GetTree, outside the lock: a test's hook
	// to hold a load or watch the ones in flight.
	onGet func(ctx context.Context, treeId string)

	// mu guards what a round's workers and overlapping rounds write.
	mu    sync.Mutex
	calls []string
	// firstCalls counts PullFirst lookups.
	firstCalls int
	released   []string
}

func (r *scriptedRegistry) GetTree(ctx context.Context, _, treeId string) (objecttree.ObjectTree, error) {
	r.mu.Lock()
	r.calls = append(r.calls, treeId)
	r.mu.Unlock()
	if r.onGet != nil {
		r.onGet(ctx, treeId)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.first = append(r.first, r.grow[treeId]...)
	if err, ok := r.fail[treeId]; ok {
		return nil, err
	}
	if tr, ok := r.trees[treeId]; ok {
		return tr, nil
	}
	// nil tree is fine for SyncAll: the synctree.SyncTree assertion
	// just comes back false and the ping is skipped.
	return nil, nil
}

func (r *scriptedRegistry) got() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

func (r *scriptedRegistry) reset(fail map[string]error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = nil
	r.fail = fail
}

func (r *scriptedRegistry) HasTree(_ context.Context, _, treeId string) (bool, error) {
	return r.has[treeId], nil
}

func (r *scriptedRegistry) PutTree(context.Context, string, treestorage.TreeStorageCreatePayload) error {
	return nil
}
func (r *scriptedRegistry) MarkTreeDeleted(context.Context, string, string) error { return nil }
func (r *scriptedRegistry) DeleteTree(context.Context, string, string) error      { return nil }
func (r *scriptedRegistry) ShouldPullTree(context.Context, string, string, *treechangeproto.RawTreeChangeWithId, []string) bool {
	return true
}
func (r *scriptedRegistry) PullFirst(context.Context, string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.firstCalls++
	return slices.Clone(r.first)
}
func (r *scriptedRegistry) PullFirstTypes(string) []string { return r.firstTypes }
func (r *scriptedRegistry) ReleaseTree(_, treeId string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.released = append(r.released, treeId)
}

func (r *scriptedRegistry) releasedIds() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.released)
}

// A round handles the registry's pull-first trees before its other
// ids, wherever the round found them: missing, changed or parked. The
// rest keep their order, and a pull-first id the round does not work
// on adds no work.
func TestTreeSyncerHandlesPullFirstTreesFirst(t *testing.T) {
	p := fakePeer{id: "peer1"}

	t.Run("missing", func(t *testing.T) {
		reg := &scriptedRegistry{first: []string{"index", "absent"}}
		ts := newTreeSyncer("space1", reg, nil)
		require.NoError(t, ts.SyncAll(context.Background(), p, []string{"e1"}, []string{"m1", "m2", "index", "m3"}))
		require.Equal(t, []string{"index", "m1", "m2", "m3", "e1"}, reg.calls)
	})

	t.Run("changed", func(t *testing.T) {
		reg := &scriptedRegistry{first: []string{"index"}}
		ts := newTreeSyncer("space1", reg, nil)
		require.NoError(t, ts.SyncAll(context.Background(), p, []string{"e1", "index"}, []string{"m1", "m2"}))
		require.Equal(t, []string{"index", "m1", "m2", "e1"}, reg.calls)
	})

	t.Run("parked", func(t *testing.T) {
		reg := &scriptedRegistry{first: []string{"index"}, fail: map[string]error{"index": errors.New("boom")}}
		ts := newTreeSyncer("space1", reg, nil)
		require.NoError(t, ts.SyncAll(context.Background(), p, nil, []string{"index"}))
		require.Equal(t, 1, ts.Stats()[0].Pending)

		// The diff no longer offers the parked id; the retry still runs
		// ahead of the round's backlog.
		delete(reg.fail, "index")
		reg.calls = nil
		require.NoError(t, ts.SyncAll(context.Background(), p, []string{"e1"}, []string{"m1", "m2"}))
		require.Equal(t, []string{"index", "m1", "m2", "e1"}, reg.calls)
		require.Equal(t, 0, ts.Stats()[0].Pending)
	})

	t.Run("named by a tree handled first", func(t *testing.T) {
		// The index names root1 only once it is local.
		reg := &scriptedRegistry{first: []string{"index"}, grow: map[string][]string{"index": {"root1"}}}
		ts := newTreeSyncer("space1", reg, nil)
		require.NoError(t, ts.SyncAll(context.Background(), p, []string{"e1"}, []string{"m1", "root1", "index", "m2"}))
		require.Equal(t, []string{"index", "root1", "m1", "m2", "e1"}, reg.calls)
	})

	t.Run("not in the round", func(t *testing.T) {
		reg := &scriptedRegistry{first: []string{"index"}}
		ts := newTreeSyncer("space1", reg, nil)
		require.NoError(t, ts.SyncAll(context.Background(), p, []string{"e1"}, []string{"m2", "m1"}))
		require.Equal(t, []string{"m2", "m1", "e1"}, reg.calls)
	})

	t.Run("nothing to order", func(t *testing.T) {
		reg := &scriptedRegistry{first: []string{"index"}}
		ts := newTreeSyncer("space1", reg, nil)
		require.NoError(t, ts.SyncAll(context.Background(), p, nil, nil))
		require.NoError(t, ts.SyncAll(context.Background(), p, nil, []string{"index"}))
		require.Equal(t, []string{"index"}, reg.calls)
		require.Zero(t, reg.firstCalls, "a round with fewer than two ids does not ask the registry")
	})
}

// TestTreeSyncerRetriesFailedGetTree pins the parked-tree repair loop:
// a tree whose GetTree fails during a SyncAll round must be retried on
// a later round even when that round's diff is EMPTY. That empty-diff
// retry is the whole point — the failed fetch typically already wrote
// the tree's changes to storage, so the headsync diff converges and
// never offers the id again; without the parked set the CRDT
// projection silently diverges from storage until process restart.
func TestTreeSyncerRetriesFailedGetTree(t *testing.T) {
	reg := &scriptedRegistry{fail: map[string]error{"t1": errors.New("boom")}}
	ts := newTreeSyncer("space1", reg, nil)
	p := fakePeer{id: "peer1"}

	require.NoError(t, ts.SyncAll(context.Background(), p, nil, []string{"t1", "t2"}))
	require.Equal(t, []string{"t1", "t2"}, reg.calls)
	stats := ts.Stats()
	require.Len(t, stats, 1)
	require.Equal(t, 1, stats[0].Pending, "failed id must be parked")

	// Next round: diff converged (no missing, no existing). The parked
	// id must still be retried — and recover once GetTree succeeds.
	delete(reg.fail, "t1")
	reg.calls = nil
	require.NoError(t, ts.SyncAll(context.Background(), p, nil, nil))
	require.Equal(t, []string{"t1"}, reg.calls)
	require.Equal(t, 0, ts.Stats()[0].Pending, "recovered id must clear")

	// And once clear, quiet rounds stay no-op.
	reg.calls = nil
	require.NoError(t, ts.SyncAll(context.Background(), p, nil, nil))
	require.Empty(t, reg.calls)
}

// TestTreeSyncerParksOnDeadCtx: when the round context is already dead,
// remaining ids are parked without pointless GetTree attempts and
// retried on the next round.
func TestTreeSyncerParksOnDeadCtx(t *testing.T) {
	reg := &scriptedRegistry{}
	ts := newTreeSyncer("space1", reg, nil)
	p := fakePeer{id: "peer1"}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, ts.SyncAll(ctx, p, []string{"t2"}, []string{"t1"}))
	require.Empty(t, reg.calls, "dead ctx must not hit the registry")
	require.Equal(t, 2, ts.Stats()[0].Pending)

	require.NoError(t, ts.SyncAll(context.Background(), p, nil, nil))
	require.ElementsMatch(t, []string{"t1", "t2"}, reg.calls)
	require.Equal(t, 0, ts.Stats()[0].Pending)
}

// A round whose context ends during a fetch parks the tree in flight
// and the ones after it, so the next round picks them up.
func TestTreeSyncerParksWhenTheRoundEndsMidFetch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reg := &scriptedRegistry{fail: map[string]error{"t1": context.Canceled}}
	reg.onGet = func(context.Context, string) { cancel() }
	ts := newTreeSyncer("space1", reg, nil)
	require.NoError(t, ts.SyncAll(ctx, fakePeer{id: "peer1"}, nil, []string{"t1", "t2"}))
	require.Equal(t, []string{"t1"}, reg.calls)
	require.Equal(t, 2, ts.pendingCount())
}

// TestTreeSyncerDedupsPendingAgainstDiff: an id present in both the
// round's diff and the parked set is resolved once per round.
func TestTreeSyncerDedupsPendingAgainstDiff(t *testing.T) {
	reg := &scriptedRegistry{fail: map[string]error{"t1": errors.New("boom")}}
	ts := newTreeSyncer("space1", reg, nil)
	p := fakePeer{id: "peer1"}

	require.NoError(t, ts.SyncAll(context.Background(), p, nil, []string{"t1"}))
	delete(reg.fail, "t1")
	reg.calls = nil
	// t1 arrives in the diff again while also parked.
	require.NoError(t, ts.SyncAll(context.Background(), p, nil, []string{"t1"}))
	require.Equal(t, []string{"t1"}, reg.calls)
	require.Equal(t, 0, ts.Stats()[0].Pending)
}

// headsTree is the slice of an object tree SyncAll reads after a fetch.
type headsTree struct {
	objecttree.ObjectTree
	heads []string
}

func (h headsTree) Lock()           {}
func (h headsTree) Unlock()         {}
func (h headsTree) Heads() []string { return h.heads }

// pingTree counts the full-sync requests SyncAll queues for it.
type pingTree struct {
	synctree.SyncTree
	pings atomic.Int64
}

func (p *pingTree) Lock()           {}
func (p *pingTree) Unlock()         {}
func (p *pingTree) Heads() []string { return []string{"h"} }
func (p *pingTree) SyncWithPeer(context.Context, peer.Peer) error {
	p.pings.Add(1)
	return nil
}

// A tree fetched whole from the peer is reported with its heads; a
// tree that already existed is not.
func TestTreeSyncerReportsFetchedTrees(t *testing.T) {
	reg := &scriptedRegistry{trees: map[string]objecttree.ObjectTree{
		"missing":  headsTree{heads: []string{"h1", "h2"}},
		"existing": headsTree{heads: []string{"h3"}},
	}}
	ts := newTreeSyncer("space1", reg, nil)
	var got []string
	ts.onFetched = func(peerId, treeId string, heads []string) {
		got = append(got, peerId+"/"+treeId+"/"+strings.Join(heads, ","))
	}
	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "n1"}, []string{"existing"}, []string{"missing"}))
	require.Equal(t, []string{"n1/missing/h1,h2"}, got)
}

// A changed tree is pinged with a full-sync request, and so is a tree
// the diff names as missing that is in storage already (held as its
// root alone); a tree fetched from the peer is in sync with it already
// and is not.
func TestTreeSyncerPingsLocalTreesOnly(t *testing.T) {
	missing, rootOnly, changed := &pingTree{}, &pingTree{}, &pingTree{}
	reg := &scriptedRegistry{
		trees: map[string]objecttree.ObjectTree{"missing": missing, "rootOnly": rootOnly, "changed": changed},
		has:   map[string]bool{"rootOnly": true, "changed": true},
	}
	ts := newTreeSyncer("space1", reg, nil)
	var reported []string
	ts.onFetched = func(_, treeId string, _ []string) { reported = append(reported, treeId) }
	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "n1"}, []string{"changed"}, []string{"missing", "rootOnly"}))
	require.Equal(t, int64(1), changed.pings.Load())
	require.Equal(t, int64(1), rootOnly.pings.Load())
	require.Zero(t, missing.pings.Load())
	require.Equal(t, []string{"missing"}, reported)
}

// A tree declined by selective sync is not a failure: the stub it
// leaves behind converges the diff, so the id must not be parked (it
// would be re-probed every round and hold ParkedTreeCount above zero
// for the process lifetime). A park from an earlier transient failure
// is released once the tree is classified as skipped.
func TestTreeSyncerSkippedTreeIsNotParked(t *testing.T) {
	reg := &scriptedRegistry{fail: map[string]error{"t": errors.New("boom")}}
	ts := newTreeSyncer("space1", reg, nil)
	core, logs := observer.New(zapcore.DebugLevel)
	ts.log = logger.CtxLogger{Logger: zap.New(core)}
	p := fakePeer{id: "peer1"}

	require.NoError(t, ts.SyncAll(context.Background(), p, nil, []string{"t"}))
	require.Equal(t, 1, ts.Stats()[0].Pending)

	reg.fail["t"] = fmt.Errorf("spaceobjects: BuildTree t: %w", ErrTreeTypeSkipped)
	reg.calls = nil
	require.NoError(t, ts.SyncAll(context.Background(), p, nil, nil))
	require.Equal(t, []string{"t"}, reg.calls)
	require.Equal(t, 0, ts.Stats()[0].Pending)
	reg.calls = nil
	require.NoError(t, ts.SyncAll(context.Background(), p, nil, nil))
	require.Empty(t, reg.calls, "an unparked id is not retried")
	require.Len(t, logs.FilterLevelExact(zapcore.WarnLevel).All(), 1, "only the transient failure warns")

	// Never parked when skipped on first sight.
	reg.fail["u"] = reg.fail["t"]
	require.NoError(t, ts.SyncAll(context.Background(), p, nil, []string{"u"}))
	require.Equal(t, 0, ts.Stats()[0].Pending)
}

// A peer that turns a fetch away as busy is asked again within the
// round: the tree is not parked for a refusal that passes on its own.
func TestTreeSyncerRetriesABusyPeer(t *testing.T) {
	prevBackoff := getTreeBackoff
	getTreeBackoff = time.Millisecond
	t.Cleanup(func() { getTreeBackoff = prevBackoff })

	reg := &busyRegistry{busy: map[string]int{"t1": 2, "t2": getTreeRetries + 1}}
	ts := newTreeSyncer("space1", reg, nil)
	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, nil, []string{"t1", "t2"}))
	require.Equal(t, 3, reg.gets["t1"], "refused twice, fetched on the third ask")
	require.Equal(t, getTreeRetries+1, reg.gets["t2"])
	require.Equal(t, 1, ts.Stats()[0].Pending, "only the tree the peer kept refusing is parked")
	require.Less(t, ts.limits.of("peer1").width, peerLimitMax, "refusals over the cap narrow the peer's budget")
}

// A round that ends while it waits to ask a busy peer again asks
// nothing more.
func TestTreeSyncerBusyRetryStopsWithTheRound(t *testing.T) {
	prevBackoff := getTreeBackoff
	getTreeBackoff = 30 * time.Millisecond
	t.Cleanup(func() { getTreeBackoff = prevBackoff })

	reg := &busyRegistry{busy: map[string]int{"t1": 100}}
	ts := newTreeSyncer("space1", reg, nil)
	ctx, cancel := context.WithCancel(context.Background())
	reg.onGet = func() { cancel() } // the round ends during the first refusal
	require.NoError(t, ts.SyncAll(ctx, fakePeer{id: "peer1"}, nil, []string{"t1", "t2"}))
	require.Equal(t, 1, reg.gets["t1"])
	require.Zero(t, reg.gets["t2"], "a finished round fetches nothing")
}

// busyRegistry refuses the first busy[id] fetches of a tree the way a
// peer over its request cap does.
type busyRegistry struct {
	scriptedRegistry
	busy map[string]int
	// onGet runs inside every fetch.
	onGet func()

	getsMu sync.Mutex
	gets   map[string]int
}

func (r *busyRegistry) GetTree(_ context.Context, _, treeId string) (objecttree.ObjectTree, error) {
	if r.onGet != nil {
		r.onGet()
	}
	r.getsMu.Lock()
	defer r.getsMu.Unlock()
	if r.gets == nil {
		r.gets = map[string]int{}
	}
	r.gets[treeId]++
	if r.gets[treeId] <= r.busy[treeId] {
		return nil, fmt.Errorf("spaceobjects: BuildTree %s: %w", treeId, spacesyncproto.ErrTooManyRequestsFromPeer)
	}
	return nil, nil
}

// The tests of this package assert the order trees are handled in, so
// they run one tree at a time; a test of the parallel groups sets the
// worker counts itself.
func TestMain(m *testing.M) {
	treeSyncWorkers = 1
	treeFetchWorkers = 1
	os.Exit(m.Run())
}

func withWorkers(t *testing.T, fetch, sync int) {
	t.Helper()
	prevFetch, prevSync := treeFetchWorkers, treeSyncWorkers
	treeFetchWorkers, treeSyncWorkers = fetch, sync
	t.Cleanup(func() { treeFetchWorkers, treeSyncWorkers = prevFetch, prevSync })
}

// flight watches the calls in flight: every call waits until want of
// them overlap (or a timeout), so a group that runs one at a time
// stalls and never reaches the peak.
type flight struct {
	want           int64
	wait           time.Duration
	inFlight, peak atomic.Int64
	full           chan struct{}
	once           sync.Once
}

func newFlight(want int, wait time.Duration) *flight {
	return &flight{want: int64(want), wait: wait, full: make(chan struct{})}
}

func (f *flight) hold(context.Context, string) {
	n := f.inFlight.Add(1)
	defer f.inFlight.Add(-1)
	for {
		if p := f.peak.Load(); n <= p || f.peak.CompareAndSwap(p, n) {
			break
		}
	}
	if n >= f.want {
		f.once.Do(func() { close(f.full) })
	}
	select {
	case <-f.full:
	case <-time.After(f.wait):
	}
}

// backlog returns n tree ids, m01..mNN.
func backlog(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("m%02d", i+1)
	}
	return out
}
