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

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/headsync/headstorage"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// scriptedRegistry fails GetTree for the ids in fail; every call is
// recorded so tests can assert retry behavior. first is what PullFirst
// reports; a fetched id adds its grow entry to it, the way a fetched
// spaceIndex makes its bundle roots known. firstTypes is what
// PullFirstTypes reports.
type scriptedRegistry struct {
	fail       map[string]error
	trees      map[string]objecttree.ObjectTree
	first      []string
	grow       map[string][]string
	firstTypes []string
	// onGet runs inside every GetTree, outside the lock: a test's hook
	// to hold a fetch or watch the ones in flight.
	onGet func(ctx context.Context, treeId string)

	// mu guards what a round's workers and overlapping rounds write.
	mu    sync.Mutex
	calls []string
	// firstCalls counts PullFirst lookups.
	firstCalls int
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

func (r *scriptedRegistry) HasTree(_ context.Context, _, treeId string) (bool, error) {
	_, ok := r.trees[treeId]
	return ok, nil
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

// A tree is local when the space's head storage has any entry for it;
// only a missing document means it is not.
func TestHasHeadEntry(t *testing.T) {
	ctx := context.Background()
	has, err := hasHeadEntry(ctx, headEntriesFunc(func(string) error { return nil }), "t1")
	require.NoError(t, err)
	require.True(t, has)

	has, err = hasHeadEntry(ctx, headEntriesFunc(func(string) error {
		return fmt.Errorf("get entry: %w", anystore.ErrDocNotFound)
	}), "t1")
	require.NoError(t, err)
	require.False(t, has)

	_, err = hasHeadEntry(ctx, headEntriesFunc(func(string) error { return errors.New("storage closed") }), "t1")
	require.Error(t, err)
}

type headEntriesFunc func(id string) error

func (f headEntriesFunc) GetEntry(_ context.Context, id string) (headstorage.HeadsEntry, error) {
	return headstorage.HeadsEntry{Id: id}, f(id)
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

// The tests above and in treeprobe_test.go assert the order of a
// round's fetches, so they run one tree at a time; the tests below set
// the worker count themselves.
func TestMain(m *testing.M) {
	treeSyncWorkers = 1
	os.Exit(m.Run())
}

func withTreeSyncWorkers(t *testing.T, n int) {
	t.Helper()
	prev := treeSyncWorkers
	treeSyncWorkers = n
	t.Cleanup(func() { treeSyncWorkers = prev })
}

// flight watches the fetches in flight: every fetch waits until want of
// them overlap (or a timeout), so a round that syncs one tree at a time
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

// classified returns a syncer whose probes read every missing tree's
// root: "object" unless types says otherwise.
func classified(t *testing.T, reg *scriptedRegistry, types map[string]string) *treeSyncerAdapter {
	t.Helper()
	if reg.firstTypes == nil {
		reg.firstTypes = []string{"type", "collection"}
	}
	return probeSyncer(t, reg, &scriptedProber{types: types})
}

// The missing trees a probe classified as defining nothing sync several
// at a time, never more than the worker count.
func TestTreeSyncerSyncsClassifiedTreesInParallel(t *testing.T) {
	const workers = 4
	withTreeSyncWorkers(t, workers)

	f := newFlight(workers, 5*time.Second)
	reg := &scriptedRegistry{onGet: f.hold}
	ts := classified(t, reg, nil)
	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, nil, backlog(12)))
	require.Equal(t, int64(workers), f.peak.Load())
	require.ElementsMatch(t, backlog(12), reg.calls)
}

// Everything else syncs one tree at a time, whatever the worker count:
// a definition must not be replayed next to a tree that may look it
// up, and a tree no probe classified may be one.
func TestTreeSyncerSyncsTheRestOneAtATime(t *testing.T) {
	withTreeSyncWorkers(t, 4)
	p := fakePeer{id: "peer1"}
	serial := func(t *testing.T, reg *scriptedRegistry, ts *treeSyncerAdapter, existing, missing []string) {
		t.Helper()
		f := newFlight(2, 20*time.Millisecond) // two never overlap
		reg.onGet = f.hold
		require.NoError(t, ts.SyncAll(context.Background(), p, existing, missing))
		require.Equal(t, int64(1), f.peak.Load())
		require.Len(t, reg.calls, len(existing)+len(missing))
	}

	t.Run("unprobed missing trees", func(t *testing.T) {
		reg := &scriptedRegistry{}
		serial(t, reg, newTreeSyncer("space1", reg, nil), nil, backlog(6))
	})
	t.Run("types and collections", func(t *testing.T) {
		reg := &scriptedRegistry{}
		defs := map[string]string{}
		for i, id := range backlog(6) {
			defs[id] = []string{"type", "collection"}[i%2]
		}
		serial(t, reg, classified(t, reg, defs), nil, backlog(6))
	})
	t.Run("trees whose probe failed", func(t *testing.T) {
		reg := &scriptedRegistry{firstTypes: []string{"type"}}
		prober := &scriptedProber{fail: map[string]error{}}
		for _, id := range backlog(6) {
			prober.fail[id] = errors.New("refused")
		}
		serial(t, reg, probeSyncer(t, reg, prober), nil, backlog(6))
	})
	t.Run("changed trees", func(t *testing.T) {
		reg := &scriptedRegistry{}
		serial(t, reg, classified(t, reg, nil), backlog(6), nil)
	})
	t.Run("retries", func(t *testing.T) {
		reg := &scriptedRegistry{}
		ts := classified(t, reg, nil)
		for _, id := range backlog(6) {
			ts.markPending(id, true)
		}
		f := newFlight(2, 20*time.Millisecond)
		reg.onGet = f.hold
		require.NoError(t, ts.SyncAll(context.Background(), p, nil, nil))
		require.Equal(t, int64(1), f.peak.Load())
		require.Len(t, reg.calls, 6)
	})
}

// The fetches of a round stay within the peer's request budget, however
// many workers the round has; a tree that is already local takes no
// slot, so it syncs even with the budget spent.
func TestTreeSyncerFetchesStayWithinThePeerBudget(t *testing.T) {
	withTreeSyncWorkers(t, 6)

	t.Run("missing trees", func(t *testing.T) {
		f := newFlight(6, 30*time.Millisecond) // six never overlap: the budget is two
		reg := &scriptedRegistry{onGet: f.hold}
		ts := classified(t, reg, nil)
		ts.limits.of("peer1").width = 2
		// Four trees: too few answered requests for the budget to widen
		// before the last fetch starts.
		require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, nil, backlog(4)))
		require.Equal(t, int64(2), f.peak.Load())
	})

	t.Run("local trees", func(t *testing.T) {
		reg := &scriptedRegistry{}
		ts := newTreeSyncer("space1", reg, nil)
		limit := ts.limits.of("peer1")
		for i := 0; i < peerLimitMax; i++ {
			_, err := limit.acquire(context.Background())
			require.NoError(t, err)
		}
		done := make(chan error, 1)
		go func() { done <- ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, backlog(4), nil) }()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("changed trees waited for a request slot")
		}
		require.Len(t, reg.calls, 4)
	})
}

// The groups of a round keep their order, each done before the next
// starts: the trees the registry names, the probed types and
// collections, the classified rest (side by side), the trees no probe
// classified, the changed trees, the retries.
func TestTreeSyncerFinishesAGroupBeforeTheNext(t *testing.T) {
	withTreeSyncWorkers(t, 4)

	defs := map[string]string{"m03": "type", "m07": "collection", "m11": "type"}
	unknown := map[string]bool{"m05": true, "m09": true}
	group := func(id string) int {
		switch {
		case id == "index":
			return 0
		case defs[id] != "":
			return 1
		case unknown[id]:
			return 3
		case strings.HasPrefix(id, "m"):
			return 2
		case strings.HasPrefix(id, "e"):
			return 4
		default:
			return 5
		}
	}
	missing := append(backlog(16), "index")
	existing := []string{"e1", "e2", "e3"}
	parked := []string{"p1", "p2", "p3"}
	size := map[int]int{0: 1, 1: len(defs), 2: 16 - len(defs) - len(unknown), 3: len(unknown), 4: len(existing), 5: len(parked)}

	var (
		mu          sync.Mutex
		done        = map[int]int{}
		earlyStarts []string
	)
	reg := &scriptedRegistry{first: []string{"index"}, firstTypes: []string{"type", "collection"}}
	prober := &scriptedProber{types: defs, fail: map[string]error{}}
	for id := range unknown {
		prober.fail[id] = errors.New("refused")
	}
	ts := probeSyncer(t, reg, prober)
	for _, id := range parked {
		ts.markPending(id, false)
	}
	reg.onGet = func(_ context.Context, id string) {
		g := group(id)
		mu.Lock()
		for earlier := 0; earlier < g; earlier++ {
			if done[earlier] < size[earlier] {
				earlyStarts = append(earlyStarts, id)
				break
			}
		}
		mu.Unlock()
		if g < 5 {
			time.Sleep(5 * time.Millisecond) // let a later group overtake, if it could
		}
		mu.Lock()
		done[g]++
		mu.Unlock()
	}

	require.NoError(t, ts.SyncAll(context.Background(), fakePeer{id: "peer1"}, existing, missing))
	require.Empty(t, earlyStarts)
	require.Len(t, reg.calls, len(missing)+len(existing)+len(parked))
}

// A failure in one tree of a parallel group parks that tree and does
// not stop the others; the next round retries the parked ones.
func TestTreeSyncerParallelRoundParksFailures(t *testing.T) {
	withTreeSyncWorkers(t, 4)
	reg := &scriptedRegistry{fail: map[string]error{"m02": errors.New("boom"), "m05": errors.New("boom")}}
	ts := classified(t, reg, nil)
	p := fakePeer{id: "peer1"}

	require.NoError(t, ts.SyncAll(context.Background(), p, nil, backlog(8)))
	require.ElementsMatch(t, backlog(8), reg.calls)
	require.Equal(t, 2, ts.Stats()[0].Pending)

	reg.mu.Lock()
	reg.fail = nil
	reg.calls = nil
	reg.mu.Unlock()
	require.NoError(t, ts.SyncAll(context.Background(), p, nil, nil))
	require.ElementsMatch(t, []string{"m02", "m05"}, reg.calls)
	require.Equal(t, 0, ts.Stats()[0].Pending)
}

// When the round's budget runs out with several trees in flight, the
// round returns and the trees it did not reach are parked without a
// fetch. Trees in flight are counted apart from the parked ones: the
// parked count stays what a finished round left behind.
func TestTreeSyncerRoundDeadlineParksTheRest(t *testing.T) {
	withTreeSyncWorkers(t, 4)

	ctx, cancel := context.WithCancel(context.Background())
	var started atomic.Int64
	reg := &scriptedRegistry{}
	ts := classified(t, reg, nil)
	inFlight := make(chan int, 1)
	reg.onGet = func(ctx context.Context, _ string) {
		if started.Add(1) == 4 {
			inFlight <- ts.syncingCount()
			cancel()
		}
		<-ctx.Done()
	}
	reg.fail = map[string]error{}
	for _, id := range backlog(14) {
		reg.fail[id] = context.Canceled // what a fetch cut short returns
	}

	require.NoError(t, ts.SyncAll(ctx, fakePeer{id: "peer1"}, nil, backlog(14)))
	require.Equal(t, 4, <-inFlight, "the four trees in flight are counted while they sync")
	require.Equal(t, int64(4), started.Load(), "nothing is fetched once the round is over")
	require.Equal(t, 14, ts.Stats()[0].Pending)
	require.Equal(t, 14, ts.pendingCount())
	require.Zero(t, ts.syncingCount())
}
