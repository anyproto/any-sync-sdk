package anysyncx

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/treesyncer"
	"github.com/anyproto/any-sync/commonspace/objecttreebuilder"
	"github.com/anyproto/any-sync/commonspace/spacestate"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/net/peer"
	"go.uber.org/zap"
)

var tsLog = logger.NewNamed("anysyncx.treesyncer")

// treeSyncWorkers bounds the trees one round syncs at once, where it
// syncs several (SyncAll says which). A missing tree costs a round trip
// to the peer, and a round that waits for each one in turn is paced by
// latency alone, whatever the device and the link could carry. Kept
// under the read connections of a space's
// storage (eight), each replay holding one, so other readers of the
// space are not starved; the requests themselves are bounded per peer,
// across spaces, by peerLimits. A var so tests can pin the order.
var treeSyncWorkers = 6

// ErrTreeTypeSkipped is a tree fetch declined by selective sync before
// any tree-storage write. The registry records a heads-only stub in its
// place (or finds the entry already deleted), so the diff converges
// with nothing to materialize and the syncer never parks it.
var ErrTreeTypeSkipped = errors.New("anysyncx: tree type not selected for sync")

// PeerSyncSnapshot is the latest per-peer headsync result the
// adapter has observed. Returned by Stats() — debug surface only.
type PeerSyncSnapshot struct {
	PeerId     string
	LastSyncAt time.Time
	New        int
	Changed    int
	// Pending is the space-wide parked-tree count at the time of this
	// round (see treeSyncerAdapter.pending) — nonzero means some trees'
	// GetTree failed and is awaiting retry. Same value on every peer's
	// row of the same round; kept per-row so the debug surface needs no
	// second query.
	Pending int
	LastErr string
}

// treeSyncerAdapter is registered by anysyncx as the per-space
// commonspace.Deps.TreeSyncer. On SyncAll it builds (fetches) missing
// trees and pushes existing ones via SyncWithPeer; the bulk of a
// backlog several trees at a time. Trees are not closed here — they
// stay in ocache for the async exchange to land.
//
// SyncAll routes through SpaceRegistry.GetTree (NOT through the
// per-space TreeBuilder directly): the registry's per-space
// implementation is the only place that wires our SDK-side
// UpdateListener onto the tree, so that inbound changes fan out to
// the CRDT controller. A bare TreeBuilder.BuildTree call sets no
// listener, which silently breaks cold sync — the tree storage
// receives changes but our controller never sees them. See
// docs/sync-listener-wiring or the cold-sync e2e for the trail.
// PeerRoundCallback fires after every SyncAll completes. Wired by
// the App to route 0/0 success rounds into the syncstatus tracker
// for the space-level "all synced" sweep. peerId is the responsible
// peer (caller filters); newCount / changedCount are the post-
// deletionState-filter sizes (what SyncAll actually acted on); err
// is the SyncAll error, nil on success.
type PeerRoundCallback func(peerId string, newCount, changedCount int, err error)

// TreeFetchedCallback fires after a tree missing locally was fetched
// from a peer during SyncAll, with the heads it now holds: the tree is
// in sync with that peer by construction.
type TreeFetchedCallback func(peerId, treeId string, heads []string)

type treeSyncerAdapter struct {
	spaceId     string
	treeBuilder objecttreebuilder.TreeBuilder
	// registry is shared across all per-space treeSyncerAdapters; it
	// dispatches between tech-space and regular spaces and binds the
	// right listener for each.
	registry SpaceRegistry

	// statsMu guards stats. The map keys by peer.Id() and holds the
	// latest snapshot per peer (one row per peer ever seen since
	// boot). In-memory only; cleared on SDK restart.
	statsMu sync.Mutex
	stats   map[string]PeerSyncSnapshot

	// onRound is the optional space-level "round done" callback —
	// fires on every SyncAll. nil-safe.
	onRound PeerRoundCallback
	// onFetched reports a tree fetched whole from a peer. nil-safe.
	onFetched TreeFetchedCallback

	// pendingMu guards pending: tree ids whose GetTree failed during a
	// SyncAll round. The any-sync fetch may write a tree's changes to
	// storage BEFORE our GetTree materialization fails (round deadline,
	// transient build error) — after which every later headsync diff
	// sees converged heads and never offers the id again, leaving the
	// CRDT projection permanently behind storage with no error anywhere.
	// Parking the id here and retrying on subsequent SyncAll calls
	// (diffsyncer invokes SyncAll every headsync period even with an
	// empty diff) closes that gap. Ids clear on the first successful
	// GetTree, or once a fetch is classified as declined by selective
	// sync (nothing was persisted); any other failure keeps its entry —
	// giving up would re-create the silent divergence, and the
	// per-round Warn keeps a stuck id visible.
	pendingMu sync.Mutex
	// pending maps a parked id to whether it was missing locally when
	// parked, so a recovered fetch still reports through onFetched.
	pending map[string]bool
	// syncing counts the trees a round is syncing right now; see
	// syncingCount.
	syncing atomic.Int64

	// limits is the request budget this adapter's probes take from,
	// per peer and shared with every other space's adapter.
	limits *peerLimits

	// prober fetches a missing tree's root without its changes; the
	// space's tree builder. See treeprobe.go.
	prober rootProber
	// hasTree reports whether a tree is in the space's local storage.
	// Read from the space's own head storage: a probe must not reach
	// the registry's store, which a round racing the space's teardown
	// would rebuild.
	hasTree func(ctx context.Context, treeId string) (bool, error)
	// probeMu guards the probe state below.
	probeMu sync.Mutex
	// probed holds what is known of every probed root whose tree a
	// recent round's diff still offered; probeRound counts the rounds.
	probed     map[string]probeAnswer
	probeRound uint64
	// probing holds the trees a probe is in flight for: overlapping
	// rounds see much the same diff, and a peer refuses a second
	// request for a tree it is already serving.
	probing map[string]struct{}
	// noProbe holds the peers that answer a probe with change bodies:
	// they ignore the flag, and probing them would download every tree
	// twice.
	noProbe map[string]struct{}

	log logger.CtxLogger
}

// Compile-time check: the adapter implements any-sync's optional
// PullFilter extension (selective sync by tree type).
var _ treesyncer.PullFilter = (*treeSyncerAdapter)(nil)

func newTreeSyncer(spaceId string, registry SpaceRegistry, onRound PeerRoundCallback) *treeSyncerAdapter {
	return &treeSyncerAdapter{
		spaceId:  spaceId,
		registry: registry,
		stats:    map[string]PeerSyncSnapshot{},
		onRound:  onRound,
		pending:  map[string]bool{},
		limits:   newPeerLimits(),
		probed:   map[string]probeAnswer{},
		probing:  map[string]struct{}{},
		noProbe:  map[string]struct{}{},
		log:      tsLog,
	}
}

func (t *treeSyncerAdapter) Init(a *app.App) error {
	// spaceId is pre-bound at construction (App.newTreeSyncerForSpace).
	// Cross-check against spacestate in case any-sync constructs the
	// per-space app for a different id — a wiring bug we want to fail
	// loudly on rather than silently mis-record stats.
	got := a.MustComponent(spacestate.CName).(*spacestate.SpaceState).SpaceId
	if t.spaceId == "" {
		t.spaceId = got
	}
	t.treeBuilder = a.MustComponent(objecttreebuilder.CName).(objecttreebuilder.TreeBuilder)
	t.prober = t.treeBuilder
	storage := a.MustComponent(spacestorage.CName).(spacestorage.SpaceStorage)
	t.hasTree = func(ctx context.Context, treeId string) (bool, error) {
		_, err := storage.HeadStorage().GetEntry(ctx, treeId)
		if errors.Is(err, anystore.ErrDocNotFound) {
			return false, nil
		}
		return err == nil, err
	}
	return nil
}

func (t *treeSyncerAdapter) Name() string { return treesyncer.CName }

func (t *treeSyncerAdapter) Run(_ context.Context) error   { return nil }
func (t *treeSyncerAdapter) Close(_ context.Context) error { return nil }

func (t *treeSyncerAdapter) StartSync()               {}
func (t *treeSyncerAdapter) StopSync()                {}
func (t *treeSyncerAdapter) ShouldSync(_ string) bool { return true }

// ShouldPull implements any-sync's optional treesyncer.PullFilter: it is
// consulted by objectsync when a head update arrives for a tree that
// does not exist locally, and delegates the decision to the registry
// (selective sync by tree type). A nil registry — boot-order edge —
// pulls as usual; the fetch path re-checks via its tree validator.
func (t *treeSyncerAdapter) ShouldPull(ctx context.Context, objectId string, root *treechangeproto.RawTreeChangeWithId, heads []string) bool {
	if t.registry == nil {
		return true
	}
	return t.registry.ShouldPullTree(ctx, t.spaceId, objectId, root, heads)
}

// SyncAll resolves each tree id through the SpaceRegistry so its
// listener is bound, then asks the resulting SyncTree to ping the
// peer. For missing-locally trees the registry path also performs the
// remote fetch (BuildSyncTreeOrGetRemote) — same end state as the
// previous direct-treeBuilder call, with the listener now wired so
// the CRDT controller can replay the inbound changes.
//
// We deliberately do NOT fall back to TreeBuilder.BuildTree on
// registry errors: a missing registry would mean we have a bug in
// SDK boot ordering, and silently bypassing it would re-create the
// listener-loss bug this method exists to fix.
func (t *treeSyncerAdapter) SyncAll(ctx context.Context, p peer.Peer, existing, missing []string) (err error) {
	defer func() { t.record(p.Id(), len(missing), len(existing), err) }()
	if t.registry == nil {
		return ErrSpaceRegistryUnset
	}
	peerCtx := peer.CtxWithPeerId(ctx, p.Id())
	fetched := make(map[string]struct{}, len(missing))
	for _, id := range missing {
		fetched[id] = struct{}{}
	}
	pending := t.pendingIds()
	seen := make(map[string]struct{}, len(missing)+len(existing))
	// handle syncs the ids the round has not handled yet and returns
	// when all of them are done: a later group never starts ahead of an
	// earlier one. atOnce lets the group sync several trees at a time.
	handle := func(ids []string, atOnce bool) {
		todo := make([]string, 0, len(ids))
		for _, id := range ids {
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			todo = append(todo, id)
		}
		workers := 1
		if atOnce {
			workers = treeSyncWorkers
		}
		eachTree(todo, workers, func(id string) {
			_, wasMissing := fetched[id]
			t.syncTree(ctx, peerCtx, p, id, wasMissing)
		})
	}
	// Ahead of the diff order: the trees the registry names — asked
	// again after an answer was handled, because handling the first
	// (the spaceIndex) can add to the second (the bundle roots it
	// lists) — then the missing trees whose probed root is of a
	// pull-first changeType (types and collections). The dedup skips
	// all of them at their own position.
	for i := 0; i < 2; i++ {
		first := t.roundFirst(ctx, seen, missing, existing, pending)
		if len(first) == 0 {
			break
		}
		handle(first, false)
	}
	handle(t.probeFirst(ctx, p, missing, seen), false)
	// Only the missing trees whose root says they define nothing sync
	// several at a time, and only now that the round's definitions are
	// in. A definition must not be replayed next to a tree that may
	// look it up: any-store shows a collection to readers before the
	// transaction creating it commits, and a reader that gets there
	// first fails with a read error. Readers of collections that
	// already exist are unaffected, which is what makes the rest safe.
	// Everything a probe did not classify — a small backlog, a peer
	// that ignores probes, the changed trees, the retries — syncs one
	// tree at a time.
	handle(t.probedOther(missing, seen), true)
	handle(missing, false)
	handle(existing, false)
	handle(pending, false)
	return nil
}

// syncTree resolves one tree of a round through the registry — which
// fetches it when it is missing locally — and pings the peer with it.
// A failure parks the id for the next round. Safe to run for several
// trees at once.
func (t *treeSyncerAdapter) syncTree(ctx, peerCtx context.Context, p peer.Peer, id string, wasMissing bool) {
	t.syncing.Add(1)
	defer t.syncing.Add(-1)
	if ctx.Err() != nil {
		// Round budget exhausted: park everything unresolved for
		// the next round instead of burning through the rest
		// with guaranteed failures.
		t.markPending(id, wasMissing)
		return
	}
	tree, regErr := t.getTree(ctx, peerCtx, p.Id(), id, wasMissing || t.parkedMissing(id))
	if errors.Is(regErr, ErrTreeTypeSkipped) {
		// Declined by selective sync before any tree-storage
		// write; the stub it recorded converges the diff. A park
		// would re-probe the peer every round and hold
		// ParkedTreeCount above zero for the process lifetime.
		if recovered, _ := t.clearPending(id); recovered {
			t.log.Info("parked tree skipped by selective sync",
				zap.String("spaceId", t.spaceId), zap.String("treeId", id),
				zap.String("peerId", p.Id()))
		}
		return
	}
	if regErr != nil {
		// See the pending field doc: the fetch may already have
		// landed in storage, so this id may never show up in a
		// diff again — park it or the controller replay is lost
		// for the process lifetime.
		t.markPending(id, wasMissing)
		if errors.Is(regErr, list.ErrNoReadKey) {
			// Expected long-lived state, not a failure: the tree's
			// changes are stored but this account holds no read key
			// yet (access pending or revoked). Park quietly — the
			// retry sweep runs every round, and recovery logs
			// "parked tree recovered" once the key arrives.
			t.log.Debug("tree parked: no read key",
				zap.String("spaceId", t.spaceId), zap.String("treeId", id),
				zap.String("peerId", p.Id()))
		} else if peerBusy(regErr) {
			// The peer is still turning requests away after the
			// retries; nothing is wrong with the tree.
			t.log.Debug("tree parked: peer busy",
				zap.String("spaceId", t.spaceId), zap.String("treeId", id),
				zap.String("peerId", p.Id()), zap.Error(regErr))
		} else {
			t.log.Warn("tree sync failed; parked for retry",
				zap.String("spaceId", t.spaceId), zap.String("treeId", id),
				zap.String("peerId", p.Id()), zap.Error(regErr))
		}
		return
	}
	recovered, parkedMissing := t.clearPending(id)
	if recovered {
		t.log.Info("parked tree recovered",
			zap.String("spaceId", t.spaceId), zap.String("treeId", id),
			zap.String("peerId", p.Id()))
	}
	if (wasMissing || parkedMissing) && t.onFetched != nil && tree != nil {
		tree.Lock()
		heads := slices.Clone(tree.Heads())
		tree.Unlock()
		t.onFetched(p.Id(), id, heads)
	}
	if st, ok := tree.(synctree.SyncTree); ok {
		_ = st.SyncWithPeer(ctx, p)
	}
	// Don't close — async exchange may still need it.
}

// eachTree runs fn over ids, in order, on up to workers goroutines,
// and returns when every call has returned.
func eachTree(ids []string, workers int, fn func(id string)) {
	workers = min(workers, len(ids))
	if workers <= 1 {
		for _, id := range ids {
			fn(id)
		}
		return
	}
	var (
		wg   sync.WaitGroup
		next atomic.Int64
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= len(ids) {
					return
				}
				fn(ids[i])
			}
		}()
	}
	wg.Wait()
}

// getTreeRetries is how often a round asks again for a tree the peer
// turned away as busy, and getTreeBackoff the first wait, doubled per
// attempt. Vars so tests don't wait on them.
var (
	getTreeRetries = 4
	getTreeBackoff = 50 * time.Millisecond
)

// getTree resolves a tree through the registry, which fetches it when
// it is missing locally. A fetch holds a slot of the peer's request
// budget, shared with the rounds of every other space; a tree that is
// local needs none. A peer that turns the request away without looking
// at the tree — over its request cap, or already serving this tree to
// us, as a probe of an overlapping round makes it — is asked again
// after a short wait, the slot returned meanwhile.
func (t *treeSyncerAdapter) getTree(ctx, peerCtx context.Context, peerId, id string, mayFetch bool) (objecttree.ObjectTree, error) {
	if !mayFetch {
		return t.registry.GetTree(peerCtx, t.spaceId, id)
	}
	limit := t.limits.of(peerId)
	backoff := getTreeBackoff
	for attempt := 0; ; attempt++ {
		if err := limit.acquire(ctx); err != nil {
			return nil, err
		}
		tree, err := t.registry.GetTree(peerCtx, t.spaceId, id)
		limit.release(peerTooMany(err))
		if !peerBusy(err) || attempt >= getTreeRetries {
			return tree, err
		}
		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return nil, err
		}
	}
}

// roundFirst picks, from the ids a round works on and has not handled
// yet, the ones the registry wants handled before the rest; SyncAll's
// dedup then skips them at their own position. The diff orders ids by
// hash and a round works through them a few at a time, so on a large
// space a tree needed early (the spaceIndex, which carries the space
// name) would otherwise wait behind an arbitrary share of the whole
// space —
// whether it is missing, changed or parked. An id outside the round's
// lists is left out: the order changes, the work does not.
func (t *treeSyncerAdapter) roundFirst(ctx context.Context, seen map[string]struct{}, lists ...[]string) []string {
	total := 0
	for _, ids := range lists {
		total += len(ids)
	}
	if total < 2 {
		return nil
	}
	var out []string
	for _, id := range t.registry.PullFirst(ctx, t.spaceId) {
		if _, done := seen[id]; done {
			continue
		}
		for _, ids := range lists {
			if slices.Contains(ids, id) {
				out = append(out, id)
				break
			}
		}
	}
	return out
}

// pendingIds snapshots the parked-tree set for a retry sweep.
func (t *treeSyncerAdapter) pendingIds() []string {
	t.pendingMu.Lock()
	defer t.pendingMu.Unlock()
	if len(t.pending) == 0 {
		return nil
	}
	out := make([]string, 0, len(t.pending))
	for id := range t.pending {
		out = append(out, id)
	}
	return out
}

// pendingCount reports the parked-set size. Exposed via
// App.ParkedTreeCount for the gates that need every tree a finished
// round discovered to be applied: a nonzero count means storage holds
// trees the projection never materialized.
func (t *treeSyncerAdapter) pendingCount() int {
	t.pendingMu.Lock()
	defer t.pendingMu.Unlock()
	return len(t.pending)
}

// syncingCount reports the trees a round is syncing right now. Exposed
// via App.SyncingTreeCount for the close-time watermark gate: a fetched
// tree is in storage before its replay ends, so until then it is as
// unmaterialized as a parked one.
func (t *treeSyncerAdapter) syncingCount() int {
	return int(t.syncing.Load())
}

// parkedMissing reports whether id is parked as missing locally: its
// retry may fetch.
func (t *treeSyncerAdapter) parkedMissing(id string) bool {
	t.pendingMu.Lock()
	defer t.pendingMu.Unlock()
	return t.pending[id]
}

// markPending parks id; a parked id already marked missing stays so.
func (t *treeSyncerAdapter) markPending(id string, missing bool) {
	t.pendingMu.Lock()
	t.pending[id] = t.pending[id] || missing
	t.pendingMu.Unlock()
}

// clearPending removes id from the parked set, reporting whether it
// was there (a recovery, worth logging) and whether it was missing
// locally when parked.
func (t *treeSyncerAdapter) clearPending(id string) (recovered, missing bool) {
	t.pendingMu.Lock()
	defer t.pendingMu.Unlock()
	missing, recovered = t.pending[id]
	if recovered {
		delete(t.pending, id)
	}
	return recovered, missing
}

// record stores the latest per-peer snapshot and fires the
// onRound callback. Overwrites any prior row for peerId — only
// the most recent round is retained.
func (t *treeSyncerAdapter) record(peerId string, newCount, changedCount int, err error) {
	if peerId == "" {
		return
	}
	t.pendingMu.Lock()
	pendingCount := len(t.pending)
	t.pendingMu.Unlock()
	snap := PeerSyncSnapshot{
		PeerId:     peerId,
		LastSyncAt: time.Now(),
		New:        newCount,
		Changed:    changedCount,
		Pending:    pendingCount,
	}
	if err != nil {
		snap.LastErr = err.Error()
	}
	t.statsMu.Lock()
	t.stats[peerId] = snap
	t.statsMu.Unlock()
	if t.onRound != nil {
		t.onRound(peerId, newCount, changedCount, err)
	}
}

// Stats returns a copy of the per-peer snapshot map. Cheap; map
// is bounded by the responsible-peer set (1–3 in practice).
func (t *treeSyncerAdapter) Stats() []PeerSyncSnapshot {
	t.statsMu.Lock()
	defer t.statsMu.Unlock()
	if len(t.stats) == 0 {
		return nil
	}
	out := make([]PeerSyncSnapshot, 0, len(t.stats))
	for _, s := range t.stats {
		out = append(out, s)
	}
	return out
}
