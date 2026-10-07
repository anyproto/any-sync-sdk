package anysyncx

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/syncacl"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/treesyncer"
	"github.com/anyproto/any-sync/commonspace/spacestate"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	commonsync "github.com/anyproto/any-sync/commonspace/sync"
	"github.com/anyproto/any-sync/net/peer"
	"go.uber.org/zap"
)

var tsLog = logger.NewNamed("anysyncx.treesyncer")

// treeSyncWorkers bounds the fetched trees one pass materializes at
// once (syncjob.go). Kept under the read connections of a space's
// storage (eight), each replay holding one, so other readers of the
// space are not starved. A var so tests can pin the order.
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
// commonspace.Deps.TreeSyncer. On SyncAll it hands the round's trees
// to the space's sync job (syncjob.go), which fetches the missing ones
// and pushes the changed ones via SyncWithPeer. Trees are not closed
// here — they stay in ocache for the async exchange to land.
//
// Materialization routes through SpaceRegistry.GetTree (NOT through the
// per-space TreeBuilder directly): the registry's per-space
// implementation is the only place that wires our SDK-side
// UpdateListener onto the tree, so that inbound changes fan out to
// the CRDT controller. A bare TreeBuilder.BuildTree call sets no
// listener, which silently breaks cold sync — the tree storage
// receives changes but our controller never sees them. See
// docs/sync-listener-wiring or the cold-sync e2e for the trail. The
// fetch stage writes storage only and leaves the build to the
// registry.
//
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
	spaceId string
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
	// syncing counts the trees a round syncs inline right now; see
	// syncingCount.
	syncing atomic.Int64

	// limits is the request budget this adapter's fetches take from,
	// per peer and shared with every other space's adapter.
	limits *peerLimits

	// job syncs the trees of the rounds that queue on it; see
	// syncjob.go.
	job *syncJob
	// fetcher fetches a missing tree into storage (treefetch.go). nil
	// until Init: rounds then sync one tree at a time, inline.
	fetcher treeFetcher

	log logger.CtxLogger
}

// Compile-time check: the adapter implements any-sync's optional
// PullFilter extension (selective sync by tree type).
var _ treesyncer.PullFilter = (*treeSyncerAdapter)(nil)

func newTreeSyncer(spaceId string, registry SpaceRegistry, onRound PeerRoundCallback) *treeSyncerAdapter {
	t := &treeSyncerAdapter{
		spaceId:  spaceId,
		registry: registry,
		stats:    map[string]PeerSyncSnapshot{},
		onRound:  onRound,
		pending:  map[string]bool{},
		limits:   newPeerLimits(),
		log:      tsLog,
	}
	t.job = newSyncJob(t)
	return t
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
	t.fetcher = &storageFetcher{
		syncClient: synctree.NewSyncClient(t.spaceId, a.MustComponent(commonsync.CName).(commonsync.SyncService)),
		storage:    a.MustComponent(spacestorage.CName).(spacestorage.SpaceStorage),
		acl:        a.MustComponent(syncacl.CName).(syncacl.SyncAcl),
	}
	return nil
}

func (t *treeSyncerAdapter) Name() string { return treesyncer.CName }

func (t *treeSyncerAdapter) Run(_ context.Context) error { return nil }

// Close ends the space's sync job and waits for its workers: nothing
// fetches or replays once the space's components close.
func (t *treeSyncerAdapter) Close(_ context.Context) error {
	t.job.close()
	return nil
}

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

// SyncAll hands a round's trees to the space's sync job and waits for
// them: it returns nil once every one is handled, or the caller's
// error once its context ends — the job goes on (syncjob.go). A space
// the registry names no pull-first types for (the tech space,
// selective sync) syncs within the round instead, one tree at a time,
// so a caller that reads the space after the round reads what the
// round discovered.
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
	if t.fetcher == nil || t.registry.PullFirstTypes(t.spaceId) == nil {
		t.syncInline(ctx, p, existing, missing)
		return nil
	}
	return t.job.wait(ctx, t.job.add(p, existing, missing, t.pendingIds()))
}

// syncInline syncs a round's trees within the round, one at a time, in
// groups: the trees the registry names first, the missing ones, the
// changed ones, the retries. Once the round's context ends, the trees
// not reached are parked for the next round.
func (t *treeSyncerAdapter) syncInline(ctx context.Context, p peer.Peer, existing, missing []string) {
	isMissing := make(map[string]struct{}, len(missing))
	for _, id := range missing {
		isMissing[id] = struct{}{}
	}
	pending := t.pendingIds()
	seen := make(map[string]struct{}, len(missing)+len(existing)+len(pending))
	handle := func(ids []string) {
		for _, id := range ids {
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			_, wasMissing := isMissing[id]
			if ctx.Err() != nil || !t.syncOne(ctx, p, id, wasMissing || t.parkedMissing(id)) {
				// Round budget exhausted: park everything unresolved for
				// the next round instead of burning through the rest
				// with guaranteed failures.
				t.markPending(id, wasMissing)
			}
		}
	}
	// Asked again after an answer was handled: handling the first (the
	// spaceIndex) can add to the second (the bundle roots it lists).
	for i := 0; i < 2; i++ {
		first := t.roundFirst(ctx, seen, missing, existing, pending)
		if len(first) == 0 {
			break
		}
		handle(first)
	}
	handle(missing)
	handle(existing)
	handle(pending)
}

// syncOne resolves one tree through the registry — which fetches it
// when mayFetch says it is missing locally — and, when it was local,
// pings the peer with it: a tree fetched from the peer is in sync with
// it already. A failure parks the id for the next round. It reports
// false when ctx ended before the tree was handled either way. Safe to
// run for several trees at once.
func (t *treeSyncerAdapter) syncOne(ctx context.Context, p peer.Peer, id string, mayFetch bool) (handled bool) {
	t.syncing.Add(1)
	defer t.syncing.Add(-1)
	peerCtx := peer.CtxWithPeerId(ctx, p.Id())
	tree, err := t.getTree(ctx, peerCtx, p.Id(), id, mayFetch)
	if errors.Is(err, ErrTreeTypeSkipped) {
		// Declined by selective sync before any tree-storage
		// write; the stub it recorded converges the diff. A park
		// would re-probe the peer every round and hold
		// ParkedTreeCount above zero for the process lifetime.
		if recovered, _ := t.clearPending(id); recovered {
			t.log.Info("parked tree skipped by selective sync",
				zap.String("spaceId", t.spaceId), zap.String("treeId", id),
				zap.String("peerId", p.Id()))
		}
		return true
	}
	if err != nil {
		if ctx.Err() != nil {
			return false
		}
		// See the pending field doc: the fetch may already have
		// landed in storage, so this id may never show up in a
		// diff again — park it or the controller replay is lost
		// for the process lifetime.
		t.markPending(id, mayFetch)
		t.logParked(p.Id(), id, err)
		return true
	}
	t.recovered(p.Id(), id)
	if mayFetch {
		t.reportFetched(p.Id(), id, tree)
	} else if st, ok := tree.(synctree.SyncTree); ok {
		_ = st.SyncWithPeer(ctx, p)
	}
	// Don't close — async exchange may still need it.
	return true
}

// logParked logs a tree parked for retry at the level its failure
// deserves.
func (t *treeSyncerAdapter) logParked(peerId, id string, err error) {
	fields := []zap.Field{zap.String("spaceId", t.spaceId), zap.String("treeId", id), zap.String("peerId", peerId)}
	switch {
	case errors.Is(err, list.ErrNoReadKey):
		// Expected long-lived state, not a failure: the tree's
		// changes are stored but this account holds no read key
		// yet (access pending or revoked). Park quietly — the
		// retry sweep runs every round, and recovery logs
		// "parked tree recovered" once the key arrives.
		t.log.Debug("tree parked: no read key", fields...)
	case peerBusy(err):
		// The peer is still turning requests away after the
		// retries; nothing is wrong with the tree.
		t.log.Debug("tree parked: peer busy", append(fields, zap.Error(err))...)
	default:
		t.log.Warn("tree sync failed; parked for retry", append(fields, zap.Error(err))...)
	}
}

// recovered clears a parked id and logs the recovery.
func (t *treeSyncerAdapter) recovered(peerId, id string) {
	if recovered, _ := t.clearPending(id); recovered {
		t.log.Info("parked tree recovered",
			zap.String("spaceId", t.spaceId), zap.String("treeId", id),
			zap.String("peerId", peerId))
	}
}

// reportFetched reports a tree fetched from peerId with its heads.
func (t *treeSyncerAdapter) reportFetched(peerId, id string, tree objecttree.ObjectTree) {
	if t.onFetched == nil || tree == nil {
		return
	}
	tree.Lock()
	heads := slices.Clone(tree.Heads())
	tree.Unlock()
	t.onFetched(peerId, id, heads)
}

// logPass logs what a pass of the sync job did: Debug for a few trees,
// Info for a backlog.
func (t *treeSyncerAdapter) logPass(msg string, n int, start time.Time, fields ...zap.Field) {
	fields = append(fields, zap.String("spaceId", t.spaceId), zap.Int("trees", n), zap.Duration("dur", time.Since(start)))
	if n >= 50 {
		t.log.Info(msg, fields...)
	} else {
		t.log.Debug(msg, fields...)
	}
}

// eachOf runs fn over items, in order, on up to workers goroutines, and
// returns when every call has returned.
func eachOf[T any](items []T, workers int, fn func(T)) {
	workers = min(workers, len(items))
	if workers <= 1 {
		for _, it := range items {
			fn(it)
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
				if i >= len(items) {
					return
				}
				fn(items[i])
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

// askPeer runs ask under a slot of peerId's request budget, shared with
// the rounds of every other space. A peer that turns the request away
// without looking at the tree — over its request cap, or already
// serving this tree to us — is asked again after a short wait, the slot
// returned meanwhile.
func (t *treeSyncerAdapter) askPeer(ctx context.Context, peerId string, ask func() error) error {
	limit := t.limits.of(peerId)
	backoff := getTreeBackoff
	for attempt := 0; ; attempt++ {
		slot, err := limit.acquire(ctx)
		if err != nil {
			return err
		}
		err = ask()
		limit.release(slot, peerTooMany(err))
		if !peerBusy(err) || attempt >= getTreeRetries {
			return err
		}
		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return err
		}
		// Both may have been ready: a finished round asks nothing more.
		if ctx.Err() != nil {
			return err
		}
	}
}

// getTree resolves a tree through the registry, which fetches it when
// it is missing locally. A tree that may be fetched holds a slot of the
// peer's request budget until it is fetched and replayed (the registry
// does both in one step); a tree known to be local takes none.
func (t *treeSyncerAdapter) getTree(ctx, peerCtx context.Context, peerId, id string, mayFetch bool) (tree objecttree.ObjectTree, err error) {
	if !mayFetch {
		return t.registry.GetTree(peerCtx, t.spaceId, id)
	}
	err = t.askPeer(ctx, peerId, func() error {
		tree, err = t.registry.GetTree(peerCtx, t.spaceId, id)
		return err
	})
	return tree, err
}

// fetchTree fetches a missing tree into storage under the peer's
// request budget and reports its root changeType.
func (t *treeSyncerAdapter) fetchTree(ctx context.Context, peerId, id string) (changeType string, err error) {
	err = t.askPeer(ctx, peerId, func() error {
		fctx, cancel := context.WithTimeout(ctx, fetchTimeout)
		defer cancel()
		changeType, err = t.fetcher.fetch(fctx, peerId, id)
		return err
	})
	return changeType, err
}

// roundFirst picks, from the ids a round works on and has not handled
// yet, the ones the registry wants handled before the rest; the round's
// dedup then skips them at their own position. The diff orders ids by
// hash, so on a large space a tree needed early (the spaceIndex, which
// carries the space name) would otherwise wait behind an arbitrary
// share of the whole space — whether it is missing, changed or parked.
// An id outside the round's lists is left out: the order changes, the
// work does not.
func (t *treeSyncerAdapter) roundFirst(ctx context.Context, seen map[string]struct{}, lists ...[]string) []string {
	total := 0
	for _, ids := range lists {
		total += len(ids)
	}
	if total < 2 {
		return nil
	}
	first := t.registry.PullFirst(ctx, t.spaceId)
	if len(first) == 0 {
		return nil
	}
	// A device that holds the space names every definition it has; the
	// lists it is matched against can be the whole space.
	inRound := make(map[string]struct{}, total)
	for _, ids := range lists {
		for _, id := range ids {
			inRound[id] = struct{}{}
		}
	}
	var out []string
	for _, id := range first {
		if _, done := seen[id]; done {
			continue
		}
		if _, ok := inRound[id]; ok {
			out = append(out, id)
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

// syncingCount reports the trees queued on the sync job or syncing
// inline right now. Exposed via App.SyncingTreeCount for the close-time
// watermark gate: a fetched tree is in storage before its replay ends,
// so until then it is as unmaterialized as a parked one.
func (t *treeSyncerAdapter) syncingCount() int {
	return t.job.count() + int(t.syncing.Load())
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
