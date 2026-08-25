package anysyncx

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/treesyncer"
	"github.com/anyproto/any-sync/commonspace/objecttreebuilder"
	"github.com/anyproto/any-sync/commonspace/spacestate"
	"github.com/anyproto/any-sync/net/peer"
	"go.uber.org/zap"
)

var tsLog = logger.NewNamed("anysyncx.treesyncer")

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
// trees and pushes existing ones via SyncWithPeer. Trees are not closed
// here — they stay in ocache for the async exchange to land.
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
	// GetTree; entries are never dropped on failure — giving up would
	// re-create the silent divergence, and the per-round Warn keeps a
	// stuck id visible.
	pendingMu sync.Mutex
	// pending maps a parked id to whether it was missing locally when
	// parked, so a recovered fetch still reports through onFetched.
	pending map[string]bool
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
	seen := make(map[string]struct{}, len(missing)+len(existing))
	for _, ids := range [][]string{missing, existing, t.pendingIds()} {
		for _, id := range ids {
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			_, wasMissing := fetched[id]
			if ctx.Err() != nil {
				// Round budget exhausted: park everything unresolved for
				// the next round instead of burning through the rest
				// with guaranteed failures.
				t.markPending(id, wasMissing)
				continue
			}
			tree, regErr := t.registry.GetTree(peerCtx, t.spaceId, id)
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
					tsLog.Debug("tree parked: no read key",
						zap.String("spaceId", t.spaceId), zap.String("treeId", id),
						zap.String("peerId", p.Id()))
				} else {
					tsLog.Warn("tree sync failed; parked for retry",
						zap.String("spaceId", t.spaceId), zap.String("treeId", id),
						zap.String("peerId", p.Id()), zap.Error(regErr))
				}
				continue
			}
			recovered, parkedMissing := t.clearPending(id)
			if recovered {
				tsLog.Info("parked tree recovered",
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
	}
	return nil
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
// App.ParkedTreeCount for the SDK's close-time watermark gate: a
// nonzero count means storage holds trees the projection never
// materialized, so the space watermark must NOT be snapshotted (the
// boot replay is the only cross-restart recovery for them).
func (t *treeSyncerAdapter) pendingCount() int {
	t.pendingMu.Lock()
	defer t.pendingMu.Unlock()
	return len(t.pending)
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
