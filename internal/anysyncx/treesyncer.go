package anysyncx

import (
	"context"
	"sync"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree"
	"github.com/anyproto/any-sync/commonspace/object/treesyncer"
	"github.com/anyproto/any-sync/commonspace/objecttreebuilder"
	"github.com/anyproto/any-sync/commonspace/spacestate"
	"github.com/anyproto/any-sync/net/peer"
)

// PeerSyncSnapshot is the latest per-peer headsync result the
// adapter has observed. Returned by Stats() — debug surface only.
type PeerSyncSnapshot struct {
	PeerId     string
	LastSyncAt time.Time
	New        int
	Changed    int
	LastErr    string
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
}

func newTreeSyncer(spaceId string, registry SpaceRegistry, onRound PeerRoundCallback) *treeSyncerAdapter {
	return &treeSyncerAdapter{
		spaceId:  spaceId,
		registry: registry,
		stats:    map[string]PeerSyncSnapshot{},
		onRound:  onRound,
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
	defer t.record(p.Id(), len(missing), len(existing), err)
	if t.registry == nil {
		return ErrSpaceRegistryUnset
	}
	peerCtx := peer.CtxWithPeerId(ctx, p.Id())
	for _, ids := range [][]string{missing, existing} {
		for _, id := range ids {
			tree, regErr := t.registry.GetTree(peerCtx, t.spaceId, id)
			if regErr != nil {
				continue
			}
			if st, ok := tree.(synctree.SyncTree); ok {
				_ = st.SyncWithPeer(ctx, p)
			}
			// Don't close — async exchange may still need it.
		}
	}
	return nil
}

// record stores the latest per-peer snapshot and fires the
// onRound callback. Overwrites any prior row for peerId — only
// the most recent round is retained.
func (t *treeSyncerAdapter) record(peerId string, newCount, changedCount int, err error) {
	if peerId == "" {
		return
	}
	snap := PeerSyncSnapshot{
		PeerId:     peerId,
		LastSyncAt: time.Now(),
		New:        newCount,
		Changed:    changedCount,
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
