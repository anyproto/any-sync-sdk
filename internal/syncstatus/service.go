package syncstatus

import (
	"context"
	"sync"
	"time"

	"github.com/anyproto/any-sync-sdk/internal/fanout"
	"github.com/anyproto/any-sync-sdk/space"
)

// DefaultTickInterval is the cadence the rollup loop runs at when
// Service.Run is called without a custom interval. Matches heart's
// spacesyncstatus loop and keeps event volume low while still feeling
// live in a UI.
const DefaultTickInterval = time.Second

// NodeIdsFn resolves the responsible-node id list for spaceId. Wired
// from anysyncx — the Service doesn't import nodeconf directly so
// tests can stub it.
type NodeIdsFn func(spaceId string) []string

// TotalFn returns the count of regular objects known locally in
// spaceId. Wired from spaceobjects.Store via the space layer.
// nil means "not wired yet" — the rollup reports Total=0 in that
// case (matches a freshly-created space and is harmless for v1).
type TotalFn func(spaceId string) int

// PeerCountsFn reports live-connection counts for spaceId: responsible
// sync nodes, local-network peers and global peers sharing the space.
// Wired from anysyncx (non-dialing pool.Pick reads). nil ⇒ all report 0.
type PeerCountsFn func(spaceId string) (networkPeers, localPeers, globalPeers int)

// P2PStateFn resolves the per-space local-network state. Wired from
// anysyncx over discovery possibility + the p2p peer store. nil ⇒
// P2PStateUnknown.
type P2PStateFn func(spaceId string) space.P2PState

// Service is the per-account sync-status registry. Owns one Tracker
// per space (lazy via For) plus the account-wide subscriber registry
// fed by Service.SubscribeStatus.
//
// Construct once in anysyncx.New; expose via App.SyncStatus(). The
// Service is safe for concurrent use.
type Service struct {
	mu       sync.Mutex
	closed   bool
	trackers map[string]*Tracker

	// nodeIds resolves the responsible-node list per space. Read by
	// each Tracker via Service.isResponsibleSender. May be nil in
	// tests; then every sender is treated as responsible.
	nodeIds NodeIdsFn

	// localPeerIds resolves the connected direct peers (LAN and global)
	// per space.
	// Local peers are responsible senders too (a space synced purely
	// over the LAN must still drain pending heads and advance to
	// Synced). nil ⇒ no local peers considered.
	localPeerIds NodeIdsFn

	// totalFn supplies the per-space regular-object count for the
	// rollup. May be nil in tests; then Total reports 0.
	totalFn TotalFn

	// peerCountsFn / p2pStateFn supply the peer-presence slice of the
	// rollup. May be nil in tests — counts then read 0 / Unknown.
	peerCountsFn PeerCountsFn
	p2pStateFn   P2PStateFn

	// spaceSubs is the account-wide subscriber registry for
	// SpaceSyncStatus events. Fired by the rollup loop.
	spaceSubs *fanout.Registry[space.SpaceSyncStatus]

	// exclude is the set of tree ids the trackers should ignore.
	// Names are space-relative: the spaceImpl injects its
	// non-user-visible tree ids (ACL, spaceIndex, settings, members
	// system) per space via excludedTreesFn. nil → never exclude.
	excludedTreesFn func(spaceId string) []string

	// dirty is the set of spaceIds whose rollup has changed since the
	// last tick. refresh() inserts; tick() drains.
	dirtyMu   sync.Mutex
	dirty     map[string]struct{}
	lastEvent map[string]space.SpaceSyncStatus

	// runCancel stops the rollup loop. nil when not running (e.g.
	// tests that call Tick directly).
	runMu     sync.Mutex
	runCancel context.CancelFunc
}

// NewService constructs an empty Service. Wire NodeIdsFn / TotalFn /
// excluded-trees via the setter methods before tracker construction;
// they're snapshotted into each tracker at construction time.
//
// The rollup loop is not started here — call Run(ctx) to start
// dispatching account-wide events on the configured ticker. Tests
// can skip Run and call Tick() directly for determinism.
func NewService() *Service {
	return &Service{
		trackers:  map[string]*Tracker{},
		spaceSubs: fanout.New[space.SpaceSyncStatus](),
		dirty:     map[string]struct{}{},
		lastEvent: map[string]space.SpaceSyncStatus{},
	}
}

// Run starts the rollup loop on a 1s ticker. The loop drains the
// dirty set, recomputes each space's rollup, and dispatches
// SpaceSyncStatus events to account-wide subscribers when the
// rollup transitioned from the last-emitted snapshot.
//
// ctx controls the loop lifetime; cancelling it stops the loop.
// Close() also stops the loop via its own internal cancel. Calling
// Run more than once is a no-op — the existing loop keeps running.
func (s *Service) Run(ctx context.Context) {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	if s.runCancel != nil {
		return
	}
	loopCtx, cancel := context.WithCancel(ctx)
	s.runCancel = cancel
	go s.runLoop(loopCtx)
}

// Tick drains the dirty set once and dispatches changed rollups.
// Exposed for tests; the production path uses the goroutine started
// by Run. Safe to call from any goroutine.
func (s *Service) Tick() {
	s.dirtyMu.Lock()
	if len(s.dirty) == 0 {
		s.dirtyMu.Unlock()
		return
	}
	dirty := s.dirty
	s.dirty = map[string]struct{}{}
	s.dirtyMu.Unlock()

	hasSubs := s.spaceSubs.HasSubscribers()
	for spaceId := range dirty {
		cur := s.Status(spaceId)
		s.dirtyMu.Lock()
		prev, had := s.lastEvent[spaceId]
		s.lastEvent[spaceId] = cur
		s.dirtyMu.Unlock()
		if hasSubs && (!had || !rollupsEqual(prev, cur)) {
			s.spaceSubs.Dispatch(cur)
		}
	}
}

// runLoop is the ticker-driven dispatcher. Exits when ctx is done.
func (s *Service) runLoop(ctx context.Context) {
	t := time.NewTicker(DefaultTickInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.Tick()
		}
	}
}

// SetNodeIdsFn wires the responsible-node resolver. Pass the
// nodeconf.Service.NodeIds method bound to the running app.
func (s *Service) SetNodeIdsFn(fn NodeIdsFn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodeIds = fn
}

// SetLocalPeerIdsFn wires the connected direct-peer (LAN and global)
// resolver so those peers count as responsible senders. Pass the p2p peer store's
// LocalPeerIds method.
func (s *Service) SetLocalPeerIdsFn(fn NodeIdsFn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.localPeerIds = fn
}

// SetTotalFn wires the per-space regular-object count source. Pass
// a closure over spaceobjects.Store from the space layer.
func (s *Service) SetTotalFn(fn TotalFn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.totalFn = fn
}

// SetPeerCountsFn wires the live-connection counters (nodes + local
// peers). Pass a closure over pool.Pick from anysyncx.
func (s *Service) SetPeerCountsFn(fn PeerCountsFn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peerCountsFn = fn
}

// SetP2PStateFn wires the per-space local-network state resolver.
func (s *Service) SetP2PStateFn(fn P2PStateFn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.p2pStateFn = fn
}

// SetExcludedTreesFn wires the per-space non-user-visible tree
// id list (ACL / spaceIndex / settings / members system). nil ⇒ the
// tracker tracks every tree.
func (s *Service) SetExcludedTreesFn(fn func(spaceId string) []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.excludedTreesFn = fn
}

// For returns the Tracker for spaceId, constructing one on first
// access. The same Tracker is returned on every call until Close.
//
// The space cache wires the returned Tracker into
// commonspace.Deps.SyncStatus when the space loads.
func (s *Service) For(spaceId string) *Tracker {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return newTracker(spaceId, nil, nil) // detached; safe but never observed
	}
	t, ok := s.trackers[spaceId]
	if ok {
		s.mu.Unlock()
		return t
	}
	var excl []string
	if s.excludedTreesFn != nil {
		excl = s.excludedTreesFn(spaceId)
	}
	t = newTracker(spaceId, s, excl)
	s.trackers[spaceId] = t
	s.mu.Unlock()
	return t
}

// Status returns a snapshot of spaceId's rollup, composed inline on
// every call from the tracker state, peer counts, P2P state and the
// Total source. Cheap.
func (s *Service) Status(spaceId string) space.SpaceSyncStatus {
	t := s.trackerNoCreate(spaceId)
	out := space.SpaceSyncStatus{SpaceId: spaceId}
	out.NetworkPeers, out.LocalPeers, out.GlobalPeers = s.peerCountsFor(spaceId)
	out.P2P = s.p2pStateFor(spaceId)
	if t == nil {
		// Unknown space — return the presence fields with the id
		// stamped. State stays Unknown until somebody touches the
		// tracker.
		return out
	}
	out.LastSyncedAt = t.LastSyncedAt()
	pending := t.PendingCount()
	out.Total = s.totalFor(spaceId)
	if pending > out.Total {
		// Defensive: pending heads counted from trees the Total
		// source hasn't catalogued yet (cold restore window).
		// Showing Synced<0 would be worse than clamping.
		out.Synced = 0
	} else {
		out.Synced = out.Total - pending
	}
	out.State = computeRollup(out, pending)
	return out
}

// peerCountsFor reads the configured PeerCountsFn. Zeros when unwired.
func (s *Service) peerCountsFor(spaceId string) (networkPeers, localPeers, globalPeers int) {
	s.mu.Lock()
	fn := s.peerCountsFn
	s.mu.Unlock()
	if fn == nil {
		return 0, 0, 0
	}
	return fn(spaceId)
}

// p2pStateFor reads the configured P2PStateFn. Unknown when unwired.
func (s *Service) p2pStateFor(spaceId string) space.P2PState {
	s.mu.Lock()
	fn := s.p2pStateFn
	s.mu.Unlock()
	if fn == nil {
		return space.P2PStateUnknown
	}
	return fn(spaceId)
}

// SubscribeStatus registers cb for SpaceSyncStatus transitions.
// Account-wide; one cb sees every space.
func (s *Service) SubscribeStatus(cb func(space.SpaceSyncStatus)) func() {
	if cb == nil {
		return func() {}
	}
	return s.spaceSubs.Add(cb)
}

// Close drops every Tracker, stops the rollup loop, and rejects
// future For / Subscribe calls. Idempotent.
func (s *Service) Close() {
	s.runMu.Lock()
	if s.runCancel != nil {
		s.runCancel()
		s.runCancel = nil
	}
	s.runMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	trackers := s.trackers
	s.trackers = map[string]*Tracker{}
	s.mu.Unlock()
	for _, t := range trackers {
		t.close()
	}
	s.spaceSubs.Close()
}

// refresh is called by Trackers on every state transition. Adds
// spaceId to the dirty set; the rollup loop (or Tick) will compose
// and dispatch a SpaceSyncStatus event on the next pass if anything
// transitioned since the previous dispatch.
//
// We dirty the space even when there are no current subscribers so
// the lastEvent baseline stays current — a late subscriber's first
// real transition still fires correctly.
func (s *Service) refresh(spaceId string) {
	s.dirtyMu.Lock()
	s.dirty[spaceId] = struct{}{}
	s.dirtyMu.Unlock()
}

// Refresh marks spaceId dirty from outside the tracker path — used by
// the p2p wiring when a local peer's space set or the discovery
// possibility changes, so subscribers get a presence event without a
// tree transition.
func (s *Service) Refresh(spaceId string) { s.refresh(spaceId) }

// RefreshAll marks every tracked space dirty. Used on account-wide
// presence changes (discovery possibility flips).
func (s *Service) RefreshAll() {
	s.mu.Lock()
	ids := make([]string, 0, len(s.trackers))
	for id := range s.trackers {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	for _, id := range ids {
		s.refresh(id)
	}
}

// rollupsEqual reports whether two SpaceSyncStatus values would
// project to the same wire event. Excludes LastSyncedAt — that's a
// continuously-moving timestamp that would force a dispatch on every
// HeadsApply even when nothing else changed.
func rollupsEqual(a, b space.SpaceSyncStatus) bool {
	return a.SpaceId == b.SpaceId &&
		a.State == b.State &&
		a.Synced == b.Synced &&
		a.Total == b.Total &&
		a.NetworkPeers == b.NetworkPeers &&
		a.LocalPeers == b.LocalPeers &&
		a.GlobalPeers == b.GlobalPeers &&
		a.P2P == b.P2P
}

// isResponsibleSender reports whether senderId is in spaceId's
// nodeconf-resolved responsible-node set. No NodeIdsFn wired ⇒ true
// (test-friendly default).
func (s *Service) isResponsibleSender(spaceId, senderId string) bool {
	s.mu.Lock()
	nodeFn := s.nodeIds
	localFn := s.localPeerIds
	s.mu.Unlock()
	// No node resolver wired = test mode: every sender responsible.
	if nodeFn == nil {
		return true
	}
	for _, id := range nodeFn(spaceId) {
		if id == senderId {
			return true
		}
	}
	// A connected direct peer (LAN or global) sharing this space is also
	// a responsible sender — otherwise a space synced only over the LAN never drains
	// pending heads and sync status is stuck "syncing" forever.
	if localFn != nil {
		for _, id := range localFn(spaceId) {
			if id == senderId {
				return true
			}
		}
	}
	return false
}

// trackerNoCreate returns the existing tracker or nil. Used by
// Status — readers shouldn't create trackers as a side effect (that
// would mark every queried space as "tracked" even if it never sees
// a hook).
func (s *Service) trackerNoCreate(spaceId string) *Tracker {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.trackers[spaceId]
}

// totalFor reads the configured TotalFn for spaceId. Zero is the
// safe default — the rollup interprets it as "no objects, so
// trivially Synced".
func (s *Service) totalFor(spaceId string) int {
	s.mu.Lock()
	fn := s.totalFn
	s.mu.Unlock()
	if fn == nil {
		return 0
	}
	return fn(spaceId)
}

// computeRollup picks the SyncState for a SpaceSyncStatus given its
// counts. Peer connectivity is reported through the peer counts and
// P2P state, not through the rollup.
//
// Priority:
//   - pending > 0       → Syncing
//   - Total == 0        → Unknown (no trees seen yet)
//   - default           → Synced
func computeRollup(s space.SpaceSyncStatus, pending int) space.SyncState {
	if pending > 0 {
		return space.SyncStateSyncing
	}
	if s.Total == 0 && s.LastSyncedAt.Equal(time.Time{}) {
		return space.SyncStateUnknown
	}
	return space.SyncStateSynced
}
