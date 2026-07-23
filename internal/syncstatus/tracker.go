package syncstatus

import (
	"sync"
	"time"

	"github.com/anyproto/any-sync/app"
	anysyncstatus "github.com/anyproto/any-sync/commonspace/syncstatus"

	"github.com/anyproto/any-sync-sdk/internal/fanout"
	"github.com/anyproto/any-sync-sdk/space"
)

// Tracker satisfies any-sync's syncstatus.StatusUpdater interface so
// it can be passed as commonspace.Deps.SyncStatus. The per-space
// commonspace app graph then dispatches HeadsChange / HeadsReceive /
// ObjectReceive / HeadsApply directly to us.
var _ anysyncstatus.StatusUpdater = (*Tracker)(nil)

// Tracker holds per-space sync status: the per-tree state machine
// fed by any-sync's StatusUpdater hooks, plus the per-object
// subscriber registry. One Tracker per space, constructed lazily by
// Service.For.
//
// Phase 1 lands the state machine and subscribe registry; the
// any-sync StatusUpdater wiring (commonspace.Deps.SyncStatus) is
// added in Phase 2 along with the rollup loop. The hook methods
// (HeadsChange / ObjectReceive / HeadsApply) are already implemented
// so Phase 2 is just a wire-up change.
type Tracker struct {
	spaceId  string
	parent   *Service // for cross-tracker access (refresh, conn status); may be nil in tests
	excluded map[string]struct{}

	mu sync.Mutex
	// trees maps treeId → per-tree state. Populated by the
	// any-sync StatusUpdater hooks (HeadsChange, ObjectReceive,
	// HeadsApply) and by BulkSyncedFromPeer.
	trees map[string]*treeState
	// lastSync is the most recent HeadsApply timestamp across all
	// trees — used in the space-level rollup as LastSyncedAt.
	lastSync time.Time
	// lastAllSyncedAt is the most recent moment a responsible
	// peer reported a fully-converged diff round (0 new, 0
	// changed). When non-zero, Detail/Object for a tree the
	// tracker has never seen returns Synced anchored to this
	// timestamp — covers cold-restored trees that haven't had a
	// per-tree hook fire yet but are demonstrably in sync at the
	// space level.
	lastAllSyncedAt time.Time
	subs            *fanout.Registry[space.ObjectSyncStatus]
}

// treeState is the per-tree row of the tracker. Pending heads are
// the heads we expect a responsible-node HeadsApply to acknowledge;
// the row is Synced when pending empties.
type treeState struct {
	pending     []string
	state       space.SyncState
	lastApplied time.Time
}

// newTracker builds an empty Tracker for spaceId. excludedTrees
// names trees the tracker should ignore (e.g. ACL, spaceIndex,
// members system collection) — hooks for those treeIds are dropped
// before they touch the state machine.
func newTracker(spaceId string, parent *Service, excludedTrees []string) *Tracker {
	excl := make(map[string]struct{}, len(excludedTrees))
	for _, id := range excludedTrees {
		if id != "" {
			excl[id] = struct{}{}
		}
	}
	return &Tracker{
		spaceId:  spaceId,
		parent:   parent,
		excluded: excl,
		trees:    map[string]*treeState{},
		subs:     fanout.New[space.ObjectSyncStatus](),
	}
}

// SpaceId returns the space this Tracker serves.
func (t *Tracker) SpaceId() string { return t.spaceId }

// Init is the app.Component method invoked when the per-space
// commonspace app starts. We don't need anything from the per-space
// graph (responsible-node ids come from the account-level nodeconf,
// already bound at Service construction), so this is a no-op.
func (t *Tracker) Init(_ *app.App) error { return nil }

// Name returns the component name expected by commonspace. Must
// match anysyncstatus.CName so the app graph wires the tree-sync
// handlers to this instance.
func (t *Tracker) Name() string { return anysyncstatus.CName }

// AddExcluded registers tree ids the tracker should ignore (ACL
// tree, spaceIndex object, settings tree, etc). Additive — call as
// system trees become known. Safe to call before or after the
// tracker starts seeing hooks; previously-recorded state for an
// id that's now excluded is dropped from the rollup math.
func (t *Tracker) AddExcluded(treeIds ...string) {
	if len(treeIds) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, id := range treeIds {
		if id == "" {
			continue
		}
		t.excluded[id] = struct{}{}
		// Drop any state we may have already captured for this id
		// before the exclude landed — keeps PendingCount honest.
		delete(t.trees, id)
	}
}

// Object returns the snapshot for objectId. Unknown ids return
// State=Unknown; the tracker doesn't distinguish "never seen" from
// "no work needed" via the returned value — that's the rollup loop's
// job (which counts pending against Total).
func (t *Tracker) Object(objectId string) space.ObjectSyncStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	if st, ok := t.trees[objectId]; ok {
		return space.ObjectSyncStatus{
			ObjectId:   objectId,
			State:      st.state,
			LastSyncAt: st.lastApplied,
		}
	}
	// No per-tree row, but a responsible peer has confirmed
	// space-level convergence at lastAllSyncedAt — that anchors
	// this tree's state to Synced. Otherwise Unknown.
	if !t.lastAllSyncedAt.IsZero() {
		return space.ObjectSyncStatus{
			ObjectId:   objectId,
			State:      space.SyncStateSynced,
			LastSyncAt: t.lastAllSyncedAt,
		}
	}
	return space.ObjectSyncStatus{ObjectId: objectId, State: space.SyncStateUnknown}
}

// Detail returns the full per-tree snapshot used by the debug
// API: state, a copy of the pending-heads slice, lastApplied,
// and whether the tracker has seen this id at all.
//
// Unknown ids return known=false and zero values — debug callers
// pre-distinguish "we never saw a hook" from "we saw it and it
// converged" (the latter has lastApplied set).
func (t *Tracker) Detail(objectId string) (state space.SyncState, pending []string, lastApplied time.Time, known bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if st, ok := t.trees[objectId]; ok {
		var p []string
		if len(st.pending) > 0 {
			p = append(p, st.pending...)
		}
		return st.state, p, st.lastApplied, true
	}
	if !t.lastAllSyncedAt.IsZero() {
		// Synced anchored to the last 0/0 responsible-peer round.
		// known=false signals "no per-tree hook history" but the
		// state is still meaningful — callers can render it.
		return space.SyncStateSynced, nil, t.lastAllSyncedAt, false
	}
	return space.SyncStateUnknown, nil, time.Time{}, false
}

// SubscribeObject registers cb for state flips on objectId. Cheap;
// the dispatcher delivers on every change, not on every hook (so a
// HeadsChange followed by a HeadsApply with no net transition fires
// once, not twice).
func (t *Tracker) SubscribeObject(objectId string, cb func(space.ObjectSyncStatus)) func() {
	if cb == nil || objectId == "" {
		return func() {}
	}
	// In v1 we share one registry per Tracker and let the dispatcher
	// filter by objectId at delivery. Cheap enough — registries are
	// per-space so the fan-out is bounded.
	return t.subs.Add(func(ev space.ObjectSyncStatus) {
		if ev.ObjectId == objectId {
			cb(ev)
		}
	})
}

// PendingCount returns the number of trees currently in Syncing.
// Read by the rollup loop to compute Synced/Total.
func (t *Tracker) PendingCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, st := range t.trees {
		if st.state == space.SyncStateSyncing {
			n++
		}
	}
	return n
}

// LastSyncedAt returns the most recent HeadsApply timestamp seen by
// the tracker (across all trees). Used in the SpaceSyncStatus
// rollup.
func (t *Tracker) LastSyncedAt() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastSync
}

// HeadsChange marks the tree as Syncing with pending=heads. Called
// from any-sync after a local write (synctree.AddContent).
func (t *Tracker) HeadsChange(treeId string, heads []string) {
	if treeId == "" {
		return
	}
	t.mu.Lock()
	if t.skipTreeLocked(treeId) {
		t.mu.Unlock()
		return
	}
	st := t.ensureTreeLocked(treeId)
	st.pending = append(st.pending[:0], heads...)
	st.state = space.SyncStateSyncing
	ev := space.ObjectSyncStatus{ObjectId: treeId, State: st.state}
	t.mu.Unlock()
	t.dispatchAndRefresh(ev)
}

// HeadsReceive is a no-op in v1 — receive happens pre-apply and we
// don't show "received but not applied" as a distinct state. Matches
// heart.
func (t *Tracker) HeadsReceive(senderId, treeId string, heads []string) {}

// ObjectReceive registers a tree the tracker hadn't seen before
// (newly-pulled tree on cold restore). Doesn't change Synced/Total
// math directly — Total is sourced from the per-space `objects`
// collection — but ensures Object() returns sensible state.
func (t *Tracker) ObjectReceive(senderId, treeId string, heads []string) {
	if treeId == "" {
		return
	}
	t.mu.Lock()
	if t.skipTreeLocked(treeId) {
		t.mu.Unlock()
		return
	}
	st, existed := t.trees[treeId]
	if !existed {
		st = &treeState{state: space.SyncStateSyncing}
		t.trees[treeId] = st
	}
	if len(st.pending) == 0 {
		st.pending = append(st.pending[:0], heads...)
	}
	ev := space.ObjectSyncStatus{ObjectId: treeId, State: st.state}
	t.mu.Unlock()
	if !existed {
		t.dispatchAndRefresh(ev)
	}
}

// HeadsApply drains heads from pending when the sender is a
// responsible node and allAdded is set. Empty pending flips the tree
// to Synced. Non-responsible senders are ignored in v1 (no
// tempSynced bookkeeping).
func (t *Tracker) HeadsApply(senderId, treeId string, heads []string, allAdded bool) {
	if treeId == "" {
		return
	}
	if !allAdded || !t.isResponsibleSender(senderId) {
		return
	}
	t.mu.Lock()
	if t.skipTreeLocked(treeId) {
		t.mu.Unlock()
		return
	}
	st := t.ensureTreeLocked(treeId)
	st.pending = removeAny(st.pending, heads)
	st.lastApplied = time.Now()
	if st.lastApplied.After(t.lastSync) {
		t.lastSync = st.lastApplied
	}
	if len(st.pending) == 0 {
		st.state = space.SyncStateSynced
	}
	ev := space.ObjectSyncStatus{
		ObjectId:   treeId,
		State:      st.state,
		LastSyncAt: st.lastApplied,
	}
	t.mu.Unlock()
	t.dispatchAndRefresh(ev)
}

// BulkSyncedFromPeer is the space-level convergence hook: when a
// responsible peer reports a fully-zero diff round (no new, no
// changed trees from our side), every locally-known tree is by
// definition in sync with that peer. Sweep the tracker: clear
// pending heads on every recorded tree, flip to Synced, anchor
// lastSync to now. Unknown-to-tracker trees inherit Synced via
// lastAllSyncedAt on the next Detail/Object read.
//
// Non-responsible senders and empty peerIds are ignored — same
// rule HeadsApply uses, prevents a stray reply from collapsing
// the state machine.
func (t *Tracker) BulkSyncedFromPeer(peerId string) {
	if peerId == "" || !t.isResponsibleSender(peerId) {
		return
	}
	now := time.Now()
	t.mu.Lock()
	t.lastAllSyncedAt = now
	if now.After(t.lastSync) {
		t.lastSync = now
	}
	var events []space.ObjectSyncStatus
	for treeId, st := range t.trees {
		flipped := st.state != space.SyncStateSynced
		st.pending = st.pending[:0]
		st.state = space.SyncStateSynced
		st.lastApplied = now
		if flipped {
			events = append(events, space.ObjectSyncStatus{
				ObjectId:   treeId,
				State:      space.SyncStateSynced,
				LastSyncAt: now,
			})
		}
	}
	t.mu.Unlock()
	for _, ev := range events {
		t.subs.Dispatch(ev)
	}
	if t.parent != nil {
		t.parent.refresh(t.spaceId)
	}
}

// close drops all subscribers. Called from Service.close.
func (t *Tracker) close() {
	t.subs.Close()
}

// ensureTreeLocked returns the treeState for treeId, creating an
// empty Syncing row if missing. Caller holds t.mu.
func (t *Tracker) ensureTreeLocked(treeId string) *treeState {
	st, ok := t.trees[treeId]
	if !ok {
		st = &treeState{state: space.SyncStateSyncing}
		t.trees[treeId] = st
	}
	return st
}

// dispatchAndRefresh fires the per-object event and nudges the
// account-level rollup. Both are safe under no-parent (test) — the
// parent nudge is skipped.
func (t *Tracker) dispatchAndRefresh(ev space.ObjectSyncStatus) {
	t.subs.Dispatch(ev)
	if t.parent != nil {
		t.parent.refresh(t.spaceId)
	}
}

// skipTreeLocked reports whether the tracker should ignore treeId.
// Caller holds t.mu. Used to exclude non-user-visible trees (ACL,
// spaceIndex, settings, members system) from the rollup math.
func (t *Tracker) skipTreeLocked(treeId string) bool {
	_, ok := t.excluded[treeId]
	return ok
}

// isResponsibleSender reports whether senderId is in this space's
// responsible-node list. Phase 1 returns true unconditionally when
// no parent is wired (test mode); Phase 2 will route through
// Service.nodeIdsFor(spaceId).
func (t *Tracker) isResponsibleSender(senderId string) bool {
	if t.parent == nil {
		return true
	}
	return t.parent.isResponsibleSender(t.spaceId, senderId)
}

// removeAny returns pending with any element of toRemove dropped.
// Stable order, O(len(pending)*len(toRemove)) — both are tiny in
// practice (a tree's pending heads are typically 1-2).
func removeAny(pending, toRemove []string) []string {
	if len(pending) == 0 || len(toRemove) == 0 {
		return pending
	}
	rm := make(map[string]struct{}, len(toRemove))
	for _, h := range toRemove {
		rm[h] = struct{}{}
	}
	out := pending[:0]
	for _, h := range pending {
		if _, drop := rm[h]; drop {
			continue
		}
		out = append(out, h)
	}
	return out
}
