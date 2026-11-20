package space

import (
	"context"
	"time"
)

// DebugAPI exposes a read-only diagnostic surface for one space.
// Obtained via Space.Debug(). Not a stable interface — fields and
// methods may grow or move as debug needs evolve. Production UI
// code should use SyncStatusAPI instead.
//
// Two reads:
//
//   - Object(id) — per-object snapshot (tree structure + sync state
//   - local CRDT state). Triggers cold-restore on first touch of
//     an object; not a hot path.
//   - Space()    — per-peer headsync counters from the last
//     diffsyncer round (in-memory, lost on restart).
type DebugAPI interface {
	Object(ctx context.Context, objectId string) (ObjectDebug, error)
	Space() SpaceDebug
}

// ObjectDebug is a point-in-time snapshot of one object's
// tree-structure + sync-state + local CRDT state. Returned by
// DebugAPI.Object.
//
// The tree-structure fields (Heads, HeadsCount, BranchCount,
// TreeLen, Snapshots, LatestVersionId) are read under the object
// tree's mutex so they're jointly consistent. Local writes are
// blocked for the duration of the read — on a million-change
// tree the walk can take a noticeable pause, which is the
// caller's price for an atomic snapshot.
type ObjectDebug struct {
	ObjectId string

	// Sync state — from the per-space syncstatus.Tracker. Pending
	// is the set of heads the tracker is waiting for a
	// responsible-node HeadsApply on; empty means converged with
	// the last sender we trust.
	SyncState  SyncState
	Pending    []string
	LastSyncAt time.Time

	// Tree structure — read from any-sync's ObjectTree.
	//
	// HeadsCount == len(Heads), exposed as a separate field so a
	// caller checking "is this object diverged right now" doesn't
	// need to len() the slice.
	//
	// BranchCount counts every branch that ever existed in the
	// DAG: merged-in lineages (one per extra parent on a merge
	// change) plus still-open ones (HeadsCount - 1). A linear
	// tree with one head returns 0.
	//
	// Snapshots is the number of changes carrying IsSnapshot in
	// the tree. Costs a full IterateRoot walk — O(TreeLen).
	Heads       []string
	HeadsCount  int
	BranchCount int
	TreeLen     int
	Snapshots   int

	// LatestVersionId is the lexid-max OrderId across Heads. Empty
	// string when the tree has no heads (root-only or transient
	// states during cold restore). The value is local to this
	// peer's any-sync — VersionIds don't match across peers.
	LatestVersionId string

	// MaxAddSeq is the controller's delivery-order watermark — the
	// highest AddSeq seen by the apply path. Used internally for
	// cold-restore replay; surfaced here as a sanity check that
	// the controller is keeping up with the tree.
	MaxAddSeq uint64
}

// SpaceDebug is a point-in-time snapshot of this space's outbound
// headsync activity. Returned by DebugAPI.Space.
//
// Peers lists one row per responsible peer we've completed at
// least one diff round against since the SDK started. In-memory
// only — cleared on restart.
type SpaceDebug struct {
	SpaceId string
	Peers   []PeerSyncStats
}

// PeerSyncStats is the latest snapshot of one outbound diff
// round against a peer.
//
// New + Changed are the counts handed to the SDK's TreeSyncer
// after deletionState filtering — i.e. trees we'll actually pull
// or push, not the raw pre-filter diff. LastSyncAt is the
// completion time of the round; LastErr is the SyncAll error
// string if any, empty on success.
//
// "Deleted" is intentionally absent in v1 — any-sync routes
// removedIds through the deletion path before our TreeSyncer
// sees them, so we can't attribute a clean per-peer count from
// our side without an upstream hook.
type PeerSyncStats struct {
	PeerId     string
	LastSyncAt time.Time
	New        int
	Changed    int
	LastErr    string
}
