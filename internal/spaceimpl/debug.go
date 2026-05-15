package spaceimpl

import (
	"context"
	"fmt"

	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"

	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/syncstatus"
	"github.com/anyproto/any-sync-sdk/space"
)

// debugAPI implements space.DebugAPI. Thin wrapper that pulls
// from three already-existing sources:
//
//   - per-object tree (via spaceobjects.Store → object.Object →
//     objecttree.ObjectTree) for Heads / Len / Snapshots /
//     LatestVersionId / BranchCount;
//   - per-object Controller for MaxAddSeq;
//   - per-space syncstatus.Tracker for SyncState / Pending /
//     LastSyncAt;
//   - per-space treeSyncerAdapter (via anysyncx.App.PeerSyncStats)
//     for headsync peer counters.
//
// No new state; safe to construct on every call.
type debugAPI struct {
	spaceId    string
	app        *anysyncx.App
	store      *spaceobjects.Store
	syncStatus *syncstatus.Service
}

func newDebugAPI(s *spaceImpl) *debugAPI {
	return &debugAPI{
		spaceId:    s.id,
		app:        s.app,
		store:      s.store,
		syncStatus: s.app.SyncStatus(),
	}
}

// Object returns the per-object diagnostic snapshot. Triggers
// cold-restore on first access of an object — not a hot path.
//
// The tree-structure fields (Heads, HeadsCount, BranchCount,
// TreeLen, Snapshots, LatestVersionId) are gathered under the
// object tree's mutex so they're a jointly-consistent snapshot;
// local writes block for the duration of the IterateRoot walk.
func (d *debugAPI) Object(ctx context.Context, objectId string) (space.ObjectDebug, error) {
	if objectId == "" {
		return space.ObjectDebug{}, fmt.Errorf("debug: objectId required")
	}
	obj, err := d.store.Get(ctx, objectId)
	if err != nil {
		return space.ObjectDebug{}, fmt.Errorf("debug: load %s: %w", objectId, err)
	}
	tree := obj.Tree()
	if tree == nil {
		return space.ObjectDebug{}, fmt.Errorf("debug: tree not bound for %s", objectId)
	}

	out := space.ObjectDebug{ObjectId: objectId}

	// ObjectTree embeds sync.Locker via TryLocker, so Heads / Len /
	// IterateRoot / GetChange all become a single atomic snapshot
	// under one external lock. Blocks local writes for the duration
	// of the walk — caller's price for joint consistency.
	tree.Lock()
	heads := append([]string(nil), tree.Heads()...)
	treeLen := tree.Len()
	snapshots, branches, walkErr := walkTreeStats(tree)
	latest := latestVersionLocked(tree, heads)
	tree.Unlock()
	if walkErr != nil {
		return space.ObjectDebug{}, fmt.Errorf("debug: tree walk %s: %w", objectId, walkErr)
	}

	if len(heads) > 1 {
		branches += len(heads) - 1
	}

	out.Heads = heads
	out.HeadsCount = len(heads)
	out.BranchCount = branches
	out.TreeLen = treeLen
	out.Snapshots = snapshots
	out.LatestVersionId = latest
	out.MaxAddSeq = obj.Controller().MaxAddSeq()

	st, pending, lastApplied, _ := d.syncStatus.For(d.spaceId).Detail(objectId)
	out.SyncState = st
	out.Pending = pending
	out.LastSyncAt = lastApplied

	return out, nil
}

// Space returns the per-peer headsync counters. In-memory only.
func (d *debugAPI) Space() space.SpaceDebug {
	snaps := d.app.PeerSyncStats(d.spaceId)
	out := space.SpaceDebug{SpaceId: d.spaceId}
	if len(snaps) == 0 {
		return out
	}
	out.Peers = make([]space.PeerSyncStats, 0, len(snaps))
	for _, s := range snaps {
		out.Peers = append(out.Peers, space.PeerSyncStats{
			PeerId:     s.PeerId,
			LastSyncAt: s.LastSyncAt,
			New:        s.New,
			Changed:    s.Changed,
			LastErr:    s.LastErr,
		})
	}
	return out
}

// walkTreeStats runs one IterateRoot pass counting snapshots
// and merge-closed branches. Branch count returned here covers
// merges only; the caller adds (HeadsCount-1) for still-open
// branches.
func walkTreeStats(tree objecttree.ObjectTree) (snapshots, branches int, err error) {
	err = tree.IterateRoot(nil, func(c *objecttree.Change) bool {
		if c == nil {
			return true
		}
		if c.IsSnapshot {
			snapshots++
		}
		if len(c.PreviousIds) > 1 {
			branches += len(c.PreviousIds) - 1
		}
		return true
	})
	return
}

// latestVersionLocked picks the lexid-max OrderId across the
// supplied heads. Caller must hold the tree mutex. Empty result
// when the tree has no heads (root-only or transient cold-
// restore states); also empty if every head's Change lookup
// fails (defensive — production trees always resolve heads).
func latestVersionLocked(tree objecttree.ObjectTree, heads []string) string {
	var best string
	for _, h := range heads {
		ch, err := tree.GetChange(h)
		if err != nil || ch == nil {
			continue
		}
		if ch.OrderId > best {
			best = ch.OrderId
		}
	}
	return best
}
