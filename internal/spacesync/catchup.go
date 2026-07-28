// Package spacesync runs the SDK's space-level catch-up against
// any-sync's head store at startup.
//
// The per-object Controller.maxAddSeq watermark only advances when an
// object is loaded (ColdRestore reads from it). Trees that a peer
// updated while the SDK was offline — or trees the SDK has never
// opened — stay invisible to derived/queryable surfaces until the
// user happens to call Get(treeId). This package closes that gap.
//
// Mechanism: persist a single per-space "highest LastAddSeq we have
// seen" in the SDK DB; on startup, query any-sync's head store for
// trees whose LastAddSeq exceeds it and force-load each (the existing
// per-object ColdRestore path then catches them up). Persist the
// snapshot at the end so the fast path no-ops on the next boot.
//
// The watermark is also snapshotted WITHOUT a replay on clean SDK
// Close (SnapshotWatermark): everything the head store accepted during
// a session was applied live as it arrived, so at close time the
// projection is current and only the persisted number lags. Without
// that snapshot every tree touched during a session sits above the
// boot-persisted watermark and the NEXT boot force-loads all of them
// for nothing.
//
// Run covers only CREATES/UPDATES (force-load trees whose head advanced).
// The deletion-reconcile backstop for the SDK-DB rebuild case lives in
// ReconcileDeletions (reconcile.go): a settings-head-gated pass that purges
// any local row for a tree any-sync has flipped to Deleted.
package spacesync

import (
	"context"
	"fmt"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/commonspace/headsync/headstorage"

	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
)

// Run executes one catch-up pass for spaceId. Best-effort by design:
// per-tree load failures are tolerated (caller logs), and the space
// watermark is advanced as long as the head-store snapshot read and
// iteration succeed. Returning an error means the snapshot itself
// could not be established; the watermark stays at the prior value so
// the next boot retries.
//
// Cost: one any-store read for the SDK watermark, one any-sync
// MaxLastAddSeq read, and a filtered IterateEntries that only yields
// trees with LastAddSeq > sdkSeq (any-sync v0.12.4+ pushes the filter
// down via IterOpts.MinLastAddSeq). For a steady-state restart with no
// missed changes, this is two cheap reads and the function returns.
func Run(ctx context.Context, app *anysyncx.App, db anystore.DB, store *spaceobjects.Store, spaceId string) error {
	metaColl, err := db.Collection(ctx, crdt.MetaCollectionName)
	if err != nil {
		return fmt.Errorf("spacesync: open _meta: %w", err)
	}
	sdkSeq, err := crdt.LoadSpaceMaxAddSeq(ctx, metaColl, spaceId)
	if err != nil {
		return fmt.Errorf("spacesync: load watermark: %w", err)
	}

	handle, err := app.GetSpace(ctx, spaceId)
	if err != nil {
		return fmt.Errorf("spacesync: get space: %w", err)
	}
	storage := handle.Inner().Storage()
	hs := storage.HeadStorage()

	// Snapshot the head-store max BEFORE iterating so any changes that
	// land mid-pass don't get marked as caught-up. They flow in via
	// the live sync path and update per-object _meta on their own; the
	// next boot will pick up the residue.
	snapshot, err := hs.MaxLastAddSeq(ctx)
	if err != nil {
		return fmt.Errorf("spacesync: MaxLastAddSeq: %w", err)
	}
	if snapshot <= sdkSeq {
		return nil
	}

	// Two-phase: collect ids while the iterator is open, then load.
	// Holding the any-store cursor across spaceobjects.Store.Get would
	// risk deadlocks (Get takes its own locks and may block on
	// network-backed BuildTree).
	var toLoad []string
	iterErr := hs.IterateEntries(ctx, headstorage.IterOpts{MinLastAddSeq: sdkSeq}, func(e headstorage.HeadsEntry) (bool, error) {
		if e.DeletedStatus != headstorage.DeletedStatusNotDeleted {
			return true, nil
		}
		toLoad = append(toLoad, e.Id)
		return true, nil
	})
	if iterErr != nil {
		return fmt.Errorf("spacesync: IterateEntries: %w", iterErr)
	}

	for _, id := range toLoad {
		// Selective mode: skip-marked trees exist only as heads-only
		// stubs — there is nothing to restore, and Get would fire a
		// pointless remote probe.
		if skipped, serr := store.IsTreeSkipped(ctx, id); serr == nil && skipped {
			continue
		}
		// Best-effort: a failure here (schema not present yet → parked
		// in _detached, decode error, etc.) does NOT halt the pass.
		// Per-object _meta already encodes per-object progress, and
		// the _detached drainer picks up parked rows on the next event.
		//
		// Drop the cached Object as soon as the restore completes —
		// the catch-up pass can touch thousands of trees, and we
		// don't want to retain them all in RAM until the cache TTL
		// expires. Subsequent live accesses re-load on demand. NOTE:
		// Drop hard-closes the object even if a concurrent caller
		// still holds a handle — the same hazard class as TTL-GC
		// eviction, amplified here because the pass touches every
		// stale tree in one sweep and (post boot-rework) runs
		// concurrently with user reads. Holders must tolerate a
		// closed handle and re-Get, as they already must for GC.
		if _, err := store.Get(ctx, id); err == nil {
			store.Drop(id)
		}
	}

	if err := crdt.PersistSpaceMaxAddSeq(ctx, metaColl, spaceId, snapshot); err != nil {
		return fmt.Errorf("spacesync: persist watermark: %w", err)
	}
	return nil
}

// SnapshotWatermark persists spaceId's current head-store MaxLastAddSeq
// as its catch-up watermark without force-loading anything. Called on
// clean SDK Close for allowlisted spaces only (SDK.caughtUp — caught
// up by a boot Run or created/derived this session): everything the
// head store accepted DURING the session was applied to the projection
// live as it arrived (or parked durably in _detached, which the
// drainer resumes — the same tolerance Run itself has), so the
// snapshot is valid and the next boot's Run fast-path no-ops instead
// of force-loading every tree the session touched. A crash skips this
// persist and the boot replay remains the fallback. Same
// snapshot-before-teardown property as Run: anything landing after the
// read stays above the persisted watermark and replays next boot. The
// caller must additionally skip spaces with a non-empty treesyncer
// parked set (App.ParkedTreeCount) — parked trees are
// storage-committed but never materialized, and the boot replay is
// their only cross-restart recovery.
func SnapshotWatermark(ctx context.Context, db anystore.DB, handle anysyncx.SpaceHandle, spaceId string) error {
	metaColl, err := db.Collection(ctx, crdt.MetaCollectionName)
	if err != nil {
		return fmt.Errorf("spacesync: open _meta: %w", err)
	}
	snapshot, err := handle.Inner().Storage().HeadStorage().MaxLastAddSeq(ctx)
	if err != nil {
		return fmt.Errorf("spacesync: MaxLastAddSeq: %w", err)
	}
	_, err = persistWatermarkIfAhead(ctx, metaColl, spaceId, snapshot)
	return err
}

// persistWatermarkIfAhead advances the stored space watermark to
// snapshot iff it is strictly ahead; the watermark never regresses.
// Split out of SnapshotWatermark so the monotonic guard is testable
// without a live commonspace handle.
func persistWatermarkIfAhead(ctx context.Context, metaColl anystore.Collection, spaceId string, snapshot uint64) (advanced bool, err error) {
	sdkSeq, err := crdt.LoadSpaceMaxAddSeq(ctx, metaColl, spaceId)
	if err != nil {
		return false, fmt.Errorf("spacesync: load watermark: %w", err)
	}
	if snapshot <= sdkSeq {
		return false, nil
	}
	if err := crdt.PersistSpaceMaxAddSeq(ctx, metaColl, spaceId, snapshot); err != nil {
		return false, fmt.Errorf("spacesync: persist watermark: %w", err)
	}
	return true, nil
}
