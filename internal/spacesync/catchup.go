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
		// Best-effort: a failure here (schema not present yet → parked
		// in _detached, decode error, etc.) does NOT halt the pass.
		// Per-object _meta already encodes per-object progress, and
		// the _detached drainer picks up parked rows on the next event.
		_, _ = store.Get(ctx, id)
	}

	if err := crdt.PersistSpaceMaxAddSeq(ctx, metaColl, spaceId, snapshot); err != nil {
		return fmt.Errorf("spacesync: persist watermark: %w", err)
	}
	return nil
}
