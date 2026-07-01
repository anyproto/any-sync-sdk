package spacesync

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/commonspace/headsync/headstorage"

	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
)

// reconcileVersion is the anytype-heart ForceIdxRerunCounter analog: bump it
// to force one reconcile sweep per space on the next boot when the
// purge/reconcile logic changes, independently of the settings-tree head
// content signal. Compared with `storedVer < reconcileVersion` (monotone),
// so a binary downgrade does not re-trigger a full-space rescan.
const reconcileVersion = 1

// ReconcileDeletions purges local projections for objects any-sync has
// flipped to DeletedStatusDeleted but whose SDK row survived — the sdk.db
// REBUILD case: any-sync only re-fires the delete callback for Queued ids,
// and forward catch-up (Run) excludes deleted trees, so a previously-deleted
// object could otherwise re-surface as a live row (the SDK keeps no
// `objects` tombstone). Purging also stamps the consumer deletion feed for
// any surviving row (see spaceobjects.Store.PurgeObjects).
//
// Gated on the any-sync settings-tree head: the settings tree records only
// deletions, so its head advances iff a deletion happened. On boot we read
// it and compare against the value persisted on the space's `_meta` row; an
// unchanged head (and no reconcileVersion bump) means no new deletions since
// the last sweep — two point reads and return. A wiped sdk.db drops the
// persisted gate, forcing exactly one sweep.
//
// Sibling of Run (not nested): Run fast-returns on snapshot<=sdkSeq before
// any deletion logic, and the two gates ("q" watermark vs "dh"/"dv") fire
// independently. Best-effort, same contract as Run — a returned error means
// the gate stays stale and the next boot retries.
func ReconcileDeletions(ctx context.Context, app *anysyncx.App, db anystore.DB, store *spaceobjects.Store, spaceId string) error {
	metaColl, err := db.Collection(ctx, crdt.MetaCollectionName)
	if err != nil {
		return fmt.Errorf("spacesync: reconcile open _meta: %w", err)
	}
	storedHead, storedVer, err := crdt.LoadSpaceDeletedGate(ctx, metaColl, spaceId)
	if err != nil {
		return fmt.Errorf("spacesync: load deleted gate: %w", err)
	}

	handle, err := app.GetSpace(ctx, spaceId)
	if err != nil {
		return fmt.Errorf("spacesync: reconcile get space: %w", err)
	}
	storage := handle.Inner().Storage()
	hs := storage.HeadStorage()
	settingsId := storage.StateStorage().SettingsId()

	entry, err := hs.GetEntry(ctx, settingsId)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return nil // uninitialized space: nothing to reconcile
		}
		return fmt.Errorf("spacesync: reconcile settings head: %w", err)
	}
	cur := settingsHeadChecksum(entry.Heads)

	// Steady-state skip: no version bump AND settings head unchanged.
	if storedVer >= reconcileVersion && storedHead == cur {
		return nil
	}

	// Phase 1: collect Deleted ids while the <spaceId>.db head cursor is
	// open. Filter to DeletedStatusDeleted — Queued ids are any-sync's
	// deleteLoop's job (its initial pass runs on space load and re-fires
	// them every boot); reconciling them here only races that loop.
	var deleted []string
	iterErr := hs.IterateEntries(ctx, headstorage.IterOpts{Deleted: true}, func(e headstorage.HeadsEntry) (bool, error) {
		if e.DeletedStatus == headstorage.DeletedStatusDeleted {
			deleted = append(deleted, e.Id)
		}
		return true, nil
	})
	if iterErr != nil {
		return fmt.Errorf("spacesync: iterate deleted: %w", iterErr)
	}

	// Phase 2: batch-purge, head cursor closed. PurgeObjects FindId-gates
	// each id (never-materialized ids are cheap no-ops) and lists collection
	// names once, so the O(all-collections) drop cost is paid at most once
	// regardless of how many ids survived.
	if err := store.PurgeObjects(ctx, deleted); err != nil {
		return fmt.Errorf("spacesync: reconcile purge: %w", err)
	}

	// Persist the START-of-pass head only after a fully successful sweep; a
	// deletion landing mid-pass advances the live head so the next boot
	// re-runs.
	if err := crdt.PersistSpaceDeletedGate(ctx, metaColl, spaceId, cur, reconcileVersion); err != nil {
		return fmt.Errorf("spacesync: persist deleted gate: %w", err)
	}
	return nil
}

// settingsHeadChecksum sorts (mandatory — .Heads is a set that can hold >1
// element after concurrent merges) then joins, so peers with identical CRDT
// state compute the same checksum.
func settingsHeadChecksum(heads []string) string {
	s := append([]string(nil), heads...)
	sort.Strings(s)
	return strings.Join(s, ",")
}
