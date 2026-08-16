package spaceimpl

import (
	"context"
	"errors"
	"strings"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/app/ocache"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
)

var offloadLog = logger.NewNamed("sdk.spaceoffload")

// OffloadSpace tears down all local state for spaceId — in-memory
// watchers and the Store, the cached any-sync space, every SDK CRDT
// collection, the any-sync on-disk DB, and the account-values carrier.
// It does NOT touch the network or the tech-space index row: the row is
// the durable delete tombstone the reconciler scans, and callers
// (Delete / inbound detection) set its status before offloading.
//
// Idempotent and retryable: each step tolerates already-gone state so
// a re-run after a crash mid-offload, or an offload of a space that was
// never fully loaded, completes without error. Steps are best-effort
// ONLY for the not-found/already-gone class: a failed collection sweep
// (meta purge / drop / chunk commit / enumeration error) skips the
// any-sync storage removal — with the file in place `SpaceExists` stays
// true, so the deleted-row boot gate and the next Delete re-offload the
// remainder (committed chunks are durable). The later per-space
// cleanups (file store, file jobs, account values, identities) are
// independent of the sweep and always run — notably the file-job
// removal, since an orphaned job would retry against the deleted space
// forever.
func (s *Service) OffloadSpace(ctx context.Context, spaceId string) {
	// 1–3. Stop watchers, close + forget the Store, evict the any-sync
	// space — the shared close-without-delete teardown Evict also uses.
	s.closeSpaceRuntime(ctx, spaceId)

	// 4. Drop every SDK CRDT collection for the space from the shared DB
	// (meta watermarks purged first — see dropSpaceCollections).
	sweepErr := s.dropSpaceCollections(ctx, spaceId)
	if sweepErr != nil {
		offloadLog.Warn("drop collections; storage kept for retry", zap.String("spaceId", spaceId), zap.Error(sweepErr))
	}

	// 5. Remove the any-sync per-space DB file (the bulk of the
	// footprint) — only when the sweep fully committed: the file is the
	// retry gate, and removing it over a partial sweep would leak the
	// remaining collections permanently.
	if sweepErr == nil {
		if err := s.app.DeleteSpaceStorage(ctx, spaceId); err != nil {
			offloadLog.Warn("delete storage", zap.String("spaceId", spaceId), zap.Error(err))
		}
	}

	// 6. Drop the space's file CARs + metadata wholesale (the per-space
	// store layout exists for exactly this one recursive remove), and
	// its pending file jobs — an orphaned job would retry against the
	// deleted space forever.
	if s.fstore != nil {
		if err := s.fstore.DeleteSpace(ctx, spaceId); err != nil {
			offloadLog.Warn("delete file store", zap.String("spaceId", spaceId), zap.Error(err))
		}
	}
	if s.fqueue != nil {
		if err := s.fqueue.RemoveSpace(ctx, spaceId); err != nil {
			offloadLog.Warn("drop file jobs", zap.String("spaceId", spaceId), zap.Error(err))
		}
	}

	// 7. Drop the account-values carrier tree in tech space.
	if err := s.tsp.DropAccountValues(ctx, spaceId); err != nil {
		offloadLog.Debug("drop account values", zap.String("spaceId", spaceId), zap.Error(err))
	}

	// 8. Prune this space from the identities directory so spaceIds keeps
	// reflecting live memberships.
	if err := s.tsp.RemoveSpaceFromIdentities(ctx, spaceId); err != nil {
		offloadLog.Debug("prune identities", zap.String("spaceId", spaceId), zap.Error(err))
	}
}

// Evict closes a space without deleting anything: watchers stop, the
// Store and the any-sync commonspace handle are released. Disk state —
// the any-sync per-space DB and every SDK CRDT collection — stays, so a
// later Get reopens the space from local storage (and any-sync resumes
// syncing it). Idempotent: evicting a space that isn't open is a no-op.
//
// The tech space is refused — it is the registry Get itself depends on.
func (s *Service) Evict(ctx context.Context, spaceId string) error {
	if spaceId == "" {
		return errors.New("spaceimpl: Evict: spaceId required")
	}
	if spaceId == s.tsp.SpaceId() {
		return errors.New("spaceimpl: Evict: cannot evict the tech space")
	}
	s.closeSpaceRuntime(ctx, spaceId)
	return nil
}

// closeSpaceRuntime tears down the in-memory side of one space — the
// shared steps of Evict (which stops here) and OffloadSpace (which goes
// on to reclaim disk):
//
//  1. stop this space's watchers (members poller, spaceIndex watcher,
//     account mirror) so nothing writes to the store while it closes;
//  2. close + forget the per-space Store and its allocator/wiring;
//  3. evict the any-sync commonspace handle (closes its storage too).
//
// Idempotent and best-effort: every step tolerates already-gone state,
// so re-running on a closed (or never-opened) space is a no-op. Errors
// are logged, not propagated.
func (s *Service) closeSpaceRuntime(ctx context.Context, spaceId string) {
	s.watchers.stopForSpace(spaceId)

	// Drop pubsub subscriptions + remote interest before the space
	// evicts. Deliberately here — the deliberate teardown funnel — and
	// not in the cache eviction path, so a background eviction can
	// never kill live subscriptions.
	s.app.PubSubCloseSpace(spaceId)

	s.mu.Lock()
	store := s.stores[spaceId]
	delete(s.stores, spaceId)
	delete(s.allocs, spaceId)
	delete(s.spaceIndexIds, spaceId)
	delete(s.spaceIndexWatchers, spaceId)
	delete(s.accountMirrors, spaceId)
	delete(s.memberWatchers, spaceId)
	delete(s.aclMirrorWatchers, spaceId)
	// The mux only holds the stopped watchers above; drop it so a
	// reload starts from an empty subscriber list (rewiring re-adds).
	delete(s.aclMuxes, spaceId)
	delete(s.pubsubAclWired, spaceId)
	s.mu.Unlock()
	if store != nil {
		if err := store.Close(); err != nil {
			offloadLog.Warn("close store", zap.String("spaceId", spaceId), zap.Error(err))
		}
	}

	if err := s.app.EvictSpace(ctx, spaceId); err != nil && !errors.Is(err, ocache.ErrNotExists) {
		offloadLog.Warn("evict space", zap.String("spaceId", spaceId), zap.Error(err))
	}
}

// dropSpaceCollections deletes every any-store collection belonging to
// spaceId from the shared SDK DB. Three name shapes are covered:
//   - space-scoped: `<spaceId>_objects`, `<spaceId>__detached`
//   - per-object datasets: `<objectId>_<dataset>` for every object in
//     the space (object ids enumerated from `<spaceId>_objects` before
//     it is dropped) — this also catches type-owned `<typeId>_shortIds`
//     / `<typeId>_properties`, since types are objects in the space.
//
// The shared `_meta` collection is purged per-row (PurgeSpaceMeta), not
// dropped: it holds other spaces' rows too. Purging is required, not
// hygiene — a space CAN re-materialize (guest rejoin), and a stale
// MaxAddSeq watermark would make the rebuilt controllers skip the whole
// cold-restore replay: synced trees, zero rows. Dropping the space's
// `space:<id>` row also rotates the change-feed generation, so
// consumers full-reindex instead of trusting a renumbered applySeq axis.
//
// The sweep commits in CHUNKS of offloadDropChunk drops, each its own
// WriteTx, in a fixed order: meta purge FIRST (own chunk), then the
// per-object drops, then `<spaceId>_objects` last. It returns the
// first chunk error (meta purge, failed drop or commit, enumeration),
// leaving the rest of the sweep for the next attempt.
func (s *Service) dropSpaceCollections(ctx context.Context, spaceId string) error {
	objectIds, enumErr := s.spaceObjectIds(ctx, spaceId)
	if enumErr != nil {
		offloadLog.Warn("enumerate space objects", zap.String("spaceId", spaceId), zap.Error(enumErr))
	}

	names, err := s.db.GetCollectionNames(ctx)
	if err != nil {
		return err
	}

	objectsColl := spaceId + "_" + spaceobjects.SpaceObjectsCollection
	spacePrefix := spaceId + "_"
	toDrop := make([]string, 0, len(names))
	for _, name := range names {
		if name == objectsColl {
			continue
		}
		if strings.HasPrefix(name, spacePrefix) || ownedByObject(name, objectIds) {
			toDrop = append(toDrop, name)
		}
	}

	// Meta dies BEFORE collections, always — same invariant as the
	// startup GC sweep (gc.go): a stale MaxAddSeq watermark outliving
	// its dropped collections makes a re-materialized space skip the
	// cold-restore replay — synced trees, zero rows — while the reverse
	// leak (meta purged, collections still present, then failure)
	// re-flags the owner on the next attempt and converges. Own chunk,
	// so its progress persists independently of later failures.
	if pErr := s.purgeMetaChunk(ctx, spaceId, objectIds); pErr != nil {
		return pErr
	}

	// Chunked commits (see offloadDropChunk). On a ctx that already
	// carries a caller tx, each chunk's WriteTx savepoint-joins it —
	// chunking degrades to savepoints inside that tx and the caller
	// keeps commit/rollback authority. A chunk failure returns
	// immediately: prior chunks are durable, the remaining collections
	// are left for the next attempt.
	for start := 0; start < len(toDrop); start += offloadDropChunk {
		if cErr := s.dropChunk(ctx, toDrop[start:min(start+offloadDropChunk, len(toDrop))]); cErr != nil {
			return cErr
		}
	}

	// `<spaceId>_objects` is the enumeration source for the per-object
	// sweep, so it goes LAST — only when enumeration succeeded AND every
	// prior chunk committed. A failure anywhere above (returned, so
	// OffloadSpace keeps the retry gate armed) leaves the next attempt
	// (re-offload or the startup orphan GC) able to re-enumerate instead
	// of leaking the per-object collections permanently.
	if enumErr != nil {
		return enumErr
	}
	return s.dropChunk(ctx, []string{objectsColl})
}

// offloadDropChunk bounds how many collection drops share one WriteTx.
// Trade-off: any-store has a single global writer, so one tx for the
// whole sweep stalls every local write for the sweep's duration (worst
// caller: the background deletion reconciler offloading a large
// remote-deleted space), while per-drop commits make offload wall-clock
// O(objects). Chunking keeps commits O(objects/N), bounds the
// writer-lock stall to one chunk, persists progress incrementally (a
// failed chunk leaves prior chunks committed for the retry), and caps
// the life of any savepoint orphaned by a failed inner drop to its
// chunk.
const offloadDropChunk = 256

// sweepChunkCommitted, when set, observes every committed sweep chunk
// (n = ops in the chunk). Test seam.
var sweepChunkCommitted func(n int)

// dropChunk drops one chunk of collections under a single WriteTx.
// A not-found drop is ignored (already gone); any other drop error
// aborts the chunk — the tx rolls back (killing any savepoint the
// failed drop orphaned) and the error propagates, same as a commit
// failure: none of the chunk's drops persisted, the retry redoes them.
func (s *Service) dropChunk(ctx context.Context, names []string) error {
	tx, err := s.db.WriteTx(ctx)
	if err != nil {
		return err
	}
	// No-op after Commit; on an error or panic mid-chunk it rolls the
	// chunk back and releases the write lock.
	defer func() { _ = tx.Rollback() }()
	txCtx := tx.Context()
	for _, name := range names {
		if dErr := s.dropCollection(txCtx, name); dErr != nil {
			return dErr
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if sweepChunkCommitted != nil {
		sweepChunkCommitted(len(names))
	}
	return nil
}

// purgeMetaChunk runs PurgeSpaceMeta in its own chunk tx. A missing
// _meta collection is the already-gone case (nothing ever watermarked);
// any other error is returned — leaving a stale watermark behind is
// exactly the state offload must not commit to.
func (s *Service) purgeMetaChunk(ctx context.Context, spaceId string, objectIds map[string]struct{}) error {
	metaColl, err := s.db.OpenCollection(ctx, crdt.MetaCollectionName)
	if err != nil {
		if errors.Is(err, anystore.ErrCollectionNotFound) {
			return nil
		}
		return err
	}
	tx, err := s.db.WriteTx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	ids := make([]string, 0, len(objectIds))
	for id := range objectIds {
		ids = append(ids, id)
	}
	if pErr := crdt.PurgeSpaceMeta(tx.Context(), metaColl, spaceId, ids); pErr != nil {
		return pErr
	}
	if cErr := tx.Commit(); cErr != nil {
		return cErr
	}
	if sweepChunkCommitted != nil {
		sweepChunkCommitted(len(ids) + 1)
	}
	return nil
}

// spaceObjectIds reads the ids of every object in the space from its
// `<spaceId>_objects` collection. Returns nil (no error) when the
// collection was never created.
func (s *Service) spaceObjectIds(ctx context.Context, spaceId string) (map[string]struct{}, error) {
	collName := spaceId + "_" + spaceobjects.SpaceObjectsCollection
	coll, err := s.db.OpenCollection(ctx, collName)
	if err != nil {
		if errors.Is(err, anystore.ErrCollectionNotFound) {
			return nil, nil
		}
		return nil, err
	}
	iter, err := coll.Find(nil).Iter(ctx)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	ids := make(map[string]struct{})
	for iter.Next() {
		doc, derr := iter.Doc()
		if derr != nil {
			continue
		}
		if id := doc.Value().GetString("id"); id != "" {
			ids[id] = struct{}{}
		}
	}
	return ids, iter.Err()
}

// ownedByObject reports whether a collection name is a per-object
// dataset (`<objectId>_<dataset>`) for one of the space's objects.
func ownedByObject(name string, objectIds map[string]struct{}) bool {
	i := strings.IndexByte(name, '_')
	if i <= 0 {
		return false
	}
	_, ok := objectIds[name[:i]]
	return ok
}

// dropCollection opens and drops one collection. A not-found is the
// already-gone case: ignored, nil. Any other error is logged and
// returned — best-effort stops at the not-found class; a real failed
// drop swallowed here would leak the collection while the caller
// reports success (see dropChunk / OffloadSpace).
func (s *Service) dropCollection(ctx context.Context, name string) error {
	coll, err := s.db.OpenCollection(ctx, name)
	if err != nil {
		if errors.Is(err, anystore.ErrCollectionNotFound) {
			return nil
		}
		offloadLog.Warn("open collection for drop", zap.String("coll", name), zap.Error(err))
		return err
	}
	if err := coll.Drop(ctx); err != nil {
		offloadLog.Warn("drop collection", zap.String("coll", name), zap.Error(err))
		return err
	}
	return nil
}
