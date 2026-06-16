package spaceimpl

import (
	"context"
	"errors"
	"strings"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/app/logger"
	"go.uber.org/zap"

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
// Idempotent and best-effort: each step tolerates already-gone state so
// a re-run after a crash mid-offload, or an offload of a space that was
// never fully loaded, completes without error. Errors from individual
// disk steps are logged, not propagated — a half-offloaded space is
// reclaimed on the next attempt and never resurrected (the row stays
// deleted).
func (s *Service) OffloadSpace(ctx context.Context, spaceId string) {
	// 1. Stop this space's watchers (members poller, spaceIndex watcher,
	// account mirror) so nothing writes to the store or carrier while we
	// tear them down.
	s.watchers.stopForSpace(spaceId)

	// 2. Close + forget the per-space Store and its allocator/wiring.
	s.mu.Lock()
	store := s.stores[spaceId]
	delete(s.stores, spaceId)
	delete(s.allocs, spaceId)
	delete(s.spaceIndexIds, spaceId)
	delete(s.spaceIndexWatchers, spaceId)
	delete(s.accountMirrors, spaceId)
	delete(s.memberWatchers, spaceId)
	s.mu.Unlock()
	if store != nil {
		if err := store.Close(); err != nil {
			offloadLog.Warn("close store", zap.String("spaceId", spaceId), zap.Error(err))
		}
	}

	// 3. Evict the any-sync commonspace handle (closes its storage too).
	if err := s.app.EvictSpace(ctx, spaceId); err != nil {
		offloadLog.Warn("evict space", zap.String("spaceId", spaceId), zap.Error(err))
	}

	// 4. Drop every SDK CRDT collection for the space from the shared DB.
	if err := s.dropSpaceCollections(ctx, spaceId); err != nil {
		offloadLog.Warn("drop collections", zap.String("spaceId", spaceId), zap.Error(err))
	}

	// 5. Remove the any-sync per-space DB file (the bulk of the footprint).
	if err := s.app.DeleteSpaceStorage(ctx, spaceId); err != nil {
		offloadLog.Warn("delete storage", zap.String("spaceId", spaceId), zap.Error(err))
	}

	// 6. Drop the account-values carrier tree in tech space.
	if err := s.tsp.DropAccountValues(ctx, spaceId); err != nil {
		offloadLog.Debug("drop account values", zap.String("spaceId", spaceId), zap.Error(err))
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
// The global `_meta` watermark collection is shared across spaces and
// intentionally left alone; stale rows for offloaded objects are inert.
func (s *Service) dropSpaceCollections(ctx context.Context, spaceId string) error {
	objectIds, err := s.spaceObjectIds(ctx, spaceId)
	if err != nil {
		offloadLog.Debug("enumerate space objects", zap.String("spaceId", spaceId), zap.Error(err))
	}

	names, err := s.db.GetCollectionNames(ctx)
	if err != nil {
		return err
	}

	spacePrefix := spaceId + "_"
	for _, name := range names {
		if strings.HasPrefix(name, spacePrefix) || ownedByObject(name, objectIds) {
			s.dropCollection(ctx, name)
		}
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

// dropCollection opens and drops one collection, ignoring a
// not-found (already gone). Errors are logged, not returned — offload
// is best-effort.
func (s *Service) dropCollection(ctx context.Context, name string) {
	coll, err := s.db.OpenCollection(ctx, name)
	if err != nil {
		if !errors.Is(err, anystore.ErrCollectionNotFound) {
			offloadLog.Warn("open collection for drop", zap.String("coll", name), zap.Error(err))
		}
		return
	}
	if err := coll.Drop(ctx); err != nil {
		offloadLog.Warn("drop collection", zap.String("coll", name), zap.Error(err))
	}
}
