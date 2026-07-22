package spaceimpl

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/ipfs/go-cid"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
)

var gcLog = logger.NewNamed("sdk.collectiongc")

// SweepOrphanCollections is the startup orphan-collection GC: it drops
// every SDK CRDT collection whose owner — the `<spaceId>_` or
// `<objectId>_` name prefix — no longer exists. Offload and object
// purge are best-effort, so a crash mid-teardown (or a deletion from a
// build that predated the per-object drop) can leak per-object
// collections in the shared DB forever; this sweep turns every such
// leak, past or future, into a self-healing condition.
//
// Call once at SDK open, after the tech space is up and BEFORE any
// other space loads — with no applies running, the liveness reads
// below can't race an in-flight object materialization. Best-effort:
// a failed sweep is logged and boot continues.
//
// The sweep is scoped to one DB: names, rosters and meta rows are all
// read from s.db. If storage ever moves to per-space DBs (dbRouter,
// docs/06 § Storage Topology), space offload becomes a file delete and
// this same per-DB pass — run against each space DB — keeps healing
// object-purge leaks inside live spaces.
func (s *Service) SweepOrphanCollections(ctx context.Context) {
	rows := s.tsp.List(ctx)
	if len(rows) == 0 {
		// List returns nil for "no spaces" AND for every error (index
		// not open, ocache failure). Mistaking an errored list for an
		// empty account would classify every space dead and drop the
		// lot — and a genuinely fresh account has nothing to sweep —
		// so zero rows always means skip.
		gcLog.Debug("skip sweep: empty space list")
		return
	}
	liveSpaces := map[string]struct{}{s.tsp.SpaceId(): {}}
	deadSpaces := map[string]struct{}{}
	for _, r := range rows {
		// Pending rows (joining / incoming 1-1 / pending invite) count
		// live: pre-guard builds may have materialized their storage,
		// and the eager-loader's contract is that acceptance loads it
		// as-is (see sdk.Open) — the sweep must honor the same rule.
		if r.IsDeleted() {
			deadSpaces[r.Id] = struct{}{}
		} else {
			liveSpaces[r.Id] = struct{}{}
		}
	}
	if err := s.sweepOrphans(ctx, liveSpaces, deadSpaces); err != nil {
		gcLog.Warn("orphan-collection sweep aborted", zap.Error(err))
	}
}

// sweepOrphans runs one sweep against the given space classification.
//
// Sized for millions of objects: no live-object roster is ever
// materialized. Candidates come from the collection catalog alone;
// liveness is decided per unique owner with POINT reads — the `_meta`
// row first (one FindId resolves every scoped object), the live
// spaces' `objects` rosters as FindId fallback for legacy pre-scoping
// rows. Names are sorted so an owner's collections are adjacent and
// the decision runs once per owner, and every read errs toward "live"
// — a collection is only ever dropped on positive evidence of a dead
// owner.
func (s *Service) sweepOrphans(ctx context.Context, liveSpaces, deadSpaces map[string]struct{}) error {
	names, err := s.db.GetCollectionNames(ctx)
	if err != nil {
		return fmt.Errorf("list collections: %w", err)
	}
	sort.Strings(names)

	var metaColl anystore.Collection
	if coll, merr := s.db.OpenCollection(ctx, crdt.MetaCollectionName); merr == nil {
		metaColl = coll
	} else if !errors.Is(merr, anystore.ErrCollectionNotFound) {
		return fmt.Errorf("open meta: %w", merr)
	}
	rosters := newRosterCache(s.db, liveSpaces)

	// Meta dies BEFORE collections, always: a stale watermark on a
	// re-materializable owner makes the rebuilt controllers skip the
	// cold-restore replay (see PurgeSpaceMeta) — synced trees, zero
	// rows — while the reverse crash (meta purged, collections
	// leaked) re-flags the owner on the next boot and converges.
	// Dead tombstone rows are also purged UNCONDITIONALLY, not just
	// when collections still exist: a crash between an offload's (or
	// this sweep's) collection drops and its meta purge leaves
	// meta-only leaks nothing else would ever revisit.
	metaPurgedSpaces := map[string]struct{}{}
	purgeSpaceMeta := func(spaceId string) {
		if metaColl == nil {
			return
		}
		if _, done := metaPurgedSpaces[spaceId]; done {
			return
		}
		metaPurgedSpaces[spaceId] = struct{}{}
		if perr := crdt.PurgeSpaceMeta(ctx, metaColl, spaceId, nil); perr != nil {
			gcLog.Warn("purge space meta", zap.String("spaceId", spaceId), zap.Error(perr))
		}
	}
	for spaceId := range deadSpaces {
		purgeSpaceMeta(spaceId)
	}

	var dropped int
	sweptSpaces := map[string]struct{}{}
	sweptObjects := map[string]struct{}{}
	lastOwner, lastDead := "", false
	for _, name := range names {
		owner, kind := collectionOwner(name)
		switch kind {
		case ownerSpace:
			if _, live := liveSpaces[owner]; live {
				continue
			}
			// Dead tombstone row (meta purged above) or a ghost with
			// no row at all — both dead owners.
			purgeSpaceMeta(owner)
			s.dropCollection(ctx, name)
			dropped++
			sweptSpaces[owner] = struct{}{}
		case ownerObject:
			if owner != lastOwner {
				lastOwner = owner
				var keepMeta bool
				lastDead, keepMeta = s.objectDead(ctx, metaColl, rosters, owner, liveSpaces)
				if lastDead && !keepMeta {
					// keepMeta exempts purged-object rows in live
					// spaces: the sticky del marker is the
					// change-feed deletion signal.
					sweptObjects[owner] = struct{}{}
					if metaColl != nil {
						if derr := metaColl.DeleteId(ctx, owner); derr != nil && !errors.Is(derr, anystore.ErrDocNotFound) {
							gcLog.Warn("purge object meta", zap.String("objectId", owner), zap.Error(derr))
						}
					}
				}
			}
			if !lastDead {
				continue
			}
			s.dropCollection(ctx, name)
			dropped++
		}
	}

	if dropped > 0 {
		gcLog.Info("orphan collections swept",
			zap.Int("collections", dropped),
			zap.Int("spaces", len(sweptSpaces)),
			zap.Int("objects", len(sweptObjects)))
	}
	return nil
}

// objectDead decides one object owner's fate. dead: no live space
// claims the object — its collections drop. keepMeta: the `_meta` row
// carries the sticky purge marker for a live space (a pre-drop purge
// leak) — the collections go but the row stays as the change-feed
// deletion signal.
//
// Read order matters for the (tiny, boot-time) concurrency window: the
// candidate came from a catalog listing taken earlier, and both reads
// here happen after it — an object materializing in between is seen
// live. Any read error also resolves to live: never drop on
// uncertainty.
func (s *Service) objectDead(ctx context.Context, metaColl anystore.Collection, rosters *rosterCache, owner string, liveSpaces map[string]struct{}) (dead, keepMeta bool) {
	if metaColl != nil {
		doc, err := metaColl.FindId(ctx, owner)
		switch {
		case err == nil:
			spaceId, deleted := crdt.MetaOwnership(doc.Value())
			if spaceId == "" {
				// Pre-scoping row: no ownership evidence either way,
				// and base-dataset-only objects legitimately have no
				// roster row — uncertainty resolves live. The row
				// backfills `sp` on the object's next change.
				return false, false
			}
			if _, live := liveSpaces[spaceId]; live {
				return deleted, deleted
			}
			// Scoped to a dead/unknown space — dead, unless a live
			// roster still claims it (fall through to be safe).
		case !errors.Is(err, anystore.ErrDocNotFound):
			gcLog.Warn("read object meta", zap.String("objectId", owner), zap.Error(err))
			return false, false
		}
	}
	// Meta-less objects (and dead-`sp` fall-through): point lookup in
	// each live space's objects roster.
	return !rosters.contains(ctx, owner), false
}

// rosterCache lazily opens the live spaces' `<spaceId>_objects`
// collections for point membership lookups. Handles are cached; a
// space that never created its roster resolves to "not a member".
type rosterCache struct {
	db     anystore.DB
	spaces []string
	colls  map[string]anystore.Collection
}

func newRosterCache(db anystore.DB, liveSpaces map[string]struct{}) *rosterCache {
	spaces := make([]string, 0, len(liveSpaces))
	for id := range liveSpaces {
		spaces = append(spaces, id)
	}
	sort.Strings(spaces)
	return &rosterCache{db: db, spaces: spaces, colls: map[string]anystore.Collection{}}
}

// contains reports whether any live space's roster has an objectId row.
// Open/read errors count as membership — never drop on uncertainty.
func (r *rosterCache) contains(ctx context.Context, objectId string) bool {
	for _, spaceId := range r.spaces {
		coll, ok := r.colls[spaceId]
		if !ok {
			opened, err := r.db.OpenCollection(ctx, spaceId+"_"+spaceobjects.SpaceObjectsCollection)
			if err != nil {
				if !errors.Is(err, anystore.ErrCollectionNotFound) {
					gcLog.Warn("open objects roster", zap.String("spaceId", spaceId), zap.Error(err))
					return true
				}
				opened = nil // roster never created — remember the miss
			}
			r.colls[spaceId] = opened
			coll = opened
		}
		if coll == nil {
			continue
		}
		if _, err := coll.FindId(ctx, objectId); err == nil {
			return true
		} else if !errors.Is(err, anystore.ErrDocNotFound) {
			gcLog.Warn("read objects roster", zap.String("spaceId", spaceId), zap.Error(err))
			return true
		}
	}
	return false
}

// ownerKind classifies a collection-name owner prefix.
type ownerKind int

const (
	// ownerNone — shared/fixed collection (`_meta`, `files_local`,
	// `_history_traces`, …); never swept.
	ownerNone ownerKind = iota
	ownerSpace
	ownerObject
)

// collectionOwner splits a collection name at its first `_` and
// classifies the prefix by shape: `<cid>.<repKey>` → space,
// bare cid → object, anything else → none. The shape check is what
// keeps the sweep structurally unable to touch a shared collection,
// present or future — their prefixes are never valid content ids.
func collectionOwner(name string) (owner string, kind ownerKind) {
	i := strings.IndexByte(name, '_')
	if i <= 0 {
		return "", ownerNone
	}
	owner = name[:i]
	if validateSpaceId(owner) == nil {
		return owner, ownerSpace
	}
	if _, err := cid.Decode(owner); err == nil {
		return owner, ownerObject
	}
	return "", ownerNone
}
