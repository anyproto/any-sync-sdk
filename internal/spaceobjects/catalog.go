package spaceobjects

// Runtime dataset catalog (SYN-147): the store-level snapshot of every
// user-defined dataset compiled from type objects' `datasets` records.
//
// Built once at store open (one indexed pass over the shared objects
// collection's `__type__` rows + one defs compile per type object) and
// refreshed ONLY when a dataset-defs change applies (afterApplyFor →
// refreshType) — never on controller construction, which happens on
// every object load and must stay free of storage scans. Readers take
// the copy-on-write snapshot through one atomic load.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/types"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
)

// LiveTypeRowsFilter selects live `__type__` rows from the per-space
// objects collection. Built once with the typed query package; shared
// with the types API's List so the marker/tombstone condition has a
// single definition.
var LiveTypeRowsFilter query.Filter = func() query.Filter {
	a := &anyenc.Arena{}
	return query.And{
		query.Key{Path: []string{"any", "types"}, Filter: query.NewInValue(a.NewString(typetype.MetaTypeMarker))},
		query.Key{Path: []string{"_deletedAt"}, Filter: query.Not{Filter: query.Exists{}}},
	}
}()

// catalogSnapshot is the immutable resolved catalog. byName excludes
// invalid definitions and names shadowed by built-ins /
// config-registered datasets (the static catalog always wins), and
// resolves cross-type name conflicts to the smallest DefId. handlers
// holds one SchemaHandler per registered dataset, built once per
// snapshot and shared across controllers — SchemaHandler is read-only
// after construction, so sharing is safe (unlike registry-holding
// handlers).
type catalogSnapshot struct {
	byName   map[string]types.CompiledDataset
	byType   map[string][]types.CompiledDataset
	handlers map[string]*crdt.SchemaHandler
}

var emptyCatalogSnapshot = &catalogSnapshot{}

// runtimeCatalog holds the snapshot pointer; mu serializes rebuilders
// (refreshType clones + swaps under it), readers are lock-free.
type runtimeCatalog struct {
	snap atomic.Pointer[catalogSnapshot]
	mu   sync.Mutex
}

func newRuntimeCatalog() *runtimeCatalog {
	c := &runtimeCatalog{}
	c.snap.Store(emptyCatalogSnapshot)
	return c
}

// snapshot returns the current catalog — one atomic load, hot-path safe.
func (c *runtimeCatalog) snapshot() *catalogSnapshot {
	if c == nil {
		return emptyCatalogSnapshot
	}
	return c.snap.Load()
}

// lookup resolves a runtime dataset by collection name.
func (c *runtimeCatalog) lookup(name string) (types.CompiledDataset, bool) {
	ds, ok := c.snapshot().byName[name]
	return ds, ok
}

// initCatalog builds the full snapshot at store open. Failures degrade
// to an empty catalog (the drainer/refresh path repairs it as defs
// apply) — a store must open even when the objects collection doesn't
// exist yet (fresh space).
func (s *Store) initCatalog(ctx context.Context) {
	if s.catalog == nil || s.db == nil {
		return
	}
	typeIds, err := s.catalogTypeIds(ctx)
	if err != nil {
		storeLog.Warn("catalog: initial type scan failed", zap.Error(err))
		return
	}
	byType := make(map[string][]types.CompiledDataset, len(typeIds))
	for _, typeId := range typeIds {
		compiled, cerr := types.CompileDatasetDefs(ctx, s.db, typeId)
		if cerr != nil {
			storeLog.Warn("catalog: compile failed", zap.String("typeId", typeId), zap.Error(cerr))
			continue
		}
		if len(compiled) > 0 {
			byType[typeId] = compiled
		}
	}
	s.catalog.mu.Lock()
	defer s.catalog.mu.Unlock()
	s.catalog.snap.Store(s.resolveCatalog(byType))
}

// catalogHasType reports whether the runtime catalog currently carries
// datasets for typeId — one lock-free snapshot load. The purge paths
// use it to skip per-object catalog rebuilds for ordinary objects.
func (s *Store) catalogHasType(typeId string) bool {
	if s.catalog == nil || typeId == "" {
		return false
	}
	snap := s.catalog.snap.Load()
	if snap == nil {
		return false
	}
	_, ok := snap.byType[typeId]
	return ok
}

// refreshType recompiles one type object's defs and swaps the snapshot.
// Called from afterApplyFor when a `datasets` change applies, and from
// the drain path. Reads committed collections via the DB directly —
// never through the object cache (deadlock: afterApply may run inside a
// LoadFunc, see postValueFor).
func (s *Store) refreshType(ctx context.Context, typeId string) {
	if s.catalog == nil || typeId == "" {
		return
	}
	compiled, err := types.CompileDatasetDefs(ctx, s.db, typeId)
	if err != nil {
		storeLog.Warn("catalog: refresh compile failed", zap.String("typeId", typeId), zap.Error(err))
		return
	}
	s.catalog.mu.Lock()
	defer s.catalog.mu.Unlock()
	prev := s.catalog.snap.Load()
	byType := make(map[string][]types.CompiledDataset, len(prev.byType)+1)
	for k, v := range prev.byType {
		byType[k] = v
	}
	if len(compiled) == 0 {
		delete(byType, typeId)
	} else {
		byType[typeId] = compiled
	}
	s.catalog.snap.Store(s.resolveCatalog(byType))
}

// resolveCatalog folds per-type compiles into the name-resolved
// snapshot: invalid definitions never register; static catalog names
// (built-ins + config-registered datasets) always win; cross-type
// conflicts resolve to the smallest DefId. DefId is content-addressed
// and identical on every replica — creation `_ver.id`s are per-tree
// and NOT comparable across type objects, so no first-writer order
// exists to honor here.
func (s *Store) resolveCatalog(byType map[string][]types.CompiledDataset) *catalogSnapshot {
	byName := make(map[string]types.CompiledDataset)
	for _, list := range byType {
		for _, ds := range list {
			if ds.Invalid {
				continue
			}
			if _, static := s.dataVersions[ds.Name]; static {
				storeLog.Warn("catalog: dataset name shadowed by static catalog",
					zap.String("name", ds.Name), zap.String("typeId", ds.TypeId))
				continue
			}
			if w, dup := byName[ds.Name]; dup && !catalogWins(ds, w) {
				continue
			}
			byName[ds.Name] = ds
		}
	}
	handlers := make(map[string]*crdt.SchemaHandler, len(byName))
	for name, ds := range byName {
		sh, err := crdt.NewSchemaHandler(ds.Schema)
		if err != nil {
			// A compiled (valid) declaration is also constructible;
			// failure means catalog-layer drift — drop the dataset
			// rather than wedge every object load.
			storeLog.Warn("catalog: schema handler construction failed",
				zap.String("dataset", name), zap.Error(err))
			delete(byName, name)
			continue
		}
		handlers[name] = sh
	}
	return &catalogSnapshot{byName: byName, byType: byType, handlers: handlers}
}

// catalogWins resolves a cross-type name conflict: smallest DefId wins
// — DefIds are unique and identical on every replica, so the pick is
// replica-stable. TypeId breaks the (pathological) identical-DefId tie.
func catalogWins(candidate, winner types.CompiledDataset) bool {
	if candidate.DefId != winner.DefId {
		return candidate.DefId < winner.DefId
	}
	return candidate.TypeId < winner.TypeId
}

// catalogTypeIds scans the shared objects collection for live type
// objects. Missing collection (fresh space) → no types.
func (s *Store) catalogTypeIds(ctx context.Context) ([]string, error) {
	coll, err := s.db.OpenCollection(ctx, s.spaceId+"_"+SpaceObjectsCollection)
	if err != nil {
		if errors.Is(err, anystore.ErrCollectionNotFound) {
			return nil, nil
		}
		return nil, err
	}
	iter, err := coll.Find(LiveTypeRowsFilter).Iter(ctx)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []string
	for iter.Next() {
		doc, derr := iter.Doc()
		if derr != nil {
			return nil, derr
		}
		if v := doc.Value(); v != nil {
			if id := v.GetString("id"); id != "" {
				out = append(out, id)
			}
		}
	}
	return out, iter.Err()
}
