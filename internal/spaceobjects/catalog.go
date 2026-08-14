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
	"github.com/anyproto/any-store/v2/query"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/types"
)

// catalogTypesFilter selects live `__type__` rows — same condition the
// types API uses for List, compiled once.
var catalogTypesFilter = query.MustParseCondition(
	`{"any.types":{"$in":["__type__"]},"_deletedAt":{"$exists":false}}`,
)

// catalogSnapshot is the immutable resolved catalog. byName excludes
// names shadowed by built-ins / config-registered datasets (the static
// catalog always wins) and resolves cross-type name conflicts to the
// head record with the smallest creation `_ver.id`.
type catalogSnapshot struct {
	byName map[string]types.CompiledDataset
	byType map[string][]types.CompiledDataset
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
// snapshot: static catalog names (built-ins + config-registered
// datasets) always win; cross-type conflicts resolve to the smallest
// head-creation `_ver.id` — deterministic on converged defs.
func (s *Store) resolveCatalog(byType map[string][]types.CompiledDataset) *catalogSnapshot {
	byName := make(map[string]types.CompiledDataset)
	for _, list := range byType {
		for _, ds := range list {
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
	return &catalogSnapshot{byName: byName, byType: byType}
}

// catalogWins reports whether candidate beats the current winner: the
// smaller head-creation `_ver.id` (first writer in the converged
// version order) wins; equal versions (impossible across trees,
// defensive) tiebreak on DefId.
func catalogWins(candidate, winner types.CompiledDataset) bool {
	if candidate.CreatedVer != winner.CreatedVer {
		return candidate.CreatedVer < winner.CreatedVer
	}
	return candidate.DefId < winner.DefId
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
	iter, err := coll.Find(catalogTypesFilter).Iter(ctx)
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
