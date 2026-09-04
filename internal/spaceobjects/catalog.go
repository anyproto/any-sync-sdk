package spaceobjects

// Runtime dataset catalog: the store-level snapshot of every dataset a
// type declares inside its parts, compiled from type objects'
// `datasets` records.
//
// Built once at store open (one indexed pass over the shared objects
// collection's `__type__` rows + one compile per type object) and
// refreshed ONLY when a dataset-defs change applies (afterApplyFor →
// refreshType) — never on controller construction, which happens on
// every object load and must stay free of storage scans. Readers take
// the copy-on-write snapshot through one atomic load.
//
// Two kinds of entry come out of a compile. A NAMESPACED dataset
// (`<typeId>_<key>`) gets its own registration — the generic schema
// handler for `records`, the module's factory for anything else — and
// is owned by exactly one type. A SHARED dataset names the module's
// canonical collection, which every controller registers statically;
// the catalog only records which types own it.

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/handler"
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

// catalogSnapshot is the immutable resolved catalog.
type catalogSnapshot struct {
	// byName holds the namespaced datasets by collection name. Invalid
	// definitions never enter; namespaced names cannot collide across
	// types by construction.
	byName map[string]types.CompiledDataset
	// byType holds every compiled type, invalid entries included, for
	// the management reads and the purge paths.
	byType map[string]*types.CompiledType
	// regs holds one pre-built registration per byName entry — the
	// generic schema handler for records, a module instance otherwise.
	// Handlers are read-only after construction, so sharing them across
	// controllers is safe.
	regs map[string]crdt.HandlerReg
	// sharedOwners maps a module's canonical collection to the types
	// declaring a shared dataset of it; moduleOwners maps a module name
	// to the types declaring any dataset of it (shared or namespaced).
	sharedOwners map[string]map[string]struct{}
	moduleOwners map[string]map[string]struct{}
	// moduleOf maps a namespaced collection to its module.
	moduleOf map[string]string
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

// lookup resolves a namespaced runtime dataset by collection name.
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
	byType := make(map[string]*types.CompiledType, len(typeIds))
	for _, typeId := range typeIds {
		compiled, cerr := types.CompileTypeParts(ctx, s.db, typeId, s.moduleInfos)
		if cerr != nil {
			storeLog.Warn("catalog: compile failed", zap.String("typeId", typeId), zap.Error(cerr))
			continue
		}
		if compiled != nil && len(compiled.Datasets) > 0 {
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
	compiled, err := types.CompileTypeParts(ctx, s.db, typeId, s.moduleInfos)
	if err != nil {
		storeLog.Warn("catalog: refresh compile failed", zap.String("typeId", typeId), zap.Error(err))
		return
	}
	s.catalog.mu.Lock()
	defer s.catalog.mu.Unlock()
	prev := s.catalog.snap.Load()
	byType := make(map[string]*types.CompiledType, len(prev.byType)+1)
	for k, v := range prev.byType {
		byType[k] = v
	}
	if compiled == nil || len(compiled.Datasets) == 0 {
		delete(byType, typeId)
	} else {
		byType[typeId] = compiled
	}
	s.catalog.snap.Store(s.resolveCatalog(byType))
}

// resolveCatalog folds per-type compiles into the resolved snapshot:
// invalid definitions never register; a shared dataset only adds its
// type to the canonical collection's owner set; a namespaced dataset
// gets its registration built from the module (or the generic schema
// handler for records).
func (s *Store) resolveCatalog(byType map[string]*types.CompiledType) *catalogSnapshot {
	snap := &catalogSnapshot{
		byName:       map[string]types.CompiledDataset{},
		byType:       byType,
		regs:         map[string]crdt.HandlerReg{},
		sharedOwners: map[string]map[string]struct{}{},
		moduleOwners: map[string]map[string]struct{}{},
		moduleOf:     map[string]string{},
	}
	own := func(set map[string]map[string]struct{}, key, typeId string) {
		m := set[key]
		if m == nil {
			m = map[string]struct{}{}
			set[key] = m
		}
		m[typeId] = struct{}{}
	}
	for _, ct := range byType {
		for _, ds := range ct.Datasets {
			if ds.Invalid {
				continue
			}
			if ds.Shared {
				if _, static := s.dataVersions[ds.Name]; !static {
					// The compile already refused a shared dataset of a
					// module without a canonical; a canonical the store
					// does not register is a module the config lacks.
					storeLog.Warn("catalog: shared dataset of an unregistered module",
						zap.String("collection", ds.Name), zap.String("typeId", ds.TypeId))
					continue
				}
				own(snap.sharedOwners, ds.Name, ds.TypeId)
				own(snap.moduleOwners, ds.Module, ds.TypeId)
				continue
			}
			if _, static := s.dataVersions[ds.Name]; static {
				storeLog.Warn("catalog: dataset name shadowed by static catalog",
					zap.String("name", ds.Name), zap.String("typeId", ds.TypeId))
				continue
			}
			reg, err := s.instanceReg(ds)
			if err != nil {
				// A compiled (valid) declaration is also constructible;
				// failure means catalog-layer drift — drop the dataset
				// rather than wedge every object load.
				storeLog.Warn("catalog: registration construction failed",
					zap.String("dataset", ds.Name), zap.Error(err))
				continue
			}
			snap.byName[ds.Name] = ds
			snap.regs[ds.Name] = reg
			snap.moduleOf[ds.Name] = ds.Module
			own(snap.moduleOwners, ds.Module, ds.TypeId)
		}
	}
	return snap
}

// instanceReg builds the registration for one namespaced dataset: the
// generic schema handler over the compiled declaration for records, the
// module's factory output for a module instance.
func (s *Store) instanceReg(ds types.CompiledDataset) (crdt.HandlerReg, error) {
	if ds.Module == types.RecordsModule {
		sh, err := crdt.NewSchemaHandler(ds.Schema)
		if err != nil {
			return crdt.HandlerReg{}, err
		}
		return crdt.HandlerReg{
			Name:        ds.Name,
			Handler:     sh,
			Schema:      ds.Schema,
			SchemaRev:   ds.SchemaRev,
			SkipHistory: ds.SkipHistory,
			Version:     crdt.SchemaHandlerVersion,
		}, nil
	}
	m, ok := s.modules[ds.Module]
	if !ok {
		return crdt.HandlerReg{}, errors.New("module not registered: " + ds.Module)
	}
	reg, err := moduleReg(m, handler.ModuleInstance{
		TypeId: ds.TypeId, Key: ds.Key, Collection: ds.Name,
	})
	if err != nil {
		return crdt.HandlerReg{}, err
	}
	reg.SchemaRev = ds.SchemaRev
	return reg, nil
}

// moduleReg instantiates a module for one collection and turns the
// returned dataset into a controller registration. A nil Handler gets
// the generic schema handler over the module's schema.
func moduleReg(m handler.Module, inst handler.ModuleInstance) (crdt.HandlerReg, error) {
	d := m.New(inst)
	h := d.Handler
	version := crdt.NormalizedVersion(m.HandlerVersion)
	if h == nil {
		sh, err := crdt.NewSchemaHandler(datasetSchema(d))
		if err != nil {
			return crdt.HandlerReg{}, err
		}
		h = sh
		version = crdt.ComposeVersion(crdt.SchemaHandlerVersion, version)
	}
	return crdt.HandlerReg{
		Name:                  inst.Collection,
		Handler:               h,
		Indexes:               d.Indexes,
		Schema:                datasetSchema(d),
		Version:               version,
		ReadTracking:          d.ReadTracking,
		SkipHistory:           d.SkipHistory,
		DisableFilteredReplay: d.DisableFilteredReplay,
	}, nil
}

// sortedCatalogNames returns the snapshot's namespaced collection
// names in stable order so controller reg sets are deterministic across
// loads.
func sortedCatalogNames(snap *catalogSnapshot) []string {
	if len(snap.byName) == 0 {
		return nil
	}
	names := make([]string, 0, len(snap.byName))
	for name := range snap.byName {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// sortedOwners returns an owner set as a sorted slice.
func sortedOwners(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
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
