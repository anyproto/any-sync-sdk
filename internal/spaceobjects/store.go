// Package spaceobjects owns the per-space cache of *object.Object
// instances. One Store per loaded space; each Store wires the space's
// shared VersionAllocator + the SDK DB into per-objectId Controllers.
//
// Hides the detail that any-sync trees are loaded inside the
// commonspace.Space (which is itself ocache-managed). Callers ask for
// Get(objectId) and get back a ready *object.Object — under the hood
// we go through anysyncx.GetSpace, BuildTree, ColdRestore.
//
// MVP scope: every Object registers the same handler set —
// SystemPropertiesHandler on "properties", typetype.PropertyHandler
// on "defs", DefaultHandler on "shortIds". Type-ness is convention,
// not a separate object kind. Schema validation is wired through the
// LiveRegistry (built-in + registered + user-type schemas): strict on
// local writes (pre-flight), defensive per-op on apply.
package spaceobjects

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree/updatelistener"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/objecttreebuilder"
	"github.com/anyproto/any-sync/util/crypto"

	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/subscribe"
	"github.com/anyproto/any-sync-sdk/internal/types"
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
)

// ErrUnknownDataset is returned by DataVersion when the caller asks
// for a dataset the store doesn't know how to version.
var ErrUnknownDataset = errors.New("spaceobjects: unknown dataset")

// builtinDataVersions is the dataset → DataVersion stamp for the
// SDK's built-in handlers (always registered on every controller).
// External Registrations supplied via NewStore extend this map at
// construction time; collisions with built-ins are rejected up front.
var builtinDataVersions = map[string]string{
	properties.Dataset:           properties.HandlerVersion,
	typetype.DatasetPropertyDefs: typetype.HandlerVersion,
	typetype.ShortIdsDataset:     "shortIds-v1",
}

// SpaceObjectsCollection is the on-disk collection name for the
// per-space values collection. Holds one row per object (regular
// AND type — types are just objects too with `any.name = "Movie"`
// alongside everyone else), keyed by objectId. The CRDT-side dataset
// name (properties.Dataset = "objects") matches by convention but is
// independent.
const SpaceObjectsCollection = "objects"

// CreateOpts is the input to Store.Create. ChangeType lets the caller
// stamp a one-shot label on the root any-sync change ("type" vs
// "object") for log / debug reads of the DAG.
type CreateOpts struct {
	ChangeType    string
	ChangePayload []byte
}

// DeriveOpts is the input to Store.Derive. ChangePayload is the seed
// hashed into the derived id.
type DeriveOpts struct {
	ChangeType    string
	ChangePayload []byte
}

// Store owns per-object Controllers and handed-out *object.Object
// instances for one space. Constructed once per loaded space by the
// space-level service.
//
// Backed by ocache: LoadFunc builds Object + Controller + tree +
// ColdRestore atomically; concurrent Get on the same id is
// serialized by ocache. TTL-evicted Objects detach their synctree
// listener via TryClose, so stale tree references can't fire
// callbacks on a freshly-loaded peer Object.
type Store struct {
	app     *anysyncx.App
	db      anystore.DB
	signKey crypto.PrivKey
	alloc   *object.VersionAllocator
	spaceId string
	reg     *types.LiveRegistry

	// extTypes are the caller-supplied type catalog entries; their
	// handlers are carried onto every per-object Controller
	// alongside the built-ins. dataVersions overlays the built-in
	// map with each type's registered handlers.
	extTypes     []handler.Type
	dataVersions map[string]string
	// datasetOwners maps an external type-owned dataset name to the
	// typeId that owns it (N datasets → one owner). Built from extTypes.
	// Used to enforce that an object implements a type before writing to
	// one of its datasets. Built-in datasets (objects/properties/shortIds)
	// are absent.
	datasetOwners map[string]string

	// cache is the per-space *object.Object cache. LoadFunc holds
	// the per-id lock during build+ColdRestore, so peers waiting on
	// the same id never see partial state.
	cache ocache.OCache

	mu         sync.Mutex
	sharedColl anystore.Collection // per-space `objects` collection, lazy-opened
	detached   anystore.Collection // per-space `_detached` collection, lazy-opened

	// drainer runs Store.Drain asynchronously off the apply path —
	// afterApply hooks push pairs in, the drainer consumes them and
	// coalesces bursts into single Drain passes. See drainer.go.
	drainer *drainer

	// engine drives windowed live queries (Query.Subscribe). The
	// afterApply hook gates on engine.HasSubscribers so the cold-
	// restore path stays a single atomic load when nobody is
	// listening.
	engine *subscribe.Engine

	// customHandlers, when non-nil, makes this a "raw" store: every
	// controller registers EXACTLY these handlers (no shared `objects`
	// collection, no built-in properties/typetype/shortIds regs, no
	// extTypes). Used by the tech space, whose index object holds its
	// own datasets (spaces/profile) with bespoke handlers rather than
	// the regular type/properties model. reg is nil in this mode.
	customHandlers []crdt.HandlerReg
	// disableGate skips the schema-gate on every object. Set together
	// with customHandlers — the gate is a registry/type-system concept
	// the tech space doesn't participate in.
	disableGate bool
}

// objectCacheTTL is the idle window before a cached Object is
// eligible for eviction. objectCacheGC is how often the GC ticker
// runs. Eviction safety relies on Object.TryClose detaching the
// synctree listener under tree.Lock — see object.Close docstring.
const (
	objectCacheTTL = time.Minute
	objectCacheGC  = 20 * time.Second
)

// loadPayloadKey is the context-key type for the optional
// TreeStorageCreatePayload threaded into LoadFunc by Create / Derive
// / PutTreeFromPayload. Absent → LoadFunc takes the BuildTree path.
type loadPayloadKey struct{}

func ctxWithLoadPayload(ctx context.Context, p *treestorage.TreeStorageCreatePayload) context.Context {
	return context.WithValue(ctx, loadPayloadKey{}, p)
}

func loadPayloadFromCtx(ctx context.Context) *treestorage.TreeStorageCreatePayload {
	p, _ := ctx.Value(loadPayloadKey{}).(*treestorage.TreeStorageCreatePayload)
	return p
}

// StoreConfig is the input to NewStoreWithConfig. It carries both the
// regular type/properties path (ExtTypes) and the "raw" tech-space path
// (Handlers + DisableGate + DataVersions). Exactly one path is active:
// when Handlers is non-nil the store skips the LiveRegistry, the shared
// `objects` collection, the built-in handler set, and the schema gate.
type StoreConfig struct {
	App     *anysyncx.App
	DB      anystore.DB
	SignKey crypto.PrivKey
	SpaceId string
	Alloc   *object.VersionAllocator

	// ExtTypes is the caller-supplied type catalog (regular path).
	ExtTypes []handler.Type

	// Handlers, when non-nil, switches the store to raw mode: every
	// controller registers EXACTLY these handlers. DataVersions then
	// supplies the dataset → DataVersion stamp (no built-ins).
	Handlers     []crdt.HandlerReg
	DisableGate  bool
	DataVersions map[string]string
}

// NewStore constructs a regular type/properties-backed Store. The
// allocator is per-space (shared across all objects in this space).
//
// extTypes is the caller-supplied type catalog. Each type's handlers
// are wired onto every per-object Controller built by this store,
// alongside the built-in system handlers. Validation happens in
// ValidateExternalTypes — call it before NewStore at the SDK
// boundary so collisions are caught at Open time.
func NewStore(app *anysyncx.App, db anystore.DB, signKey crypto.PrivKey, spaceId string, alloc *object.VersionAllocator, extTypes []handler.Type) *Store {
	return NewStoreWithConfig(StoreConfig{
		App: app, DB: db, SignKey: signKey, SpaceId: spaceId, Alloc: alloc, ExtTypes: extTypes,
	})
}

// NewStoreWithConfig constructs a Store from cfg. The async drainer is
// built and started here — it lives until Close. The subscribe engine
// is always built (both paths support Query.Subscribe). In raw mode
// (cfg.Handlers != nil) the LiveRegistry is nil and the schema gate is
// disabled — see newController / loadObject.
func NewStoreWithConfig(cfg StoreConfig) *Store {
	s := &Store{
		app:            cfg.App,
		db:             cfg.DB,
		signKey:        cfg.SignKey,
		alloc:          cfg.Alloc,
		spaceId:        cfg.SpaceId,
		engine:         subscribe.New(cfg.SpaceId),
		customHandlers: cfg.Handlers,
		disableGate:    cfg.DisableGate,
	}
	if cfg.Handlers != nil {
		// Raw mode: no registry, no built-in dataset versions; the
		// caller's DataVersions are authoritative.
		s.dataVersions = cfg.DataVersions
	} else {
		dv := make(map[string]string, len(builtinDataVersions))
		for k, v := range builtinDataVersions {
			dv[k] = v
		}
		owners := make(map[string]string)
		for _, t := range cfg.ExtTypes {
			for _, d := range t.Datasets {
				dv[d.Name] = d.DataVersion
				owners[d.Name] = t.Id
			}
		}
		s.reg = types.NewLiveRegistry(cfg.DB, buildStaticSchema(cfg.ExtTypes))
		s.extTypes = cfg.ExtTypes
		s.dataVersions = dv
		s.datasetOwners = owners
	}
	s.cache = ocache.New(
		s.loadObject,
		ocache.WithTTL(objectCacheTTL),
		ocache.WithGCPeriod(objectCacheGC),
	)
	s.drainer = newDrainer(s)
	s.drainer.Run()
	return s
}

// buildStaticSchema assembles the registry's static overlay: the
// schema for types whose definitions don't live in a per-type-object
// `properties` collection — the built-in `any` / `spaceIndex` tables
// and every registered external type's declared Properties. User
// types are absent (resolved from their defs collection at lookup).
//
// Returns typeId → propId → PropInfo. Safe with nil/empty extTypes.
func buildStaticSchema(extTypes []handler.Type) map[string]map[string]types.PropInfo {
	static := make(map[string]map[string]types.PropInfo, 2+len(extTypes))

	anyProps := make(map[string]types.PropInfo, len(anytype.Properties))
	for _, p := range anytype.Properties {
		anyProps[p.Id] = types.PropInfo{Id: p.Id, Name: p.Name, Kind: p.Kind}
	}
	static[anytype.TypeId] = anyProps

	siProps := make(map[string]types.PropInfo, len(spaceindex.Properties))
	for _, p := range spaceindex.Properties {
		siProps[p.Id] = types.PropInfo{Id: p.Id, Name: p.Name, Kind: p.Kind}
	}
	static[spaceindex.TypeId] = siProps

	for _, t := range extTypes {
		if len(t.Properties) == 0 {
			// A registered type that contributes no `objects`-namespace
			// values (owns only separate datasets). Record it as known
			// with an empty prop set so writes under its namespace fail
			// with unknown_property rather than type_unknown.
			static[t.Id] = map[string]types.PropInfo{}
			continue
		}
		props := make(map[string]types.PropInfo, len(t.Properties))
		for _, p := range t.Properties {
			props[p.Id] = types.PropInfo{Id: p.Id, Name: p.Name, Kind: propertyKindToSchema(p.Kind)}
		}
		static[t.Id] = props
	}
	return static
}

// propertyKindToSchema maps the public handler.PropertyKind enum to
// the internal schema.Kind. Returns KindUnknown for the zero value —
// ValidateExternalTypes rejects that up front, so it never reaches a
// live overlay.
func propertyKindToSchema(k handler.PropertyKind) schema.Kind {
	switch k {
	case handler.PropertyKindString:
		return schema.KindString
	case handler.PropertyKindNumber:
		return schema.KindNumber
	case handler.PropertyKindBoolean:
		return schema.KindBoolean
	case handler.PropertyKindNull:
		return schema.KindNull
	case handler.PropertyKindArray:
		return schema.KindArray
	case handler.PropertyKindObject:
		return schema.KindObject
	}
	return schema.KindUnknown
}

// ValidateExternalTypes checks the caller-supplied catalog against
// the built-in dataset names, against each other, and for internal
// well-formedness. Returns the first error encountered. Called once
// at sdk.Open before any Store is constructed.
func ValidateExternalTypes(extTypes []handler.Type) error {
	seenTypeIds := make(map[string]struct{}, len(extTypes))
	seenDatasets := make(map[string]struct{}, len(extTypes))
	for i, t := range extTypes {
		if t.Id == "" {
			return fmt.Errorf("spaceobjects: type[%d]: empty Id", i)
		}
		if _, dup := seenTypeIds[t.Id]; dup {
			return fmt.Errorf("spaceobjects: type[%d]: duplicate type Id %q", i, t.Id)
		}
		seenTypeIds[t.Id] = struct{}{}
		// A type may own datasets, properties, both, or neither (a pure
		// declaration / tag). Only a non-empty Id is required.
		for j, d := range t.Datasets {
			if d.Handler == nil {
				return fmt.Errorf("spaceobjects: type[%d] (%q) dataset[%d]: nil Handler", i, t.Id, j)
			}
			if d.Name == "" {
				return fmt.Errorf("spaceobjects: type[%d] (%q) dataset[%d]: empty Name", i, t.Id, j)
			}
			if d.DataVersion == "" {
				return fmt.Errorf("spaceobjects: type[%d] (%q) dataset[%d] (%q): empty DataVersion", i, t.Id, j, d.Name)
			}
			if _, dup := builtinDataVersions[d.Name]; dup {
				return fmt.Errorf("spaceobjects: type[%d] (%q) dataset[%d]: name %q is reserved by a built-in", i, t.Id, j, d.Name)
			}
			if _, dup := seenDatasets[d.Name]; dup {
				return fmt.Errorf("spaceobjects: type[%d] (%q) dataset[%d]: duplicate dataset name %q across catalog", i, t.Id, j, d.Name)
			}
			seenDatasets[d.Name] = struct{}{}
		}
		seenProps := make(map[string]struct{}, len(t.Properties))
		for k, p := range t.Properties {
			if p.Id == "" {
				return fmt.Errorf("spaceobjects: type[%d] (%q) property[%d]: empty Id", i, t.Id, k)
			}
			if p.Id == "id" || strings.HasPrefix(p.Id, "_") || strings.ContainsAny(p.Id, ".") {
				return fmt.Errorf("spaceobjects: type[%d] (%q) property[%d]: reserved or invalid Id %q (no \"id\", no \"_\" prefix, no dots)", i, t.Id, k, p.Id)
			}
			if propertyKindToSchema(p.Kind) == schema.KindUnknown {
				return fmt.Errorf("spaceobjects: type[%d] (%q) property[%d] (%q): invalid Kind %d", i, t.Id, k, p.Id, p.Kind)
			}
			if _, dup := seenProps[p.Id]; dup {
				return fmt.Errorf("spaceobjects: type[%d] (%q) property[%d]: duplicate property Id %q", i, t.Id, k, p.Id)
			}
			seenProps[p.Id] = struct{}{}
		}
	}
	return nil
}

// Close shuts down per-Store background workers (drainer +
// dispatcher) and tears down the object cache (which closes every
// resident Object). Safe to call multiple times.
func (s *Store) Close() error {
	derr := s.drainer.Close()
	if s.cache != nil {
		_ = s.cache.Close()
	}
	if s.engine != nil {
		_ = s.engine.Close()
	}
	return derr
}

// SubEngine returns the per-space live-query engine. Used by the
// space layer to back Query.Subscribe.
func (s *Store) SubEngine() *subscribe.Engine { return s.engine }

// NamedSchema pairs a dataset name with its declared schema. Returned by
// Schemas for consumer discovery.
type NamedSchema struct {
	Name   string
	Schema schema.Dataset
}

// Schemas returns the declared schema of every dataset this store hosts —
// the same schemas its controllers enforce. Used by the space layer to
// expose dataset discovery to consumers.
func (s *Store) Schemas() []NamedSchema {
	if s.customHandlers != nil {
		out := make([]NamedSchema, 0, len(s.customHandlers))
		for _, h := range s.customHandlers {
			out = append(out, NamedSchema{Name: h.Name, Schema: h.Schema})
		}
		return out
	}
	out := []NamedSchema{
		{Name: properties.Dataset, Schema: objectsDatasetSchema()},
		{Name: typetype.DatasetPropertyDefs, Schema: schema.Dataset{Dynamic: true}},
		{Name: typetype.ShortIdsDataset, Schema: schema.Dataset{Dynamic: true}},
	}
	for _, t := range s.extTypes {
		for _, d := range t.Datasets {
			out = append(out, NamedSchema{Name: d.Name, Schema: datasetSchema(d)})
		}
	}
	return out
}

// datasetSchema resolves an external dataset's declared schema, applying
// the backward-compatible default: a zero Schema (no Fields, not Dynamic)
// is treated as a Dynamic synced keyspace — the pre-schema behavior — so
// handlers that predate Dataset.Schema keep registering unchanged.
func datasetSchema(d handler.Dataset) schema.Dataset {
	if len(d.Schema.Fields) == 0 && !d.Schema.Dynamic {
		return schema.Dataset{Dynamic: true}
	}
	return d.Schema
}

// NotifyDrainer is the public hook used by callers (e.g. the
// space service on first-touch) to trigger an asynchronous Drain
// pass. The afterApply path notifies internally; this is for
// out-of-band wakeups.
func (s *Store) NotifyDrainer(pair types.DataVersionPair) {
	s.drainer.Notify(pair)
}

// Registry returns the per-space LiveRegistry — read-only lookups
// for property kinds, known shortIds, and latest shortIds. Used by
// the writer-side stamping path and the apply-time gate.
func (s *Store) Registry() *types.LiveRegistry { return s.reg }

// ExternalTypes returns the caller-supplied type catalog passed at
// construction time (config.Config.Types). Read-only — the slice is
// shared, callers must not mutate. Surfaced for the public
// space.Types() API to enumerate registered catalog entries
// alongside user-created types.
func (s *Store) ExternalTypes() []handler.Type { return s.extTypes }

// SpaceId returns the id of the space this store serves.
func (s *Store) SpaceId() string { return s.spaceId }

// DatasetOwner returns the typeId that owns an external type-owned
// dataset, or ("", false) for built-in / unknown datasets. Used by the
// write path to enforce that an object implements a type before writing
// into one of its datasets.
func (s *Store) DatasetOwner(dataset string) (string, bool) {
	owner, ok := s.datasetOwners[dataset]
	return owner, ok
}

// ObjectTypes returns the typeIds an object implements (its any.types),
// read from the shared per-space `objects` row. Returns (nil, nil) when
// the object has no row yet — callers treat that as "implements nothing".
func (s *Store) ObjectTypes(ctx context.Context, objectId string) ([]string, error) {
	coll, err := s.SharedObjects(ctx)
	if err != nil {
		return nil, err
	}
	doc, err := coll.FindId(ctx, objectId)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return nil, nil
		}
		return nil, err
	}
	v := doc.Value()
	if v == nil {
		return nil, nil
	}
	arr := v.GetArray("any", "types")
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		out = append(out, string(e.GetStringBytes()))
	}
	return out, nil
}

// RegularObjectCount returns the count of rows in the per-space
// `objects` collection — one row per user-visible regular object.
// Used by the sync-status rollup as the Total denominator.
//
// Returns 0 if the collection hasn't been opened yet (cold start
// before any object lives in this space) or on read error — both
// produce the right rollup result (Synced/0 = trivially Synced).
func (s *Store) RegularObjectCount(ctx context.Context) int {
	coll, err := s.SharedObjects(ctx)
	if err != nil {
		return 0
	}
	n, err := coll.Count(ctx)
	if err != nil {
		return 0
	}
	return n
}

// OpenObjectCollection opens the per-object any-store collection
// `{objectId}/{dataset}` without binding the any-sync tree. Read-only
// callers (Properties.Get, queries against per-object datasets, type
// `defs` reads, etc.) take this route to skip BuildSyncTree +
// ColdRestore — the persisted any-store state is already what they
// want.
//
// Open-only: returns anystore.ErrCollectionNotFound if the writer
// hasn't initialised the dataset yet. Callers should treat that as
// "no records" — equivalent to an empty controller.
func (s *Store) OpenObjectCollection(ctx context.Context, objectId, dataset string) (anystore.Collection, error) {
	if objectId == "" {
		return nil, errors.New("spaceobjects: OpenObjectCollection: objectId required")
	}
	if dataset == "" {
		return nil, errors.New("spaceobjects: OpenObjectCollection: dataset required")
	}
	return s.db.OpenCollection(ctx, objectId+"_"+dataset)
}

// SharedObjects returns the per-space `objects` collection, opening
// it on first call. Callers can hit it directly for cross-object
// queries (find all rows where any.name = X, etc.). First open also
// installs the standing read-side indexes.
func (s *Store) SharedObjects(ctx context.Context) (anystore.Collection, error) {
	s.mu.Lock()
	if s.sharedColl != nil {
		coll := s.sharedColl
		s.mu.Unlock()
		return coll, nil
	}
	s.mu.Unlock()
	collName := s.spaceId + "_" + SpaceObjectsCollection
	coll, err := s.db.Collection(ctx, collName)
	if err != nil {
		return nil, fmt.Errorf("spaceobjects: open %s: %w", collName, err)
	}
	// Sparse index on `any.types` so the type-marker query
	// (typesAPI.List → `any.types $in ["__type__"]`) doesn't scan
	// every object's row. Sparse keeps the index small: only rows
	// that actually carry a type list contribute entries. EnsureIndex
	// is idempotent — safe to call on every fresh open.
	if err := coll.EnsureIndex(ctx, anystore.IndexInfo{
		Fields: []string{"any.types"},
		Sparse: true,
	}); err != nil {
		return nil, fmt.Errorf("spaceobjects: ensure any.types index: %w", err)
	}
	s.mu.Lock()
	if s.sharedColl == nil {
		s.sharedColl = coll
	} else {
		coll = s.sharedColl
	}
	s.mu.Unlock()
	return coll, nil
}

// DataVersion looks up the DataVersion stamp for a known dataset.
// Used by space.Modify to populate crdt.Change.DataVersion. The
// map is the union of the built-ins and any external Registrations
// supplied at construction time.
func (s *Store) DataVersion(dataset string) (string, error) {
	v, ok := s.dataVersions[dataset]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownDataset, dataset)
	}
	return v, nil
}

// Allocator returns the shared per-space VersionAllocator. Exposed so
// the space-level service can reuse it for restore-time bumping.
func (s *Store) Allocator() *object.VersionAllocator { return s.alloc }

// Drop evicts the cached *object.Object for objectId. Used after
// any-sync DeleteTree fires (so a subsequent Get rebuilds or fails
// with the appropriate any-sync deletion error) and by the
// cold-restore catch-up pass to release Objects ASAP.
//
// Does NOT touch the any-store collections — record rows persist
// until a separate cleanup pass. v1: leave them; queries skip
// tombstones, and a deleted object's id is content-addressable so
// it never reuses.
func (s *Store) Drop(objectId string) {
	_, _ = s.cache.Remove(context.Background(), objectId)
}

// PutTreeFromPayload binds a tree delivered by a remote peer.
// Used by the SpaceRegistry's PutTree route — any-sync's space-sync
// delivers a TreeStorageCreatePayload for a tree we don't have
// locally yet. Falls back to BuildTree on ErrTreeExists, matching
// the Derive idempotency contract.
func (s *Store) PutTreeFromPayload(ctx context.Context, payload treestorage.TreeStorageCreatePayload) (*object.Object, error) {
	return s.Get(ctxWithLoadPayload(ctx, &payload), payload.RootRawChange.Id)
}

// DeleteTree marks the underlying any-sync tree as deleted and drops
// the cached *object.Object. Wired into the SpaceRegistry's
// DeleteTree route so the deletion-manager's per-tree cleanup pass
// removes both the any-sync storage flag and our in-memory state.
//
// any-store rows for the deleted object are NOT cleaned up — same
// rationale as Drop: queries skip tombstones, ids are content-
// addressable.
func (s *Store) DeleteTree(ctx context.Context, treeId string) error {
	obj, err := s.Get(ctx, treeId)
	if err != nil {
		return fmt.Errorf("spaceobjects: load %s: %w", treeId, err)
	}
	tree := obj.Tree()
	if tree == nil {
		s.Drop(treeId)
		return nil
	}
	if err := tree.Delete(); err != nil {
		return fmt.Errorf("spaceobjects: tree.Delete %s: %w", treeId, err)
	}
	s.Drop(treeId)
	return nil
}

// Get returns the *object.Object for objectId, lazy-loading on first
// access via the ocache LoadFunc. Concurrent Get on the same id
// share one load; the LoadFunc runs Controller build, tree open,
// SetTree, and ColdRestore atomically before returning, so peers
// never observe a partial Object.
func (s *Store) Get(ctx context.Context, objectId string) (*object.Object, error) {
	v, err := s.cache.Get(ctx, objectId)
	if err != nil {
		return nil, err
	}
	return v.(*object.Object), nil
}

// Create makes a new object on the space and returns its bound
// *object.Object. The caller-supplied opts.ChangeType / ChangePayload
// land on the root any-sync change; for the MVP they're informational
// only.
func (s *Store) Create(ctx context.Context, opts CreateOpts) (*object.Object, error) {
	handle, err := s.app.GetSpace(ctx, s.spaceId)
	if err != nil {
		return nil, fmt.Errorf("spaceobjects: get space: %w", err)
	}
	seed := make([]byte, 32)
	if _, err := rand.Read(seed); err != nil {
		return nil, err
	}
	payload, err := handle.Inner().TreeBuilder().CreateTree(ctx, objecttree.ObjectTreeCreatePayload{
		PrivKey:       s.signKey,
		ChangeType:    opts.ChangeType,
		ChangePayload: opts.ChangePayload,
		SpaceId:       s.spaceId,
		IsEncrypted:   true,
		Seed:          seed,
		// Timestamp is the creation moment baked into the immutable
		// root change. SystemPropertiesHandler reads it back via
		// tree.Root().Timestamp to auto-stamp `createdAt` on the
		// object's row at first property write — without setting it
		// here, the root carries 0 and the auto-stamp is silently
		// skipped (see properties.stampAutoFields).
		Timestamp: time.Now().Unix(),
	})
	if err != nil {
		return nil, fmt.Errorf("spaceobjects: CreateTree: %w", err)
	}
	return s.Get(ctxWithLoadPayload(ctx, &payload), payload.RootRawChange.Id)
}

// Derive makes a deterministic object on the space. Idempotent — a
// second Derive with the same opts.ChangePayload returns the same
// objectId. If the tree already exists locally, ocache's per-id
// LoadFunc serialization deduplicates parallel callers.
func (s *Store) Derive(ctx context.Context, opts DeriveOpts) (*object.Object, error) {
	handle, err := s.app.GetSpace(ctx, s.spaceId)
	if err != nil {
		return nil, fmt.Errorf("spaceobjects: get space: %w", err)
	}
	payload, err := handle.Inner().TreeBuilder().DeriveTree(ctx, objecttree.ObjectTreeDerivePayload{
		ChangeType:    opts.ChangeType,
		ChangePayload: opts.ChangePayload,
		SpaceId:       s.spaceId,
		IsEncrypted:   true,
	})
	if err != nil {
		return nil, fmt.Errorf("spaceobjects: DeriveTree: %w", err)
	}
	return s.Get(ctxWithLoadPayload(ctx, &payload), payload.RootRawChange.Id)
}

// loadObject is the ocache.LoadFunc. Builds Controller + Object +
// any-sync tree + ColdRestore in one atomic step under ocache's
// per-id load lock. Reads an optional TreeStorageCreatePayload from
// ctx (set by Create / Derive / PutTreeFromPayload via
// ctxWithLoadPayload) — payload present takes the PutTree path with
// BuildTree fallback on ErrTreeExists; absent takes BuildTree
// directly.
func (s *Store) loadObject(ctx context.Context, objectId string) (ocache.Object, error) {
	handle, err := s.app.GetSpace(ctx, s.spaceId)
	if err != nil {
		return nil, fmt.Errorf("spaceobjects: get space: %w", err)
	}
	ctrl, err := s.newController(ctx, objectId)
	if err != nil {
		return nil, err
	}
	payload := loadPayloadFromCtx(ctx)
	var gate object.ApplyGate
	if !s.disableGate {
		gate = s.gateFor(objectId)
	}
	obj, err := object.New(object.Config{
		SpaceId:    s.spaceId,
		SignKey:    s.signKey,
		Controller: ctrl,
		Allocator:  s.alloc,
		Gate:       gate,
		AfterApply: s.afterApplyFor(),
	}, func(listener updatelistener.UpdateListener) (objecttree.ObjectTree, error) {
		return s.openTree(ctx, handle, objectId, payload, listener)
	})
	if err != nil {
		return nil, err
	}
	if err := obj.ColdRestore(ctx); err != nil {
		return nil, fmt.Errorf("spaceobjects: cold restore %s: %w", objectId, err)
	}
	return obj, nil
}

// openTree picks PutTree vs BuildTree based on whether the caller has
// a creation payload in hand. Falls through to BuildTree when PutTree
// reports ErrTreeExists — supports idempotent Derive.
//
// Sets SetDeferredUpdater(true) on the resulting tree: any-sync's
// default order in AddRawChangesWithUpdater is
// addChangesToTree → updater → storage.AddAll, which means our
// listener (Object.Update → replayLocked) runs *before* the new
// change is persisted to tree storage. replayLocked scans storage
// via tree.IterateAfterAddSeq(ctx, MaxAddSeq, …); with the change
// not yet in storage, GetAfterAddSeq returns nothing, no
// applyDecodedLocked fires, the controller stays out of sync with
// the tree, and inbound-only writes (e.g. a writer joiner's
// sp.Delete that Owner pulls via HeadUpdate) never project into
// the controller's collection — the in-memory tree advances heads
// but the materialised state stays stuck.
//
// `SetDeferredUpdater(true)` flips the order so storage.AddAll
// runs first; the listener then sees the new change via
// IterateAfterAddSeq. Local writes are unaffected: tree.AddContent
// in any-sync persists to storage before broadcasting, regardless
// of this flag.
func (s *Store) openTree(ctx context.Context, handle anysyncx.SpaceHandle, objectId string, payload *treestorage.TreeStorageCreatePayload, listener updatelistener.UpdateListener) (objecttree.ObjectTree, error) {
	tb := handle.Inner().TreeBuilder()
	if payload != nil {
		tree, err := tb.PutTree(ctx, *payload, listener)
		if err == nil {
			deferIfSyncTree(tree)
			return tree, nil
		}
		if !errors.Is(err, treestorage.ErrTreeExists) {
			return nil, fmt.Errorf("spaceobjects: PutTree %s: %w", objectId, err)
		}
		// fall through to BuildTree on ErrTreeExists
	}
	tree, err := tb.BuildTree(ctx, objectId, objecttreebuilder.BuildTreeOpts{Listener: listener})
	if err != nil {
		return nil, fmt.Errorf("spaceobjects: BuildTree %s: %w", objectId, err)
	}
	deferIfSyncTree(tree)
	return tree, nil
}

// deferIfSyncTree flips the SyncTree into deferred-updater mode (see
// openTree's docstring for the rationale). No-op when the returned
// objecttree implementation isn't a SyncTree (defensive — every
// production path returns a SyncTree, but tests may stub).
func deferIfSyncTree(tree objecttree.ObjectTree) {
	type deferredSetter interface {
		SetDeferredUpdater(deferred bool)
	}
	if d, ok := tree.(deferredSetter); ok {
		d.SetDeferredUpdater(true)
	}
}

// newController opens or creates the per-object Controller, registering
// the standard handler set. Each object lives in the SDK DB with
// collection-name prefix `<objectId>/`.
// newController builds the per-object Controller. Every object
// routes its `objects` dataset writes to the per-space shared
// collection (id = ch.ObjectId, one row per object). Type objects
// are no different — their own metadata (any.name etc.) lives in
// the same per-space collection as everyone else; what's
// type-specific is the `properties` dataset (definitions), which
// stays per-type-object.
// objectsDatasetSchema is the per-space `objects` (properties) dataset
// schema: Dynamic (user props are `{typeId}.{propId}`, allowed as
// synced) with the built-in `any` fields declared by class — ScopeAuto
// auto-fields (author/createdAt/spaceId/id) as Derived (handler-only),
// ScopeBase fields (name/description/…) as Synced.
func objectsDatasetSchema() schema.Dataset {
	fields := make([]schema.Field, 0, len(anytype.Properties))
	for _, p := range anytype.Properties {
		cls := schema.ScopeSynced
		if p.Scope == anytype.ScopeAuto {
			cls = schema.ScopeDerived
		}
		fields = append(fields, schema.Field{Id: p.Id, Name: p.Name, Schema: schema.Leaf(p.Kind), Scope: cls})
	}
	return schema.Dataset{Fields: fields, Dynamic: true}
}

func (s *Store) newController(ctx context.Context, objectId string) (*crdt.Controller, error) {
	if s.customHandlers != nil {
		// Raw mode: exactly the caller's handlers, each on its own
		// per-object collection (<objectId>_<dataset>). No shared
		// `objects` collection, no built-in regs.
		return crdt.NewController(ctx, objectId, s.db, s.customHandlers...)
	}
	coll, err := s.SharedObjects(ctx)
	if err != nil {
		return nil, err
	}
	shared := crdt.SharedCollections{properties.Dataset: coll}
	regs := []crdt.HandlerReg{
		{Name: properties.Dataset, Handler: properties.New(s.reg), Schema: objectsDatasetSchema()},
		// `properties` defs + `shortIds` carry content-addressed / dynamic
		// keyspaces — declared Dynamic (synced).
		{Name: typetype.DatasetPropertyDefs, Handler: typetype.PropertyHandler{}, Schema: schema.Dataset{Dynamic: true}},
		{Name: typetype.ShortIdsDataset, Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}},
	}
	for _, t := range s.extTypes {
		for _, d := range t.Datasets {
			regs = append(regs, crdt.HandlerReg{Name: d.Name, Handler: d.Handler, Indexes: d.Indexes, Schema: datasetSchema(d)})
		}
	}
	return crdt.NewControllerWithShared(ctx, objectId, s.db, shared, regs...)
}
