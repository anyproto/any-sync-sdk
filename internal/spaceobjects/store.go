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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	anystorev1 "github.com/anyproto/any-store"
	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/anyproto/any-sync/commonspace/headsync/headstorage"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree/updatelistener"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/objecttreebuilder"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/util/crypto"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/fanout"
	"github.com/anyproto/any-sync-sdk/internal/history"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/payloads"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/readstate"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/subscribe"
	"github.com/anyproto/any-sync-sdk/internal/types"
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
	"github.com/anyproto/any-sync-sdk/space"
)

var storeLog = logger.NewNamed("sdk.spaceobjects")

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
	typetype.DatasetDefs:         typetype.DatasetDefsHandlerVersion,
	payloads.Dataset:             payloads.HandlerVersion,
	spaceindex.BundlesDataset:    spaceindex.BundlesHandlerVersion,
}

// plaintextSpecs declares the plaintext (node-readable) object
// classes, keyed by tree-root ChangeType. A payloads object ships its
// changes unencrypted and may carry ONLY the payloads dataset —
// LocalWrite hard-errors on anything else and inbound replay skips it
// (see object.PlaintextSpec).
var plaintextSpecs = map[string]object.PlaintextSpec{
	payloads.ChangeType: {Datasets: map[string]struct{}{payloads.Dataset: {}}},
}

// validateEncryptionClass pins the tree-encryption invariant: a tree
// ships unencrypted IFF its root ChangeType is a registered plaintext
// class. Enforced at every create/derive so a callsite can neither
// ship cleartext for an encrypted-class type nor (since the flag is
// baked into the root bytes) silently fork a derived id by
// disagreeing with the class registry.
func validateEncryptionClass(changeType string, unencrypted bool) error {
	_, plaintext := plaintextSpecs[changeType]
	if unencrypted != plaintext {
		return fmt.Errorf("spaceobjects: ChangeType %q: Unencrypted=%v but plaintext-class registration=%v — the flag must match the class",
			changeType, unencrypted, plaintext)
	}
	return nil
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
	// Unencrypted creates a plaintext (node-readable) tree: the root
	// payload carries IsEncrypted:false and the object's ChangeType
	// must be a registered plaintext class (object.PlaintextSpec) so
	// every change ships unencrypted. Regular objects leave it false.
	Unencrypted bool
}

// DeriveOpts is the input to Store.Derive. ChangePayload is the seed
// hashed into the derived id.
type DeriveOpts struct {
	ChangeType    string
	ChangePayload []byte
	// ParentId binds the derived tree to a parent so any-sync cascade-
	// deletes it with the parent. It is also hashed into the derived id.
	ParentId string
	// Unencrypted derives a plaintext (node-readable) tree — see
	// CreateOpts.Unencrypted. NOTE: the flag participates in the
	// derived root bytes, so it changes the derived object id; every
	// deriver of a given plaintext object must pass the same value.
	Unencrypted bool
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

	// catalog is the runtime dataset snapshot compiled from type
	// objects' `datasets` records. Readers
	// take the copy-on-write snapshot lock-free; refreshed only when a
	// dataset-defs change applies. See catalog.go.
	catalog *runtimeCatalog

	// staticSchemaHandlers memoizes the generic schema handler for
	// config-registered datasets that declare a schema and no bespoke
	// Handler. Built once at store open — buildRegs runs on every
	// object load and must not recompile declarations. SchemaHandler
	// is read-only after construction, safe to share.
	staticSchemaHandlers map[string]*crdt.SchemaHandler

	// cache is the per-space *object.Object cache. LoadFunc holds
	// the per-id lock during build+ColdRestore, so peers waiting on
	// the same id never see partial state.
	cache ocache.OCache

	mu         sync.Mutex
	sharedColl anystore.Collection // per-space `objects` collection, lazy-opened
	detached   anystore.Collection // per-space `_detached` collection, lazy-opened

	// drainMu serializes Drain passes — see Store.Drain.
	drainMu sync.Mutex

	// drainer runs Store.Drain asynchronously off the apply path —
	// afterApply hooks push pairs in, the drainer consumes them and
	// coalesces bursts into single Drain passes. See drainer.go.
	drainer *drainer

	// engine drives windowed live queries (Query.Subscribe). The
	// afterApply hook gates on engine.HasSubscribers so the cold-
	// restore path stays a single atomic load when nobody is
	// listening.
	engine *subscribe.Engine

	// changeSubs is the change-index live feed (consumer-side FTS /
	// vector indexers). afterApply dispatches (objectId, applySeq)
	// here, gated on hasSubscribers so an idle space pays nothing.
	changeSubs *fanout.Registry[ObjectChange]

	// applySeqs mints the per-space apply sequence shared by every
	// controller — the consumer-feed watermark covering DAG, mirror,
	// and local applies. Seeded lazily from the persisted max (post
	// legacy backfill, guarded by applySeqBackfill).
	applySeqs           *crdt.ApplySeqAllocator
	applySeqBackfill    sync.Once
	applySeqBackfillErr error

	// rowEvents notifies objects-row creations/deletions — the account
	// mirror's replay and GC triggers. See SubscribeRowEvents.
	rowEvents *fanout.Registry[RowEvent]

	// stampPending holds, per objectId, the object stamps landed during
	// the current replay batch, emitted as one event per object by the
	// AfterReplay hook. See dispatchObjectStamps.
	stampPending sync.Map

	// readTracking maps a tracked dataset to its registration;
	// readState is the per-space read/unread engine. Both nil/empty
	// when nothing in this space opted into tracking. selfIdentity is
	// the account id self-authored changes are matched against.
	readTracking map[string]*crdt.ReadTracking
	readState    *readstate.Engine
	selfIdentity string
	readMat      *readMaterializer

	// The background re-index sweep: one per store, started once, stopped
	// with the store and waited for (Close must not return while a
	// rebuild is still writing — an offload drops collections right
	// after). See reindex.go.
	sweepOnce  sync.Once
	sweepStop  chan struct{}
	sweepClose sync.Once
	sweepDone  chan struct{}
	// readSeedPending marks objects mid-first-restore: the apply hook
	// skips tracking for them (the seed covers everything present).
	readSeedPending sync.Map
	// seedHeads consults the account's published frontiers before
	// first-sight seeding — see SetSeedHeadsProvider.
	seedHeads SeedHeadsProvider

	// systemRegs are per-store type-less built-ins registered on every
	// controller after the shared built-in set — the tech space's
	// spaces/profile/devices/… datasets. Ungated like payloads/bundles:
	// no owner, a hardcoded DataVersion stamp, rows only on the objects
	// that write them by convention.
	systemRegs []crdt.HandlerReg
	// disableHistory keeps the history index closed for this store:
	// no eager open, no apply hook, no purge bookkeeping. Set for the
	// tech space, which has no history surface.
	disableHistory bool

	// selective is the selective-sync tree-type allowlist (root
	// changeType → allowed). Nil/empty = sync everything. See
	// selective.go for the full mechanism.
	selective map[string]struct{}

	// historyIx is the per-space version-history index once opened.
	// Published via atomic pointer so the apply hook reads it without
	// taking historyMu — the opener may block on the any-store writer
	// (collection DDL) while an apply holds it, and an apply hook
	// waiting on historyMu in that state would ABBA-deadlock.
	historyIx atomic.Pointer[history.Index]
	// historyMu serializes index opens only. Open failures are NOT
	// cached — the next HistoryIndex call retries, so a transient
	// fault never disables history for the Store's lifetime.
	historyMu sync.Mutex
	// historySkipIndex marks objects mid-cold-restore: the apply hook
	// skips history-index rows for them (the object is marked stale
	// and lazily backfilled instead — proposal §4.4 cold path). Same
	// pattern as readSeedPending.
	historySkipIndex sync.Map
	// historyPendingStale collects objectIds whose index rows were
	// skipped or failed while the index was unavailable (or whose
	// in-tx MarkStale failed). Flushed to MarkStale on every
	// successful HistoryIndex call so the lazy backfill repairs the
	// gap instead of it becoming permanent.
	historyPendingStale sync.Map
	// historyPendingPurge collects objectIds whose space-level history
	// rows (trace rows, _history_meta) could not be purged — index
	// unavailable at purge time, or the purge itself failed. Flushed
	// on every successful HistoryIndex call, same contract as
	// historyPendingStale; without it a purge skipped once would leak
	// the rows forever.
	historyPendingPurge sync.Map

	// writeGateErr, when non-nil, rejects every user-authored synced
	// write (Store.Create and every Object's LocalWrite) with that
	// error — read-only spaces (guest access, reader role). An atomic
	// so the per-write check is a single load: the value is maintained
	// event-driven, not computed per write — seeded at construction
	// (guest rows) and updated by the per-space ACL mirror on role
	// changes. Inbound apply / local-set paths are never gated.
	writeGateErr atomic.Pointer[error]
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

// SystemDataset is a per-store type-less built-in: a handler
// registered on every controller of that store plus the hardcoded
// DataVersion its writers stamp. The tech space registers its
// spaces/profile/devices/… datasets this way.
type SystemDataset struct {
	Reg         crdt.HandlerReg
	DataVersion string
}

// StoreConfig is the input to NewStoreWithConfig. Every store runs the
// same path: LiveRegistry, shared `objects` collection, built-in
// handler set, runtime dataset catalog, schema gate. ExtTypes and
// SystemDatasets extend the handler set.
type StoreConfig struct {
	App     *anysyncx.App
	DB      anystore.DB
	SignKey crypto.PrivKey
	SpaceId string
	Alloc   *object.VersionAllocator

	// ExtTypes is the caller-supplied type catalog.
	ExtTypes []handler.Type

	// SystemDatasets are extra ungated built-ins for this store.
	SystemDatasets []SystemDataset

	// DisableHistory keeps the history index closed — for stores with
	// no history surface (the tech space).
	DisableHistory bool

	// SelectiveTypes is the selective-sync tree-type allowlist. Only
	// the regular-space path (NewStore) sets it — the tech space is
	// always fully synced. See selective.go.
	SelectiveTypes []string
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
	var selectiveTypes []string
	if app != nil {
		selectiveTypes = app.SelectiveTreeTypes()
	}
	return NewStoreWithConfig(StoreConfig{
		App: app, DB: db, SignKey: signKey, SpaceId: spaceId, Alloc: alloc, ExtTypes: extTypes,
		SelectiveTypes: selectiveTypes,
	})
}

// SetWriteGateErr installs (non-nil) or clears (nil) the user-write
// gate — see Store.writeGateErr. Safe at any time; cached Objects read
// the live value on every write.
func (s *Store) SetWriteGateErr(err error) {
	if err == nil {
		s.writeGateErr.Store(nil)
		return
	}
	s.writeGateErr.Store(&err)
}

// CheckWrite applies the gate; nil = writable. Exposed for write entry
// points that mutate outside the store's own DAG-write funnel (tree
// deletion, file-node uploads).
func (s *Store) CheckWrite() error {
	if p := s.writeGateErr.Load(); p != nil {
		return *p
	}
	return nil
}

// NewStoreWithConfig constructs a Store from cfg. The async drainer is
// built and started here — it lives until Close.
func NewStoreWithConfig(cfg StoreConfig) *Store {
	s := &Store{
		app:            cfg.App,
		db:             cfg.DB,
		signKey:        cfg.SignKey,
		alloc:          cfg.Alloc,
		spaceId:        cfg.SpaceId,
		engine:         subscribe.New(cfg.SpaceId),
		changeSubs:     fanout.New[ObjectChange](),
		rowEvents:      fanout.New[RowEvent](),
		disableHistory: cfg.DisableHistory,
	}
	if len(cfg.SelectiveTypes) > 0 {
		s.selective = make(map[string]struct{}, len(cfg.SelectiveTypes))
		for _, t := range cfg.SelectiveTypes {
			s.selective[t] = struct{}{}
		}
	}
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
	// System datasets get a stamp but no owner: DatasetOwner stays
	// false, so the type-membership check is a no-op for them.
	for _, sd := range cfg.SystemDatasets {
		dv[sd.Reg.Name] = sd.DataVersion
		s.systemRegs = append(s.systemRegs, sd.Reg)
	}
	s.reg = types.NewLiveRegistry(cfg.DB, buildStaticSchema(cfg.ExtTypes))
	s.extTypes = cfg.ExtTypes
	s.dataVersions = dv
	s.datasetOwners = owners
	s.staticSchemaHandlers = make(map[string]*crdt.SchemaHandler)
	for _, t := range cfg.ExtTypes {
		for _, d := range t.Datasets {
			if d.Handler != nil {
				continue
			}
			if sh, err := crdt.NewSchemaHandler(datasetSchema(d)); err == nil {
				s.staticSchemaHandlers[d.Name] = sh
			}
		}
	}
	s.catalog = newRuntimeCatalog()
	s.initCatalog(context.Background())
	s.applySeqs = crdt.NewApplySeqAllocator(func(ctx context.Context) (uint64, error) {
		coll, err := s.applySeqMeta(ctx)
		if err != nil {
			return 0, err
		}
		return crdt.MaxObjectApplySeq(ctx, coll, s.spaceId)
	})
	if s.signKey != nil {
		s.selfIdentity = s.signKey.GetPublic().Account()
	}
	s.readTracking = buildReadTracking(s.extTypes, s.systemRegs)
	if len(s.readTracking) > 0 {
		s.readState = readstate.New(s.db, s.spaceId, s.applySeqs.Next, s.readResolver())
		if needsReadMaterializer(s.readTracking) {
			s.readMat = newReadMaterializer(s)
		}
	}
	s.cache = ocache.New(
		s.loadObject,
		ocache.WithTTL(objectCacheTTL),
		ocache.WithGCPeriod(objectCacheGC),
	)
	s.sweepStop = make(chan struct{})
	s.drainer = newDrainer(s)
	s.drainer.Run()
	return s
}

// buildStaticSchema assembles the registry's static overlay: the
// schema for types whose definitions don't live in a per-type-object
// `properties` collection — the built-in `any` / `spaceIndex` /
// `type` tables and every registered external type's declared
// Properties. User types are absent (resolved from their defs
// collection at lookup).
//
// Returns typeId → propId → PropInfo. Safe with nil/empty extTypes.
func buildStaticSchema(extTypes []handler.Type) map[string]map[string]types.PropInfo {
	static := make(map[string]map[string]types.PropInfo, 3+len(extTypes))

	anyProps := make(map[string]types.PropInfo, len(anytype.Properties))
	for _, p := range anytype.Properties {
		anyProps[p.Id] = types.PropInfo{Id: p.Id, Name: p.Name, Kind: p.Kind, Scope: p.Scope}
	}
	static[anytype.TypeId] = anyProps

	siProps := make(map[string]types.PropInfo, len(spaceindex.Properties))
	for _, p := range spaceindex.Properties {
		siProps[p.Id] = types.PropInfo{Id: p.Id, Name: p.Name, Kind: p.Kind, Scope: p.Scope}
	}
	static[spaceindex.TypeId] = siProps

	// The meta-type's namespace. Without this entry every type Create
	// fails: the `type.xkey` write resolves no type in the registry
	// and PreValidate rejects the whole change.
	mtProps := make(map[string]types.PropInfo, len(typetype.Properties))
	for _, p := range typetype.Properties {
		mtProps[p.Id] = types.PropInfo{Id: p.Id, Name: p.Name, Kind: p.Kind, Scope: p.Scope}
	}
	static[typetype.TypeId] = mtProps

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
			props[p.Id] = types.PropInfo{Id: p.Id, Name: p.Name, Kind: propertyKindToSchema(p.Kind), Scope: p.Scope}
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
	case handler.PropertyKindDatetime:
		return schema.KindDatetime
	}
	return schema.KindUnknown
}

// ReservedTypeIds are the synthetic built-ins: they own a namespace in
// the static schema and a row in Types().List, so a caller-registered
// type may not claim one.
var ReservedTypeIds = map[string]struct{}{
	anytype.TypeId:    {},
	spaceindex.TypeId: {},
	typetype.TypeId:   {},
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
		if _, reserved := ReservedTypeIds[t.Id]; reserved {
			// buildStaticSchema writes the built-in tables first and the
			// ext-type loop REPLACES by id, so a registration under a
			// built-in id silently takes over its namespace — for `type`
			// that breaks every Types().Create carrying an XKey. Fail at
			// boot instead.
			return fmt.Errorf("spaceobjects: type[%d]: Id %q is reserved for a built-in type", i, t.Id)
		}
		seenTypeIds[t.Id] = struct{}{}
		// A type may own datasets, properties, both, or neither (a pure
		// declaration / tag). Only a non-empty Id is required.
		for j, d := range t.Datasets {
			if d.Handler == nil {
				// No bespoke handler: the dataset gets the generic
				// schema handler, which requires a declared schema and a
				// well-formed behavioral declaration.
				if len(d.Schema.Fields) == 0 && !d.Schema.Dynamic {
					return fmt.Errorf("spaceobjects: type[%d] (%q) dataset[%d]: nil Handler requires a declared Schema", i, t.Id, j)
				}
				if err := schema.ValidateDatasetDecl(d.Schema); err != nil {
					return fmt.Errorf("spaceobjects: type[%d] (%q) dataset[%d] (%q): %w", i, t.Id, j, d.Name, err)
				}
			}
			if d.Name == "" {
				return fmt.Errorf("spaceobjects: type[%d] (%q) dataset[%d]: empty Name", i, t.Id, j)
			}
			if strings.HasPrefix(d.Name, "_") {
				// The "_" prefix is reserved for internal collections
				// (`<objectId>__history*`, `_meta`, `_detached`, …) —
				// a dataset named "_history" would collide with the
				// per-object history collections.
				return fmt.Errorf("spaceobjects: type[%d] (%q) dataset[%d]: name %q is reserved (\"_\" prefix)", i, t.Id, j, d.Name)
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
			// Scope: zero (defaults to synced) or an explicit creatable
			// class. Derived is reserved for SDK built-ins.
			switch p.Scope {
			case 0, schema.ScopeSynced, schema.ScopeAccount, schema.ScopeLocal:
			default:
				return fmt.Errorf("spaceobjects: type[%d] (%q) property[%d] (%q): invalid Scope %d (synced/account/local only)", i, t.Id, k, p.Id, p.Scope)
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
	s.stopSweep()
	if s.readMat != nil {
		s.readMat.close()
	}
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
	// TypeId is the owning type for type-owned datasets (registered or
	// runtime); empty for space-level built-ins. External indexers key
	// their type gating on it.
	TypeId string
}

// Schemas returns the declared schema of every dataset this store hosts —
// the same schemas its controllers enforce. Used by the space layer to
// expose dataset discovery to consumers.
func (s *Store) Schemas() []NamedSchema {
	out := []NamedSchema{
		{Name: properties.Dataset, Schema: objectsDatasetSchema()},
		{Name: typetype.DatasetPropertyDefs, Schema: schema.Dataset{Dynamic: true}},
		{Name: typetype.ShortIdsDataset, Schema: schema.Dataset{Dynamic: true}},
		{Name: typetype.DatasetDefs, Schema: schema.Dataset{Dynamic: true}},
	}
	for _, h := range s.systemRegs {
		out = append(out, NamedSchema{Name: h.Name, Schema: h.Schema})
	}
	for _, t := range s.extTypes {
		for _, d := range t.Datasets {
			out = append(out, NamedSchema{Name: d.Name, Schema: datasetSchema(d), TypeId: t.Id})
		}
	}
	snap := s.catalog.snapshot()
	for _, name := range sortedCatalogNames(snap) {
		ds := snap.byName[name]
		out = append(out, NamedSchema{Name: ds.Name, Schema: ds.Schema, TypeId: ds.TypeId})
	}
	return out
}

// SystemSchemas returns the declared schema of this store's system
// datasets only (the tech space's spaces/profile/devices/…).
func (s *Store) SystemSchemas() []NamedSchema {
	out := make([]NamedSchema, 0, len(s.systemRegs))
	for _, h := range s.systemRegs {
		out = append(out, NamedSchema{Name: h.Name, Schema: h.Schema})
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
	return d.Schema.Normalized()
}

// DatasetDefs returns the compiled runtime dataset definitions of one
// type object (deterministic fold of its `datasets` records).
func (s *Store) DatasetDefs(ctx context.Context, typeId string) ([]types.CompiledDataset, error) {
	return types.CompileDatasetDefs(ctx, s.db, typeId)
}

// DatasetHeadIds lists the live head ids declaring name on the type
// object, duplicates included. See types.DatasetHeadIds.
func (s *Store) DatasetHeadIds(ctx context.Context, typeId, name string) ([]string, error) {
	return types.DatasetHeadIds(ctx, s.db, typeId, name)
}

// DatasetHeadName resolves a live head's dataset name by record id.
// See types.DatasetHeadName.
func (s *Store) DatasetHeadName(ctx context.Context, typeId, defId string) (string, error) {
	return types.DatasetHeadName(ctx, s.db, typeId, defId)
}

// HasDatasetDefs reports whether anything was ever declared on the
// type object, removed definitions included. See types.HasDatasetDefs.
func (s *Store) HasDatasetDefs(ctx context.Context, typeId string) (bool, error) {
	return types.HasDatasetDefs(ctx, s.db, typeId)
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
	if owner, ok := s.datasetOwners[dataset]; ok {
		return owner, ok
	}
	if ds, ok := s.catalog.lookup(dataset); ok {
		return ds.TypeId, true
	}
	return "", false
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
//
// Ownership note: handles opened through here bypass the controller's
// eviction-time release (Controller.CloseOwnedCollections) — they stay
// in any-store's registry until process exit. Today's callers are the
// types registry reads (`<typeId>_propertyDefs` / `<typeId>_shortIds`
// via spaceimpl/types.go) and the tech-space accountvalues dataset —
// bounded O(types + tech-space objects), deliberately out of scope for
// the per-object eviction fix. Don't route per-object DATA datasets
// through here; those belong to the object's controller.
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
	// Dense ascending index on the derived `modifiedAt` stamp — the
	// recency ordering (`sort: ["-modifiedAt"]`) is the default object-
	// list sort for clients, which would otherwise scan the whole
	// collection per query (SYN-98). Dense, not sparse: a sort index
	// must cover every row.
	if err := coll.EnsureIndex(ctx, anystore.IndexInfo{
		Fields: []string{"modifiedAt"},
	}); err != nil {
		return nil, fmt.Errorf("spaceobjects: ensure modifiedAt index: %w", err)
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

// DataVersionFor resolves the DataVersion stamp for any known dataset:
// the static map for built-ins / config-registered datasets, and for
// runtime datasets the owning type's latest shortId encoded as a
// `typeId:shortId` pair — so peers gate the data change against the
// writer's schema state (dataVersionForTypes model). Falls back to the
// defs handler version when the type has no shortId rows yet (fresh
// hand-built state; the string parse-fails and passes the gate).
func (s *Store) DataVersionFor(ctx context.Context, dataset string) (string, error) {
	if v, ok := s.dataVersions[dataset]; ok {
		return v, nil
	}
	ds, ok := s.catalog.lookup(dataset)
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownDataset, dataset)
	}
	latest, err := s.reg.LatestShortId(ctx, ds.TypeId)
	if err != nil {
		return "", err
	}
	if latest == "" {
		return typetype.DatasetDefsHandlerVersion, nil
	}
	return types.EncodeDataVersion([]types.DataVersionPair{{TypeId: ds.TypeId, ShortId: latest}}), nil
}

// RuntimeDataset resolves a runtime dataset's compiled declaration by
// collection name — one atomic snapshot load.
func (s *Store) RuntimeDataset(dataset string) (types.CompiledDataset, bool) {
	return s.catalog.lookup(dataset)
}

// DatasetDecl resolves any known dataset's schema declaration:
// config-registered datasets first, then the runtime catalog. Built-ins
// are absent on purpose (their declarations are SDK-internal).
func (s *Store) DatasetDecl(dataset string) (schema.Dataset, bool) {
	for _, t := range s.extTypes {
		for _, d := range t.Datasets {
			if d.Name == dataset {
				return datasetSchema(d), true
			}
		}
	}
	if ds, ok := s.catalog.lookup(dataset); ok {
		return ds.Schema, true
	}
	return schema.Dataset{}, false
}

// SelfIdentity returns this replica's account identity (empty when the
// store has no signing key — raw/test mode).
func (s *Store) SelfIdentity() string { return s.selfIdentity }

// EnsureDatasetRegistered makes sure the RESIDENT controller for
// objectId (if any) carries the dataset's CURRENT registration. A
// controller built before a runtime dataset was defined lacks its
// handler; one built before a field was added/removed carries a stale
// SchemaRev. Both fix by eviction — the next Get rebuilds through
// buildRegs, which reads the current catalog snapshot. Lazy and
// demand-driven: cost lands only on the first touch per object, never
// as a fleet-wide sweep on schema apply.
func (s *Store) EnsureDatasetRegistered(ctx context.Context, objectId, dataset string) {
	if _, static := s.dataVersions[dataset]; static {
		return
	}
	ds, known := s.catalog.lookup(dataset)
	if !known {
		return
	}
	v, err := s.cache.Pick(ctx, objectId)
	if err != nil {
		return // not resident: the next load registers it naturally
	}
	obj, ok := v.(*object.Object)
	if !ok || obj.Controller() == nil {
		return
	}
	if ctrl := obj.Controller(); ctrl.HasDataset(dataset) && ctrl.DatasetSchemaRev(dataset) == ds.SchemaRev {
		return
	}
	s.Drop(objectId)
}

// datasetRegistered reports whether the dataset is currently known to
// any registration source (static catalog or runtime snapshot).
func (s *Store) datasetRegistered(dataset string) bool {
	if _, static := s.dataVersions[dataset]; static {
		return true
	}
	_, known := s.catalog.lookup(dataset)
	return known
}

// controllerStaleFor reports whether ctrl's registration for dataset is
// missing, out of rev against the current catalog snapshot, or carries
// a runtime dataset the catalog no longer knows (removed definition —
// residents must stop applying what fresh controllers park, or
// replicas diverge).
func (s *Store) controllerStaleFor(ctrl *crdt.Controller, dataset string) bool {
	if ctrl == nil {
		return false
	}
	if !ctrl.HasDataset(dataset) {
		return true
	}
	rev := ctrl.DatasetSchemaRev(dataset)
	if ds, ok := s.catalog.lookup(dataset); ok {
		return rev != ds.SchemaRev
	}
	// Rev-tracked (runtime-registered) but absent from the catalog:
	// the definition was removed since this controller was built.
	return rev != ""
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

// DeleteTree reflects an any-sync tree deletion into local state: it
// tombstones the tree in any-sync storage, purges the object's
// materialized state from the SDK DB, and evicts the cached object.
// Wired into the SpaceRegistry's DeleteTree route so the deletion
// manager's per-tree cleanup pass reclaims both any-sync storage and
// our on-disk + in-memory state.
//
// Ordering is deliberate: tree.Delete() runs FIRST, so any-sync marks
// the tree deleted (a permanent, cross-device-authoritative flag) and
// then rejects every further apply to it — the purge below therefore
// cannot race a concurrent materialization of the same object.
//
// A purge failure is PROPAGATED, not swallowed: any-sync advances the
// tree from Queued to Deleted only after this callback returns success,
// so returning an error keeps it Queued and the deletion loop re-fires
// the callback until the purge commits — the at-least-once self-heal for
// a crash between tree.Delete() and the purge.
func (s *Store) DeleteTree(ctx context.Context, treeId string) error {
	obj, err := s.Get(ctx, treeId)
	if err != nil {
		return fmt.Errorf("spaceobjects: load %s: %w", treeId, err)
	}
	tree := obj.Tree()
	if tree == nil {
		if err := s.purgeObject(ctx, treeId); err != nil {
			return err
		}
		s.Drop(treeId)
		return nil
	}
	if err := tree.Delete(); err != nil {
		return fmt.Errorf("spaceobjects: tree.Delete %s: %w", treeId, err)
	}
	if err := s.purgeObject(ctx, treeId); err != nil {
		return err
	}
	s.Drop(treeId)
	return nil
}

// MarkTreeDeleted is the soft-delete callback any-sync fires when it
// catches a settings-tree deletion for a tree not present in local
// storage (a device that never synced the object, or a re-fired callback
// after tree.Delete() already removed it). Purge any materialized state
// and evict the cache; there is no local tree to tombstone. Like
// DeleteTree, a purge failure is returned so the deletion loop re-fires
// until it commits.
func (s *Store) MarkTreeDeleted(ctx context.Context, treeId string) error {
	if err := s.purgeObject(ctx, treeId); err != nil {
		return err
	}
	s.Drop(treeId)
	return nil
}

// purgeObject hard-removes an object's local projection from the SDK
// DB — the shared `objects` row and every per-object dataset collection
// (`<objectId>_<dataset>`) — stamps a durable deletion marker for the
// consumer change-index feed, and notifies live subscribers + the account
// mirror.
//
// The SDK keeps NO local `objects` tombstone: any-sync's head storage is
// the durable, cross-device record that the tree is deleted (set once,
// never cleared, no inbound path resurrects it), so the row simply ceases
// to exist. Instead the object's `_meta` row (deliberately KEPT) is stamped
// del=true with a fresh applySeq, so the deletion surfaces through the
// change-index feed as ObjectChange{Deleted:true} — the consumer's durable
// eviction signal (see PersistDeletionMark). The `_meta` row is kept
// anyway: the applySeq allocator seeds from max(applySeq) over these rows,
// so removing the highest-applySeq row would rewind the allocator.
//
// Atomicity: the `objects` row removal AND the del-stamp commit in ONE
// WriteTx, mirroring the apply path (record + watermark move together). A
// crash before commit persists neither; any-sync keeps the tree Queued
// (it advances Queued->Deleted only after the callback returns success) and
// re-fires on restart. There is no window where the row is gone but the
// deletion unannounced, nor announced but the row still live.
//
// Device-local: no DAG or ACL write; idempotent (a second call with the
// row already gone removes nothing and fires no Removed event).
//
// Error contract: the correctness-critical steps — removing the shared
// `objects` row and stamping the deletion — RETURN an error on failure so
// the caller propagates it and any-sync re-fires the callback until the
// purge commits (a stale row would surface a deleted object as live; a
// missing stamp would leave it forever in a consumer's index). The
// per-object data-collection drop and the notifications stay best-effort.
func (s *Store) purgeObject(ctx context.Context, objectId string) error {
	coll, err := s.SharedObjects(ctx)
	if err != nil {
		return fmt.Errorf("spaceobjects: purge open shared objects %s: %w", objectId, err)
	}
	metaColl, err := s.metaCollection(ctx)
	if err != nil {
		return fmt.Errorf("spaceobjects: purge open _meta %s: %w", objectId, err)
	}

	tx, err := s.db.WriteTx(ctx)
	if err != nil {
		return fmt.Errorf("spaceobjects: purge tx %s: %w", objectId, err)
	}
	removed, seq, stamped, perr := s.purgeRowInTx(tx.Context(), coll, metaColl, objectId)
	if perr != nil {
		_ = tx.Rollback()
		return perr
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("spaceobjects: purge commit %s: %w", objectId, err)
	}

	// Best-effort from here — disk reclaim + notifications, not correctness.
	// dropObjectCollections sweeps every `<objectId>_*` collection, which
	// includes the per-object history collections; the space-level history
	// leftovers (trace rows, stale-flag row) need their own purge.
	s.dropObjectCollections(ctx, objectId)
	s.purgeHistoryRows(ctx, objectId)
	_ = s.unmarkSkipped(ctx, objectId)
	// A deleted TYPE object must leave the runtime catalog, or its
	// dataset names stay occupied forever (blocking e.g. a bundle
	// reinstall after uninstall). refreshType compiles from the now-
	// dropped defs collection — empty — and removes the entry. Gated
	// on catalog membership so a bulk purge of ordinary objects never
	// pays a rebuild per row.
	if s.catalogHasType(objectId) {
		s.refreshType(ctx, objectId)
	}
	s.fireDeletionEvents(objectId, removed, stamped, seq)
	return nil
}

// purgeRowInTx removes the shared `objects` row for objectId (if present)
// and stamps its kept `_meta` row as deleted with a fresh applySeq — all
// inside the caller's WriteTx. Returns whether a live row was removed, the
// stamped applySeq, and whether a del-stamp was written. Stamps whenever the
// object was materialized in the feed: a live `objects` row (removed) OR a
// base-dataset-only object with a scoped `_meta` row. A never-materialized
// id (a delete callback for an object this device never had) stamps nothing.
func (s *Store) purgeRowInTx(txCtx context.Context, coll, metaColl anystore.Collection, objectId string) (removed bool, seq uint64, stamped bool, err error) {
	if _, ferr := coll.FindId(txCtx, objectId); ferr == nil {
		if derr := coll.DeleteId(txCtx, objectId); derr != nil {
			return false, 0, false, fmt.Errorf("spaceobjects: purge remove row %s: %w", objectId, derr)
		}
		removed = true
	} else if !errors.Is(ferr, anystore.ErrDocNotFound) {
		return false, 0, false, fmt.Errorf("spaceobjects: purge read row %s: %w", objectId, ferr)
	}

	stamp := removed
	if !stamp {
		ok, merr := crdt.MetaExists(txCtx, metaColl, objectId, s.spaceId)
		if merr != nil {
			return false, 0, false, fmt.Errorf("spaceobjects: purge meta check %s: %w", objectId, merr)
		}
		stamp = ok
	}
	if stamp {
		if seq, err = s.applySeqs.Next(txCtx); err != nil {
			return false, 0, false, fmt.Errorf("spaceobjects: purge alloc applySeq %s: %w", objectId, err)
		}
		if err = crdt.PersistDeletionMark(txCtx, metaColl, objectId, s.spaceId, seq); err != nil {
			return false, 0, false, fmt.Errorf("spaceobjects: purge stamp del %s: %w", objectId, err)
		}
		stamped = true
	}
	return removed, seq, stamped, nil
}

// fireDeletionEvents emits the best-effort notifications after a committed
// purge: the live query engine + row-event Removed (only when a live row was
// actually removed, so a redundant purge fires nothing), and the
// change-index deletion entry (when a del-stamp was written).
func (s *Store) fireDeletionEvents(objectId string, removed, stamped bool, seq uint64) {
	if removed {
		if s.engine != nil {
			s.engine.NotifyDeleted(s.spaceId, properties.Dataset, objectId)
		}
		if s.rowEvents != nil && s.rowEvents.HasSubscribers() {
			s.rowEvents.Dispatch(RowEvent{ObjectId: objectId, Deleted: true})
		}
	}
	if stamped && s.changeSubs.HasSubscribers() {
		s.changeSubs.Dispatch(ObjectChange{ObjectId: objectId, ApplySeq: seq, Deleted: true})
	}
}

// PurgeObjects hard-removes the local projection for a batch of objectIds in
// ONE WriteTx (row removal + del-stamp per materialized id), then reclaims
// per-object collections listing collection names ONCE, drops the cache, and
// emits one deletion feed entry per stamped id. Used by the startup
// deletion-reconcile to purge many stale rows off the SDK.Open path without
// the O(N x all-collections) cost of per-id purgeObject. Ids with neither a
// live `objects` row nor a scoped `_meta` row (never materialized here) are
// skipped. Idempotent.
func (s *Store) PurgeObjects(ctx context.Context, objectIds []string) error {
	if len(objectIds) == 0 {
		return nil
	}
	coll, err := s.SharedObjects(ctx)
	if err != nil {
		return fmt.Errorf("spaceobjects: batch purge open shared objects: %w", err)
	}
	metaColl, err := s.metaCollection(ctx)
	if err != nil {
		return fmt.Errorf("spaceobjects: batch purge open _meta: %w", err)
	}

	tx, err := s.db.WriteTx(ctx)
	if err != nil {
		return fmt.Errorf("spaceobjects: batch purge tx: %w", err)
	}
	txCtx := tx.Context()
	type purgedRow struct {
		id      string
		seq     uint64
		removed bool
		stamped bool
	}
	var purged []purgedRow
	for _, id := range objectIds {
		removed, seq, stamped, perr := s.purgeRowInTx(txCtx, coll, metaColl, id)
		if perr != nil {
			_ = tx.Rollback()
			return perr
		}
		if removed || stamped {
			purged = append(purged, purgedRow{id, seq, removed, stamped})
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("spaceobjects: batch purge commit: %w", err)
	}

	// Best-effort tail — list collection names ONCE for the whole batch.
	names, nerr := s.db.GetCollectionNames(ctx)
	if nerr != nil {
		storeLog.Warn("batch purge: list collections", zap.Error(nerr))
	}
	for _, p := range purged {
		if nerr == nil {
			s.dropObjectCollectionsNamed(ctx, p.id, names)
		}
		if nerr != nil {
			// The batch listing failed — drop per object so the defs
			// collection is gone before the catalog recompile, or the
			// refresh below would re-add the deleted type's names.
			s.dropObjectCollections(ctx, p.id)
		}
		s.purgeHistoryRows(ctx, p.id)
		s.Drop(p.id)
		// See purgeObject: a deleted type object leaves the catalog.
		if s.catalogHasType(p.id) {
			s.refreshType(ctx, p.id)
		}
		s.fireDeletionEvents(p.id, p.removed, p.stamped, p.seq)
	}
	if s.SelectiveMode() {
		// Skip markers exist for ids that never materialized, so sweep
		// the full input list, not just the purged rows.
		for _, id := range objectIds {
			_ = s.unmarkSkipped(ctx, id)
		}
	}
	return nil
}

// dropObjectCollections drops every `<objectId>_<dataset>` collection
// owned by objectId — mirroring the per-object sweep the space offload
// path performs, scoped to a single object. The id segment is matched
// exactly (up to the first `_`) so an object whose id merely shares a
// prefix is never touched.
func (s *Store) dropObjectCollections(ctx context.Context, objectId string) {
	names, err := s.db.GetCollectionNames(ctx)
	if err != nil {
		storeLog.Warn("purge: list collections", zap.String("treeId", objectId), zap.Error(err))
		return
	}
	s.dropObjectCollectionsNamed(ctx, objectId, names)
}

// dropObjectCollectionsNamed drops every `<objectId>_<dataset>` collection
// found in the pre-listed names slice. The batch purge lists collection
// names once and calls this per id, avoiding an O(N x all-collections)
// re-listing.
func (s *Store) dropObjectCollectionsNamed(ctx context.Context, objectId string, names []string) {
	for _, name := range names {
		i := strings.IndexByte(name, '_')
		if i <= 0 || name[:i] != objectId {
			continue
		}
		s.dropCollectionByName(ctx, name)
	}
}

// dropCollectionByName opens and drops one collection, retrying once
// when the handle turns out closed: an eviction (or the twin deletion
// callback's own drop) can close the handle between our open and the
// Drop, and without the retry the purge would report success while the
// collection survives on disk. The retry's fresh open disambiguates
// what the error alone cannot — ErrCollectionClosed WRAPS
// ErrCollectionNotFound by definition, so "closed but still on disk"
// and "concurrently reclaimed" look identical on the failed Drop. The
// reopen resolves from the catalog: a live handle to drop, or a bare
// ErrCollectionNotFound meaning the concurrent path already reclaimed
// it (the goal state, not worth a warn).
func (s *Store) dropCollectionByName(ctx context.Context, name string) {
	for attempt := 0; ; attempt++ {
		coll, err := s.db.OpenCollection(ctx, name)
		if err != nil {
			if !errors.Is(err, anystore.ErrCollectionNotFound) {
				storeLog.Warn("purge: open collection", zap.String("coll", name), zap.Error(err))
			}
			return
		}
		err = coll.Drop(ctx)
		if err == nil {
			return
		}
		if errors.Is(err, anystore.ErrCollectionClosed) && attempt == 0 {
			continue // reopen resolves: still on disk → drop; gone → silent
		}
		if errors.Is(err, anystore.ErrCollectionNotFound) {
			return // already reclaimed by the concurrent deletion path
		}
		storeLog.Warn("purge: drop collection", zap.String("coll", name), zap.Error(err))
		return
	}
}

// TreeDeleted reports whether any-sync's head storage records treeId as
// deleted — the permanent, cross-device-authoritative deletion flag
// (set once, never cleared, no inbound path resurrects the tree). Lets
// consumers tell a deleted object (whose local row was hard-removed)
// apart from one that was never materialized.
func (s *Store) TreeDeleted(ctx context.Context, treeId string) (bool, error) {
	handle, err := s.app.GetSpace(ctx, s.spaceId)
	if err != nil {
		return false, err
	}
	st := handle.Inner().Storage()
	if st == nil {
		return false, nil
	}
	entry, err := st.HeadStorage().GetEntry(ctx, treeId)
	if err != nil {
		if isDocNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return entry.DeletedStatus != headstorage.DeletedStatusNotDeleted, nil
}

// TreeIsDerived reports whether treeId is a DERIVED object — the root's
// IsDerived flag, mirrored into head storage at tree-storage creation
// (see objecttree.createTreeStorage) — and whether the tree is present
// in local storage. A tree absent locally reports (false, false, nil):
// its class can't be read, so the caller can't classify it.
//
// Distinguishing a derived owner from a signed one is what lets its
// payloads child pick a derivation: any-sync rejects a derived object as
// a tree parent (ErrDerivedParent), so a derived owner's payloads object
// is derived unparented (see payloads.DerivedOwnerSeed) while a signed
// owner's stays parented. Mirrors TreeDeleted's head-storage lookup.
func (s *Store) TreeIsDerived(ctx context.Context, treeId string) (isDerived bool, present bool, err error) {
	handle, err := s.app.GetSpace(ctx, s.spaceId)
	if err != nil {
		return false, false, err
	}
	st := handle.Inner().Storage()
	if st == nil {
		return false, false, nil
	}
	entry, err := st.HeadStorage().GetEntry(ctx, treeId)
	if err != nil {
		if isDocNotFound(err) {
			return false, false, nil
		}
		return false, false, err
	}
	return entry.IsDerived, true, nil
}

// isDocNotFound matches any-store's document-not-found across BOTH
// major versions: any-sync's storage returns any-store v1's instance,
// the SDK's own DB returns v2's — same text, different error values,
// so a single errors.Is silently misses one of them.
func isDocNotFound(err error) bool {
	return errors.Is(err, anystore.ErrDocNotFound) || errors.Is(err, anystorev1.ErrDocNotFound)
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
	if err := s.CheckWrite(); err != nil {
		return nil, err
	}
	if err := validateEncryptionClass(opts.ChangeType, opts.Unencrypted); err != nil {
		return nil, err
	}
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
		IsEncrypted:   !opts.Unencrypted,
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

// DeriveId computes the deterministic objectId Derive(opts) would
// produce, WITHOUT creating or loading anything. Pure: DeriveTree
// only builds the root change in memory (all inputs — including
// Unencrypted and ParentId — are baked into the root bytes, so they
// participate in the id). Read paths use this to resolve lazily-
// created objects and treat a missing tree as "no rows yet".
func (s *Store) DeriveId(ctx context.Context, opts DeriveOpts) (string, error) {
	if err := validateEncryptionClass(opts.ChangeType, opts.Unencrypted); err != nil {
		return "", err
	}
	handle, err := s.app.GetSpace(ctx, s.spaceId)
	if err != nil {
		return "", fmt.Errorf("spaceobjects: get space: %w", err)
	}
	payload, err := handle.Inner().TreeBuilder().DeriveTree(ctx, objecttree.ObjectTreeDerivePayload{
		ChangeType:    opts.ChangeType,
		ChangePayload: opts.ChangePayload,
		SpaceId:       s.spaceId,
		IsEncrypted:   !opts.Unencrypted,
		ParentId:      opts.ParentId,
	})
	if err != nil {
		return "", fmt.Errorf("spaceobjects: DeriveTree: %w", err)
	}
	return payload.RootRawChange.Id, nil
}

// HasTree reports whether the tree exists in local storage (deleted
// trees count as existing — TreeDeleted distinguishes them).
func (s *Store) HasTree(ctx context.Context, treeId string) (bool, error) {
	handle, err := s.app.GetSpace(ctx, s.spaceId)
	if err != nil {
		return false, err
	}
	st := handle.Inner().Storage()
	if st == nil {
		return false, nil
	}
	if _, err := st.HeadStorage().GetEntry(ctx, treeId); err != nil {
		if isDocNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// TreeIdsByChangeType returns the ids of every materialized tree in
// this space whose root changeType equals changeType. The root is the
// typed source of truth — signed, content-addressed (the tree id is the
// cid of the root bytes) and cleartext, so keyed and keyless readers
// classify identically and a peer can't relabel a tree into the result.
//
// Two-phase like the spacesync catch-up: collect ids while the
// headstorage iterator is open, classify after. Trees whose root can't
// be read are skipped — heads-only stubs (selective sync) have no
// change rows by construction, and an unreadable root is not evidence
// of the requested type.
func (s *Store) TreeIdsByChangeType(ctx context.Context, changeType string) ([]string, error) {
	handle, err := s.app.GetSpace(ctx, s.spaceId)
	if err != nil {
		return nil, fmt.Errorf("spaceobjects: get space: %w", err)
	}
	st := handle.Inner().Storage()
	if st == nil {
		return nil, errors.New("spaceobjects: space storage unavailable")
	}
	var candidates []string
	if err := st.HeadStorage().IterateEntries(ctx, headstorage.IterOpts{}, func(e headstorage.HeadsEntry) (bool, error) {
		if e.DeletedStatus != headstorage.DeletedStatusNotDeleted {
			return true, nil
		}
		candidates = append(candidates, e.Id)
		return true, nil
	}); err != nil {
		return nil, fmt.Errorf("spaceobjects: iterate heads entries: %w", err)
	}
	var out []string
	for _, id := range candidates {
		ts, err := st.TreeStorage(ctx, id)
		if err != nil {
			continue
		}
		rootCh, err := ts.Root(ctx)
		if err != nil {
			continue
		}
		root, err := parseVerifiedRoot(rootCh.RawTreeChangeWithId())
		if err != nil {
			continue
		}
		if root.ChangeType == changeType {
			out = append(out, id)
		}
	}
	return out, nil
}

// Derive makes a deterministic object on the space. Idempotent — a
// second Derive with the same opts.ChangePayload returns the same
// objectId. If the tree already exists locally, ocache's per-id
// LoadFunc serialization deduplicates parallel callers.
func (s *Store) Derive(ctx context.Context, opts DeriveOpts) (*object.Object, error) {
	if err := validateEncryptionClass(opts.ChangeType, opts.Unencrypted); err != nil {
		return nil, err
	}
	handle, err := s.app.GetSpace(ctx, s.spaceId)
	if err != nil {
		return nil, fmt.Errorf("spaceobjects: get space: %w", err)
	}
	payload, err := handle.Inner().TreeBuilder().DeriveTree(ctx, objecttree.ObjectTreeDerivePayload{
		ChangeType:    opts.ChangeType,
		ChangePayload: opts.ChangePayload,
		SpaceId:       s.spaceId,
		IsEncrypted:   !opts.Unencrypted,
		ParentId:      opts.ParentId,
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
	// A handler-version bump recorded on the object's _meta row rebuilds
	// the object before anything reads it (see reindex.go). The verdict is
	// read here; the wipe itself waits until the tree is open, below. A
	// rebuild an earlier load started and never finished (its captured
	// local leaves are still on the row) resumes: the replay continues
	// from the persisted watermark and the leaves are restored after it.
	stale := ctrl.StaleDatasets()
	resume := len(stale) == 0 && ctrl.ReindexPending()
	rebuilding := len(stale) > 0 || resume
	payload := loadPayloadFromCtx(ctx)
	// First tracked load: prefer the account's published read state
	// (tech-space KV usually syncs before chat trees) — restore with
	// tracking ON and merge the published frontiers after. Only when
	// nothing was published does first-sight seeding apply, with
	// tracking skipped during the restore (the seed covers it all).
	seedPending := false
	var publishedSets [][]string
	if s.readSeedable() {
		if seeded, sErr := s.readState.Seeded(ctx, objectId); sErr == nil && !seeded {
			seedPending = true
			publishedSets = s.publishedSeedHeads(ctx, objectId)
			if len(publishedSets) == 0 {
				s.readSeedPending.Store(objectId, struct{}{})
				defer s.readSeedPending.Delete(objectId)
			}
		}
	}
	if rebuilding {
		// The replay re-applies every change in the tree, which would mark
		// the whole object unread. Read state itself survives the wipe (it
		// lives outside the object's collections) and the materializer
		// re-projects the flags once the rows are back.
		s.readSeedPending.Store(objectId, struct{}{})
		defer s.readSeedPending.Delete(objectId)
	}
	gate := s.gateFor(objectId, ctrl)
	obj, err := object.New(object.Config{
		SpaceId:        s.spaceId,
		SignKey:        s.signKey,
		Controller:     ctrl,
		Allocator:      s.alloc,
		Gate:           gate,
		AfterApply:     s.afterApplyFor(),
		AfterReplay:    s.afterReplayFor(),
		WriteGate:      s.CheckWrite,
		PlaintextSpecs: plaintextSpecs,
		OnClose: func() {
			s.releaseHistoryHandle(objectId)
			s.stampPending.Delete(objectId)
		},
	}, func(listener updatelistener.UpdateListener) (objecttree.ObjectTree, error) {
		return s.openTree(ctx, handle, objectId, payload, listener)
	})
	if err != nil {
		return nil, err
	}
	// Tree is open, so the replay can actually run: capture device-local
	// values, rewind and wipe. Doing this before object.New would leave a
	// failed open (offline, tree removed, selective-mode reject) with the
	// rows gone, nothing to replay them back, and the captured local
	// values dropped on the error path.
	var reindexLeaves []localLeaf
	if len(stale) > 0 {
		if reindexLeaves, err = s.reindexPrepare(ctx, objectId, ctrl, stale); err != nil {
			return nil, err
		}
	} else if resume {
		storeLog.Info("reindex: resuming interrupted rebuild", zap.String("objectId", objectId))
		reindexLeaves = ctrl.ReindexLocalLeaves()
	}
	// First materialization (fresh controller, no watermark): the cold
	// restore may drain the whole tree — skip per-change history-index
	// rows (protected perf path, proposal §4.4) and mark the object
	// stale for lazy backfill if anything was actually restored. A
	// resumed rebuild is the same case mid-way: its first attempt skipped
	// the rows and never reached the mark.
	firstRestore := ctrl.MaxAddSeq() == 0 || resume
	if firstRestore {
		s.historySkipIndex.Store(objectId, struct{}{})
		defer s.historySkipIndex.Delete(objectId)
	}
	if err := obj.ColdRestore(ctx); err != nil {
		return nil, fmt.Errorf("spaceobjects: cold restore %s: %w", objectId, err)
	}
	if firstRestore && ctrl.MaxAddSeq() > 0 {
		if ix, ixErr := s.HistoryIndex(ctx); ixErr == nil {
			if mErr := ix.MarkStale(ctx, objectId); mErr != nil {
				storeLog.Warn("history: mark stale failed", zap.String("objectId", objectId), zap.Error(mErr))
				s.historyPendingStale.Store(objectId, struct{}{})
			}
		} else if !errors.Is(ixErr, ErrHistoryUnavailable) {
			// Index not openable right now: queue the mark so the next
			// successful open repairs it.
			s.historyPendingStale.Store(objectId, struct{}{})
		}
	}
	if rebuilding {
		s.reindexFinish(ctx, obj, ctrl, reindexLeaves)
	}
	if seedPending {
		if len(publishedSets) > 0 {
			s.seedFromPublished(ctx, objectId, publishedSets)
		} else {
			s.seedReadState(ctx, obj, objectId)
		}
	}
	return obj, nil
}

// releaseHistoryHandle closes the registry handle of the object's
// `<objectId>__history` collection on eviction — the history index
// opens it by name per write/read (never caching), but any-store keeps
// every opened handle in its registry, so without this the handle (and
// its planner sketch + caches) outlives the object's residency, one
// per object ever applied. Best-effort: a collection that was never
// materialised (ErrCollectionNotFound) or a concurrent close are both
// fine — the next history write/read re-opens by name.
//
// Serialization mirrors the controller handles: the only `__history`
// writer inside an apply runs under this object's tree.Lock, which the
// Object close path holds/held before firing OnClose. The lazy history
// backfill iterates a separate history tree without that lock — it can
// race this close, fail one flush with ErrCollectionClosed, and be
// retried by the next history query (the stale flag only clears on a
// completed backfill).
func (s *Store) releaseHistoryHandle(objectId string) {
	if s.disableHistory {
		return
	}
	coll, err := s.db.OpenCollection(context.Background(), objectId+history.HistoryCollectionSuffix)
	if err != nil {
		return
	}
	if err := coll.Close(); err != nil {
		storeLog.Warn("close history handle", zap.String("objectId", objectId), zap.Error(err))
	}
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
	opts := objecttreebuilder.BuildTreeOpts{Listener: listener}
	if s.SelectiveMode() {
		// Probe-first: a remote fetch of an unknown tree asks for root +
		// heads only; the validator classifies by root changeType before
		// anything is persisted. Local trees are unaffected (probe only
		// applies when tree storage misses).
		opts.Probe = true
		opts.TreeValidator = s.selectiveTreeValidator(true)
	}
	tree, err := tb.BuildTree(ctx, objectId, opts)
	if errors.Is(err, errProbeSelectedType) {
		// Selected type — fetch for real. The validator re-checks the
		// type (a different peer may serve this request) and then
		// delegates to any-sync's default validation.
		tree, err = tb.BuildTree(ctx, objectId, objecttreebuilder.BuildTreeOpts{
			Listener:      listener,
			TreeValidator: s.selectiveTreeValidator(false),
		})
	}
	if err != nil {
		return nil, buildTreeError(objectId, err)
	}
	deferIfSyncTree(tree)
	return tree, nil
}

// buildTreeError wraps a BuildTree failure, joining
// space.ErrObjectNotFound when the tree is unknown here or already
// deleted — the two ways an object is not addressable on this device.
// Consumers match that one sentinel at the API boundary instead of
// any-sync's storage errors; the underlying error stays in the chain,
// so callers branching on treestorage.ErrUnknownTreeId still match.
func buildTreeError(objectId string, err error) error {
	if errors.Is(err, treestorage.ErrUnknownTreeId) || errors.Is(err, spacestorage.ErrTreeStorageAlreadyDeleted) {
		return fmt.Errorf("spaceobjects: BuildTree %s: %w: %w", objectId, space.ErrObjectNotFound, err)
	}
	return fmt.Errorf("spaceobjects: BuildTree %s: %w", objectId, err)
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
// synced) with the built-in `any` fields declared by their unified
// schema.Scope class — derived auto-fields (author/createdAt/spaceId/
// id) are handler-only, the rest synced.
//
// Note this declares the TOP-LEVEL field heads only (`any`, typeIds are
// dynamic). Per-PROPERTY scope (synced/account/local on a user propId)
// is enforced by SystemPropertiesHandler against the type Registry —
// the dataset schema can't see second path segments.
func objectsDatasetSchema() schema.Dataset {
	fields := make([]schema.Field, 0, len(anytype.Properties))
	for _, p := range anytype.Properties {
		fields = append(fields, schema.Field{Id: p.Id, Name: p.Name, Schema: schema.Leaf(p.Kind), Scope: p.Scope})
	}
	return schema.Dataset{Fields: fields, Dynamic: true}
}

func (s *Store) newController(ctx context.Context, objectId string) (*crdt.Controller, error) {
	regs, sharedNames, err := s.buildRegs()
	if err != nil {
		return nil, err
	}
	// Open the history index eagerly, outside any apply tx, so the
	// apply hook only ever reads the atomic pointer (see HistoryIndex
	// for why opening in-tx is unsafe). A failure here is tolerated —
	// the hook defers to the stale/backfill path and the next open
	// retries.
	if !s.disableHistory {
		if _, ixErr := s.HistoryIndex(ctx); ixErr != nil {
			storeLog.Warn("history index open failed; deferring to backfill", zap.Error(ixErr))
		}
	}
	shared := crdt.SharedCollections{}
	for _, name := range sharedNames {
		coll, cerr := s.SharedObjects(ctx)
		if cerr != nil {
			return nil, cerr
		}
		shared[name] = coll
	}
	ctrl, err := crdt.NewControllerWithShared(ctx, objectId, s.db, shared, regs...)
	if err != nil {
		return nil, err
	}
	ctrl.SetSpaceId(s.spaceId)
	ctrl.SetApplySeqAllocator(s.applySeqs)
	ctrl.SetApplyHook(s.composedApplyHook(ctrl))
	return ctrl, nil
}

// buildRegs returns the handler registrations every controller in this
// store runs, plus the dataset names that project into a per-space
// shared collection (row id = ObjectId). Shared collections are NOT
// opened here — the live path opens the space one (SharedObjects), the
// history replay path opens scratch ones (proposal §4.1).
func (s *Store) buildRegs() ([]crdt.HandlerReg, []string, error) {
	regs := []crdt.HandlerReg{
		// DynamicScopeByKey: undeclared heads (`any`, typeIds) carry
		// per-PROPERTY scopes resolved from the type registry — the
		// handler enforces them on the DAG route, Properties.Set on
		// the local/account routes. Declared derived heads (author /
		// createdAt / spaceId) stay controller-enforced.
		{Name: properties.Dataset, Handler: properties.New(s.reg), Schema: objectsDatasetSchema(), DynamicScopeByKey: true, Version: properties.LocalVersion},
		// `properties` defs + `shortIds` carry content-addressed / dynamic
		// keyspaces — declared Dynamic (synced).
		{Name: typetype.DatasetPropertyDefs, Handler: typetype.PropertyHandler{}, Schema: schema.Dataset{Dynamic: true}, Version: typetype.PropertyHandlerLocalVersion},
		// `_ver.id` index backs LiveRegistry.LatestShortId, which reads the
		// greatest `_ver.id` (Sort("-_ver.id").Limit(1)) to derive a type's
		// DataVersion. `_ver.id` is stamped on every row, so the index is
		// dense (never sparse) — a reverse-scan to the last key replaces a
		// full-collection scan+sort as the shortIds dataset grows.
		{Name: typetype.ShortIdsDataset, Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}, Indexes: []anystore.IndexInfo{{Name: "idx__ver_id", Fields: []string{"_ver.id"}}}},
		// `datasets` — runtime dataset definitions on type objects
		// (docs: SYN-147). Dynamic keyspace; the handler pins the
		// schema-bearing fields and projects shortId rows so the
		// DataVersion gate covers dataset-def state too.
		{Name: typetype.DatasetDefs, Handler: typetype.DatasetDefsHandler{}, Schema: schema.Dataset{Dynamic: true}, Version: typetype.DatasetDefsLocalVersion},
		// `payloads` — the node-readable per-file index. Registered on
		// every controller (uniform handler set), but only payloads
		// objects (plaintext class, see plaintextSpecs) ever write it:
		// LocalWrite on a regular object could technically carry it,
		// which is harmless (encrypted change, empty collection) and
		// fenced off at the public Modify API anyway.
		{Name: payloads.Dataset, Handler: payloads.Handler{}, Schema: payloads.Schema()},
		// `bundles` — the per-space installed-bundles registry.
		// Registered on every controller (uniform handler set); only the
		// spaceIndex object carries rows by convention — the typed
		// Bundles API always targets it.
		{Name: spaceindex.BundlesDataset, Handler: spaceindex.BundlesHandler{}, Schema: spaceindex.BundlesSchema()},
	}
	// Per-store system datasets: same footing as payloads/bundles.
	regs = append(regs, s.systemRegs...)
	for _, t := range s.extTypes {
		for _, d := range t.Datasets {
			h := d.Handler
			if h == nil {
				// Declared-schema dataset with no bespoke behavior: the
				// generic schema handler enforces the declaration.
				// Memoized at store open (SchemaHandler is read-only
				// after construction — sharing across controllers and
				// history scratch replays is safe).
				if h = s.staticSchemaHandlers[d.Name]; h == nil {
					sh, err := crdt.NewSchemaHandler(datasetSchema(d))
					if err != nil {
						return nil, nil, fmt.Errorf("spaceobjects: dataset %q: %w", d.Name, err)
					}
					h = sh
				}
			}
			version := crdt.NormalizedVersion(d.HandlerVersion)
			if d.Handler == nil {
				// Declared-schema dataset: the SDK owns the apply logic,
				// so its version participates too. Encoded as a pair
				// rather than summed — a sum lets a consumer bump cancel
				// an SDK bump and suppress the rebuild both asked for.
				version = crdt.ComposeVersion(crdt.SchemaHandlerVersion, version)
			}
			regs = append(regs, crdt.HandlerReg{
				Name: d.Name, Handler: h, Indexes: d.Indexes, Schema: datasetSchema(d),
				Version:               version,
				ReadTracking:          d.ReadTracking,
				SkipHistory:           d.SkipHistory,
				DisableFilteredReplay: d.DisableFilteredReplay,
			})
		}
	}
	// Runtime datasets (SYN-147): one generic schema-handler reg per
	// catalog entry. One atomic snapshot load, handlers pre-built per
	// snapshot — no storage reads or declaration compiles on the
	// controller-construction path.
	snap := s.catalog.snapshot()
	for _, name := range sortedCatalogNames(snap) {
		ds := snap.byName[name]
		sh := snap.handlers[name]
		if sh == nil {
			continue
		}
		regs = append(regs, crdt.HandlerReg{
			Name:        ds.Name,
			Handler:     sh,
			Schema:      ds.Schema,
			SchemaRev:   ds.SchemaRev,
			SkipHistory: ds.SkipHistory,
			Version:     crdt.SchemaHandlerVersion,
		})
	}
	return regs, []string{properties.Dataset}, nil
}

// sortedCatalogNames returns the snapshot's dataset names in stable
// order so controller reg sets are deterministic across loads.
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

// HistoryReplayRegs returns a fresh handler-reg set plus the shared
// dataset names for a history scratch replay (history.ViewParams).
// Fresh per call: handlers like properties.New hold registry pointers
// and must not be shared with live controllers' mutable state.
func (s *Store) HistoryReplayRegs() ([]crdt.HandlerReg, []string, error) {
	return s.buildRegs()
}

// ErrHistoryUnavailable — this store has no history index
// (DisableHistory: the tech space carries internal bookkeeping only,
// no history surface).
var ErrHistoryUnavailable = errors.New("spaceobjects: history index unavailable for this store")

// HistoryIndex returns the per-space version-history index, opening it
// on first use. The skip list is derived from the registered datasets'
// SkipHistory flags. Open failures are returned but NOT cached — the
// next call retries. Every successful call also flushes pending stale
// marks and deferred history purges recorded while the index was
// unavailable.
//
// Never call this from inside an apply WriteTx (the collection DDL
// would nest in — and be reverted with — the apply tx while the opened
// handles survive in memory); the apply hook reads the atomic pointer
// instead and defers indexing via historyPendingStale until an
// out-of-tx caller (newController, a history query) has opened it.
func (s *Store) HistoryIndex(ctx context.Context) (*history.Index, error) {
	if s.disableHistory {
		return nil, ErrHistoryUnavailable
	}
	ix := s.historyIx.Load()
	if ix == nil {
		s.historyMu.Lock()
		if ix = s.historyIx.Load(); ix == nil {
			regs, _, err := s.buildRegs()
			if err != nil {
				s.historyMu.Unlock()
				return nil, err
			}
			var skip []string
			for _, reg := range regs {
				if reg.SkipHistory {
					skip = append(skip, reg.Name)
				}
			}
			ix, err = history.OpenIndex(ctx, s.db, s.spaceId, skip)
			if err != nil {
				s.historyMu.Unlock()
				return nil, err
			}
			s.historyIx.Store(ix)
		}
		s.historyMu.Unlock()
	}
	s.flushPendingStale(ctx, ix)
	s.flushPendingPurge(ctx, ix)
	return ix, nil
}

// flushPendingStale marks every deferred object stale so the lazy
// backfill repairs rows the warm path could not write. Entries that
// fail to mark stay queued for the next flush.
func (s *Store) flushPendingStale(ctx context.Context, ix *history.Index) {
	s.historyPendingStale.Range(func(key, _ any) bool {
		objectId := key.(string)
		if err := ix.MarkStale(ctx, objectId); err != nil {
			storeLog.Warn("history: pending stale mark failed",
				zap.String("objectId", objectId), zap.Error(err))
			return true
		}
		s.historyPendingStale.Delete(objectId)
		return true
	})
}

// purgeHistoryRows removes a deleted object's space-level history rows
// (trace rows, _history_meta), queueing a retry when the index is
// unavailable or the purge fails. A queued purge supersedes any queued
// stale mark — a deleted object must not be re-marked for backfill.
func (s *Store) purgeHistoryRows(ctx context.Context, objectId string) {
	if s.disableHistory {
		return
	}
	s.historyPendingStale.Delete(objectId)
	ix := s.historyIx.Load()
	if ix == nil {
		s.historyPendingPurge.Store(objectId, struct{}{})
		return
	}
	if err := ix.PurgeObject(ctx, objectId); err != nil {
		storeLog.Warn("purge: history index cleanup", zap.String("treeId", objectId), zap.Error(err))
		s.historyPendingPurge.Store(objectId, struct{}{})
		return
	}
	s.historyPendingPurge.Delete(objectId)
}

// flushPendingPurge retries deferred history purges. Runs AFTER
// flushPendingStale so a pending purge also removes any meta row a
// stale flush just (re)wrote for the same object. Entries that fail
// stay queued.
func (s *Store) flushPendingPurge(ctx context.Context, ix *history.Index) {
	s.historyPendingPurge.Range(func(key, _ any) bool {
		objectId := key.(string)
		if err := ix.PurgeObject(ctx, objectId); err != nil {
			storeLog.Warn("history: pending purge failed",
				zap.String("objectId", objectId), zap.Error(err))
			return true
		}
		s.historyPendingPurge.Delete(objectId)
		return true
	})
}

// composedApplyHook chains the read-tracking hook and the history-index
// hook. Nil when both are disabled so untracked spaces keep paying a
// single nil check per apply.
func (s *Store) composedApplyHook(ctrl *crdt.Controller) crdt.ApplyHook {
	read := s.readApplyHook(ctrl)
	hist := s.historyApplyHook()
	switch {
	case read == nil && hist == nil:
		return nil
	case hist == nil:
		return read
	case read == nil:
		return hist
	}
	return func(txCtx context.Context, ch *crdt.Change, recordIds []string, res *crdt.ApplyResult) error {
		if err := read(txCtx, ch, recordIds, res); err != nil {
			return err
		}
		return hist(txCtx, ch, recordIds, res)
	}
}

// historyApplyHook writes warm-path history-index rows in the apply tx
// (proposal §4.4). Nil under DisableHistory (tech space): its objects
// are internal bookkeeping with no history surface, and indexing them
// would grow a permanent index nobody can query. Objects mid-cold-
// restore are skipped — they're marked stale and lazily backfilled
// instead. Index failures never fail the apply (history is best-effort
// metadata; a genuine storage fault surfaces through the apply tx
// itself) but they DO mark the object stale so the lazy backfill
// closes the gap instead of it becoming permanent.
func (s *Store) historyApplyHook() crdt.ApplyHook {
	if s.disableHistory {
		return nil
	}
	return func(txCtx context.Context, ch *crdt.Change, recordIds []string, _ *crdt.ApplyResult) error {
		if ch.ChangeId == "" || ch.Local || ch.Injected {
			return nil
		}
		if _, restoring := s.historySkipIndex.Load(ch.ObjectId); restoring {
			return nil
		}
		// Atomic read only — never open the index from inside the
		// apply tx (see HistoryIndex). Not open yet: defer to the
		// stale/backfill path.
		ix := s.historyIx.Load()
		if ix == nil {
			s.historyPendingStale.Store(ch.ObjectId, struct{}{})
			return nil
		}
		if err := ix.IndexChange(txCtx, ch, recordIds); err != nil {
			storeLog.Warn("history index row failed; marking object stale",
				zap.String("changeId", ch.ChangeId), zap.Error(err))
			// In-tx mark: atomic with the apply. If the tx is already
			// poisoned this fails too — queue for the next flush (a
			// poisoned tx also rolls back the apply, which re-replays
			// and re-indexes the change anyway).
			if mErr := ix.MarkStale(txCtx, ch.ObjectId); mErr != nil {
				s.historyPendingStale.Store(ch.ObjectId, struct{}{})
			}
		}
		return nil
	}
}
