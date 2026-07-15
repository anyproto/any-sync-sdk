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

	anystorev1 "github.com/anyproto/any-store"
	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/anyproto/any-sync/commonspace/headsync/headstorage"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree/updatelistener"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/objecttreebuilder"
	"github.com/anyproto/any-sync/util/crypto"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
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
	payloads.Dataset:             payloads.HandlerVersion,
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
	changeSubs *changeRegistry

	// applySeqs mints the per-space apply sequence shared by every
	// controller — the consumer-feed watermark covering DAG, mirror,
	// and local applies. Seeded lazily from the persisted max (post
	// legacy backfill, guarded by applySeqBackfill).
	applySeqs           *crdt.ApplySeqAllocator
	applySeqBackfill    sync.Once
	applySeqBackfillErr error

	// rowEvents notifies objects-row creations/deletions — the account
	// mirror's replay and GC triggers. See SubscribeRowEvents.
	rowEvents *rowEventRegistry

	// readTracking maps a tracked dataset to its registration;
	// readState is the per-space read/unread engine. Both nil/empty
	// when nothing in this space opted into tracking. selfIdentity is
	// the account id self-authored changes are matched against.
	readTracking map[string]*crdt.ReadTracking
	readState    *readstate.Engine
	selfIdentity string
	readMat      *readMaterializer
	// readSeedPending marks objects mid-first-restore: the apply hook
	// skips tracking for them (the seed covers everything present).
	readSeedPending sync.Map
	// seedHeads consults the account's published frontiers before
	// first-sight seeding — see SetSeedHeadsProvider.
	seedHeads SeedHeadsProvider

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

	// selective is the selective-sync tree-type allowlist (root
	// changeType → allowed). Nil/empty = sync everything. See
	// selective.go for the full mechanism.
	selective map[string]struct{}
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
		changeSubs:     newChangeRegistry(),
		rowEvents:      newRowEventRegistry(),
		customHandlers: cfg.Handlers,
		disableGate:    cfg.DisableGate,
	}
	if len(cfg.SelectiveTypes) > 0 {
		s.selective = make(map[string]struct{}, len(cfg.SelectiveTypes))
		for _, t := range cfg.SelectiveTypes {
			s.selective[t] = struct{}{}
		}
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
	s.readTracking = buildReadTracking(s.extTypes, s.customHandlers)
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
		anyProps[p.Id] = types.PropInfo{Id: p.Id, Name: p.Name, Kind: p.Kind, Scope: p.Scope}
	}
	static[anytype.TypeId] = anyProps

	siProps := make(map[string]types.PropInfo, len(spaceindex.Properties))
	for _, p := range spaceindex.Properties {
		siProps[p.Id] = types.PropInfo{Id: p.Id, Name: p.Name, Kind: p.Kind, Scope: p.Scope}
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
	s.dropObjectCollections(ctx, objectId)
	_ = s.unmarkSkipped(ctx, objectId)
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
		if s.rowEvents != nil && s.rowEvents.hasSubscribers() {
			s.rowEvents.dispatch(RowEvent{ObjectId: objectId, Deleted: true})
		}
	}
	if stamped && s.changeSubs.hasSubscribers() {
		s.changeSubs.dispatch(ObjectChange{ObjectId: objectId, ApplySeq: seq, Deleted: true})
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
		s.Drop(p.id)
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
		coll, err := s.db.OpenCollection(ctx, name)
		if err != nil {
			if !errors.Is(err, anystore.ErrCollectionNotFound) {
				storeLog.Warn("purge: open collection", zap.String("coll", name), zap.Error(err))
			}
			continue
		}
		if err := coll.Drop(ctx); err != nil {
			storeLog.Warn("purge: drop collection", zap.String("coll", name), zap.Error(err))
		}
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
	var gate object.ApplyGate
	if !s.disableGate {
		gate = s.gateFor(objectId)
	}
	obj, err := object.New(object.Config{
		SpaceId:        s.spaceId,
		SignKey:        s.signKey,
		Controller:     ctrl,
		Allocator:      s.alloc,
		Gate:           gate,
		AfterApply:     s.afterApplyFor(),
		PlaintextSpecs: plaintextSpecs,
	}, func(listener updatelistener.UpdateListener) (objecttree.ObjectTree, error) {
		return s.openTree(ctx, handle, objectId, payload, listener)
	})
	if err != nil {
		return nil, err
	}
	if err := obj.ColdRestore(ctx); err != nil {
		return nil, fmt.Errorf("spaceobjects: cold restore %s: %w", objectId, err)
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
	if s.customHandlers != nil {
		// Raw mode: exactly the caller's handlers, each on its own
		// per-object collection (<objectId>_<dataset>). No shared
		// `objects` collection, no built-in regs.
		ctrl, err := crdt.NewController(ctx, objectId, s.db, s.customHandlers...)
		if err != nil {
			return nil, err
		}
		ctrl.SetSpaceId(s.spaceId)
		ctrl.SetApplySeqAllocator(s.applySeqs)
		ctrl.SetApplyHook(s.readApplyHook(ctrl))
		return ctrl, nil
	}
	coll, err := s.SharedObjects(ctx)
	if err != nil {
		return nil, err
	}
	shared := crdt.SharedCollections{properties.Dataset: coll}
	regs := []crdt.HandlerReg{
		// DynamicScopeByKey: undeclared heads (`any`, typeIds) carry
		// per-PROPERTY scopes resolved from the type registry — the
		// handler enforces them on the DAG route, Properties.Set on
		// the local/account routes. Declared derived heads (author /
		// createdAt / spaceId) stay controller-enforced.
		{Name: properties.Dataset, Handler: properties.New(s.reg), Schema: objectsDatasetSchema(), DynamicScopeByKey: true},
		// `properties` defs + `shortIds` carry content-addressed / dynamic
		// keyspaces — declared Dynamic (synced).
		{Name: typetype.DatasetPropertyDefs, Handler: typetype.PropertyHandler{}, Schema: schema.Dataset{Dynamic: true}},
		// `_ver.id` index backs LiveRegistry.LatestShortId, which reads the
		// greatest `_ver.id` (Sort("-_ver.id").Limit(1)) to derive a type's
		// DataVersion. `_ver.id` is stamped on every row, so the index is
		// dense (never sparse) — a reverse-scan to the last key replaces a
		// full-collection scan+sort as the shortIds dataset grows.
		{Name: typetype.ShortIdsDataset, Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}, Indexes: []anystore.IndexInfo{{Name: "idx__ver_id", Fields: []string{"_ver.id"}}}},
		// `payloads` — the node-readable per-file index. Registered on
		// every controller (uniform handler set), but only payloads
		// objects (plaintext class, see plaintextSpecs) ever write it:
		// LocalWrite on a regular object could technically carry it,
		// which is harmless (encrypted change, empty collection) and
		// fenced off at the public Modify API anyway.
		{Name: payloads.Dataset, Handler: payloads.Handler{}, Schema: payloads.Schema()},
	}
	for _, t := range s.extTypes {
		for _, d := range t.Datasets {
			regs = append(regs, crdt.HandlerReg{Name: d.Name, Handler: d.Handler, Indexes: d.Indexes, Schema: datasetSchema(d), ReadTracking: d.ReadTracking})
		}
	}
	ctrl, err := crdt.NewControllerWithShared(ctx, objectId, s.db, shared, regs...)
	if err != nil {
		return nil, err
	}
	ctrl.SetSpaceId(s.spaceId)
	ctrl.SetApplySeqAllocator(s.applySeqs)
	ctrl.SetApplyHook(s.readApplyHook(ctrl))
	return ctrl, nil
}
