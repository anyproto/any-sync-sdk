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
// not a separate object kind. Schema validation is disabled
// (registry=nil). Subscriptions and projections aren't wired.
package spaceobjects

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/objecttreebuilder"
	"github.com/anyproto/any-sync/util/crypto"

	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/types"
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
	properties.Dataset:         properties.HandlerVersion,
	typetype.DatasetProperties: typetype.HandlerVersion,
	typetype.ShortIdsDataset:   "shortIds-v1",
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
// Not safe for concurrent Create on the same id; concurrent Get on
// distinct ids is fine (per-objectId mutex inside).
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

	mu          sync.Mutex
	objects     map[string]*object.Object
	// restored maps objectId → ready channel. The channel is created
	// the first time we see an objectId; the cold-restore goroutine
	// closes it after restore returns, success or fail. Concurrent
	// Get callers wait on the channel before handing the *Object to
	// the caller, so reads (which don't take tree.Lock or o.mu) never
	// see partial state. Replaces the old `loaded map[string]bool`
	// flag-then-restore which exposed the partial state.
	restored    map[string]chan struct{}
	sharedColl  anystore.Collection // per-space `objects` collection, lazy-opened
	detached    anystore.Collection // per-space `_detached` collection, lazy-opened

	// drainer runs Store.Drain asynchronously off the apply path —
	// afterApply hooks push pairs in, the drainer consumes them and
	// coalesces bursts into single Drain passes. See drainer.go.
	drainer *drainer
}

// NewStore constructs a Store. The allocator is per-space (shared
// across all objects in this space). The async drainer is built and
// started here — it lives until Close.
//
// extTypes is the caller-supplied type catalog. Each type's handlers
// are wired onto every per-object Controller built by this store,
// alongside the built-in system handlers. Validation happens in
// ValidateExternalTypes — call it before NewStore at the SDK
// boundary so collisions are caught at Open time.
func NewStore(app *anysyncx.App, db anystore.DB, signKey crypto.PrivKey, spaceId string, alloc *object.VersionAllocator, extTypes []handler.Type) *Store {
	dv := make(map[string]string, len(builtinDataVersions))
	for k, v := range builtinDataVersions {
		dv[k] = v
	}
	for _, t := range extTypes {
		for _, r := range t.Handlers {
			dv[r.Handler.Dataset()] = r.DataVersion
		}
	}
	s := &Store{
		app:          app,
		db:           db,
		signKey:      signKey,
		alloc:        alloc,
		spaceId:      spaceId,
		reg:          types.NewLiveRegistry(db),
		extTypes:     extTypes,
		dataVersions: dv,
		objects:      make(map[string]*object.Object),
		restored:     make(map[string]chan struct{}),
	}
	s.drainer = newDrainer(s)
	s.drainer.Run()
	return s
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
		if len(t.Handlers) == 0 {
			return fmt.Errorf("spaceobjects: type[%d] (%q): zero handlers — types must own at least one dataset", i, t.Id)
		}
		for j, r := range t.Handlers {
			if r.Handler == nil {
				return fmt.Errorf("spaceobjects: type[%d] (%q) handler[%d]: nil Handler", i, t.Id, j)
			}
			name := r.Handler.Dataset()
			if name == "" {
				return fmt.Errorf("spaceobjects: type[%d] (%q) handler[%d]: empty Dataset()", i, t.Id, j)
			}
			if r.DataVersion == "" {
				return fmt.Errorf("spaceobjects: type[%d] (%q) handler[%d] (%q): empty DataVersion", i, t.Id, j, name)
			}
			if _, dup := builtinDataVersions[name]; dup {
				return fmt.Errorf("spaceobjects: type[%d] (%q) handler[%d]: dataset %q is reserved by a built-in", i, t.Id, j, name)
			}
			if _, dup := seenDatasets[name]; dup {
				return fmt.Errorf("spaceobjects: type[%d] (%q) handler[%d]: duplicate dataset %q across catalog", i, t.Id, j, name)
			}
			seenDatasets[name] = struct{}{}
		}
	}
	return nil
}

// Close shuts down per-Store background workers (currently the
// drainer). Safe to call multiple times.
func (s *Store) Close() error {
	return s.drainer.Close()
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

// SharedObjects returns the per-space `objects` collection, opening
// it on first call. Callers can hit it directly for cross-object
// queries (find all rows where any.name = X, etc.).
func (s *Store) SharedObjects(ctx context.Context) (anystore.Collection, error) {
	s.mu.Lock()
	if s.sharedColl != nil {
		coll := s.sharedColl
		s.mu.Unlock()
		return coll, nil
	}
	s.mu.Unlock()
	collName := s.spaceId + "/" + SpaceObjectsCollection
	coll, err := s.db.Collection(ctx, collName)
	if err != nil {
		return nil, fmt.Errorf("spaceobjects: open %s: %w", collName, err)
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

// Drop removes the cached *object.Object for objectId. Used after
// any-sync DeleteTree fires so a subsequent Get rebuilds (or fails
// with the appropriate any-sync deletion error).
//
// Does NOT touch the any-store collections — record rows persist
// until a separate cleanup pass. v1: leave them; queries skip
// tombstones, and a deleted object's id is content-addressable so
// it never reuses.
func (s *Store) Drop(objectId string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, objectId)
	delete(s.restored, objectId)
}

// Get returns the *object.Object for objectId, lazy-loading on first
// access. The space is acquired through the App's space cache; the
// per-object Controller is built once and reused.
func (s *Store) Get(ctx context.Context, objectId string) (*object.Object, error) {
	s.mu.Lock()
	if obj, ok := s.objects[objectId]; ok {
		s.mu.Unlock()
		return obj, nil
	}
	s.mu.Unlock()

	handle, err := s.app.GetSpace(ctx, s.spaceId)
	if err != nil {
		return nil, fmt.Errorf("spaceobjects: get space: %w", err)
	}
	obj, err := s.bind(ctx, handle, objectId, nil) // nil → BuildTree (existing tree)
	if err != nil {
		return nil, err
	}
	if err := s.coldRestoreOnce(ctx, objectId, obj); err != nil {
		return nil, err
	}
	return obj, nil
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
	})
	if err != nil {
		return nil, fmt.Errorf("spaceobjects: CreateTree: %w", err)
	}
	objectId := payload.RootRawChange.Id
	return s.bind(ctx, handle, objectId, &payload)
}

// Derive makes a deterministic object on the space. Idempotent — a
// second Derive with the same opts.ChangePayload returns the same
// objectId. If the tree already exists locally, falls back to Get.
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
	objectId := payload.RootRawChange.Id
	obj, err := s.bind(ctx, handle, objectId, &payload)
	if err != nil {
		return nil, err
	}
	if err := s.coldRestoreOnce(ctx, objectId, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

// bind constructs and caches the *object.Object for objectId. When
// payload is non-nil, PutTree is used (creation/derivation path); on
// ErrTreeExists or nil payload, falls back to BuildTree.
//
// Uses the cached kind to decide whether to wire the per-space
// `objects` shared collection — regular objects project values
// there; type objects keep their own metadata local.
func (s *Store) bind(ctx context.Context, handle anysyncx.SpaceHandle, objectId string, payload *treestorage.TreeStorageCreatePayload) (*object.Object, error) {
	s.mu.Lock()
	if obj, ok := s.objects[objectId]; ok {
		s.mu.Unlock()
		return obj, nil
	}
	s.mu.Unlock()

	ctrl, err := s.newController(ctx, objectId)
	if err != nil {
		return nil, err
	}
	obj := object.NewObject(s.spaceId, s.signKey, ctrl, s.alloc)
	obj.SetGate(s.gateFor(objectId))
	obj.SetAfterApply(s.afterApplyFor())

	tree, err := s.openTree(ctx, handle, objectId, payload, obj)
	if err != nil {
		return nil, err
	}
	obj.SetTree(tree)

	s.mu.Lock()
	if existing, ok := s.objects[objectId]; ok {
		s.mu.Unlock()
		return existing, nil
	}
	s.objects[objectId] = obj
	s.mu.Unlock()
	return obj, nil
}

// openTree picks PutTree vs BuildTree based on whether the caller has
// a creation payload in hand. Falls through to BuildTree when PutTree
// reports ErrTreeExists — supports idempotent Derive.
func (s *Store) openTree(ctx context.Context, handle anysyncx.SpaceHandle, objectId string, payload *treestorage.TreeStorageCreatePayload, listener *object.Object) (objecttree.ObjectTree, error) {
	tb := handle.Inner().TreeBuilder()
	if payload != nil {
		tree, err := tb.PutTree(ctx, *payload, listener)
		if err == nil {
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
	return tree, nil
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
func (s *Store) newController(ctx context.Context, objectId string) (*crdt.Controller, error) {
	coll, err := s.SharedObjects(ctx)
	if err != nil {
		return nil, err
	}
	shared := crdt.SharedCollections{properties.Dataset: coll}
	handlers := []crdt.Handler{
		properties.New(nil),
		typetype.PropertyHandler{},
		crdt.DefaultHandler{DatasetName: typetype.ShortIdsDataset, HandlerVersion: 1},
	}
	for _, t := range s.extTypes {
		for _, r := range t.Handlers {
			handlers = append(handlers, r.Handler)
		}
	}
	return crdt.NewControllerWithShared(ctx, objectId, s.db, shared, handlers...)
}

// coldRestoreOnce runs Object.ColdRestore the first time we see an
// objectId on this Store. Subsequent / concurrent calls block on
// the per-object ready channel until the first call's restore
// finishes — neither writers nor readers see a partial controller.
//
// The channel is closed in BOTH success and failure paths; if
// restore failed, the in-memory state is what it is, but at least
// callers don't deadlock waiting on a never-ready Object.
func (s *Store) coldRestoreOnce(ctx context.Context, objectId string, obj *object.Object) error {
	s.mu.Lock()
	if ready, ok := s.restored[objectId]; ok {
		s.mu.Unlock()
		// Another goroutine is doing (or did) the restore — wait it out.
		select {
		case <-ready:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ready := make(chan struct{})
	s.restored[objectId] = ready
	s.mu.Unlock()
	defer close(ready)
	if err := obj.ColdRestore(ctx); err != nil {
		return fmt.Errorf("spaceobjects: cold restore %s: %w", objectId, err)
	}
	return nil
}
