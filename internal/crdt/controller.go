package crdt

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"

	"github.com/anyproto/any-sync-sdk/internal/anyencx"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// Sentinel errors.
var (
	ErrMissingRecordId       = errors.New("crdt: RecordChange.Id is empty and Change.ChangeId is also empty")
	ErrEmptyIdRequiresUpsert = errors.New("crdt: RecordChange.Id is empty but Upsert is false")
	ErrMissingDataVersion    = errors.New("crdt: Change.DataVersion is empty")

	// ErrStrictSkipAbsent surfaces the documented strict-mode behaviour
	// (Upsert=false on an absent record = silent no-op) as a per-record
	// rejection. Without this, callers who happen to send a
	// non-existent record id with Upsert=false get a fully successful
	// WriteResult (RecordIds populated, Rejections empty) but the
	// record never lands in the projection — and the same change still
	// commits to the tree, so peers see the same skip. The rejection
	// lets writers detect the case via ApplyResult.Rejections without
	// changing the strict-mode semantics that other paths rely on.
	ErrStrictSkipAbsent = errors.New("crdt: strict modify (Upsert=false) skipped because record id does not exist")
)

// OpRejection records one per-op handler rejection. The op was
// validated, its prerequisites checked, and the handler returned an
// error — so the apply path skipped this op while letting siblings
// in the same RecordChange land. Surfaces through ApplyResult so
// the writer can tell that the change committed but with a hole.
//
// Whole-record drops (handler.BeforeCreate / BeforeDelete returning
// an error) also surface here with OpIndex = -1.
type OpRejection struct {
	RecordIndex int
	OpIndex     int // -1 for whole-record drops (BeforeCreate / BeforeDelete)
	RecordId    string
	Err         error
}

// ApplyResult is the auxiliary return from ApplyChangeWithResult —
// the change still committed (with a fresh VersionId), but specific
// ops or records were dropped. Empty Rejections means everything
// landed.
//
// DerivedOps surfaces, per ch.Records index, the extra ops the apply
// path stamped onto the record beyond the input ops: handler-emitted
// sink.derived ops (author, createdAt, …) and the modifier's own
// auto-stamped fields (_ver.id on create). The dispatcher merges
// these into the EventRecord so a viewer can reconstruct a fresh
// record without having to know which fields are "auto" vs "user".
// Nil when no record had extras (the common update path); per-record
// entry is nil when that record had none.
type ApplyResult struct {
	Rejections []OpRejection
	DerivedOps [][]Op
	// ApplySeq is the per-space apply sequence allocated for this
	// change (0 when the Controller has no allocator — unit tests).
	// Forwarded to the change-index feed so consumers cursor on it.
	ApplySeq uint64
}

// Reserved field names.
const (
	IdField        = "id"
	DeletedAtField = "_deletedAt"
	// AddSeqField records any-sync's per-space AddSeq watermark for the
	// change that last touched this record. Stamped by the apply path
	// (not a handler), reserved by the `_` prefix so user writes can't
	// collide. Backs the consumer-side change-index "records changed
	// since N" scans.
	AddSeqField = "_addSeq"
)

// Controller is the CRDT entrypoint for one any-sync object tree. It
// applies version-gated changes to an any-store database, one collection
// per dataset. The Controller owns no arena — any-store's DocBuffer pool
// provides arenas inside UpsertId/UpdateId modifiers.
//
// Transaction model: ApplyChange wraps its mutations in a WriteTx. When
// the caller already holds a WriteTx (batch apply for restore), pass its
// context — ApplyChange will create a savepoint inside the existing tx.
//
// Not safe for concurrent use.
type Controller struct {
	objectId string
	// spaceId scopes the per-object _meta row so the change-index query
	// can filter "objects in THIS space" — the SDK DB is shared across
	// spaces by default, and AddSeq numbering is per-space. Empty in
	// unit tests (the row is then unscoped and invisible to the
	// space-scoped query, which is fine for tests). Set via SetSpaceId.
	spaceId  string
	db       anystore.DB
	handlers map[string]Handler
	// versions and indexes are keyed by dataset name, populated from each
	// HandlerReg at registration. versions feeds the persisted _meta
	// handler-version map; indexes are ensured when the dataset's
	// collection is first opened.
	versions map[string]int
	indexes  map[string][]anystore.IndexInfo
	// schemas is the per-dataset field declaration (field classes +
	// Dynamic). Source of truth for synced/derived/local enforcement.
	schemas map[string]schema.Dataset
	// schemaRevs holds each runtime dataset's registered SchemaRev
	// fingerprint (nil/absent for static regs). Immutable after
	// construction, read lock-free by the store's staleness checks.
	schemaRevs map[string]string

	// collMu guards collections. Per-object collections are opened
	// lazily — on first write via db.Collection (creates), on read via
	// db.OpenCollection (no-create). Shared (per-space) handles are
	// installed at construction and never replaced. Reads can race
	// applies because they don't go through tree.Lock; the mutex
	// protects map access only.
	collMu      sync.Mutex
	collections map[string]anystore.Collection
	// collsReleased flips once CloseOwnedCollections has run (object
	// evicted from the space cache). After that the lazy open paths
	// stop caching handles: a stale Controller held past eviction
	// opens by name per call, so it can never retain a handle that a
	// successor controller's eviction later closes under it.
	collsReleased atomic.Bool
	// shared maps a dataset to a "shared" (typically per-space)
	// collection that supersedes the per-object collection. Writes to
	// such a dataset use the change's ObjectId as the row id (not
	// RecordChange.Id), so all objects in the space project into one
	// row each. Used for the per-space `objects` values collection.
	shared map[string]struct{}
	// scopeByKey marks datasets whose undeclared field heads carry
	// per-key scopes (HandlerReg.DynamicScopeByKey).
	scopeByKey map[string]bool

	metaColl    anystore.Collection // _meta — persisted maxAddSeq/maxApplySeq + handler versions
	maxAddSeq   uint64
	maxApplySeq uint64
	// applySeqs mints the per-space apply sequence (shared across the
	// space's Controllers). nil disables applySeq stamping (unit
	// tests, callers without a consumer feed).
	applySeqs *ApplySeqAllocator

	// applyHook runs inside the apply WriteTx after the record loop —
	// the read-tracking classification seam. nil disables it.
	applyHook ApplyHook

	// Pools for the per-RecordChange hot path. Both are scoped to this
	// Controller to keep contention bounded; the apply loop is single-
	// threaded today, so contention should be effectively zero.
	sinkPool     sync.Pool
	modifierPool sync.Pool
}

// SharedCollections lets callers supply pre-opened collections for
// specific datasets, overriding the default per-object naming. Used
// for the per-space `objects` values collection.
type SharedCollections map[string]anystore.Collection

// NewController opens or creates the per-dataset collections and returns a
// ready Controller. The caller provides the any-store DB (which may be
// shared across multiple Controllers for different objects). Collection
// names are `{objectId}/{datasetName}` unless a shared collection is
// supplied for that dataset, in which case the shared one is used and
// row ids in writes default to the change's ObjectId.
func NewController(ctx context.Context, objectId string, db anystore.DB, regs ...HandlerReg) (*Controller, error) {
	return NewControllerWithShared(ctx, objectId, db, nil, regs...)
}

// NewControllerWithShared opens the per-dataset collections, accepting
// shared overrides for specific datasets (typically the per-space
// `objects` values collection). Datasets not in `shared` get the
// usual per-object collection.
func NewControllerWithShared(ctx context.Context, objectId string, db anystore.DB, shared SharedCollections, regs ...HandlerReg) (*Controller, error) {
	c := &Controller{
		objectId:    objectId,
		db:          db,
		handlers:    make(map[string]Handler, len(regs)),
		versions:    make(map[string]int, len(regs)),
		indexes:     make(map[string][]anystore.IndexInfo, len(regs)),
		schemas:     make(map[string]schema.Dataset, len(regs)),
		collections: make(map[string]anystore.Collection, len(regs)),
		shared:      make(map[string]struct{}, len(shared)),
	}
	c.sinkPool.New = func() any { return &Sink{} }
	c.modifierPool.New = func() any { return &recordModifier{} }
	for name, coll := range shared {
		c.collections[name] = coll
		c.shared[name] = struct{}{}
	}
	for _, reg := range regs {
		if err := c.registerHandler(ctx, reg); err != nil {
			return nil, err
		}
	}
	// Open the per-DB _meta collection and seed maxAddSeq from any
	// previously-persisted watermark. Without this, every restart
	// would start at 0 and replayLocked would re-iterate every change
	// in the tree — and any silent skip during that replay would
	// permanently put a change behind the new in-session watermark.
	metaColl, err := db.Collection(ctx, MetaCollectionName)
	if err != nil {
		return nil, fmt.Errorf("crdt: open meta collection: %w", err)
	}
	c.metaColl = metaColl
	if err := ensureMetaIndexes(ctx, metaColl); err != nil {
		return nil, fmt.Errorf("crdt: ensure meta indexes: %w", err)
	}
	if _, err := c.LoadAndSeedMeta(ctx, metaColl); err != nil {
		return nil, fmt.Errorf("crdt: load meta: %w", err)
	}
	return c, nil
}

func (c *Controller) ObjectId() string  { return c.objectId }
func (c *Controller) MaxAddSeq() uint64 { return c.maxAddSeq }

// SetSpaceId scopes this controller's persisted _meta row to a space.
// Call once right after construction, before the first apply, so the
// change-index query can filter object rows by space. No-op on a nil
// controller.
func (c *Controller) SetSpaceId(spaceId string) {
	if c == nil {
		return
	}
	c.spaceId = spaceId
}

// SetMaxAddSeq seeds the watermark from persisted storage on restore.
func (c *Controller) SetMaxAddSeq(seq uint64) { c.maxAddSeq = seq }

// SetApplySeqAllocator wires the per-space apply-sequence allocator.
// Call once right after construction, before the first apply. No-op on
// a nil controller.
func (c *Controller) SetApplySeqAllocator(a *ApplySeqAllocator) {
	if c == nil {
		return
	}
	c.applySeqs = a
}

// ApplyHook runs inside the apply WriteTx after every record of a
// change has applied, before the watermark persist and commit — a
// returned error rolls the whole change back. recordIds are the
// resolved per-record ids (ChangeId sugar applied, shared-dataset
// collapse done). The read-tracking classification closure installs
// here so unread entries are atomic with the change.
type ApplyHook func(txCtx context.Context, ch *Change, recordIds []string, res *ApplyResult) error

// SetApplyHook wires the in-tx apply hook. Call once right after
// construction, before the first apply. No-op on a nil controller.
func (c *Controller) SetApplyHook(h ApplyHook) {
	if c == nil {
		return
	}
	c.applyHook = h
}

// RegisterHandler adds a handler at runtime (for late-bound datasets).
func (c *Controller) RegisterHandler(ctx context.Context, reg HandlerReg) error {
	return c.registerHandler(ctx, reg)
}

func (c *Controller) registerHandler(ctx context.Context, reg HandlerReg) error {
	name := reg.Name
	if name == "" {
		return fmt.Errorf("crdt: handler registration with empty dataset name")
	}
	if reg.Handler == nil {
		return fmt.Errorf("crdt: nil handler for dataset %q", name)
	}
	if _, dup := c.handlers[name]; dup {
		return fmt.Errorf("crdt: duplicate handler for dataset %q", name)
	}
	// A dataset must describe itself: either declare its fields or mark
	// itself Dynamic. This is the source of truth for synced/derived/
	// local enforcement — a silently-undeclared dataset would accept
	// anything as synced, defeating the point.
	if len(reg.Schema.Fields) == 0 && !reg.Schema.Dynamic {
		return fmt.Errorf("crdt: dataset %q registered without a schema (declare Fields or set Dynamic)", name)
	}
	if err := reg.Handler.Init(ctx); err != nil {
		return fmt.Errorf("crdt: init handler %q: %w", name, err)
	}
	c.handlers[name] = reg.Handler
	version := reg.Version
	if version == 0 {
		version = 1
	}
	c.versions[name] = version
	c.indexes[name] = reg.Indexes
	// Zero-value scopes are resolved once here (stamps → derived, rest →
	// synced) so field-class enforcement never sees an unset scope.
	c.schemas[name] = reg.Schema.Normalized()
	if reg.SchemaRev != "" {
		if c.schemaRevs == nil {
			c.schemaRevs = make(map[string]string)
		}
		c.schemaRevs[name] = reg.SchemaRev
	}
	if reg.DynamicScopeByKey {
		if c.scopeByKey == nil {
			c.scopeByKey = make(map[string]bool)
		}
		c.scopeByKey[name] = true
	}
	// Per-object collections are opened lazily — on first write
	// (creates) or on first read (no-create). This keeps unwritten
	// datasets (e.g. typetype's `properties` / `shortIds` on regular
	// objects, or an external type's handlers attached to every
	// Controller) from materialising empty rows in any-store.
	// Shared (per-space) collections are wired at construction time
	// by the caller and live in c.collections from the start — for
	// those we ensure indexes now, since the lazy path won't fire.
	if coll, ok := c.collections[name]; ok {
		if err := ensureHandlerIndexes(ctx, reg.Indexes, coll); err != nil {
			return fmt.Errorf("crdt: ensure indexes for %q: %w", name, err)
		}
		if err := ensureBuiltinIndexes(ctx, coll); err != nil {
			return fmt.Errorf("crdt: ensure builtin indexes for %q: %w", name, err)
		}
	}
	return nil
}

// HasDataset reports whether a handler is registered for the dataset.
// The handler map is immutable after construction, so this is safe to
// call concurrently (same contract as ValidateChange). Used by the
// store to detect controllers built before a runtime dataset appeared.
func (c *Controller) HasDataset(dataset string) bool {
	_, ok := c.handlers[dataset]
	return ok
}

// DatasetSchemaRev returns the SchemaRev the dataset registered with
// ("" for static regs / unknown datasets). Immutable after
// construction; concurrency-safe like HasDataset.
func (c *Controller) DatasetSchemaRev(dataset string) string {
	return c.schemaRevs[dataset]
}

// ensureHandlerIndexes calls EnsureIndex for every IndexInfo a dataset
// declared on its HandlerReg. No-op for an empty slice. EnsureIndex is
// idempotent, so callers may invoke this on every open without checking
// persistence.
func ensureHandlerIndexes(ctx context.Context, indexes []anystore.IndexInfo, coll anystore.Collection) error {
	for _, idx := range indexes {
		if err := coll.EnsureIndex(ctx, idx); err != nil {
			return err
		}
	}
	return nil
}

// builtinIndexes are ensured on every data collection (per-object and
// shared) on top of the handler's own indexes. The ascending _addSeq
// index backs the "records changed since N" scans the change-index feed
// relies on.
var builtinIndexes = []anystore.IndexInfo{{Name: "idx__addSeq", Fields: []string{AddSeqField}}}

// ensureBuiltinIndexes ensures the apply-layer indexes that every data
// collection gets regardless of which handler owns it. Idempotent.
func ensureBuiltinIndexes(ctx context.Context, coll anystore.Collection) error {
	return ensureHandlerIndexes(ctx, builtinIndexes, coll)
}

// collectionForWrite returns the on-disk collection for the dataset,
// opening it (and creating it if absent) on first use. Caches the
// handle so subsequent writes skip the open. Used by the apply path.
func (c *Controller) collectionForWrite(ctx context.Context, dataset string) (anystore.Collection, error) {
	c.collMu.Lock()
	if coll, ok := c.collections[dataset]; ok {
		c.collMu.Unlock()
		return coll, nil
	}
	c.collMu.Unlock()
	collName := c.objectId + "_" + dataset
	coll, err := c.db.Collection(ctx, collName)
	if err != nil {
		return nil, fmt.Errorf("crdt: open collection %q: %w", collName, err)
	}
	if err := ensureHandlerIndexes(ctx, c.indexes[dataset], coll); err != nil {
		return nil, fmt.Errorf("crdt: ensure indexes for %q: %w", dataset, err)
	}
	if err := ensureBuiltinIndexes(ctx, coll); err != nil {
		return nil, fmt.Errorf("crdt: ensure builtin indexes for %q: %w", dataset, err)
	}
	c.collMu.Lock()
	if existing, ok := c.collections[dataset]; ok {
		coll = existing
	} else if !c.collsReleased.Load() {
		c.collections[dataset] = coll
	}
	c.collMu.Unlock()
	return coll, nil
}

// collectionForRead returns the on-disk collection for the dataset
// without creating it. Returns nil when no writer has materialised
// the dataset yet — callers treat that as "no rows". Caches the
// handle on first successful open.
//
// The nil also covers an eviction race: any-store's ErrCollectionClosed
// WRAPS ErrCollectionNotFound, so a handle closed between resolution
// and use reads as "absent" for one call — the next call re-opens by
// name and sees the data again. Callers must treat the empty result as
// a snapshot, never persist it as a negative fact (none do today; keep
// it that way).
func (c *Controller) collectionForRead(ctx context.Context, dataset string) anystore.Collection {
	c.collMu.Lock()
	if coll, ok := c.collections[dataset]; ok {
		c.collMu.Unlock()
		return coll
	}
	c.collMu.Unlock()
	if _, ok := c.handlers[dataset]; !ok {
		return nil
	}
	coll, err := c.db.OpenCollection(ctx, c.objectId+"_"+dataset)
	if err != nil {
		return nil
	}
	if err := ensureHandlerIndexes(ctx, c.indexes[dataset], coll); err != nil {
		return nil
	}
	if err := ensureBuiltinIndexes(ctx, coll); err != nil {
		return nil
	}
	c.collMu.Lock()
	if existing, ok := c.collections[dataset]; ok {
		coll = existing
	} else if !c.collsReleased.Load() {
		c.collections[dataset] = coll
	}
	c.collMu.Unlock()
	return coll
}

// CloseOwnedCollections closes and forgets every cached PER-OBJECT
// collection handle (`<objectId>_<dataset>`). Shared (per-space)
// handles and the `_meta` collection stay untouched — the Store owns
// those, and they are shared across every controller in the space.
//
// Called on ocache eviction (Object.Close / TryClose): each open
// any-store handle pins its query-planner sketch and value caches for
// the process lifetime, so with a collection-per-object schema an
// account's heap grows linearly with objects ever touched unless idle
// objects release their handles.
//
// Recovery is open-by-name: the lazy paths (collectionForWrite /
// collectionForRead) re-open on the next touch, and after this call
// they stop caching (collsReleased), so a stale Controller retained
// past eviction can never hold a closed — or later-closed — handle.
//
// Serialization: the caller (Object close path) holds — or held —
// tree.Lock with Object.closed set, so no apply is in flight on this
// controller. Unlocked readers racing the close get a one-shot
// ErrCollectionClosed and re-open by name on retry.
func (c *Controller) CloseOwnedCollections() error {
	if c == nil {
		return nil
	}
	c.collMu.Lock()
	c.collsReleased.Store(true)
	var toClose []anystore.Collection
	for name, coll := range c.collections {
		if _, isShared := c.shared[name]; isShared {
			continue
		}
		delete(c.collections, name)
		toClose = append(toClose, coll)
	}
	c.collMu.Unlock()
	var firstErr error
	for _, coll := range toClose {
		if err := coll.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Collection returns the any-store collection backing the named
// dataset. Exposed so query / iteration paths can hit any-store
// directly without going through Controller's own helpers. Returns
// nil when the dataset has no registered handler or no writer has
// materialised the per-object collection on disk yet.
func (c *Controller) Collection(ctx context.Context, dataset string) anystore.Collection {
	c.collMu.Lock()
	coll, ok := c.collections[dataset]
	c.collMu.Unlock()
	if ok {
		return coll
	}
	return c.collectionForRead(ctx, dataset)
}

// RecordGetter returns a ChangeCtx.Get implementation bound to ctx —
// the one wrapper both the handler-hook and read-tracking classify
// seams share. When ctx carries a WriteTx (the apply path), reads
// reuse that tx (any-store's getReadTx), so calling it from inside a
// Modify callback or an ApplyHook is safe. Empty ids resolve to nil
// without a store round-trip.
func (c *Controller) RecordGetter(ctx context.Context) func(dataset, id string) *anyenc.Value {
	return func(dataset, id string) *anyenc.Value {
		if id == "" {
			return nil
		}
		return c.Get(ctx, dataset, id)
	}
}

// Get returns the record by id from the dataset, or nil if absent.
//
// The returned *anyenc.Value is cloned off any-store's pooled doc
// buffer — safe to retain past the FindId call. Without the clone
// the value would alias a buffer that's released back to the pool
// before this function returns (any-store's FindId releases via
// defer).
func (c *Controller) Get(ctx context.Context, dataset, id string) *anyenc.Value {
	coll := c.collectionForRead(ctx, dataset)
	if coll == nil {
		return nil
	}
	doc, err := coll.FindId(ctx, id)
	if err != nil {
		return nil
	}
	return anyencx.Clone(doc.Value())
}

// NextLocalVersion returns the VersionId to stamp on a device-local
// (Change.Local) write: one version for the whole change, strictly
// greater than every targeted field's current version on its record.
// Because local fields are handler-exclusive and never written by a
// synced change, a single bump above their current versions is
// monotonic and cannot be overshadowed by a competing synced write.
// Reads the current records by their explicit ids (local writes never
// use empty-id resolution). Safe to call under the apply lock.
func (c *Controller) NextLocalVersion(ctx context.Context, ch *Change) VersionId {
	var max VersionId
	for i := range ch.Records {
		rec := c.Get(ctx, ch.Dataset, ch.Records[i].Id)
		if rec == nil {
			continue
		}
		for _, op := range ch.Records[i].Ops {
			if v := GetRecordVersion(rec, op.Path...); v > max {
				max = v
			}
		}
	}
	return NextVersion(max)
}

// IsShared reports whether the dataset uses a per-space shared
// collection (write-side rule: row id = ObjectId, not RecordChange.Id).
// Subscribers projecting changes back to a path-based wire need this
// to look up the right post-apply row.
func (c *Controller) IsShared(dataset string) bool {
	if c == nil {
		return false
	}
	_, ok := c.shared[dataset]
	return ok
}

// Records returns all live (non-tombstone) records in the dataset.
//
// Each value is cloned off the iterator's reusable buffer onto its
// own parser arena so caller-held pointers stay valid past iteration
// (any-store's iter.Doc reuses one DocBuffer across Next() calls).
// For streaming over many rows, hit Collection(dataset).Find(...) +
// Iter directly and clone selectively — Records always materialises.
func (c *Controller) Records(ctx context.Context, dataset string) []*anyenc.Value {
	coll := c.collectionForRead(ctx, dataset)
	if coll == nil {
		return nil
	}
	iter, err := coll.Find(nil).Iter(ctx)
	if err != nil {
		return nil
	}
	defer iter.Close()
	var out []*anyenc.Value
	for iter.Next() {
		doc, err := iter.Doc()
		if err != nil {
			continue
		}
		v := doc.Value()
		if isTombstone(v) {
			continue
		}
		if cloned := anyencx.Clone(v); cloned != nil {
			out = append(out, cloned)
		}
	}
	return out
}

// ValidateChange runs the cheap up-front checks ApplyChange would
// otherwise do — without needing the change's final ChangeId or
// any DB reads:
//
//   - DataVersion non-empty.
//   - Dataset has a registered handler + collection.
//   - ObjectId, when set, matches the controller.
//   - Every op's path syntax legal.
//   - Every op honors its dataset's field-class scope (a synced
//     change may not write a local/derived field, etc.) — so a
//     scope-violating change never reaches AddContent and the DAG.
//   - Records that submit empty Id flag Upsert=true (the only
//     legal way to get an auto-derived id).
//
// Used by the local-write path to fail fast BEFORE handing the
// payload to any-sync's AddContent — a structurally invalid change
// stays out of the DAG entirely. Handler-level rejection (kind
// mismatch, terminal-status, etc.) still fires at apply time
// because it depends on the existing record state and needs a DB
// read.
//
// Safe to call concurrently; no state mutation.
func (c *Controller) ValidateChange(ch Change) error {
	if ch.DataVersion == "" {
		return ErrMissingDataVersion
	}
	if ch.ObjectId != "" && c.objectId != "" && ch.ObjectId != c.objectId {
		return fmt.Errorf("crdt: change for object %q applied to controller for %q", ch.ObjectId, c.objectId)
	}
	if _, ok := c.handlers[ch.Dataset]; !ok {
		return ErrUnknownDataset
	}
	ds := c.schemas[ch.Dataset]
	scopeByKey := c.scopeByKey[ch.Dataset]
	route := routeOf(&ch)
	for i, rc := range ch.Records {
		// Empty record id only resolves at apply time (when ChangeId
		// is known); enforcing the Upsert requirement here keeps
		// callers honest before AddContent.
		if rc.Id == "" && !rc.Upsert {
			return fmt.Errorf("%w (record index %d)", ErrEmptyIdRequiresUpsert, i)
		}
		for _, op := range rc.Ops {
			if err := opContentValid(ds, route, scopeByKey, op); err != nil {
				return errors.Join(ErrValidation, fmt.Errorf("record index %d op %s: %w", i, op.Type, err))
			}
		}
	}
	return nil
}

// PreValidateLocal runs the dataset handler's optional LocalPreValidator
// against a LOCAL change before it enters the DAG. No-op when the
// dataset has no handler or the handler doesn't implement the
// interface. Reads the current value of the change's target record and
// hands it to PreValidate; a non-nil error rejects the whole write.
//
// Unlike ValidateChange (pure, structural), this does a DB read — but
// it runs only on the local-write path, never on the inbound hot path.
// ch.ChangeId is not yet known here, so empty record ids can't be
// resolved; for the shared `objects` dataset the row id is the
// controller's own objectId, which is always known.
func (c *Controller) PreValidateLocal(ctx context.Context, ch *Change) error {
	if c == nil || ch == nil {
		return nil
	}
	h, ok := c.handlers[ch.Dataset]
	if !ok {
		return nil
	}
	// Multi-record validators resolve pre-state per record via the
	// getter — the batch-friendly path (a change carrying N explicit-id
	// records gets each one's current value, not just the first's).
	if pvm, ok := h.(LocalPreValidatorMulti); ok {
		return pvm.PreValidateMulti(ch, func(id string) *anyenc.Value {
			if id == "" {
				return nil
			}
			return c.Get(ctx, ch.Dataset, id)
		})
	}
	pv, ok := h.(LocalPreValidator)
	if !ok {
		return nil
	}
	var before *anyenc.Value
	if c.IsShared(ch.Dataset) {
		before = c.Get(ctx, ch.Dataset, c.objectId)
	} else if ids, err := ResolveRecordIds(*ch); err == nil && len(ids) > 0 {
		before = c.Get(ctx, ch.Dataset, ids[0])
	}
	return pv.PreValidate(ch, before)
}

// ApplyChange is the simple entry point — wraps ApplyChangeWithResult
// and discards the per-op rejection list. Existing callers that
// don't need rejection info (tests, replay paths) keep the original
// signature.
func (c *Controller) ApplyChange(ctx context.Context, ch Change) error {
	_, err := c.ApplyChangeWithResult(ctx, ch)
	return err
}

// ApplyChangeWithResult applies a single change per spec §7 and
// returns the list of per-op handler rejections. The change still
// commits even when ops are rejected — silent drops were the
// previous behavior; surfacing them here lets the writer detect
// "committed with a hole" without scanning post-state.
//
// Wraps all mutations in a WriteTx for atomicity. When the ctx
// already carries a WriteTx (batch mode), a savepoint is used
// instead — lightweight and correct.
//
// Path syntax validation is performed up-front and aborts the whole
// Change on failure (protocol-level bug). Handler validation runs
// inside the per-record Modify callback and drops only the
// offending op (recorded into ApplyResult.Rejections).
func (c *Controller) ApplyChangeWithResult(ctx context.Context, ch Change) (ApplyResult, error) {
	var res ApplyResult
	if ch.DataVersion == "" {
		return res, ErrMissingDataVersion
	}
	if ch.ObjectId != "" && c.objectId != "" && ch.ObjectId != c.objectId {
		return res, fmt.Errorf("crdt: change for object %q applied to controller for %q", ch.ObjectId, c.objectId)
	}
	handler, ok := c.handlers[ch.Dataset]
	if !ok {
		return res, ErrUnknownDataset
	}
	// Lazy-open the per-object collection on first write. Reads stay
	// tolerant of "not yet materialised" — see collectionForRead.
	coll, err := c.collectionForWrite(ctx, ch.Dataset)
	if err != nil {
		return res, err
	}

	// Resolve empty ids from ChangeId. For shared (per-space) datasets,
	// every record write coalesces into a single row keyed by the
	// change's ObjectId — collapse all resolved ids onto that.
	resolvedIds, err := resolveRecordIds(ch)
	if err != nil {
		return res, err
	}
	if _, isShared := c.shared[ch.Dataset]; isShared && ch.ObjectId != "" {
		for i := range resolvedIds {
			resolvedIds[i] = ch.ObjectId
		}
	}

	// Field-class enforcement (the dataset schema is the source of truth):
	//   - Derived fields are handler-only — no input op may write one;
	//   - Local fields never sync — only a Change.Local may write one,
	//     and a Change.Local may write ONLY Local fields;
	//   - on a non-Dynamic dataset, an undeclared field is rejected.
	// This keeps the classes disjoint, so a local field's locally-allocated
	// version never competes with a synced field's any-sync OrderId.
	//
	// Local+Injected mutual exclusion is a constructed-change invariant
	// (a DAG change is neither), so it stays a hard abort.
	if ch.Local && ch.Injected {
		return res, errors.Join(ErrValidation, errors.New("crdt: Local and Injected are mutually exclusive"))
	}
	// Per-op content validation (op-path syntax + field-class scope) is
	// NON-fatal here: an invalid op is dropped and recorded in
	// res.Rejections while the rest of the change still commits. This
	// honors the replay contract (object.replayLocked) — a single bad
	// historical change must not halt cold restore. Canonical case: a
	// field that was synced-scope when an older peer wrote it, later
	// reclassified local-scope; its historical synced $set now violates
	// field-class and would otherwise wedge every boot. For a multi-field
	// $set the offending field is shed key-by-key (filterOpFields) so the
	// op's still-valid siblings survive — without this, an old create that
	// packed the reclassified field in one op with its synced fields would
	// drop wholesale and the record would never materialize. Local writers
	// still fail fast — ValidateChange runs the same checks before
	// AddContent, so a fresh scope-violating change never enters the DAG.
	ds := c.schemas[ch.Dataset]
	scopeByKey := c.scopeByKey[ch.Dataset]
	route := routeOf(&ch)
	recordsCopied := false
	var filterArena anyenc.Arena // backs rewritten multi-field payloads; lives through apply
	for i := range ch.Records {
		ops := ch.Records[i].Ops
		kept := ops
		filtered := false
		for j, op := range ops {
			outOp, rejErrs, drop := filterOpFields(&filterArena, ds, route, scopeByKey, op)
			for _, e := range rejErrs {
				res.Rejections = append(res.Rejections, OpRejection{
					RecordIndex: i,
					OpIndex:     j,
					RecordId:    resolvedIds[i],
					Err:         errors.Join(ErrValidation, fmt.Errorf("op %s: %w", op.Type, e)),
				})
			}
			if (drop || len(rejErrs) > 0) && !filtered {
				// A dropped or rewritten op forces a copy of the caller's
				// ops slice so we never mutate it in place.
				kept = append([]Op(nil), ops[:j]...)
				filtered = true
			}
			if drop {
				continue
			}
			if filtered {
				kept = append(kept, outOp)
			}
		}
		if filtered {
			// Copy the records slice on first change, then swap in the trimmed ops.
			if !recordsCopied {
				ch.Records = append([]RecordChange(nil), ch.Records...)
				recordsCopied = true
			}
			ch.Records[i].Ops = kept
		}
	}

	// Apply inside a WriteTx (or savepoint if one is already active).
	tx, err := c.db.WriteTx(ctx)
	if err != nil {
		return res, fmt.Errorf("crdt: begin tx: %w", err)
	}
	txCtx := tx.Context()

	// Allocate the apply sequence AFTER the WriteTx is held: any-store's
	// single writer then serializes allocation order = commit order, so
	// an ascending-cursor consumer can't observe seq N+1 before N
	// committed. A rolled-back tx just skips its seq (gaps are fine).
	// Replays of already-applied changes re-stamp (the modifier can't
	// see whether ops gated to no-ops) — a spurious bump costs a
	// consumer one idempotent re-chunk, never a miss.
	if c.applySeqs != nil {
		ch.ApplySeq, err = c.applySeqs.Next(txCtx)
		if err != nil {
			_ = tx.Rollback()
			return res, fmt.Errorf("crdt: allocate applySeq: %w", err)
		}
	}

	// One getter for the whole change — backs ChangeCtx.Get in every
	// record's handler hooks (in-tx reads; see RecordGetter).
	getter := c.RecordGetter(txCtx)
	for i := range ch.Records {
		id := resolvedIds[i]
		recRej, recDerived, err := c.applyRecordChange(txCtx, coll, handler, &ch, id, &ch.Records[i], getter)
		if err != nil {
			_ = tx.Rollback()
			return res, err
		}
		// Tag rejections with the record-level context for the caller.
		for j := range recRej {
			recRej[j].RecordIndex = i
			recRej[j].RecordId = id
		}
		res.Rejections = append(res.Rejections, recRej...)
		if recDerived != nil {
			// Lazy-allocate so updates (where every record returns nil
			// derived) don't pay for the parent slice.
			if res.DerivedOps == nil {
				res.DerivedOps = make([][]Op, len(ch.Records))
			}
			res.DerivedOps[i] = recDerived
		}
	}

	if c.applyHook != nil {
		if err := c.applyHook(txCtx, &ch, resolvedIds, &res); err != nil {
			_ = tx.Rollback()
			return res, fmt.Errorf("crdt: apply hook: %w", err)
		}
	}

	// Persist the watermarks in the same WriteTx as the record
	// mutations: maxAddSeq/maxApplySeq + record state move atomically.
	// Without this, a crash between commit and a separate persist would
	// re-replay the change on next boot and (with the halt-on-error
	// iter) potentially get stuck if anything errored mid-iter. The
	// in-tx persist is also what makes the applySeq allocator
	// crash-safe without a counter row of its own — re-seeding reads
	// the max persisted stamp.
	newMaxAddSeq := c.maxAddSeq
	if ch.AddSeq > newMaxAddSeq {
		newMaxAddSeq = ch.AddSeq
	}
	newMaxApplySeq := c.maxApplySeq
	if ch.ApplySeq > newMaxApplySeq {
		newMaxApplySeq = ch.ApplySeq
	}
	if c.metaColl != nil {
		if err := PersistMeta(txCtx, c.metaColl, c.objectId, newMaxAddSeq, newMaxApplySeq, c.HandlerVersions(), c.spaceId); err != nil {
			_ = tx.Rollback()
			return res, fmt.Errorf("crdt: persist meta: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return res, fmt.Errorf("crdt: commit: %w", err)
	}

	// Commit succeeded — advance the in-memory mirrors.
	c.maxAddSeq = newMaxAddSeq
	c.maxApplySeq = newMaxApplySeq
	res.ApplySeq = ch.ApplySeq
	return res, nil
}

// routeOf maps a change's flags to the write route it arrived on —
// the scope a field must declare for the write to be admissible.
func routeOf(ch *Change) schema.Scope {
	switch {
	case ch.Local:
		return schema.ScopeLocal
	case ch.Injected:
		return schema.ScopeAccount
	default:
		return schema.ScopeSynced
	}
}

// classifyFieldWrite enforces the dataset field-class rules for an
// input op writing `field` (a top-level field name) on a change that
// arrived via `route` (synced DAG / LocalSet / the account mirror's
// InjectedSet). Reserved fields (id, _*) are already rejected by
// validateOpPaths, so this only sees user-facing field heads.
//
// scopeByKey (HandlerReg.DynamicScopeByKey) exempts UNDECLARED heads
// from the direction check: those datasets carry per-key scopes the
// controller can't see (the objects dataset's per-property scope),
// enforced by the dataset's handler (DAG route) and writer layer
// (local/account routes). Declared heads stay fully enforced.
func classifyFieldWrite(ds schema.Dataset, field string, route schema.Scope, scopeByKey bool) error {
	sc, declared := ds.ScopeOf(field)
	if !declared {
		if !ds.Dynamic {
			return fmt.Errorf("undeclared field on a non-dynamic dataset")
		}
		if scopeByKey {
			return nil // per-key scope — owned by the dataset layer
		}
		sc = schema.ScopeSynced // dynamic datasets default undeclared fields to synced
	}
	if sc == schema.ScopeDerived {
		return fmt.Errorf("derived field is handler-only, not writable by an input op")
	}
	if sc != route {
		return fmt.Errorf("%s field is not writable by the %s route", sc, route)
	}
	return nil
}

// opContentValid runs the per-op content checks — op-path syntax plus
// dataset field-class (scope) enforcement — for an op arriving via
// `route`. Shared by ValidateChange (local fail-fast: a failure keeps
// the whole change out of the DAG) and ApplyChangeWithResult (replay /
// inbound: a failure drops just the offending op so one bad change
// can't wedge cold restore). Reserved heads are already screened by
// validateOpPaths, so classifyFieldWrite only sees user field heads.
func opContentValid(ds schema.Dataset, route schema.Scope, scopeByKey bool, op Op) error {
	if err := validateOpPaths(op); err != nil {
		return err
	}
	for _, field := range opFieldHeads(op) {
		if err := classifyFieldWrite(ds, field, route, scopeByKey); err != nil {
			return err
		}
	}
	return nil
}

// filterOpFields runs the same per-op content checks as opContentValid,
// but salvages the valid parts of a multi-field $set/$unset instead of
// dropping the whole op. Each offending key is removed and reported in
// rejErrs; the surviving keys are kept in a rewritten op (out). drop is
// true only when the op carries no usable content — a single-path op
// that violates any rule (nothing to salvage), or a multi-field op whose
// every key offends.
//
// The canonical multi-field case is an old create that packed a
// since-reclassified field in with its siblings: e.g. spaceIndex
// localStatus, synced-scope when the create was written, later
// reclassified ScopeLocal. Its historical synced $set bundled
// type/name/remoteStatus/localStatus in one multi-field op; dropping the
// whole op on replay would lose the record entirely, so only the
// localStatus key is shed and the create still materializes. (localStatus
// is device-local and rebuilt off the local route anyway.)
//
// arena backs the rewritten payload container. The surviving values are
// referenced from the caller's change buffer — alive through apply, and
// re-cloned into the modifier arena by applySet — so they are not copied.
func filterOpFields(arena *anyenc.Arena, ds schema.Dataset, route schema.Scope, scopeByKey bool, op Op) (out Op, rejErrs []error, drop bool) {
	// Only the multi-field form has sub-structure to salvage. Single-path
	// ops, deletes, and malformed (non-object) multi-field payloads keep
	// whole-op semantics via opContentValid.
	isMultiField := (op.Type == OpSet || op.Type == OpUnset) && len(op.Path) == 0 &&
		op.Payload != nil && op.Payload.Type() == anyenc.TypeObject
	if !isMultiField {
		if err := opContentValid(ds, route, scopeByKey, op); err != nil {
			return op, []error{err}, true
		}
		return op, nil, false
	}

	kept := arena.NewObject()
	survivors := 0
	obj, _ := op.Payload.Object()
	obj.Visit(func(k []byte, v *anyenc.Value) {
		key := string(k)
		segments := strings.Split(key, ".")
		if err := validatePath(segments); err != nil {
			rejErrs = append(rejErrs, fmt.Errorf("key %q: %w", key, err))
			return
		}
		if err := classifyFieldWrite(ds, segments[0], route, scopeByKey); err != nil {
			rejErrs = append(rejErrs, fmt.Errorf("key %q: %w", key, err))
			return
		}
		kept.Set(key, v)
		survivors++
	})

	if len(rejErrs) == 0 {
		return op, nil, false // every key valid — keep the original op untouched
	}
	if survivors == 0 {
		return op, rejErrs, true // nothing salvageable — drop the whole op
	}
	out = op
	out.Payload = kept
	return out, rejErrs, false
}

// applyRecordChange applies one RecordChange via UpsertId or UpdateId,
// drains the handler's sink, and applies any sibling writes. Returns
// the per-op rejections recorded by the handler — the apply path
// keeps going past per-op failures, but surfaces them so callers
// know the change committed with a hole — plus the extra ops the
// modifier stamped beyond rc.Ops (handler-emitted derived ops,
// _ver.id creation marker), which the dispatcher merges into the
// EventRecord.
func (c *Controller) applyRecordChange(ctx context.Context, coll anystore.Collection, handler Handler, ch *Change, id string, rc *RecordChange, getter func(dataset, id string) *anyenc.Value) ([]OpRejection, []Op, error) {
	sink := c.sinkPool.Get().(*Sink)
	mod := c.modifierPool.Get().(*recordModifier)
	mod.set(handler, ch, id, rc, sink, getter)

	defer func() {
		mod.clear()
		c.modifierPool.Put(mod)
		sink.reset()
		c.sinkPool.Put(sink)
	}()

	if hasDelete(rc.Ops) || rc.Upsert {
		if _, err := coll.UpsertId(ctx, id, mod); err != nil {
			return nil, nil, err
		}
	} else {
		if _, err := coll.UpdateId(ctx, id, mod); err != nil {
			if errors.Is(err, anystore.ErrDocNotFound) {
				// Strict mode: the record doesn't exist. The change still
				// commits to the tree (caller already ran AddContent), but
				// nothing lands in the projection. Surface this as a
				// per-record rejection so callers can distinguish "wrote
				// successfully" from "structural id resolved but no
				// projection happened".
				return []OpRejection{{OpIndex: -1, RecordId: id, Err: ErrStrictSkipAbsent}}, nil, nil
			}
			return nil, nil, err
		}
	}

	rejections := mod.takeRejections()
	derived := mod.takeDerived()

	// Sibling writes go on different collections and so cannot reuse
	// the same modifier instance (collection-keyed locks). Each runs
	// its own UpsertId in the same tx. If the record itself was
	// rejected by the handler, drop siblings too — Project calls
	// before the rejection are honored only when the record lands.
	if mod.recordErr() != nil {
		return rejections, derived, nil
	}
	if len(sink.sibling) > 0 {
		if err := c.applySiblings(ctx, ch, sink.sibling); err != nil {
			return rejections, derived, err
		}
	}
	return rejections, derived, nil
}

// applySiblings runs each Sibling as its own UpsertId on the matching
// collection. Sibling.Record's ops are subject to the same _ver stamping
// as the triggering change. Unknown dataset = silent skip (the Sibling's
// dataset wasn't registered on this Controller).
func (c *Controller) applySiblings(ctx context.Context, ch *Change, siblings []Sibling) error {
	for i := range siblings {
		sib := &siblings[i]
		if _, ok := c.handlers[sib.Dataset]; !ok {
			// Unknown dataset for this controller: silent skip
			// (Sibling.Dataset wasn't registered).
			continue
		}
		coll, err := c.collectionForWrite(ctx, sib.Dataset)
		if err != nil {
			return err
		}
		mod := c.modifierPool.Get().(*recordModifier)
		mod.setSibling(ch, sib)
		op := sib.Record
		if hasDelete(op.Ops) || op.Upsert {
			_, err = coll.UpsertId(ctx, sib.Record.Id, mod)
		} else {
			_, err = coll.UpdateId(ctx, sib.Record.Id, mod)
			if errors.Is(err, anystore.ErrDocNotFound) {
				err = nil
			}
		}
		mod.clear()
		c.modifierPool.Put(mod)
		if err != nil {
			return err
		}
	}
	return nil
}

// ResolveRecordIds replaces empty RecordChange.Ids with ChangeId-
// derived values and returns the resolved id per record. First
// empty-id gets `base58(xxh3-64(ChangeId))`; subsequent empty ids get
// that seed with `:<index>` appended. Caller-supplied ids pass through
// unchanged.
//
// The suffix separator is `:` (not `/`) so resolved ids are safe to
// embed in REST URL path segments without extra encoding.
//
// Exposed so the write path can return the resolved record ids
// alongside VersionId/ChangeId — useful for property creates where
// the propId is the resolved (auto-derived) record id.
func ResolveRecordIds(ch Change) ([]string, error) { return resolveRecordIds(ch) }

func resolveRecordIds(ch Change) ([]string, error) {
	out := make([]string, len(ch.Records))
	emptySeen := 0
	var seed string
	for i, rc := range ch.Records {
		if rc.Id != "" {
			out[i] = rc.Id
			continue
		}
		if !rc.Upsert {
			return nil, fmt.Errorf("%w (record index %d)", ErrEmptyIdRequiresUpsert, i)
		}
		if ch.ChangeId == "" {
			return nil, ErrMissingRecordId
		}
		if seed == "" {
			seed = DeriveRecordId(ch.ChangeId)
		}
		if emptySeen == 0 {
			out[i] = seed
		} else {
			out[i] = seed + ":" + strconv.Itoa(emptySeen)
		}
		emptySeen++
	}
	return out, nil
}

func isTombstone(r *anyenc.Value) bool {
	if r == nil {
		return false
	}
	return r.Get(DeletedAtField) != nil
}

// recordModifier implements query.Modifier without a closure allocation
// per UpsertId call. Pulled from a Controller-scoped sync.Pool, populated
// via set or setSibling, used once, returned to the pool. All fields are
// pointers; after clear() the struct is garbage-free.
type recordModifier struct {
	handler Handler
	ch      *Change
	id      string
	rec     *RecordChange
	sink    *Sink

	// getter backs ChangeCtx.Get for the handler hooks; nil in sibling
	// mode (siblings run no hooks).
	getter func(dataset, id string) *anyenc.Value

	// sibling mode: when true, the modifier is applying a Sibling.Record
	// without invoking handler hooks.
	siblingMode bool
	siblingRec  *RecordChange

	// recordedErr is the first error that aborted Modify; ApplyChange
	// reads it after UpsertId returns.
	recordedErr error

	// rejections collects per-op handler errors during BeforeModify
	// drops. Populated only when the handler rejects an op but the
	// record itself still applies; whole-record drops use
	// recordedErr instead and surface as an OpIndex=-1 rejection.
	rejections []OpRejection

	// appliedDerived collects the extra ops that landed on the record
	// outside of rc.Ops: handler-emitted sink.derived stamps (author,
	// createdAt, …) and the modifier's own auto-stamped record fields
	// (_ver.id on create). Surfaced through takeDerived so the
	// dispatcher can project them onto the wire — without them, a
	// viewer reconstructing a new record would miss every auto field.
	// Buffer is pool-recycled; payloads live on appliedDerivedArena.
	appliedDerived      []Op
	appliedDerivedArena *anyenc.Arena
}

func (m *recordModifier) set(h Handler, ch *Change, id string, rc *RecordChange, sink *Sink, getter func(dataset, id string) *anyenc.Value) {
	m.handler = h
	m.ch = ch
	m.id = id
	m.rec = rc
	m.sink = sink
	m.getter = getter
	m.siblingMode = false
	m.siblingRec = nil
	m.recordedErr = nil
	m.rejections = m.rejections[:0]
	m.appliedDerived = m.appliedDerived[:0]
	m.appliedDerivedArena = nil
}

func (m *recordModifier) setSibling(ch *Change, sib *Sibling) {
	m.handler = nil
	m.ch = ch
	m.id = sib.Record.Id
	m.rec = nil
	m.sink = nil
	m.getter = nil
	m.siblingMode = true
	m.siblingRec = &sib.Record
	m.recordedErr = nil
	m.rejections = m.rejections[:0]
	m.appliedDerived = m.appliedDerived[:0]
	m.appliedDerivedArena = nil
}

func (m *recordModifier) clear() {
	m.handler = nil
	m.ch = nil
	m.id = ""
	m.rec = nil
	m.sink = nil
	m.getter = nil
	m.siblingRec = nil
	m.recordedErr = nil
	m.rejections = m.rejections[:0]
	m.appliedDerived = m.appliedDerived[:0]
	m.appliedDerivedArena = nil
}

func (m *recordModifier) recordErr() error { return m.recordedErr }

// takeRejections returns the per-record rejections accumulated by
// the modifier and resets the buffer. The returned slice is a fresh
// copy so the caller can retain it past the next set().
func (m *recordModifier) takeRejections() []OpRejection {
	if len(m.rejections) == 0 {
		return nil
	}
	out := make([]OpRejection, len(m.rejections))
	copy(out, m.rejections)
	m.rejections = m.rejections[:0]
	return out
}

// takeDerived returns the extra ops the modifier applied beyond
// rc.Ops — handler-emitted sink.derived stamps plus the modifier's
// own auto-stamped fields (_ver.id on create). The returned slice is
// a fresh copy so the caller can retain it past the next set();
// payload pointers stay rooted on appliedDerivedArena (kept alive by
// the Op references). Returns nil when the modifier emitted no
// extras, so the common update path pays one length check.
func (m *recordModifier) takeDerived() []Op {
	if len(m.appliedDerived) == 0 {
		return nil
	}
	out := make([]Op, len(m.appliedDerived))
	copy(out, m.appliedDerived)
	m.appliedDerived = m.appliedDerived[:0]
	return out
}

// derivedArena returns the modifier-scoped arena for synthetic op
// payloads (the auto-stamped record fields). Lazily allocated on
// first use so updates don't pay for it. Cleared on set/clear so
// each record gets its own arena and we don't accidentally retain
// payloads from a previous apply.
func (m *recordModifier) derivedArena() *anyenc.Arena {
	if m.appliedDerivedArena == nil {
		m.appliedDerivedArena = &anyenc.Arena{}
	}
	return m.appliedDerivedArena
}

// Modify is the apply algorithm hot path. Branches:
//   - sibling mode: apply the Sibling's ops as a plain modify, no handler
//   - delete: handler.BeforeDelete, then write tombstone
//   - new record: handler.BeforeCreate, then apply ops + drain sink.derived
//   - existing record: per-op handler.BeforeModify with rejection-skipping,
//     then drain sink.derived
func (m *recordModifier) Modify(a *anyenc.Arena, existing *anyenc.Value) (*anyenc.Value, bool, error) {
	if m.siblingMode {
		return m.applySibling(a, existing)
	}

	rc := m.rec
	ch := m.ch

	if hasDelete(rc.Ops) {
		if isTombstone(existing) {
			if rc.Upsert && lowerCreationMarker(a, existing, ch.VersionId) {
				stampAddSeq(a, existing, ch.AddSeq)
				stampApplySeq(a, existing, ch.ApplySeq)
				updateTraces(a, existing, *ch)
				return existing, true, nil
			}
			return existing, false, nil
		}
		ctx := &ChangeCtx{Change: ch, Before: existing, Get: m.getter}
		if err := m.handler.BeforeDelete(ctx, rc, m.sink); err != nil {
			m.recordedErr = err
			m.rejections = append(m.rejections, OpRejection{OpIndex: -1, Err: err})
			return existing, false, nil // drop record, batch keeps going
		}
		tomb := newTombstone(a, m.id, *ch, existing)
		updateTraces(a, tomb, *ch)
		return tomb, true, nil
	}

	// Detect new record: UpsertId pre-creates {id: ...} but no _ver.
	creating := existing.Get(VersionsKey) == nil

	if creating {
		// Stamp creation marker before BeforeCreate so handlers reading
		// existing see _ver.id already in place.
		ver := a.NewObject()
		ver.Set(IdField, a.NewString(string(ch.VersionId)))
		existing.Set(VersionsKey, ver)
		// Mirror the stamp as a synthetic derived op so the dispatcher
		// includes _ver.id in the create event. Allocates on the
		// modifier's own arena — a's lifetime ends with this Modify
		// call (any-store resets its DocBuffer), so we can't reuse
		// `ver` for the wire payload.
		m.appliedDerived = append(m.appliedDerived, Op{
			Type:    OpSet,
			Path:    []string{VersionsKey, IdField},
			Payload: m.derivedArena().NewString(string(ch.VersionId)),
		})

		// Local and Injected materializations are handler-exclusive —
		// their validation is writer-side (Properties.Set / the mirror).
		if !ch.Local && !ch.Injected {
			ctx := &ChangeCtx{Change: ch, Before: nil, Get: m.getter}
			if err := m.handler.BeforeCreate(ctx, rc, m.sink); err != nil {
				m.recordedErr = err
				m.rejections = append(m.rejections, OpRejection{OpIndex: -1, Err: err})
				return existing, false, nil
			}
			// Creation stamps ride the marker, not op verdicts: the
			// record now exists with _ver.id = ch.VersionId, so the
			// stamps are derived even when every op was dropped above.
			// Touch stamps ride the record-change itself (dense-index
			// contract — see TouchStamper).
			m.deriveCreateStamps(ctx)
			m.deriveTouchStamps(ctx)
		}
		for i := range rc.Ops {
			applyOp(a, existing, *ch, rc.Ops[i])
		}
		m.drainDerivedTo(a, existing, ch)
	} else if isTombstone(existing) {
		// Delete-wins: none of the ops land — the tombstone is sticky.
		// Report the absorption so a local caller can tell it from a
		// successful create; remote replays discard rejections. The
		// convergence bookkeeping below is unchanged.
		m.rejections = append(m.rejections, OpRejection{OpIndex: -1, Err: ErrRecordDeleted})
		if rc.Upsert && lowerCreationMarker(a, existing, ch.VersionId) {
			stampAddSeq(a, existing, ch.AddSeq)
			updateTraces(a, existing, *ch)
			return existing, true, nil
		}
		return existing, false, nil
	} else {
		if rc.Upsert {
			lowerCreationMarker(a, existing, ch.VersionId)
		}
		ctx := &ChangeCtx{Change: ch, Before: existing, Get: m.getter}
		// Creation-stamp offers are emitted at the same sites that touch
		// the _ver.id marker — every upsert on a live record — never from
		// the per-op accept path. Gating them on op acceptance diverges:
		// create-path and modify-path validation are asymmetric
		// (write-once / author-gated fields), so the causally-earliest
		// upsert can be all-rejected here while it stamped on the peer
		// where it created the record; the marker would move without an
		// offer and the stamps would split while _ver.id agrees. The
		// $setCreate min gate makes losing offers no-ops, so emitting
		// unconditionally is convergent (see CreateStamper).
		if !ch.Local && !ch.Injected {
			if rc.Upsert {
				m.deriveCreateStamps(ctx)
			}
			// Touch stamps bump for EVERY synced record-change — upsert
			// or strict, ops surviving or not (see TouchStamper).
			m.deriveTouchStamps(ctx)
		}
		for i := range rc.Ops {
			op := &rc.Ops[i]
			// Local and Injected materializations are handler-exclusive
			// (see Change.Local / Change.Injected): no handler
			// validation/derivation, just the gated apply.
			if ch.Local || ch.Injected {
				applyOp(a, existing, *ch, *op)
				continue
			}
			m.beforeModifyApply(a, existing, ctx, op, i)
		}
		m.drainDerivedTo(a, existing, ch)
	}

	stampAddSeq(a, existing, ch.AddSeq)
	stampApplySeq(a, existing, ch.ApplySeq)
	updateTraces(a, existing, *ch)
	compactVersions(a, existing)
	return existing, true, nil
}

// beforeModifyApply runs the handler's BeforeModify gate and applies op
// to existing when it passes, recording per-op rejections otherwise (the
// rest of the RecordChange still applies — a drop leaves a hole, never a
// fatal). For the multi-field $set/$unset form it salvages per-key: when
// the handler rejects the combined op, each key is re-probed as the
// independent single-path op it is defined to be (spec §5.1) and only the
// rejected keys are shed — the surviving keys still apply.
//
// Without this, a handler constraint that tightened AFTER a change was
// written — a field pinned/closed, a status turned terminal, or a field
// reclassified to another scope — would drop the whole bundled op and
// silently lose its unrelated sibling edits. Changing such rules is
// routine, so the all-or-nothing drop would wedge otherwise-valid history
// (e.g. an old spaceIndex create that packed `type` with name/status
// replaying through the modify path). This is the handler-rule twin of
// the field-class salvage filterOpFields does one layer up.
//
// Safe to call BeforeModify more than once: a handler's BeforeModify
// writes the sink only through per-record-deduped stamps (DeriveOnce /
// pre-checked queues — the modifiedAt bump; creation stamps are the
// modifier's job, see CreateStamper), so the combined-then-per-key
// probing queues nothing twice, and each handler's single-path branch
// is the per-key equivalent of its multi-field branch.
func (m *recordModifier) beforeModifyApply(a *anyenc.Arena, existing *anyenc.Value, ctx *ChangeCtx, op *Op, opIndex int) {
	isMultiField := (op.Type == OpSet || op.Type == OpUnset) && len(op.Path) == 0 &&
		op.Payload != nil && op.Payload.Type() == anyenc.TypeObject

	err := m.handler.BeforeModify(ctx, m.rec, op, m.sink)
	if err == nil {
		applyOp(a, existing, *m.ch, *op)
		return
	}
	if !isMultiField {
		// Single-path op (or malformed payload): nothing to salvage.
		m.rejections = append(m.rejections, OpRejection{OpIndex: opIndex, Err: err})
		return
	}

	// Combined op rejected: find the offending keys by probing each
	// independently and apply the survivors as a trimmed multi-field op.
	kept := a.NewObject()
	survivors := 0
	obj, _ := op.Payload.Object()
	obj.Visit(func(k []byte, v *anyenc.Value) {
		probe := Op{Type: op.Type, Path: strings.Split(string(k), "."), Payload: v}
		if perr := m.handler.BeforeModify(ctx, m.rec, &probe, m.sink); perr != nil {
			m.rejections = append(m.rejections, OpRejection{OpIndex: opIndex, Err: perr})
			return
		}
		kept.Set(string(k), v)
		survivors++
	})
	if survivors == 0 {
		return
	}
	applyOp(a, existing, *m.ch, Op{Type: op.Type, Payload: kept})
}

// deriveCreateStamps asks a CreateStamper handler for its $setCreate
// creation-stamp offers. Called from Modify at exactly the sites that
// touch the _ver.id marker (see CreateStamper); a handler that doesn't
// stamp, or a sibling-mode modifier with no handler, is a no-op.
func (m *recordModifier) deriveCreateStamps(ctx *ChangeCtx) {
	if m.handler == nil || m.sink == nil {
		return
	}
	if cs, ok := m.handler.(CreateStamper); ok {
		cs.DeriveCreateStamps(ctx, m.sink)
	}
}

// deriveTouchStamps asks a TouchStamper handler for its per-change
// touch stamps (modifiedAt-class fields). Called from Modify once per
// synced record-change, create and modify alike (see TouchStamper).
func (m *recordModifier) deriveTouchStamps(ctx *ChangeCtx) {
	if m.handler == nil || m.sink == nil {
		return
	}
	if ts, ok := m.handler.(TouchStamper); ok {
		ts.DeriveTouchStamps(ctx, m.sink)
	}
}

// applySibling is the modifier path for a Sibling write — no handler, no
// sink, just the same _ver stamping and apply mechanics so siblings land
// with the triggering change's VersionId.
func (m *recordModifier) applySibling(a *anyenc.Arena, existing *anyenc.Value) (*anyenc.Value, bool, error) {
	rc := m.siblingRec
	ch := m.ch

	if hasDelete(rc.Ops) {
		if isTombstone(existing) {
			if rc.Upsert && lowerCreationMarker(a, existing, ch.VersionId) {
				stampAddSeq(a, existing, ch.AddSeq)
				stampApplySeq(a, existing, ch.ApplySeq)
				updateTraces(a, existing, *ch)
				return existing, true, nil
			}
			return existing, false, nil
		}
		tomb := newTombstone(a, m.id, *ch, existing)
		updateTraces(a, tomb, *ch)
		return tomb, true, nil
	}

	if existing.Get(VersionsKey) == nil {
		ver := a.NewObject()
		ver.Set(IdField, a.NewString(string(ch.VersionId)))
		existing.Set(VersionsKey, ver)
	} else if isTombstone(existing) {
		if rc.Upsert && lowerCreationMarker(a, existing, ch.VersionId) {
			stampAddSeq(a, existing, ch.AddSeq)
			updateTraces(a, existing, *ch)
			return existing, true, nil
		}
		return existing, false, nil
	} else if rc.Upsert {
		lowerCreationMarker(a, existing, ch.VersionId)
	}

	for i := range rc.Ops {
		applyOp(a, existing, *ch, rc.Ops[i])
	}
	stampAddSeq(a, existing, ch.AddSeq)
	stampApplySeq(a, existing, ch.ApplySeq)
	updateTraces(a, existing, *ch)
	compactVersions(a, existing)
	return existing, true, nil
}

// drainDerivedTo applies and clears Sink.derived against target (the
// record root). No-op when no handler is wired (sibling path) or when
// nothing was emitted.
//
// Captures the drained ops onto m.appliedDerived so the dispatcher can
// project them onto the wire. Payloads live on the handler's own arena
// (stamping handlers — stampAutoFields, SpaceIndexHandler.BeforeCreate —
// allocate a fresh &anyenc.Arena{} per call) — the Op references hold
// that arena alive past the apply call.
func (m *recordModifier) drainDerivedTo(a *anyenc.Arena, target *anyenc.Value, ch *Change) {
	if m.sink == nil || len(m.sink.derived) == 0 {
		return
	}
	for i := range m.sink.derived {
		applyOp(a, target, *ch, m.sink.derived[i])
	}
	m.appliedDerived = append(m.appliedDerived, m.sink.derived...)
	m.sink.derived = m.sink.derived[:0]
}

// Compile-time check that recordModifier satisfies query.Modifier.
var _ query.Modifier = (*recordModifier)(nil)
