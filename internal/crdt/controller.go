package crdt

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/anyenc/anyencutil"
	"github.com/anyproto/any-store/v2/query"
)

// Sentinel errors.
var (
	ErrMissingRecordId       = errors.New("crdt: RecordChange.Id is empty and Change.ChangeId is also empty")
	ErrEmptyIdRequiresUpsert = errors.New("crdt: RecordChange.Id is empty but Upsert is false")
	ErrMissingDataVersion    = errors.New("crdt: Change.DataVersion is empty")
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
type ApplyResult struct {
	Rejections []OpRejection
}

// Reserved field names.
const (
	IdField        = "id"
	DeletedAtField = "_deletedAt"
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
	objectId    string
	db          anystore.DB
	handlers    map[string]Handler
	collections map[string]anystore.Collection
	// shared maps a dataset to a "shared" (typically per-space)
	// collection that supersedes the per-object collection. Writes to
	// such a dataset use the change's ObjectId as the row id (not
	// RecordChange.Id), so all objects in the space project into one
	// row each. Used for the per-space `objects` values collection.
	shared    map[string]struct{}
	metaColl  anystore.Collection // _meta — persisted maxAddSeq + handler versions
	maxAddSeq uint64

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
func NewController(ctx context.Context, objectId string, db anystore.DB, handlers ...Handler) (*Controller, error) {
	return NewControllerWithShared(ctx, objectId, db, nil, handlers...)
}

// NewControllerWithShared opens the per-dataset collections, accepting
// shared overrides for specific datasets (typically the per-space
// `objects` values collection). Datasets not in `shared` get the
// usual per-object collection.
func NewControllerWithShared(ctx context.Context, objectId string, db anystore.DB, shared SharedCollections, handlers ...Handler) (*Controller, error) {
	c := &Controller{
		objectId:    objectId,
		db:          db,
		handlers:    make(map[string]Handler, len(handlers)),
		collections: make(map[string]anystore.Collection, len(handlers)),
		shared:      make(map[string]struct{}, len(shared)),
	}
	c.sinkPool.New = func() any { return &Sink{} }
	c.modifierPool.New = func() any { return &recordModifier{} }
	for name, coll := range shared {
		c.collections[name] = coll
		c.shared[name] = struct{}{}
	}
	for _, h := range handlers {
		if err := c.registerHandler(ctx, h); err != nil {
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
	if _, err := c.LoadAndSeedMeta(ctx, metaColl); err != nil {
		return nil, fmt.Errorf("crdt: load meta: %w", err)
	}
	return c, nil
}

func (c *Controller) ObjectId() string  { return c.objectId }
func (c *Controller) MaxAddSeq() uint64 { return c.maxAddSeq }

// SetMaxAddSeq seeds the watermark from persisted storage on restore.
func (c *Controller) SetMaxAddSeq(seq uint64) { c.maxAddSeq = seq }

// RegisterHandler adds a handler at runtime (for late-bound datasets).
func (c *Controller) RegisterHandler(ctx context.Context, h Handler) error {
	return c.registerHandler(ctx, h)
}

func (c *Controller) registerHandler(ctx context.Context, h Handler) error {
	name := h.Dataset()
	if _, dup := c.handlers[name]; dup {
		return fmt.Errorf("crdt: duplicate handler for dataset %q", name)
	}
	if err := h.Init(ctx); err != nil {
		return fmt.Errorf("crdt: init handler %q: %w", name, err)
	}
	c.handlers[name] = h
	if _, isShared := c.shared[name]; isShared {
		// Shared collection was wired at construction time; reuse it.
		return nil
	}
	collName := c.objectId + "/" + name
	coll, err := c.db.Collection(ctx, collName)
	if err != nil {
		return fmt.Errorf("crdt: open collection %q: %w", collName, err)
	}
	c.collections[name] = coll
	return nil
}

// Collection returns the any-store collection backing the named
// dataset. Exposed so query / iteration paths can hit any-store
// directly without going through Controller's own helpers. Returns
// nil when the dataset has no registered handler.
func (c *Controller) Collection(dataset string) anystore.Collection {
	return c.collections[dataset]
}

// Get returns the record by id from the dataset, or nil if absent.
//
// The returned *anyenc.Value is cloned off any-store's pooled doc
// buffer — safe to retain past the FindId call. Without the clone
// the value would alias a buffer that's released back to the pool
// before this function returns (any-store's FindId releases via
// defer).
func (c *Controller) Get(ctx context.Context, dataset, id string) *anyenc.Value {
	coll, ok := c.collections[dataset]
	if !ok {
		return nil
	}
	doc, err := coll.FindId(ctx, id)
	if err != nil {
		return nil
	}
	return cloneValue(doc.Value())
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
	coll, ok := c.collections[dataset]
	if !ok {
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
		if cloned := cloneValue(v); cloned != nil {
			out = append(out, cloned)
		}
	}
	return out
}

// cloneValue copies an anyenc value off whatever buffer it currently
// lives on (any-store's pooled DocBuffer or an iterator's reused
// parser) onto a fresh parser arena. Wraps anyencutil.Value.FillCopy
// — the canonical helper.
//
// Each call allocates one fresh anyencutil.Value (i.e. one fresh
// Parser + scratch buf). The returned *anyenc.Value points into
// that parser's arena; Go's GC keeps the parser alive as long as
// any caller retains the value.
func cloneValue(v *anyenc.Value) *anyenc.Value {
	if v == nil {
		return nil
	}
	var w anyencutil.Value
	w.FillCopy(v)
	return w.Value
}

// ValidateChange runs the cheap up-front checks ApplyChange would
// otherwise do — without needing the change's final ChangeId or
// any DB reads:
//
//   - DataVersion non-empty.
//   - Dataset has a registered handler + collection.
//   - ObjectId, when set, matches the controller.
//   - Every op's path syntax legal.
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
	if _, ok := c.collections[ch.Dataset]; !ok {
		return ErrUnknownDataset
	}
	for i, rc := range ch.Records {
		// Empty record id only resolves at apply time (when ChangeId
		// is known); enforcing the Upsert requirement here keeps
		// callers honest before AddContent.
		if rc.Id == "" && !rc.Upsert {
			return fmt.Errorf("%w (record index %d)", ErrEmptyIdRequiresUpsert, i)
		}
		for _, op := range rc.Ops {
			if err := validateOpPaths(op); err != nil {
				return errors.Join(ErrValidation, fmt.Errorf("record index %d op %s: %w", i, op.Type, err))
			}
		}
	}
	return nil
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
	coll, ok := c.collections[ch.Dataset]
	if !ok {
		return res, ErrUnknownDataset
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

	// Path syntax: whole-change abort on failure (docs/types-properties-
	// proposal.md § "Validation atomicity"). Handler validation moves
	// into the Modify callback below.
	for i, rc := range ch.Records {
		for _, op := range rc.Ops {
			if err := validateOpPaths(op); err != nil {
				return res, errors.Join(ErrValidation, fmt.Errorf("record %q op %s: %w", resolvedIds[i], op.Type, err))
			}
		}
	}

	// Apply inside a WriteTx (or savepoint if one is already active).
	tx, err := c.db.WriteTx(ctx)
	if err != nil {
		return res, fmt.Errorf("crdt: begin tx: %w", err)
	}
	txCtx := tx.Context()

	for i := range ch.Records {
		id := resolvedIds[i]
		recRej, err := c.applyRecordChange(txCtx, coll, handler, &ch, id, &ch.Records[i])
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
	}

	// Persist the watermark in the same WriteTx as the record
	// mutations: maxAddSeq + record state move atomically. Without
	// this, a crash between commit and a separate persist would
	// re-replay the change on next boot and (with the halt-on-error
	// iter) potentially get stuck if anything errored mid-iter.
	newMaxAddSeq := c.maxAddSeq
	if ch.AddSeq > newMaxAddSeq {
		newMaxAddSeq = ch.AddSeq
	}
	if c.metaColl != nil {
		if err := PersistMeta(txCtx, c.metaColl, c.objectId, newMaxAddSeq, c.HandlerVersions()); err != nil {
			_ = tx.Rollback()
			return res, fmt.Errorf("crdt: persist meta: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return res, fmt.Errorf("crdt: commit: %w", err)
	}

	// Commit succeeded — advance the in-memory mirror.
	c.maxAddSeq = newMaxAddSeq
	return res, nil
}

// applyRecordChange applies one RecordChange via UpsertId or UpdateId,
// drains the handler's sink, and applies any sibling writes. Returns
// the per-op rejections recorded by the handler — the apply path
// keeps going past per-op failures, but surfaces them so callers
// know the change committed with a hole.
func (c *Controller) applyRecordChange(ctx context.Context, coll anystore.Collection, handler Handler, ch *Change, id string, rc *RecordChange) ([]OpRejection, error) {
	sink := c.sinkPool.Get().(*Sink)
	mod := c.modifierPool.Get().(*recordModifier)
	mod.set(handler, ch, id, rc, sink)

	defer func() {
		mod.clear()
		c.modifierPool.Put(mod)
		sink.reset()
		c.sinkPool.Put(sink)
	}()

	if hasDelete(rc.Ops) || rc.Upsert {
		if _, err := coll.UpsertId(ctx, id, mod); err != nil {
			return nil, err
		}
	} else {
		if _, err := coll.UpdateId(ctx, id, mod); err != nil {
			if errors.Is(err, anystore.ErrDocNotFound) {
				return nil, nil // strict mode: silent skip on absent
			}
			return nil, err
		}
	}

	rejections := mod.takeRejections()

	// Sibling writes go on different collections and so cannot reuse
	// the same modifier instance (collection-keyed locks). Each runs
	// its own UpsertId in the same tx. If the record itself was
	// rejected by the handler, drop siblings too — Project calls
	// before the rejection are honored only when the record lands.
	if mod.recordErr() != nil {
		return rejections, nil
	}
	if len(sink.sibling) > 0 {
		if err := c.applySiblings(ctx, ch, sink.sibling); err != nil {
			return rejections, err
		}
	}
	return rejections, nil
}

// applySiblings runs each Sibling as its own UpsertId on the matching
// collection. Sibling.Record's ops are subject to the same _ver stamping
// as the triggering change. Unknown dataset = silent skip (the Sibling's
// dataset wasn't registered on this Controller).
func (c *Controller) applySiblings(ctx context.Context, ch *Change, siblings []Sibling) error {
	for i := range siblings {
		sib := &siblings[i]
		coll, ok := c.collections[sib.Dataset]
		if !ok {
			continue
		}
		mod := c.modifierPool.Get().(*recordModifier)
		mod.setSibling(ch, sib)
		op := sib.Record
		var err error
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
// that seed with `/<index>` appended. Caller-supplied ids pass through
// unchanged.
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
			out[i] = seed + "/" + strconv.Itoa(emptySeen)
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
}

func (m *recordModifier) set(h Handler, ch *Change, id string, rc *RecordChange, sink *Sink) {
	m.handler = h
	m.ch = ch
	m.id = id
	m.rec = rc
	m.sink = sink
	m.siblingMode = false
	m.siblingRec = nil
	m.recordedErr = nil
	m.rejections = m.rejections[:0]
}

func (m *recordModifier) setSibling(ch *Change, sib *Sibling) {
	m.handler = nil
	m.ch = ch
	m.id = sib.Record.Id
	m.rec = nil
	m.sink = nil
	m.siblingMode = true
	m.siblingRec = &sib.Record
	m.recordedErr = nil
	m.rejections = m.rejections[:0]
}

func (m *recordModifier) clear() {
	m.handler = nil
	m.ch = nil
	m.id = ""
	m.rec = nil
	m.sink = nil
	m.siblingRec = nil
	m.recordedErr = nil
	m.rejections = m.rejections[:0]
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
				updateTraces(a, existing, *ch)
				return existing, true, nil
			}
			return existing, false, nil
		}
		ctx := &ChangeCtx{Change: ch, Before: existing}
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
		// existing see _ver.id already in place. Marker stays at root
		// regardless of variant — creation is record-level.
		ver := a.NewObject()
		ver.Set(IdField, a.NewString(string(ch.VersionId)))
		existing.Set(VersionsKey, ver)

		ctx := &ChangeCtx{Change: ch, Before: nil}
		if err := m.handler.BeforeCreate(ctx, rc, m.sink); err != nil {
			m.recordedErr = err
			m.rejections = append(m.rejections, OpRejection{OpIndex: -1, Err: err})
			return existing, false, nil
		}
		target := variantTarget(a, existing, rc.Variant)
		for i := range rc.Ops {
			applyOp(a, target, *ch, rc.Ops[i])
		}
		// Derived ops are record-level by convention (author, createdAt,
		// id-like markers), so they target root regardless of the
		// triggering variant.
		m.drainDerivedTo(a, existing, ch)
	} else if isTombstone(existing) {
		if rc.Upsert && lowerCreationMarker(a, existing, ch.VersionId) {
			updateTraces(a, existing, *ch)
			return existing, true, nil
		}
		return existing, false, nil
	} else {
		if rc.Upsert {
			lowerCreationMarker(a, existing, ch.VersionId)
		}
		ctx := &ChangeCtx{Change: ch, Before: existing}
		target := variantTarget(a, existing, rc.Variant)
		for i := range rc.Ops {
			op := &rc.Ops[i]
			if err := m.handler.BeforeModify(ctx, rc, op, m.sink); err != nil {
				// Per-op drop; other ops in the same RecordChange
				// still apply. Surface so the caller knows the
				// change committed with a hole.
				m.rejections = append(m.rejections, OpRejection{OpIndex: i, Err: err})
				continue
			}
			applyOp(a, target, *ch, *op)
		}
		// Derived ops are record-level — see BeforeCreate path.
		m.drainDerivedTo(a, existing, ch)
	}

	updateTraces(a, existing, *ch)
	return existing, true, nil
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
			updateTraces(a, existing, *ch)
			return existing, true, nil
		}
		return existing, false, nil
	} else if rc.Upsert {
		lowerCreationMarker(a, existing, ch.VersionId)
	}

	target := variantTarget(a, existing, rc.Variant)
	for i := range rc.Ops {
		applyOp(a, target, *ch, rc.Ops[i])
	}
	updateTraces(a, existing, *ch)
	return existing, true, nil
}

// drainDerivedTo applies and clears Sink.derived against target. Caller
// is responsible for picking target — same routing as the original ops
// (root or variant subdoc). No-op when no handler is wired (sibling
// path) or when nothing was emitted.
func (m *recordModifier) drainDerivedTo(a *anyenc.Arena, target *anyenc.Value, ch *Change) {
	if m.sink == nil || len(m.sink.derived) == 0 {
		return
	}
	for i := range m.sink.derived {
		applyOp(a, target, *ch, m.sink.derived[i])
	}
	m.sink.derived = m.sink.derived[:0]
}

// variantTarget returns the subdocument under root[variant], lazily
// creating it as an empty object if absent. Empty variant means "no
// routing" — return root itself, so existing variantless datasets
// keep their current behavior with zero overhead.
func variantTarget(a *anyenc.Arena, root *anyenc.Value, variant string) *anyenc.Value {
	if variant == "" {
		return root
	}
	sub := root.Get(variant)
	if sub == nil || sub.Type() != anyenc.TypeObject {
		sub = a.NewObject()
		root.Set(variant, sub)
	}
	return sub
}

// Compile-time check that recordModifier satisfies query.Modifier.
var _ query.Modifier = (*recordModifier)(nil)
