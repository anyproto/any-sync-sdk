package crdt

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/query"
)

// Sentinel errors.
var (
	ErrMissingRecordId       = errors.New("crdt: RecordChange.Id is empty and Change.ChangeId is also empty")
	ErrEmptyIdRequiresUpsert = errors.New("crdt: RecordChange.Id is empty but Upsert is false")
	ErrMissingDataVersion    = errors.New("crdt: Change.DataVersion is empty")
)

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
	maxAddSeq   uint64
}

// NewController opens or creates the per-dataset collections and returns a
// ready Controller. The caller provides the any-store DB (which may be
// shared across multiple Controllers for different objects). Collection
// names are `{objectId}/{datasetName}`.
func NewController(ctx context.Context, objectId string, db anystore.DB, handlers ...Handler) (*Controller, error) {
	c := &Controller{
		objectId:    objectId,
		db:          db,
		handlers:    make(map[string]Handler, len(handlers)),
		collections: make(map[string]anystore.Collection, len(handlers)),
	}
	for _, h := range handlers {
		name := h.Dataset()
		if _, dup := c.handlers[name]; dup {
			return nil, fmt.Errorf("crdt: duplicate handler for dataset %q", name)
		}
		c.handlers[name] = h
		collName := objectId + "/" + name
		coll, err := db.Collection(ctx, collName)
		if err != nil {
			return nil, fmt.Errorf("crdt: open collection %q: %w", collName, err)
		}
		c.collections[name] = coll
	}
	return c, nil
}

func (c *Controller) ObjectId() string  { return c.objectId }
func (c *Controller) MaxAddSeq() uint64 { return c.maxAddSeq }

// SetMaxAddSeq seeds the watermark from persisted storage on restore.
func (c *Controller) SetMaxAddSeq(seq uint64) { c.maxAddSeq = seq }

// RegisterHandler adds a handler at runtime (for late-bound datasets).
func (c *Controller) RegisterHandler(ctx context.Context, h Handler) error {
	name := h.Dataset()
	if _, dup := c.handlers[name]; dup {
		return fmt.Errorf("crdt: duplicate handler for dataset %q", name)
	}
	c.handlers[name] = h
	collName := c.objectId + "/" + name
	coll, err := c.db.Collection(ctx, collName)
	if err != nil {
		return fmt.Errorf("crdt: open collection %q: %w", collName, err)
	}
	c.collections[name] = coll
	return nil
}

// Get returns the record by id from the dataset, or nil if absent.
func (c *Controller) Get(ctx context.Context, dataset, id string) *anyenc.Value {
	coll, ok := c.collections[dataset]
	if !ok {
		return nil
	}
	doc, err := coll.FindId(ctx, id)
	if err != nil {
		return nil
	}
	return doc.Value()
}

// Records returns all live (non-tombstone) records in the dataset. Used
// mostly by tests; the real query API uses any-store's Query builder.
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
		if !isTombstone(v) {
			out = append(out, v)
		}
	}
	return out
}

// ApplyChange applies a single change per spec §7. Wraps all mutations in
// a WriteTx for atomicity. When the ctx already carries a WriteTx (batch
// mode), a savepoint is used instead — lightweight and correct.
func (c *Controller) ApplyChange(ctx context.Context, ch Change) error {
	if ch.DataVersion == "" {
		return ErrMissingDataVersion
	}
	if ch.ObjectId != "" && c.objectId != "" && ch.ObjectId != c.objectId {
		return fmt.Errorf("crdt: change for object %q applied to controller for %q", ch.ObjectId, c.objectId)
	}
	handler, ok := c.handlers[ch.Dataset]
	if !ok {
		return ErrUnknownDataset
	}
	coll, ok := c.collections[ch.Dataset]
	if !ok {
		return ErrUnknownDataset
	}

	// Resolve empty ids from ChangeId.
	resolvedIds, err := resolveRecordIds(ch)
	if err != nil {
		return err
	}

	// Two-phase validation: syntactic paths, then handler rules.
	for i, rc := range ch.Records {
		rcResolved := rc
		rcResolved.Id = resolvedIds[i]
		for _, op := range rc.Ops {
			if err := validateOpPaths(op); err != nil {
				return errors.Join(ErrValidation, fmt.Errorf("record %q op %s: %w", rcResolved.Id, op.Type, err))
			}
			if err := handler.Validate(rcResolved, op); err != nil {
				return errors.Join(ErrValidation, fmt.Errorf("record %q op %s: %w", rcResolved.Id, op.Type, err))
			}
		}
	}

	// Apply inside a WriteTx (or savepoint if one is already active).
	tx, err := c.db.WriteTx(ctx)
	if err != nil {
		return fmt.Errorf("crdt: begin tx: %w", err)
	}
	txCtx := tx.Context()

	for i, rc := range ch.Records {
		id := resolvedIds[i]
		if err := c.applyRecordChange(txCtx, coll, ch, id, rc); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("crdt: commit: %w", err)
	}

	// Bump watermark after successful commit.
	if ch.AddSeq > c.maxAddSeq {
		c.maxAddSeq = ch.AddSeq
	}
	return nil
}

// applyRecordChange applies one RecordChange via UpsertId or UpdateId.
// The CRDT apply algorithm runs inside the modifier callback, using the
// arena provided by any-store's DocBuffer pool.
func (c *Controller) applyRecordChange(ctx context.Context, coll anystore.Collection, ch Change, id string, rc RecordChange) error {
	if hasDelete(rc.Ops) {
		return c.applyDeleteChange(ctx, coll, ch, id, rc)
	}
	mod := c.buildModifyModifier(ch, id, rc)
	if rc.Upsert {
		_, err := coll.UpsertId(ctx, id, mod)
		return err
	}
	_, err := coll.UpdateId(ctx, id, mod)
	if errors.Is(err, anystore.ErrDocNotFound) {
		return nil // strict mode: silent skip on absent
	}
	return err
}

// buildModifyModifier returns a modifier that applies all non-delete ops
// to a record, handling upsert creation, tombstone stickiness, and the
// _ver.id min-rule.
func (c *Controller) buildModifyModifier(ch Change, id string, rc RecordChange) query.Modifier {
	return query.ModifyFunc(func(a *anyenc.Arena, existing *anyenc.Value) (*anyenc.Value, bool, error) {
		// Detect new record: UpsertId pre-creates {id: ...} but no _ver.
		if existing.Get(VersionsKey) == nil {
			// Stamp creation marker.
			ver := a.NewObject()
			ver.Set(IdField, a.NewString(string(ch.VersionId)))
			existing.Set(VersionsKey, ver)
		} else if isTombstone(existing) {
			// Sticky tombstone: skip ops, optionally lower marker.
			if rc.Upsert {
				if lowerCreationMarker(a, existing, ch.VersionId) {
					updateTraces(a, existing, ch)
					return existing, true, nil
				}
				return existing, false, nil
			}
			return existing, false, nil
		} else if rc.Upsert {
			lowerCreationMarker(a, existing, ch.VersionId)
		}

		for _, op := range rc.Ops {
			applyOp(a, existing, ch, op)
		}
		updateTraces(a, existing, ch)
		return existing, true, nil
	})
}

// applyDeleteChange handles delete ops via UpsertId — always produces a
// tombstone regardless of the upsert flag.
func (c *Controller) applyDeleteChange(ctx context.Context, coll anystore.Collection, ch Change, id string, rc RecordChange) error {
	mod := query.ModifyFunc(func(a *anyenc.Arena, existing *anyenc.Value) (*anyenc.Value, bool, error) {
		if isTombstone(existing) {
			// Already tombstoned — optionally lower marker, skip otherwise.
			if rc.Upsert {
				if lowerCreationMarker(a, existing, ch.VersionId) {
					updateTraces(a, existing, ch)
					return existing, true, nil
				}
				return existing, false, nil
			}
			return existing, false, nil
		}
		tomb := newTombstone(a, id, ch, existing)
		updateTraces(a, tomb, ch)
		return tomb, true, nil
	})
	_, err := coll.UpsertId(ctx, id, mod)
	return err
}

// resolveRecordIds replaces empty RecordChange.Ids with ChangeId-derived
// values. First empty-id gets base58(xxhash64(ChangeId)); subsequent get
// that seed with /<index> appended.
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
