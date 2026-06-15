package spaceobjects

import (
	"context"
	"sync"

	anystore "github.com/anyproto/any-store/v2"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// ObjectChange is the payload of the change-index live feed: an object
// in this space applied a change that advanced its per-space applySeq
// watermark. Consumers use it to mark the object dirty for re-indexing.
// ApplySeq covers every apply source — DAG changes, the account
// mirror's injected applies, device-local writes.
type ObjectChange struct {
	ObjectId string
	ApplySeq uint64
}

// changeRegistry is the synchronous callback firehose backing the
// change-index feed. It mirrors internal/syncstatus' registry: cb runs
// under the registry lock, on the apply path, so callers must keep cb
// cheap or hand work off to their own goroutine. A cb must not call
// add / remove on the same registry (would deadlock).
type changeRegistry struct {
	mu   sync.Mutex
	next uint64
	subs map[uint64]func(ObjectChange)
}

func newChangeRegistry() *changeRegistry {
	return &changeRegistry{subs: make(map[uint64]func(ObjectChange))}
}

// add registers cb and returns the id passed to remove. nil cb yields
// id 0 (a no-op cancel).
func (r *changeRegistry) add(cb func(ObjectChange)) uint64 {
	if cb == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.next++
	id := r.next
	r.subs[id] = cb
	return id
}

// remove drops a subscription. Idempotent; safe with id 0.
func (r *changeRegistry) remove(id uint64) {
	if id == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.subs, id)
}

// hasSubscribers is the non-blocking gate the apply hook checks before
// building an event, so a space with no change-index consumer pays
// nothing on the hot path.
func (r *changeRegistry) hasSubscribers() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.subs) > 0
}

// dispatch fans ev out to every live cb, synchronously under the lock.
func (r *changeRegistry) dispatch(ev ObjectChange) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, cb := range r.subs {
		cb(ev)
	}
}

// SubscribeChanges registers cb on the change-index feed; it fires once
// per applied change in this space with (objectId, applySeq). cb runs
// synchronously on the apply path — keep it small or hand off. The
// returned cancel is idempotent.
//
// The feed is best-effort live notification, not a durable queue: a
// consumer that misses events (crash, slow cb, was offline) recovers by
// re-running ChangedObjects from its last persisted cursor.
func (s *Store) SubscribeChanges(cb func(ObjectChange)) (cancel func()) {
	id := s.changeSubs.add(cb)
	return func() { s.changeSubs.remove(id) }
}

// ChangedObjects returns objects in this space whose persisted max
// applySeq exceeds `since`, ascending, capped at limit (0 = no cap).
// Page by passing the last returned ApplySeq as the next `since`. Backs
// the change-index pull / catch-up path.
func (s *Store) ChangedObjects(ctx context.Context, since uint64, limit int) ([]ObjectChange, error) {
	coll, err := s.applySeqMeta(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := crdt.QueryChangedObjects(ctx, coll, s.spaceId, since, limit)
	if err != nil {
		return nil, err
	}
	out := make([]ObjectChange, len(rows))
	for i, r := range rows {
		out[i] = ObjectChange{ObjectId: r.ObjectId, ApplySeq: r.ApplySeq}
	}
	return out, nil
}

// MaxApplySeq returns the highest per-object applySeq persisted in this
// space — the current upper bound of the change-index cursor.
func (s *Store) MaxApplySeq(ctx context.Context) (uint64, error) {
	coll, err := s.applySeqMeta(ctx)
	if err != nil {
		return 0, err
	}
	return crdt.MaxObjectApplySeq(ctx, coll, s.spaceId)
}

// applySeqMeta opens the _meta collection AND guarantees the one-off
// legacy backfill (applySeq := addSeq on pre-applySeq rows) has run, so
// every feed read and the allocator seed observe a complete axis.
func (s *Store) applySeqMeta(ctx context.Context) (anystore.Collection, error) {
	coll, err := s.metaCollection(ctx)
	if err != nil {
		return nil, err
	}
	s.applySeqBackfill.Do(func() {
		s.applySeqBackfillErr = crdt.BackfillApplySeq(ctx, coll, s.spaceId)
	})
	if s.applySeqBackfillErr != nil {
		return nil, s.applySeqBackfillErr
	}
	return coll, nil
}

// metaCollection opens the shared _meta collection the change-index
// query reads. Same collection the per-object Controllers persist their
// watermark into.
func (s *Store) metaCollection(ctx context.Context) (anystore.Collection, error) {
	return s.db.Collection(ctx, crdt.MetaCollectionName)
}

// RowEvent notifies a structural transition of one row in the
// per-space `objects` collection: Created fires when a change first
// materialises the row, Deleted when it tombstones. The account mirror
// keys its replay (carrier values waiting for the row) and its GC
// (drop carrier records of deleted objects) off these.
type RowEvent struct {
	ObjectId string
	Deleted  bool
}

// rowEventRegistry mirrors changeRegistry for RowEvent callbacks —
// synchronous, on the apply path, keep callbacks cheap.
type rowEventRegistry struct {
	mu   sync.Mutex
	next uint64
	subs map[uint64]func(RowEvent)
}

func newRowEventRegistry() *rowEventRegistry {
	return &rowEventRegistry{subs: make(map[uint64]func(RowEvent))}
}

func (r *rowEventRegistry) add(cb func(RowEvent)) uint64 {
	if cb == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.next++
	r.subs[r.next] = cb
	return r.next
}

func (r *rowEventRegistry) remove(id uint64) {
	if id == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.subs, id)
}

func (r *rowEventRegistry) hasSubscribers() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.subs) > 0
}

func (r *rowEventRegistry) dispatch(ev RowEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, cb := range r.subs {
		cb(ev)
	}
}

// SubscribeRowEvents registers cb for objects-collection row
// creations and deletions. cb runs synchronously on the apply path.
// The returned cancel is idempotent.
func (s *Store) SubscribeRowEvents(cb func(RowEvent)) (cancel func()) {
	id := s.rowEvents.add(cb)
	return func() { s.rowEvents.remove(id) }
}
