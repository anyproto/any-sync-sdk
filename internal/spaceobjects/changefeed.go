package spaceobjects

import (
	"context"
	"sync"

	anystore "github.com/anyproto/any-store/v2"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// ObjectChange is the payload of the change-index live feed: an object
// in this space applied a change that advanced (or re-stamped) its
// per-space AddSeq watermark. Consumers use it to mark the object dirty
// for re-indexing.
type ObjectChange struct {
	ObjectId string
	AddSeq   uint64
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
// per applied change in this space with (objectId, addSeq). cb runs
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
// AddSeq exceeds `since`, ascending, capped at limit (0 = no cap). Page
// by passing the last returned AddSeq as the next `since`. Backs the
// change-index pull / catch-up path.
func (s *Store) ChangedObjects(ctx context.Context, since uint64, limit int) ([]ObjectChange, error) {
	coll, err := s.metaCollection(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := crdt.QueryChangedObjects(ctx, coll, s.spaceId, since, limit)
	if err != nil {
		return nil, err
	}
	out := make([]ObjectChange, len(rows))
	for i, r := range rows {
		out[i] = ObjectChange{ObjectId: r.ObjectId, AddSeq: r.AddSeq}
	}
	return out, nil
}

// MaxAddSeq returns the highest per-object AddSeq persisted in this
// space — the current upper bound of the change-index cursor.
func (s *Store) MaxAddSeq(ctx context.Context) (uint64, error) {
	coll, err := s.metaCollection(ctx)
	if err != nil {
		return 0, err
	}
	return crdt.MaxObjectAddSeq(ctx, coll, s.spaceId)
}

// metaCollection opens the shared _meta collection the change-index
// query reads. Same collection the per-object Controllers persist their
// watermark into.
func (s *Store) metaCollection(ctx context.Context) (anystore.Collection, error) {
	return s.db.Collection(ctx, crdt.MetaCollectionName)
}
