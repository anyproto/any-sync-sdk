package syncstatus

import "sync"

// registry is a tiny add / remove / dispatch primitive for synchronous
// callback firehoses. cb runs under the registry lock; callers are
// expected to keep cb cheap or hand work off to their own goroutine.
//
// Used twice: once for ObjectSyncStatus (per Tracker) and once for
// SpaceSyncStatus (Service-wide).
type registry[T any] struct {
	mu     sync.Mutex
	next   uint64
	closed bool
	subs   map[uint64]func(T)
}

// newRegistry returns an empty registry ready for use.
func newRegistry[T any]() *registry[T] {
	return &registry[T]{subs: make(map[uint64]func(T))}
}

// add registers cb and returns the subscription id passed to remove.
// nil cb is ignored — caller still gets a sentinel id but dispatch
// skips it (the wrappers translate that into a no-op cancel).
func (r *registry[T]) add(cb func(T)) uint64 {
	if cb == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0
	}
	r.next++
	id := r.next
	r.subs[id] = cb
	return id
}

// remove drops a subscription. Idempotent; safe with id=0.
func (r *registry[T]) remove(id uint64) {
	if id == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.subs, id)
}

// dispatch fans ev out to every live cb. Synchronous; runs under
// r.mu, so cbs must not call add / remove on the same registry
// (would deadlock). Callers can do that by handing off to their
// own goroutine.
//
// Acceptable trade-off for v1: the firehose is low-volume (edge
// transitions only, debounced at 1s in the rollup loop), so the
// "no re-entry" rule is unlikely to bite a caller in practice. If
// it does, we'll switch to snapshot-then-dispatch.
func (r *registry[T]) dispatch(ev T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, cb := range r.subs {
		cb(ev)
	}
}

// hasSubscribers is a non-blocking fast check used by the rollup loop
// to skip event construction when nobody is listening.
func (r *registry[T]) hasSubscribers() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.subs) > 0
}

// close empties the registry and rejects future adds.
func (r *registry[T]) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.subs = map[uint64]func(T){}
}
