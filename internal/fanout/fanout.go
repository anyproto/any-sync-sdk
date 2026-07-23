// Package fanout provides the shared add / cancel / dispatch primitive
// behind the SDK's synchronous callback firehoses (sync status, change
// feed, row events, read-state pings, member events, file status).
package fanout

import "sync"

// Registry fans events out to registered callbacks.
//
// Dispatch snapshots the subscriber set under the lock and invokes the
// callbacks OUTSIDE it, so a callback may Add / cancel on the same
// registry without deadlocking. Two consequences callers must absorb:
//
//   - a callback may still fire once for a dispatch that snapshotted
//     it before its cancel returned — do not free resources the
//     callback touches right after cancel (the SDK's subscribers hand
//     off to buffered channels that outlive the subscription);
//   - concurrent Dispatch calls do not serialize against each other,
//     so subscribers must not rely on cross-event ordering. Every feed
//     built on this is a best-effort ping with a pull-based recovery
//     path.
//
// The zero value is ready for use.
type Registry[T any] struct {
	mu     sync.Mutex
	next   uint64
	closed bool
	subs   map[uint64]func(T)
}

// New returns an empty registry ready for use.
func New[T any]() *Registry[T] {
	return &Registry[T]{subs: make(map[uint64]func(T))}
}

// Add registers cb and returns its cancel. Cancel is idempotent. A nil
// cb, or an Add after Close, yields a no-op cancel.
func (r *Registry[T]) Add(cb func(T)) (cancel func()) {
	if cb == nil {
		return func() {}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return func() {}
	}
	if r.subs == nil {
		r.subs = make(map[uint64]func(T))
	}
	r.next++
	id := r.next
	r.subs[id] = cb
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(r.subs, id)
	}
}

// Dispatch fans ev out to every callback registered at snapshot time,
// synchronously on the calling goroutine.
func (r *Registry[T]) Dispatch(ev T) {
	r.mu.Lock()
	if len(r.subs) == 0 {
		r.mu.Unlock()
		return
	}
	cbs := make([]func(T), 0, len(r.subs))
	for _, cb := range r.subs {
		cbs = append(cbs, cb)
	}
	r.mu.Unlock()
	for _, cb := range cbs {
		cb(ev)
	}
}

// HasSubscribers is the non-blocking gate hot paths check before
// building an event.
func (r *Registry[T]) HasSubscribers() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.subs) > 0
}

// Close empties the registry and rejects future Adds.
func (r *Registry[T]) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.subs = map[uint64]func(T){}
}
