package spaceobjects

import (
	"context"
	"errors"
	"sync"

	"github.com/cheggaaa/mb/v3"

	"github.com/anyproto/any-sync-sdk/internal/types"
)

// drainerQueueSize bounds the in-flight notification buffer. Pairs
// dropped when full are recovered the next time afterApply pushes
// (Drain is a full _detached scan, so a missed signal at most
// delays a drain pass to the next signal).
const drainerQueueSize = 4096

// drainer is the per-Store async drain worker. afterApply hooks
// push types.DataVersionPair entries here when an apply lands
// something that may unblock parked changes (typically a typetype.
// PropertyHandler write that emitted shortIds); the worker
// goroutine coalesces signals and runs Store.Drain off the apply
// path's locks.
//
// Why async: synchronous Drain ran inside applyDecodedLocked under
// o.mu, and Drain's own ApplyDecoded path tries to retake o.mu for
// the same Object — sync.Mutex is non-reentrant, so the chain was
// a latent deadlock the moment a parked change for the same object
// got unblocked. Decoupling drain from the apply lock removes that
// hazard and gives every LocalWrite a predictable latency floor:
// the call no longer waits for unrelated parked replays.
//
// Pairs are documentary today (Drain still scans the whole
// _detached collection) — when an indexed lookup on `pending`
// lands, Notify can become a targeted dispatch.
type drainer struct {
	store  *Store
	queue  *mb.MB[types.DataVersionPair]
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// newDrainer constructs a drainer bound to the Store. The worker
// goroutine is started by Run — newDrainer alone allocates only.
func newDrainer(store *Store) *drainer {
	ctx, cancel := context.WithCancel(context.Background())
	return &drainer{
		store:  store,
		queue:  mb.New[types.DataVersionPair](drainerQueueSize),
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
}

// Notify is the hot-path push from afterApply. Best-effort
// non-blocking: a full queue drops the pair (see drainerQueueSize
// note). Safe to call on a nil drainer.
func (d *drainer) Notify(pair types.DataVersionPair) {
	if d == nil {
		return
	}
	_ = d.queue.TryAdd(pair)
}

// Run starts the worker goroutine. Idempotent.
func (d *drainer) Run() {
	if d == nil {
		return
	}
	d.once.Do(func() { go d.loop() })
}

// loop blocks for at least one pair, drains the rest of the buffer,
// and runs a single Drain pass. One scan per burst, not per pair.
func (d *drainer) loop() {
	defer close(d.done)
	for {
		// Wait blocks until at least one pair is available, then
		// returns every pair currently buffered — built-in coalescing.
		if _, err := d.queue.Wait(d.ctx); err != nil {
			return
		}
		if err := d.store.Drain(d.ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			// Per-row failures stay inside Drain (continue policy).
			// Anything else is dropped; the next Notify will retry.
		}
	}
}

// Close stops the worker and closes the queue. Safe to call multiple
// times; safe to call on a nil drainer.
func (d *drainer) Close() error {
	if d == nil {
		return nil
	}
	d.cancel()
	err := d.queue.Close()
	<-d.done
	return err
}
