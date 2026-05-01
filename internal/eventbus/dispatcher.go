package eventbus

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/cheggaaa/mb/v3"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
)

// ObjectsDataset is the CRDT dataset whose changes feed the
// property-firehose (SubscribeProperties). It is the per-space
// `objects` collection — every object's property values land here,
// regardless of object kind.
//
// User-facing terminology calls these "property values"; the
// dataset is named "objects" because each row is one object's
// values record. Unrelated to typetype.DatasetPropertyDefs (which
// holds type-definition metadata on type objects).
const ObjectsDataset = properties.Dataset

// Event is a single CRDT apply event delivered to a Subscription.
//
// v1 carries only the routing tuple plus AddSeq. Per-record deltas
// and create/update/delete classification are deliberately out of
// scope — consumers re-query for current state via the Query API.
type Event struct {
	SpaceId  string
	ObjectId string
	Dataset  string
	AddSeq   uint64
}

// Subscription is the consumer-side handle. The mailbox is exposed
// directly so callers can use the full mb/v3 vocabulary — Wait for
// batches (free coalescing), WaitOne for single-event consumers,
// NewCond for filtered/min-batch waits.
//
// Dropped reports the number of events the dispatcher had to drop
// because the mailbox was full at delivery time. Slow consumers can
// poll this to detect missed events; deliberate Close does NOT
// increment the counter.
//
// Close releases the mailbox; subsequent Wait calls return
// mb.ErrClosed.
type Subscription interface {
	Mailbox() *mb.MB[Event]
	Dropped() uint64
	Close() error
}

// Dispatcher is the per-space pub/sub for CRDT apply events. Wire
// Dispatch into the per-Object AfterApply hook; HasSubscribers gates
// the call so the cold-restore path stays a single atomic load when
// nobody is listening.
//
// Two registries:
//
//   - propSubs: firehose, fired on every change to the
//     ObjectsDataset (= per-space `objects` collection).
//   - dataSubs[objectId][dataset]: explicit, fired only when that
//     exact (object, dataset) pair changes.
//
// Both registries can fire for the same change — a property-
// firehose listener and an explicit (thisObj, "objects") listener
// both get notified when thisObj's properties change. Caller's
// choice of lens.
type Dispatcher struct {
	spaceId string

	mu       sync.RWMutex
	propSubs map[uint64]*subscription
	dataSubs map[string]map[string]map[uint64]*subscription // [objectId][dataset][id]

	counter atomic.Int64  // total live subscribers; HasSubscribers gate
	nextId  atomic.Uint64 // monotonic subscription id
	closed  atomic.Bool
}

// New constructs an empty Dispatcher for spaceId.
func New(spaceId string) *Dispatcher {
	return &Dispatcher{
		spaceId:  spaceId,
		propSubs: make(map[uint64]*subscription),
		dataSubs: make(map[string]map[string]map[uint64]*subscription),
	}
}

// SpaceId returns the space this Dispatcher serves.
func (d *Dispatcher) SpaceId() string { return d.spaceId }

// HasSubscribers reports whether any subscription is live. Single
// atomic load — call from the apply hot path before constructing
// the Event.
func (d *Dispatcher) HasSubscribers() bool {
	return d.counter.Load() > 0
}

// Dispatch routes ch to matching subscribers. No-op when the
// dispatcher is closed or has zero subscribers. Non-blocking adds
// — slow subscribers drop events (mb.TryAdd returns ErrOverflowed)
// rather than stall the apply pipeline.
func (d *Dispatcher) Dispatch(ch *crdt.Change) {
	if d.closed.Load() {
		return
	}
	if d.counter.Load() == 0 {
		return
	}
	if ch == nil {
		return
	}
	ev := Event{
		SpaceId:  ch.SpaceId,
		ObjectId: ch.ObjectId,
		Dataset:  ch.Dataset,
		AddSeq:   ch.AddSeq,
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	if ch.Dataset == ObjectsDataset {
		for _, sub := range d.propSubs {
			deliver(sub, ev)
		}
	}
	if perObj, ok := d.dataSubs[ch.ObjectId]; ok {
		if perDs, ok := perObj[ch.Dataset]; ok {
			for _, sub := range perDs {
				deliver(sub, ev)
			}
		}
	}
}

// deliver does a non-blocking add to the subscriber's mailbox.
// ErrOverflowed bumps the per-sub dropped counter (callers can
// poll Subscription.Dropped to detect lossy consumers); ErrClosed
// is silently ignored (deliberate teardown ≠ drop).
func deliver(sub *subscription, ev Event) {
	if err := sub.mb.TryAdd(ev); err != nil && errors.Is(err, mb.ErrOverflowed) {
		sub.dropped.Add(1)
	}
}

// SubscribeProperties registers a firehose for the ObjectsDataset
// across every object in this space. capacity bounds the per-
// subscriber mailbox; a full mailbox drops events.
func (d *Dispatcher) SubscribeProperties(capacity int) Subscription {
	sub := d.newSubscription(capacity, true, "", "")
	d.mu.Lock()
	if d.closed.Load() {
		d.mu.Unlock()
		sub.closed.Store(true)
		_ = sub.mb.Close()
		return sub
	}
	d.propSubs[sub.id] = sub
	d.counter.Add(1)
	d.mu.Unlock()
	return sub
}

// Subscribe registers an explicit (objectId, dataset) listener.
// capacity bounds the per-subscriber mailbox.
func (d *Dispatcher) Subscribe(objectId, dataset string, capacity int) Subscription {
	sub := d.newSubscription(capacity, false, objectId, dataset)
	d.mu.Lock()
	if d.closed.Load() {
		d.mu.Unlock()
		sub.closed.Store(true)
		_ = sub.mb.Close()
		return sub
	}
	perObj, ok := d.dataSubs[objectId]
	if !ok {
		perObj = make(map[string]map[uint64]*subscription)
		d.dataSubs[objectId] = perObj
	}
	perDs, ok := perObj[dataset]
	if !ok {
		perDs = make(map[uint64]*subscription)
		perObj[dataset] = perDs
	}
	perDs[sub.id] = sub
	d.counter.Add(1)
	d.mu.Unlock()
	return sub
}

// Close cancels every live subscription and rejects future
// Subscribe / SubscribeProperties calls. Idempotent. The CAS on
// sub.closed is the single arbiter — whoever wins owns the mailbox
// close and the counter decrement, regardless of whether
// Dispatcher.Close or subscription.Close ran first.
func (d *Dispatcher) Close() error {
	if !d.closed.CompareAndSwap(false, true) {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, sub := range d.propSubs {
		if sub.closed.CompareAndSwap(false, true) {
			d.counter.Add(-1)
			_ = sub.mb.Close()
		}
	}
	d.propSubs = map[uint64]*subscription{}
	for _, perObj := range d.dataSubs {
		for _, perDs := range perObj {
			for _, sub := range perDs {
				if sub.closed.CompareAndSwap(false, true) {
					d.counter.Add(-1)
					_ = sub.mb.Close()
				}
			}
		}
	}
	d.dataSubs = map[string]map[string]map[uint64]*subscription{}
	return nil
}

func (d *Dispatcher) newSubscription(capacity int, isProp bool, objectId, dataset string) *subscription {
	if capacity <= 0 {
		capacity = 64
	}
	return &subscription{
		id:       d.nextId.Add(1),
		mb:       mb.New[Event](capacity),
		parent:   d,
		isProp:   isProp,
		objectId: objectId,
		dataset:  dataset,
	}
}

// unregister removes sub from its registry. Counter and mailbox
// close are the caller's responsibility (subscription.Close — the
// CAS winner). Safe under a closed Dispatcher; the registries may
// have been cleared already, in which case the deletes are no-ops.
func (d *Dispatcher) unregister(sub *subscription) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if sub.isProp {
		delete(d.propSubs, sub.id)
		return
	}
	perObj, ok := d.dataSubs[sub.objectId]
	if !ok {
		return
	}
	delete(perObj[sub.dataset], sub.id)
	if len(perObj[sub.dataset]) == 0 {
		delete(perObj, sub.dataset)
	}
	if len(perObj) == 0 {
		delete(d.dataSubs, sub.objectId)
	}
}

// subscription is the internal implementation of Subscription.
// Backed by a per-subscriber mb/v3 mailbox so consumers get free
// batching (Wait), single-event consumers (WaitOne), and the rest
// of mb's vocabulary. Close is idempotent and decrements the parent
// counter.
type subscription struct {
	id       uint64
	mb       *mb.MB[Event]
	parent   *Dispatcher
	isProp   bool
	objectId string
	dataset  string
	closed   atomic.Bool
	dropped  atomic.Uint64
}

func (s *subscription) Mailbox() *mb.MB[Event] { return s.mb }
func (s *subscription) Dropped() uint64        { return s.dropped.Load() }

func (s *subscription) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	s.parent.unregister(s)
	s.parent.counter.Add(-1)
	return s.mb.Close()
}
