package eventbus

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/anyenc/anyencutil"
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
// Pipeline contract: receive change → apply to CRDT → commit the
// any-store tx → THEN fire this event. By the time a subscriber
// observes an Event, every projected $set/$unset is already durable
// in the controller's any-store; a Query against the same dataset
// run from the same process will reflect the same state.
//
// Carries the routing tuple plus enough about the change for a
// caller to apply it locally without re-querying:
//
//   - VersionId — per-change DAG order. A subscribe-then-query
//     consumer compares this with `_ver.id` on a queried record to
//     decide whether the snapshot already includes this event.
//
//   - Records — the post-apply effect of the change, projected to
//     a flat list of $set / $unset ops per record. The SDK has
//     already merged with full CRDT semantics; what we ship is the
//     resulting field-level patch, so a thin client (no CRDT
//     engine) applies it naively to a JSON-like local copy. A
//     record marked Deleted means "remove this id from your local
//     copy"; Ops are empty in that case.
//
// Op.Payload pointers in Records are deep-copied off the
// controller's post-apply storage onto event-owned arenas, so
// callers can hold an Event past the lifetime of the original
// change without aliasing pooled buffers.
type Event struct {
	SpaceId   string
	ObjectId  string
	Dataset   string
	VersionId crdt.VersionId
	Records   []EventRecord
}

// EventRecord is one record's worth of projected change inside an
// Event. Id is the record id within Dataset (for shared per-space
// datasets like "objects" this equals ObjectId).
type EventRecord struct {
	Id      string
	Variant string
	// Deleted is true when the change tombstoned this record. Ops is
	// empty in that case; the consumer should drop the record from
	// its local state.
	Deleted bool
	// Ops is the projected $set / $unset operations on the post-apply
	// record. Empty when Deleted is true.
	Ops []EventOp
}

// EventOp is one $set or $unset operation inside an EventRecord. By
// construction we never ship $inc / $addToSet / $pull / $incGated /
// delete to subscribers — those are projected to set/unset against
// the post-apply value before delivery.
//
// Path is the dotted-segment field path. For $set, an empty Path
// activates the multi-field form (Payload is an object whose keys are
// dot-separated paths); for $unset, an empty Path means "remove the
// record" (use EventRecord.Deleted instead, kept here for symmetry
// with the input op shape).
type EventOp struct {
	Type    crdt.OpType
	Path    []string
	Payload *anyenc.Value
}

// PostValueFn returns the post-apply value for one record in the
// change being dispatched. Provided by the caller of Dispatch (the
// per-space layer holds the controller). Index is into ch.Records.
//
// May return nil for tombstoned / dropped records (e.g. a gated op
// that didn't land); callers should treat nil as "the record no
// longer exists at this point in the timeline".
type PostValueFn func(recordIndex int) *anyenc.Value

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
//
// recordIds is the per-record id list as resolved by the apply
// pipeline (auto-derived empty ids → ChangeId-based, shared
// datasets → all entries equal ch.ObjectId). When len(recordIds)
// != len(ch.Records) we fall back to RecordChange.Id as best-effort.
//
// derivedOps carries, per ch.Records index, the extra $set ops the
// apply path stamped beyond the input ops (handler-emitted derived
// stamps + _ver.id creation marker). The dispatcher merges them
// into the projected EventRecord so a viewer can reconstruct a
// fresh record without distinguishing user-supplied from auto
// fields. nil / len mismatch → no merge for that record.
//
// postValue is consulted once per record to look up the merged
// row state, used to project non-set/unset ops down to a $set on
// the post-apply path value (or $unset if the path went absent).
// May be nil — in that case ops on non-set/unset types pass through
// the projector with a nil Payload, which downstream consumers
// should treat as "fetch this record fresh".
func (d *Dispatcher) Dispatch(ch *crdt.Change, recordIds []string, derivedOps [][]crdt.Op, postValue PostValueFn) {
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
		SpaceId:   ch.SpaceId,
		ObjectId:  ch.ObjectId,
		Dataset:   ch.Dataset,
		VersionId: ch.VersionId,
		Records:   projectRecords(ch, recordIds, derivedOps, postValue),
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

// projectRecords walks ch.Records and produces the EventRecord slice
// for the wire — every op is mapped to a $set or $unset against the
// post-apply state, and a record-level "delete" op is collapsed into
// EventRecord.Deleted.
//
// derivedOps[i] carries the auto-stamped extras the apply path added
// to record i beyond rc.Ops (author / createdAt / spaceId / _ver.id).
// They project through the same projectOp path as input ops — the
// EventRecord's consumer can't tell them apart, which is the point:
// a viewer reconstructs a fresh record with all its fields in one
// go, no second-class wire form for auto fields.
//
// Op.Payload values are deep-cloned via anyencutil.Value.FillCopy so
// the resulting Event is safe to outlive the dispatch call.
func projectRecords(ch *crdt.Change, recordIds []string, derivedOps [][]crdt.Op, postValue PostValueFn) []EventRecord {
	if len(ch.Records) == 0 {
		return nil
	}
	out := make([]EventRecord, 0, len(ch.Records))
	for i, rc := range ch.Records {
		rid := rc.Id
		if i < len(recordIds) && recordIds[i] != "" {
			rid = recordIds[i]
		}
		er := EventRecord{Id: rid, Variant: rc.Variant}
		if hasRecordDelete(rc.Ops) {
			er.Deleted = true
			out = append(out, er)
			continue
		}
		var post *anyenc.Value
		if postValue != nil {
			post = postValue(i)
		}
		// Variant ops apply to a sub-document rooted at rc.Variant; the
		// post-apply lookup we use targets the record root, so paths
		// in the projected ops carry the variant prefix to match.
		for _, op := range rc.Ops {
			projected, ok := projectOp(op, rc.Variant, post)
			if !ok {
				continue
			}
			er.Ops = append(er.Ops, projected)
		}
		// Derived stamps are record-level (author / createdAt / _ver.id
		// land at root regardless of which variant the triggering op
		// used), so they project with an empty variant prefix.
		if i < len(derivedOps) {
			for _, op := range derivedOps[i] {
				projected, ok := projectOp(op, "", post)
				if !ok {
					continue
				}
				er.Ops = append(er.Ops, projected)
			}
		}
		out = append(out, er)
	}
	return out
}

// hasRecordDelete reports whether ops contains a record-level delete.
// Mirrors the apply-side helper of the same name (kept private over
// here so eventbus stays self-contained).
func hasRecordDelete(ops []crdt.Op) bool {
	for _, op := range ops {
		if op.Type == crdt.OpDelete {
			return true
		}
	}
	return false
}

// projectOp converts a single input op against a post-apply record
// snapshot into a $set / $unset op the wire ships. ok=false means
// the op should be dropped from the projection (currently only used
// for crdt.OpDelete, which is signalled by EventRecord.Deleted at
// the record level).
//
// Variant prefixing: when the input op runs under a variant
// (RecordChange.Variant != ""), the post-apply value at op.Path
// lives under post[variant][path...]; we reflect that in the wire
// path so the consumer's local copy has the same nested shape.
func projectOp(op crdt.Op, variant string, post *anyenc.Value) (EventOp, bool) {
	switch op.Type {
	case crdt.OpDelete:
		// Record-level — handled out-of-band via EventRecord.Deleted.
		return EventOp{}, false
	case crdt.OpSet, crdt.OpUnset:
		// Already in the wire-friendly form. Clone the payload off
		// any caller-owned arena so the event can outlive the
		// dispatch call.
		return EventOp{
			Type:    op.Type,
			Path:    prependVariant(variant, op.Path),
			Payload: clonePayload(op.Payload),
		}, true
	default:
		// $inc / $addToSet / $pull / $incGated — derive the post-apply
		// value at the op's path and emit a $set (or $unset when the
		// op didn't actually land or the path went away).
		path := prependVariant(variant, op.Path)
		val := lookupPath(post, path)
		if val == nil {
			return EventOp{
				Type: crdt.OpUnset,
				Path: path,
			}, true
		}
		return EventOp{
			Type:    crdt.OpSet,
			Path:    path,
			Payload: clonePayload(val),
		}, true
	}
}

// prependVariant returns variant prepended to path when variant is
// non-empty. Allocates a fresh slice so callers can't mutate the
// input op's path.
func prependVariant(variant string, path []string) []string {
	if variant == "" {
		if len(path) == 0 {
			return nil
		}
		out := make([]string, len(path))
		copy(out, path)
		return out
	}
	out := make([]string, 0, len(path)+1)
	out = append(out, variant)
	out = append(out, path...)
	return out
}

// lookupPath walks v one segment at a time and returns the value at
// the path tail. Empty path returns v itself. Returns nil for any
// missing intermediate.
func lookupPath(v *anyenc.Value, path []string) *anyenc.Value {
	if v == nil {
		return nil
	}
	if len(path) == 0 {
		return v
	}
	return v.Get(path...)
}

// clonePayload deep-copies an anyenc value off its current arena onto
// a fresh parser-owned arena. Mirrors crdt.cloneValue; we duplicate
// here to keep eventbus free of crdt-internal helpers. nil-safe.
func clonePayload(v *anyenc.Value) *anyenc.Value {
	if v == nil {
		return nil
	}
	var w anyencutil.Value
	w.FillCopy(v)
	return w.Value
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
