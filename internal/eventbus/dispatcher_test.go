package eventbus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/cheggaaa/mb/v3"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// recv reads one event with a short timeout; fails the test if no
// event arrives.
func recv(t *testing.T, sub Subscription) Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	ev, err := sub.Mailbox().WaitOne(ctx)
	if err != nil {
		t.Fatalf("WaitOne: %v", err)
	}
	return ev
}

// expectNoEvent asserts that nothing is delivered within a short
// window. Treats ctx-deadline as "good"; ErrClosed or a real event
// is failure.
func expectNoEvent(t *testing.T, sub Subscription) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	msgs, err := sub.Mailbox().Wait(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		return
	}
	if errors.Is(err, mb.ErrClosed) {
		t.Fatalf("mailbox unexpectedly closed during quiet window")
	}
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if len(msgs) > 0 {
		t.Fatalf("expected no event, got %+v", msgs)
	}
}

// expectClosed asserts the subscription's mailbox is closed.
func expectClosed(t *testing.T, sub Subscription) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := sub.Mailbox().Wait(ctx)
	if !errors.Is(err, mb.ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

// changeOn builds a minimal Change for routing tests — only the
// fields the dispatcher inspects matter (SpaceId/ObjectId/Dataset),
// plus a per-test VersionId derived from seq so assertions have a
// stable identifier on the post-dispatch Event.
func changeOn(spaceId, objectId, dataset string, seq uint64) *crdt.Change {
	return &crdt.Change{
		SpaceId:   spaceId,
		ObjectId:  objectId,
		Dataset:   dataset,
		VersionId: crdt.VersionId(fmt.Sprintf("v%d", seq)),
	}
}

// vId returns the VersionId we expect on an event built via changeOn.
func vId(seq uint64) crdt.VersionId { return crdt.VersionId(fmt.Sprintf("v%d", seq)) }

func TestHasSubscribers_GateOnCounter(t *testing.T) {
	d := New("space1")
	if d.HasSubscribers() {
		t.Fatalf("fresh dispatcher reports HasSubscribers=true")
	}
	sub := d.Subscribe("obj1", "data1", 4)
	if !d.HasSubscribers() {
		t.Fatalf("after Subscribe, HasSubscribers=false")
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if d.HasSubscribers() {
		t.Fatalf("after Close, HasSubscribers=true")
	}

	psub := d.SubscribeProperties(4)
	if !d.HasSubscribers() {
		t.Fatalf("after SubscribeProperties, HasSubscribers=false")
	}
	_ = psub.Close()
	if d.HasSubscribers() {
		t.Fatalf("after props sub Close, HasSubscribers=true")
	}
}

func TestDispatch_NoSubscribers_NoOp(t *testing.T) {
	d := New("space1")
	// Nothing to assert beyond "doesn't panic / crash".
	d.Dispatch(changeOn("space1", "obj1", "data1", 7), nil, nil, nil)
	d.Dispatch(changeOn("space1", "obj1", ObjectsDataset, 8), nil, nil, nil)
}

func TestSubscribe_RoutesByObjectAndDataset(t *testing.T) {
	d := New("space1")
	t.Cleanup(func() { _ = d.Close() })

	subA := d.Subscribe("objA", "data1", 8)
	subB := d.Subscribe("objB", "data1", 8)
	subC := d.Subscribe("objA", "data2", 8)

	// Event for (objA, data1) → subA only.
	d.Dispatch(changeOn("space1", "objA", "data1", 1), nil, nil, nil)
	if got := recv(t, subA); got.ObjectId != "objA" || got.Dataset != "data1" || got.VersionId != vId(1) {
		t.Fatalf("subA unexpected: %+v", got)
	}
	expectNoEvent(t, subB)
	expectNoEvent(t, subC)

	// Event for (objB, data1) → subB only.
	d.Dispatch(changeOn("space1", "objB", "data1", 2), nil, nil, nil)
	if got := recv(t, subB); got.ObjectId != "objB" {
		t.Fatalf("subB unexpected: %+v", got)
	}
	expectNoEvent(t, subA)
	expectNoEvent(t, subC)
}

func TestSubscribeProperties_FirehoseAcrossObjects(t *testing.T) {
	d := New("space1")
	t.Cleanup(func() { _ = d.Close() })

	sub := d.SubscribeProperties(8)

	// Property changes on different objects all land.
	d.Dispatch(changeOn("space1", "objA", ObjectsDataset, 10), nil, nil, nil)
	d.Dispatch(changeOn("space1", "objB", ObjectsDataset, 11), nil, nil, nil)
	d.Dispatch(changeOn("space1", "objC", ObjectsDataset, 12), nil, nil, nil)

	// Wait coalesces — one call returns the whole batch.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	msgs, err := sub.Mailbox().Wait(ctx)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 events, got %d (%+v)", len(msgs), msgs)
	}
	want := map[crdt.VersionId]bool{vId(10): true, vId(11): true, vId(12): true}
	for _, ev := range msgs {
		if !want[ev.VersionId] {
			t.Fatalf("unexpected VersionId=%s", ev.VersionId)
		}
		delete(want, ev.VersionId)
	}
	if len(want) != 0 {
		t.Fatalf("missing events for VersionId: %+v", want)
	}

	// Non-properties dataset must NOT fire the firehose.
	d.Dispatch(changeOn("space1", "objA", "otherDataset", 13), nil, nil, nil)
	expectNoEvent(t, sub)
}

func TestSubscribeProperties_AndExplicitObjectsBothFire(t *testing.T) {
	// A property change to objA reaches BOTH the property-firehose
	// AND an explicit (objA, "objects") subscription — they're
	// different lenses on the same event.
	d := New("space1")
	t.Cleanup(func() { _ = d.Close() })

	propSub := d.SubscribeProperties(8)
	explicitSub := d.Subscribe("objA", ObjectsDataset, 8)
	otherObjSub := d.Subscribe("objB", ObjectsDataset, 8)

	d.Dispatch(changeOn("space1", "objA", ObjectsDataset, 99), nil, nil, nil)

	if ev := recv(t, propSub); ev.VersionId != vId(99) {
		t.Fatalf("propSub unexpected: %+v", ev)
	}
	if ev := recv(t, explicitSub); ev.VersionId != vId(99) {
		t.Fatalf("explicitSub unexpected: %+v", ev)
	}
	expectNoEvent(t, otherObjSub)
}

func TestMultipleSubsToSamePair_AllReceive(t *testing.T) {
	d := New("space1")
	t.Cleanup(func() { _ = d.Close() })

	sub1 := d.Subscribe("objA", "data1", 8)
	sub2 := d.Subscribe("objA", "data1", 8)

	d.Dispatch(changeOn("space1", "objA", "data1", 5), nil, nil, nil)

	if recv(t, sub1).VersionId != vId(5) {
		t.Fatalf("sub1 missed event")
	}
	if recv(t, sub2).VersionId != vId(5) {
		t.Fatalf("sub2 missed event")
	}
}

func TestSubscriptionClose_Idempotent_AndDecrementsOnce(t *testing.T) {
	d := New("space1")
	t.Cleanup(func() { _ = d.Close() })

	sub := d.Subscribe("objA", "data1", 4)
	if d.counter.Load() != 1 {
		t.Fatalf("counter after Subscribe = %d, want 1", d.counter.Load())
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if d.counter.Load() != 0 {
		t.Fatalf("counter after Close = %d, want 0", d.counter.Load())
	}
	// Second Close on an already-closed mb returns mb.ErrClosed —
	// our Close swallows it via the CAS guard.
	if err := sub.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if d.counter.Load() != 0 {
		t.Fatalf("counter after second Close = %d, want 0", d.counter.Load())
	}
}

func TestSubscriptionClose_StopsDelivery(t *testing.T) {
	d := New("space1")
	t.Cleanup(func() { _ = d.Close() })

	sub := d.Subscribe("objA", "data1", 4)
	d.Dispatch(changeOn("space1", "objA", "data1", 1), nil, nil, nil)
	if recv(t, sub).VersionId != vId(1) {
		t.Fatalf("missed pre-close event")
	}

	if err := sub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	expectClosed(t, sub)

	// Subsequent Dispatch must not panic (closed mb's TryAdd
	// returns ErrClosed; deliver discards it).
	d.Dispatch(changeOn("space1", "objA", "data1", 2), nil, nil, nil)
}

func TestDispatcherClose_ClosesAllSubs(t *testing.T) {
	d := New("space1")
	psub := d.SubscribeProperties(4)
	sub1 := d.Subscribe("objA", "data1", 4)
	sub2 := d.Subscribe("objB", "data2", 4)

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if d.counter.Load() != 0 {
		t.Fatalf("counter after Dispatcher.Close = %d, want 0", d.counter.Load())
	}

	expectClosed(t, psub)
	expectClosed(t, sub1)
	expectClosed(t, sub2)

	// Idempotent.
	if err := d.Close(); err != nil {
		t.Fatalf("second Dispatcher.Close: %v", err)
	}
	// Sub.Close after Dispatcher.Close must not double-close.
	if err := sub1.Close(); err != nil {
		t.Fatalf("sub.Close after Dispatcher.Close: %v", err)
	}
}

func TestSubscribeAfterDispatcherClose_ReturnsClosedSub(t *testing.T) {
	d := New("space1")
	_ = d.Close()

	sub := d.Subscribe("objA", "data1", 4)
	expectClosed(t, sub)
	if d.HasSubscribers() {
		t.Fatalf("HasSubscribers=true after post-Close Subscribe")
	}

	psub := d.SubscribeProperties(4)
	expectClosed(t, psub)
}

func TestDispatch_NonBlockingOnFullMailbox(t *testing.T) {
	// Capacity 1 — second Dispatch must not block; the event drops
	// (mb.TryAdd returns ErrOverflowed) and bumps Dropped().
	d := New("space1")
	t.Cleanup(func() { _ = d.Close() })

	sub := d.Subscribe("objA", "data1", 1)

	d.Dispatch(changeOn("space1", "objA", "data1", 1), nil, nil, nil)

	done := make(chan struct{})
	go func() {
		d.Dispatch(changeOn("space1", "objA", "data1", 2), nil, nil, nil) // would block on full mb; must drop
		d.Dispatch(changeOn("space1", "objA", "data1", 3), nil, nil, nil) // also drops
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("Dispatch blocked on full mailbox")
	}

	// First event lands; later were dropped, surfaced via Dropped().
	if recv(t, sub).VersionId != vId(1) {
		t.Fatalf("first event missed")
	}
	expectNoEvent(t, sub)
	if got := sub.Dropped(); got != 2 {
		t.Fatalf("Dropped() = %d, want 2", got)
	}
}

func TestDropped_ZeroOnHealthyConsumer(t *testing.T) {
	d := New("space1")
	t.Cleanup(func() { _ = d.Close() })

	sub := d.Subscribe("objA", "data1", 16)
	for i := 0; i < 8; i++ {
		d.Dispatch(changeOn("space1", "objA", "data1", uint64(i)), nil, nil, nil)
	}
	// Drain.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	msgs, err := sub.Mailbox().Wait(ctx)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if len(msgs) != 8 {
		t.Fatalf("expected 8 events, got %d", len(msgs))
	}
	if got := sub.Dropped(); got != 0 {
		t.Fatalf("Dropped() = %d on healthy consumer, want 0", got)
	}
}

func TestDropped_NotIncrementedAfterClose(t *testing.T) {
	// Once a sub is Closed it leaves the registries, so subsequent
	// Dispatches don't reach it at all — Dropped stays at whatever
	// it was at Close time. (ErrClosed from a stale parallel deliver
	// is also not counted, but that path is concurrency-sensitive
	// and not tested deterministically here.)
	d := New("space1")
	t.Cleanup(func() { _ = d.Close() })

	sub := d.Subscribe("objA", "data1", 1)
	d.Dispatch(changeOn("space1", "objA", "data1", 1), nil, nil, nil) // fills
	d.Dispatch(changeOn("space1", "objA", "data1", 2), nil, nil, nil) // drops → Dropped=1
	if got := sub.Dropped(); got != 1 {
		t.Fatalf("Dropped() before Close = %d, want 1", got)
	}
	_ = sub.Close()
	d.Dispatch(changeOn("space1", "objA", "data1", 3), nil, nil, nil)
	d.Dispatch(changeOn("space1", "objA", "data1", 4), nil, nil, nil)
	if got := sub.Dropped(); got != 1 {
		t.Fatalf("Dropped() = %d after post-Close dispatches, want 1 (frozen)", got)
	}
}

func TestConcurrent_SubscribeDispatchClose_NoRace(t *testing.T) {
	// Smoke test for the race detector. Mostly cares that close
	// CAS arbitration holds up under concurrency — no panics, no
	// counter drift.
	d := New("space1")

	const N = 50
	var wg sync.WaitGroup
	var subscribed atomic.Int64

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sub := d.Subscribe("objA", "data1", 16)
			subscribed.Add(1)
			// Drain a few before closing.
			done := time.After(20 * time.Millisecond)
		drain:
			for {
				select {
				case <-done:
					break drain
				default:
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
					_, err := sub.Mailbox().Wait(ctx)
					cancel()
					if errors.Is(err, mb.ErrClosed) {
						break drain
					}
				}
			}
			_ = sub.Close()
		}(i)
	}

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				d.Dispatch(changeOn("space1", "objA", "data1", uint64(j)), nil, nil, nil)
			}
		}()
	}

	wg.Wait()
	// All subs closed; counter must settle at zero.
	if d.counter.Load() != 0 {
		t.Fatalf("counter drift after concurrent run: %d", d.counter.Load())
	}
}

func TestUnregister_CleansEmptyNestedMaps(t *testing.T) {
	// Internal hygiene — Subscribing and closing many distinct
	// (object, dataset) pairs must not leak empty inner maps.
	d := New("space1")
	t.Cleanup(func() { _ = d.Close() })

	subs := []Subscription{}
	for i := 0; i < 5; i++ {
		subs = append(subs, d.Subscribe("objA", "ds", 4))
	}
	for _, s := range subs {
		_ = s.Close()
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	if len(d.dataSubs) != 0 {
		t.Fatalf("dataSubs not cleaned up after closing all subs: %+v", d.dataSubs)
	}
}

func TestDispatch_NilChange_NoOp(t *testing.T) {
	d := New("space1")
	t.Cleanup(func() { _ = d.Close() })
	_ = d.SubscribeProperties(4) // make sure HasSubscribers passes
	d.Dispatch(nil, nil, nil, nil)    // no panic
}

func TestSubscription_BatchDelivery(t *testing.T) {
	// One Wait returns the whole burst — the headline benefit of
	// exposing mb directly instead of a chan.
	d := New("space1")
	t.Cleanup(func() { _ = d.Close() })

	sub := d.SubscribeProperties(64)
	for i := 0; i < 10; i++ {
		d.Dispatch(changeOn("space1", "objA", ObjectsDataset, uint64(i)), nil, nil, nil)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	msgs, err := sub.Mailbox().Wait(ctx)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if len(msgs) != 10 {
		t.Fatalf("expected 10 batched events in one Wait, got %d", len(msgs))
	}
}

// TestDispatch_ProjectsRecordsToSetUnset is the contract for the
// $set/$unset wire shape: every input op has to come out as either a
// $set with the post-apply value at that path or a $unset, and a
// record-level delete collapses to EventRecord.Deleted with no ops.
//
// We simulate the apply side (post-apply value lookup) with an
// in-memory map keyed by record index — the dispatcher doesn't care
// where the value came from, only that PostValueFn returns it.
func TestDispatch_ProjectsRecordsToSetUnset(t *testing.T) {
	d := New("space1")
	t.Cleanup(func() { _ = d.Close() })

	sub := d.Subscribe("objA", "data1", 8)

	a := &anyenc.Arena{}
	post := a.NewObject()
	// post-apply state: { count: 7, tags: ["new", "hot"], leftover: "x" }
	post.Set("count", a.NewNumberInt(7))
	tags := a.NewArray()
	tags.SetArrayItem(0, a.NewString("new"))
	tags.SetArrayItem(1, a.NewString("hot"))
	post.Set("tags", tags)
	post.Set("leftover", a.NewString("x"))

	ch := &crdt.Change{
		SpaceId:   "space1",
		ObjectId:  "objA",
		Dataset:   "data1",
		AddSeq:    42,
		VersionId: crdt.VersionId("v-001"),
		ChangeId:  "ch-abc",
		Records: []crdt.RecordChange{
			{
				Id: "r-keep",
				Ops: []crdt.Op{
					{Type: crdt.OpInc, Path: []string{"count"}, Payload: a.NewNumberInt(1)},
					{Type: crdt.OpAddToSet, Path: []string{"tags"}, Payload: a.NewString("hot")},
					{Type: crdt.OpSet, Path: []string{"leftover"}, Payload: a.NewString("x")},
					{Type: crdt.OpUnset, Path: []string{"missing"}},
				},
			},
			{
				Id:  "r-gone",
				Ops: []crdt.Op{{Type: crdt.OpDelete}},
			},
		},
	}

	resolvedIds := []string{"r-keep", "r-gone"}
	postValue := func(i int) *anyenc.Value {
		if i == 0 {
			return post
		}
		return nil // r-gone is tombstoned — no post value
	}

	d.Dispatch(ch, resolvedIds, nil, postValue)
	got := recv(t, sub)

	if got.VersionId != "v-001" {
		t.Errorf("VersionId = %q, want v-001", got.VersionId)
	}
	if len(got.Records) != 2 {
		t.Fatalf("got %d records, want 2: %+v", len(got.Records), got.Records)
	}

	// Record 0: r-keep, with projected ops.
	r0 := got.Records[0]
	if r0.Id != "r-keep" || r0.Deleted {
		t.Errorf("record 0 unexpected: %+v", r0)
	}
	if len(r0.Ops) != 4 {
		t.Fatalf("record 0 ops count = %d, want 4: %+v", len(r0.Ops), r0.Ops)
	}
	// $inc → $set with the post value.
	if r0.Ops[0].Type != crdt.OpSet || pathOf(r0.Ops[0]) != "count" {
		t.Errorf("op[0] not $set count: %+v", r0.Ops[0])
	}
	if got, want := r0.Ops[0].Payload.GetInt(), 7; got != want {
		t.Errorf("op[0] payload = %d, want %d", got, want)
	}
	// $addToSet → $set with the new array.
	if r0.Ops[1].Type != crdt.OpSet || pathOf(r0.Ops[1]) != "tags" {
		t.Errorf("op[1] not $set tags: %+v", r0.Ops[1])
	}
	if arr := r0.Ops[1].Payload.GetArray(); len(arr) != 2 {
		t.Errorf("op[1] tags len = %d, want 2", len(arr))
	}
	// $set passes through.
	if r0.Ops[2].Type != crdt.OpSet || pathOf(r0.Ops[2]) != "leftover" {
		t.Errorf("op[2] not $set leftover: %+v", r0.Ops[2])
	}
	// $unset passes through.
	if r0.Ops[3].Type != crdt.OpUnset || pathOf(r0.Ops[3]) != "missing" {
		t.Errorf("op[3] not $unset missing: %+v", r0.Ops[3])
	}

	// Record 1: r-gone, deleted, no ops.
	r1 := got.Records[1]
	if r1.Id != "r-gone" || !r1.Deleted || len(r1.Ops) != 0 {
		t.Errorf("record 1 should be deleted with no ops: %+v", r1)
	}
}

// TestDispatch_PayloadOutlivesArena guarantees the Op.Payload pointers
// on the Event don't alias the post-apply arena that produced them.
// If they did, a subsequent reset of that arena (or buffer reuse on
// the next apply) would silently corrupt the consumer's view.
func TestDispatch_PayloadOutlivesArena(t *testing.T) {
	d := New("space1")
	t.Cleanup(func() { _ = d.Close() })

	sub := d.SubscribeProperties(8)

	a := &anyenc.Arena{}
	post := a.NewObject()
	post.Set("title", a.NewString("Casablanca"))

	ch := &crdt.Change{
		SpaceId:  "space1",
		ObjectId: "objA",
		Dataset:  ObjectsDataset,
		Records: []crdt.RecordChange{{
			Id: "objA",
			Ops: []crdt.Op{{
				Type:    crdt.OpInc,
				Path:    []string{"title"}, // bogus inc on a string — the projector still derives the post value.
				Payload: a.NewNumberInt(1),
			}},
		}},
	}

	d.Dispatch(ch, []string{"objA"}, nil, func(i int) *anyenc.Value {
		return post
	})
	got := recv(t, sub)

	// Recycle the source arena — if the projection didn't deep-copy,
	// reading got's Payload below would pull whatever the arena now holds.
	a.Reset()
	a.NewString("ZZZZZZZ")

	if len(got.Records) != 1 || len(got.Records[0].Ops) != 1 {
		t.Fatalf("event shape unexpected: %+v", got.Records)
	}
	if v := string(got.Records[0].Ops[0].Payload.GetStringBytes()); v != "Casablanca" {
		t.Fatalf("payload aliasing: got %q, want Casablanca", v)
	}
}

// pathOf is a tiny stringifier for op-path assertions.
func pathOf(op EventOp) string {
	if len(op.Path) == 0 {
		return ""
	}
	out := op.Path[0]
	for _, seg := range op.Path[1:] {
		out += "." + seg
	}
	return out
}
