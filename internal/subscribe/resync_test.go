package subscribe

import (
	"errors"
	"math"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// A row the apply's records took out of view and the refill put back
// with another stored value reaches the client with that value: the
// change's own event removes it, the refill's adds it again.
func TestRefill_ReinsertedRowShipsItsStoredValue(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 2, makeRow(arena, "p", 63, nil), makeRow(arena, "q", 46, nil))
	sub, view := subscribeModel(t, eng, m, 30)
	require.True(t, sub.complete)

	// One change: a lands, p is rewritten (unreadable; storage has it
	// at 11), b lands. a demotes p to sentinel, b evicts it.
	m.set(makeRow(arena, "a", 19, nil))
	m.set(makeRow(arena, "p", 11, nil))
	m.set(makeRow(arena, "b", 26, nil))
	records := []EventRecord{{Id: "a"}, {Id: "p"}, {Id: "b"}}
	eng.OnApply(Event{SpaceId: "test", ObjectId: "obj1", Dataset: "chat", VersionId: "v", Records: records},
		func(i int) (*anyenc.Value, error) {
			if i == 1 {
				return nil, errors.New("read failed")
			}
			return m.rows[records[i].Id], nil
		})
	own, resync := ownAndResync(t, sub)
	assert.ElementsMatch(t, []string{"a", "b"}, idsOf(own.Added))
	assert.ElementsMatch(t, []space.RemovedRecord{
		{Id: "p", Reason: space.RemoveDisplaced},
		{Id: "q", Reason: space.RemoveDisplaced},
	}, own.Removed)
	assert.Equal(t, []string{"p"}, idsOf(resync.Added))
	assert.Equal(t, 11, resync.Added[0].Doc.GetInt("n"))
	assert.Equal(t, []space.RemovedRecord{{Id: "b", Reason: space.RemoveDisplaced}}, resync.Removed)
	view.apply([]space.SubscriptionEvent{own, resync})
	agrees(t, sub, view, m)
}

// The refill's event is a resync from storage: it carries no VersionId
// and no ops, so a client that recognises its own write by VersionId
// and skips that event still receives what the refill found.
func TestRefill_OwnEventCarriesOnlyTheChange(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	m := newStoreModel(t, nil, 3, driftRows(8)...)
	sub, view := subscribeModel(t, eng, m, 30)

	m.delete("r1") // storage is ahead: r1's delete has not arrived
	m.delete("r0")
	ops := []space.EventOp{{Type: crdt.OpSet, Path: []string{"n"}}}
	fireEvent(eng, "obj1", "chat", "r0", nil, true, ops)
	own, resync := ownAndResync(t, sub)
	assert.Equal(t, crdt.VersionId("vr0"), own.VersionId)
	assert.Equal(t, []string{"r3"}, idsOf(own.Added), "the sentinel the change promoted")
	assert.Equal(t, []space.RemovedRecord{{Id: "r0", Reason: space.RemoveDeleted}}, own.Removed)
	assert.Equal(t, crdt.VersionId(""), resync.VersionId)
	assert.Equal(t, []string{"r4"}, idsOf(resync.Added), "the row storage held past the edge")
	assert.Equal(t, []space.RemovedRecord{{Id: "r1", Reason: space.RemoveDeleted}}, resync.Removed)
	assert.Empty(t, resync.Updated)

	// The client skipped its own write: its view is one row behind
	// until the resync, then equal.
	view.apply([]space.SubscriptionEvent{{Removed: own.Removed}, {Added: own.Added}, resync})
	agrees(t, sub, view, m)
}

// A refill receives Event.Source of the apply behind it, and nil behind
// a notification.
func TestRefill_ReceivesTheApplySource(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	m := newStoreModel(t, nil, 2, driftRows(6)...)
	sub, _ := subscribeModel(t, eng, m, 30)

	m.delete("r0")
	src := &struct{ name string }{"obj1"}
	eng.OnApply(Event{SpaceId: "test", ObjectId: "obj1", Dataset: "chat", VersionId: "v", Source: src,
		Records: []EventRecord{{Id: "r0", Deleted: true}}}, nil)
	m.delete("r1")
	eng.NotifyRecordsDeleted("test", "chat", "obj1", []string{"r1"})
	drain(t, sub)
	require.Len(t, m.sources, 2)
	assert.Same(t, src, m.sources[0])
	assert.Nil(t, m.sources[1])
	assert.Equal(t, 2, m.closed, "every read is closed")
}

// A read that fails hands no read to close; a lookup that fails fails
// the refill and leaves the held set as it was.
func TestRefill_LookupFailureChangesNothing(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	m := newStoreModel(t, nil, 3, driftRows(8)...)
	sub, err := eng.Subscribe(SubConfig{
		Scope: driftScope, Sort: m.sort, Limit: 3, MailboxCap: minMailboxCap, DriftBudget: 100,
		Refill: func(any) (RefillRead, error) {
			return RefillRead{
				Rows:   func(yield func(id string, doc *anyenc.Value)) error { return nil },
				Lookup: func(string) (*anyenc.Value, error) { return nil, errors.New("torn") },
				Close:  func() {},
			}, nil
		},
	}, snapshotFromRows(m.window()))
	require.NoError(t, err)

	m.delete("r0")
	fireEvent(eng, "obj1", "chat", "r0", nil, true, nil)
	ev, err := waitOne(t, sub)
	require.NoError(t, err)
	assert.Equal(t, []string{"r3"}, idsOf(ev.Added))
	assert.Equal(t, []string{"r0"}, removedIdsOf(ev.Removed))
	assert.Len(t, sub.entries, 3)
	assert.Equal(t, 1, sub.lost)
	assert.NoError(t, sub.Err())
}

// A record marked unreadable by the event builder is unreadable to the
// sub without a read of its own.
func TestUnreadable_EventRecordFlag(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 3, driftRows(6)...)
	sub, view := subscribeModel(t, eng, m, 30)

	edited := makeRow(arena, "r1", 1, map[string]any{"edited": true})
	m.set(edited)
	reads := 0
	eng.OnApply(Event{SpaceId: "test", ObjectId: "obj1", Dataset: "chat", VersionId: "v",
		Records: []EventRecord{{Id: "r1", Unreadable: true}}}, func(int) (*anyenc.Value, error) {
		reads++
		return edited.d, nil
	})
	own, resync := ownAndResync(t, sub)
	assert.Equal(t, 0, reads, "the sub does not read a record the builder could not")
	assert.Empty(t, own.Updated)
	assert.Equal(t, []string{"r1"}, idsOf(resync.Updated))
	view.apply([]space.SubscriptionEvent{own, resync})
	agrees(t, sub, view, m)
}

// A delete on an id settles its reason whatever an unreadable record
// before or after it did.
func TestApply_DeleteWithUnreadableRecordIsDeleted(t *testing.T) {
	for name, records := range map[string][]EventRecord{
		"delete then unreadable": {{Id: "r1", Deleted: true}, {Id: "r1"}},
		"unreadable then delete": {{Id: "r1"}, {Id: "r1", Deleted: true}},
	} {
		t.Run(name, func(t *testing.T) {
			eng := New("test")
			defer eng.Close()
			m := newStoreModel(t, nil, 3, driftRows(6)...)
			sub, view := subscribeModel(t, eng, m, 30)

			m.delete("r1")
			eng.OnApply(Event{SpaceId: "test", ObjectId: "obj1", Dataset: "chat", VersionId: "v", Records: records},
				func(int) (*anyenc.Value, error) { return nil, errors.New("read failed") })
			own, resync := ownAndResync(t, sub)
			assert.Equal(t, []space.RemovedRecord{{Id: "r1", Reason: space.RemoveDeleted}}, own.Removed)
			assert.Equal(t, []string{"r3"}, idsOf(own.Added))
			assert.Empty(t, resync.Added, "the refill restores the sentinel only")
			assert.Equal(t, 1, m.reads)
			view.apply([]space.SubscriptionEvent{own, resync})
			agrees(t, sub, view, m)
		})
	}
}

// A refill sizes its buffer from the rows it holds, not from a limit a
// caller may set as large as it likes.
func TestRefill_HugeLimit(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, math.MaxInt, driftRows(4)...)
	sub, view := subscribeModel(t, eng, m, 30)
	require.True(t, sub.complete)

	edited := makeRow(arena, "r1", 1, map[string]any{"edited": true})
	m.set(edited)
	fireUnreadable(eng, "obj1", "chat", "r1")
	_, resync := ownAndResync(t, sub)
	assert.Equal(t, []string{"r1"}, idsOf(resync.Updated))
	view.apply([]space.SubscriptionEvent{resync})
	agrees(t, sub, view, m)
	assert.NoError(t, sub.Err())
}

// With refills failing, a window costs what it did before refills
// existed: one loss per departure it could not replace. An arrival that
// sorts below the window's last ranked edge is ranked and costs
// nothing; one that sorts at or past it is unranked and costs one.
func TestDrift_ArrivalBelowTheEdgeIsRanked(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 10, driftRows(20)...)
	sub, view := subscribeModel(t, eng, m, 50)
	m.fail = errors.New("busy")

	m.delete("r5")
	fireEvent(eng, "obj1", "chat", "r5", nil, true, nil)
	view.fold(t, sub)
	assert.Equal(t, 1, sub.lost)
	assert.Equal(t, m.sort.AppendKey(nil, m.rows["r10"]), sub.edge, "the sentinel's key when the window left capacity")

	// Below every held row: ranked, and the window is at capacity and
	// equal to a fresh query again.
	x := makeRow(arena, "x", -1, nil)
	m.set(x)
	fireEvent(eng, "obj1", "chat", "x", x.d, false, nil)
	view.fold(t, sub)
	assert.Equal(t, 1, sub.lost)
	assert.False(t, sub.unranked)
	assert.True(t, sub.atCapacity())

	m.delete("r6")
	fireEvent(eng, "obj1", "chat", "r6", nil, true, nil)
	view.fold(t, sub)
	assert.Equal(t, 2, sub.lost)

	// Above every held row but below the edge: ranked as well.
	y := makeRow(arena, "y", 9, nil)
	m.set(y)
	fireEvent(eng, "obj1", "chat", "y", y.d, false, nil)
	view.fold(t, sub)
	assert.Equal(t, 2, sub.lost)
	assert.False(t, sub.unranked)

	m.delete("r7")
	fireEvent(eng, "obj1", "chat", "r7", nil, true, nil)
	view.fold(t, sub)
	assert.Equal(t, 3, sub.lost)

	// At the edge: unranked, one more loss, and the client's view
	// still equals the engine's.
	z := makeRow(arena, "z", 50, nil)
	m.set(z)
	fireEvent(eng, "obj1", "chat", "z", z.d, false, nil)
	view.fold(t, sub)
	assert.True(t, sub.unranked)
	assert.Equal(t, 4, sub.lost)
	assert.NoError(t, sub.Err())
	agreesWithEngine(t, sub, view, m)

	// Budget 50 of 10: the fifth loss closes it.
	m.delete("r8")
	fireEvent(eng, "obj1", "chat", "r8", nil, true, nil)
	view.fold(t, sub)
	assert.Equal(t, 5, sub.lost)
	assert.ErrorIs(t, sub.Err(), space.ErrSubscriptionDrifted)
}

// A departure and an arrival in one change that leave the window at
// capacity and ranked need no refill and cost nothing, even when the
// refill would fail.
func TestDrift_ReplacedDepartureIsNotALoss(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 3, driftRows(8)...)
	sub, view := subscribeModel(t, eng, m, 30)
	m.fail = errors.New("busy")

	m.delete("r1")
	x := makeRow(arena, "x", -1, nil)
	m.set(x)
	records := []EventRecord{{Id: "r1", Deleted: true}, {Id: "x"}}
	eng.OnApply(Event{SpaceId: "test", ObjectId: "obj1", Dataset: "chat", VersionId: "v", Records: records},
		func(i int) (*anyenc.Value, error) { return m.rows[records[i].Id], nil })
	ev, err := waitOne(t, sub)
	require.NoError(t, err)
	assert.Equal(t, []string{"x"}, idsOf(ev.Added))
	assert.Equal(t, []space.RemovedRecord{{Id: "r1", Reason: space.RemoveDeleted}}, ev.Removed)
	assert.Equal(t, 0, m.reads)
	assert.Equal(t, 0, sub.lost)
	view.apply([]space.SubscriptionEvent{ev})
	agrees(t, sub, view, m)
}
