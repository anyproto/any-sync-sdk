package subscribe

import (
	"errors"
	"fmt"
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

// The change's own event carries the change's effect on the window,
// under its VersionId: its records' transitions and the rows they
// moved into or out of view. The refill's event is a resync from
// storage with no VersionId and no ops, so a client's own-write check
// by VersionId sees exactly the write's effect, and a fence on
// VersionId cannot drop what the refill found.
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

	view.apply([]space.SubscriptionEvent{own, resync})
	agrees(t, sub, view, m)
}

// A held row's own sort move while the window is below capacity ranks
// against the edge: moved to or past it, the row sorts among the
// matches the window does not hold, and an arrival below the edge does
// not certify the window around it.
func TestRefill_HeldMoveBelowCapacityRanks(t *testing.T) {
	seed := func(t *testing.T, fail bool) (*Engine, *storeModel, *Sub, *clientView) {
		eng := New("test")
		arena := newArena()
		m := newStoreModel(t, nil, 2, makeRow(arena, "a", 10, nil), makeRow(arena, "b", 20, nil),
			makeRow(arena, "c", 30, nil), makeRow(arena, "d", 40, nil), makeRow(arena, "e", 50, nil))
		sub, view := subscribeModel(t, eng, m, 100)
		if fail {
			m.fail = errors.New("busy")
		}
		// One change: a deleted, b moved past the edge, x landed below
		// it, c deleted, y landed between the edge and b.
		m.delete("a")
		m.set(makeRow(arena, "b", 100, nil))
		m.set(makeRow(arena, "x", 0, nil))
		m.delete("c")
		m.set(makeRow(arena, "y", 45, nil))
		records := []EventRecord{{Id: "a", Deleted: true}, {Id: "b"}, {Id: "x"}, {Id: "c", Deleted: true}, {Id: "y"}}
		eng.OnApply(Event{SpaceId: "test", ObjectId: "obj1", Dataset: "chat", VersionId: "v", Records: records},
			func(i int) (*anyenc.Value, error) { return m.rows[records[i].Id], nil })
		return eng, m, sub, view
	}
	t.Run("refill ranks it", func(t *testing.T) {
		eng, m, sub, view := seed(t, false)
		defer eng.Close()
		assert.Equal(t, 1, m.reads)
		assert.Equal(t, 0, sub.lost)
		agrees(t, sub, view, m)
	})
	t.Run("no refill: unranked", func(t *testing.T) {
		eng, _, sub, view := seed(t, true)
		defer eng.Close()
		view.fold(t, sub)
		assert.True(t, sub.unranked, "the window is not certified")
		assert.True(t, sub.needsRefill())
		assert.Equal(t, 3, sub.lost, "two departures and the unranked row")
	})
}

// A row the records took out of view without a verdict on it (its
// record could not be read; another row's arrival demoted or evicted
// it) leaves with the reason storage gives when the refill looks it up.
func TestRefill_VerdictlessLeaverReasonComesFromStorage(t *testing.T) {
	for name, arrivals := range map[string]int{"demoted": 1, "evicted": 2} {
		t.Run(name, func(t *testing.T) {
			eng := New("test")
			defer eng.Close()
			arena := newArena()
			m := newStoreModel(t, activeOnly, 2, driftRows(4)...)
			sub, view := subscribeModel(t, eng, m, 30)

			// Storage has r1 filtered out; its record cannot be read.
			m.set(readRow(arena, "r1", 1))
			records := []EventRecord{{Id: "r1"}}
			for i := 0; i < arrivals; i++ {
				id := fmt.Sprintf("x%d", i)
				m.set(activeRow(arena, id, -10-i))
				records = append(records, EventRecord{Id: id})
			}
			eng.OnApply(Event{SpaceId: "test", ObjectId: "obj1", Dataset: "chat", VersionId: "v", Records: records},
				func(i int) (*anyenc.Value, error) {
					if i == 0 {
						return nil, errors.New("read failed")
					}
					return m.rows[records[i].Id], nil
				})
			own, resync := ownAndResync(t, sub)
			var r1 *space.RemovedRecord
			for i := range own.Removed {
				if own.Removed[i].Id == "r1" {
					r1 = &own.Removed[i]
				}
			}
			require.NotNil(t, r1, "%v", own.Removed)
			assert.Equal(t, space.RemoveFilteredOut, r1.Reason)
			assert.Contains(t, m.lookups, "r1")
			view.apply([]space.SubscriptionEvent{own, resync})
			agrees(t, sub, view, m)
		})
	}
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

// A refill whose lookups fail partway keeps none of the reasons it had
// looked up: the change's own event classifies the rows the records
// moved from the records alone, and the window stays as it was. The
// held row's lookup runs first and succeeds; the lookup of a row the
// records evicted runs after it and fails.
func TestRefill_FailedLookupKeepsNoReason(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 2, driftRows(5)...)
	sub, view := subscribeModel(t, eng, m, 500)

	// Below capacity with an edge: r2 left while refills failed.
	m.fail = errors.New("busy")
	m.delete("r2")
	fireEvent(eng, "obj1", "chat", "r2", nil, true, nil)
	view.fold(t, sub)
	m.fail = nil
	require.Equal(t, 2, len(sub.entries))

	// Storage is ahead: r0 and r1 deleted, their events still queued.
	m.delete("r0")
	m.delete("r1")
	m.lookupFail = map[string]error{"r1": errors.New("torn")}

	// n arrives past the edge (unranked); m evicts n; k evicts r1. r0
	// ends as the sentinel and is looked up first (held, not read
	// back: deleted); r1 was evicted, and its lookup fails.
	m.set(makeRow(arena, "n", 35, nil))
	m.set(makeRow(arena, "m", -5, nil))
	m.set(makeRow(arena, "k", -7, nil))
	records := []EventRecord{{Id: "n"}, {Id: "m"}, {Id: "k"}}
	eng.OnApply(Event{SpaceId: "test", ObjectId: "obj1", Dataset: "chat", VersionId: "v", Records: records},
		func(i int) (*anyenc.Value, error) { return m.rows[records[i].Id], nil })
	own, resync := ownAndResync(t, sub)
	assert.Equal(t, 2, m.reads, "the failed refill behind the departure, and this one")
	assert.Equal(t, []string{"r0", "r1"}, m.lookups[len(m.lookups)-2:])
	assert.ElementsMatch(t, []string{"k", "m"}, idsOf(own.Added))
	assert.ElementsMatch(t, []space.RemovedRecord{
		{Id: "r0", Reason: space.RemoveDisplaced},
		{Id: "r1", Reason: space.RemoveDisplaced},
	}, own.Removed, "r0 is still held as the sentinel; the records alone say displaced")
	assert.Empty(t, resync.Added)
	assert.Empty(t, resync.Removed)
	assert.NotNil(t, sub.entries["r0"])
	view.apply([]space.SubscriptionEvent{own})
	agreesWithEngine(t, sub, view, m)
	assert.NoError(t, sub.Err())
}

// A refill deferred because storage is not final counts nothing, keeps
// an unreadable row open, and marks the window for the re-read Resync
// runs when storage is final again.
func TestRefill_DeferredCountsNothingUntilResync(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 2, driftRows(6)...)
	sub, view := subscribeModel(t, eng, m, 30)
	m.fail = fmt.Errorf("%w: rebuilding", ErrRefillDeferred)

	m.delete("r0")
	fireEvent(eng, "obj1", "chat", "r0", nil, true, nil)
	m.delete("r1")
	fireEvent(eng, "obj1", "chat", "r1", nil, true, nil)
	edited := makeRow(arena, "r3", 3, map[string]any{"edited": true})
	m.set(edited)
	fireUnreadable(eng, "obj1", "chat", "r3")
	view.fold(t, sub)
	assert.Equal(t, 0, sub.lost, "deferred refills cost nothing")
	assert.True(t, sub.deferred)
	assert.NoError(t, sub.Err(), "an unreadable row waits for the re-read")
	assert.Equal(t, 3, m.reads)

	m.fail = nil
	src := &struct{ name string }{"obj1"}
	eng.Resync("obj1", src)
	agrees(t, sub, view, m)
	assert.False(t, sub.deferred)
	assert.Equal(t, src, m.sources[len(m.sources)-1])
}

// Resync re-reads a window elsewhere whose last refill was deferred (a
// shared window that could not classify the rebuilding object's row)
// and leaves other shared windows alone.
func TestEngine_ResyncReReadsDeferredSharedWindow(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	deferredM := newStoreModel(t, nil, 2, driftRows(5)...)
	deferredSub, err := eng.Subscribe(SubConfig{Scope: Scope{Shared: true}, Sort: deferredM.sort, Limit: 2, MailboxCap: minMailboxCap, Refill: deferredM.refill()}, snapshotFromRows(deferredM.window()))
	require.NoError(t, err)
	otherM := newStoreModel(t, nil, 2, driftRows(5)...)
	otherSub, err := eng.Subscribe(SubConfig{Scope: Scope{Shared: true}, Sort: otherM.sort, Limit: 2, MailboxCap: minMailboxCap, Refill: otherM.refill()}, snapshotFromRows(otherM.window()))
	require.NoError(t, err)

	// Two departures leave both windows one visible row short; the
	// deferred one could not refill, the other did.
	deferredM.fail = fmt.Errorf("%w: rebuilding", ErrRefillDeferred)
	for _, id := range []string{"r0", "r1"} {
		deferredM.delete(id)
		otherM.delete(id)
		eng.OnApply(Event{SpaceId: "test", ObjectId: id, Dataset: "objects", VersionId: "v", Records: []EventRecord{{Id: id, Deleted: true}}}, nil)
	}
	drain(t, deferredSub)
	drain(t, otherSub)
	require.True(t, deferredSub.deferred)
	require.False(t, otherSub.deferred)
	otherReads := otherM.reads

	deferredM.fail = nil
	eng.Resync("some-object", nil)
	evs := deferredSub.Events().GetAll()
	require.Len(t, evs, 1)
	assert.Equal(t, []string{"r3"}, idsOf(evs[0].Added), "the row past the edge the deferred refill owed")
	assert.False(t, deferredSub.deferred)
	assert.Equal(t, otherReads, otherM.reads, "a window not deferred is not re-read")
	assert.Empty(t, otherSub.Events().GetAll())
}

// A window that cannot re-read closes when storage moved under it with
// no event it could follow.
func TestEngine_ResyncClosesWindowsThatCannotRefill(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	m := newStoreModel(t, nil, 2, driftRows(5)...)
	offset, err := eng.Subscribe(SubConfig{Scope: driftScope, Sort: m.sort, Limit: 2, Offset: 1, MailboxCap: minMailboxCap, Refill: m.refill()}, snapshotFromRows(m.window()[1:]))
	require.NoError(t, err)
	plain, err := eng.Subscribe(SubConfig{Scope: driftScope, Sort: m.sort, Limit: 2, MailboxCap: minMailboxCap}, snapshotFromRows(m.window()))
	require.NoError(t, err)
	other, err := eng.Subscribe(SubConfig{Scope: Scope{ObjectId: "obj2", Dataset: "chat"}, Sort: m.sort, Limit: 2, Offset: 1, MailboxCap: minMailboxCap}, snapshotFromRows(m.window()[1:]))
	require.NoError(t, err)

	eng.Resync("obj1", nil)
	assert.ErrorIs(t, offset.Err(), space.ErrSubscriptionDrifted)
	assert.ErrorIs(t, plain.Err(), space.ErrSubscriptionDrifted)
	assert.NoError(t, other.Err(), "another object's window is untouched")
}

// Resync re-reads every window on an object from storage outside any
// apply: a windowed and an unbounded window on the object ship what
// storage holds that they do not, as resync events through the given
// source; a window on another object and a shared one are not read.
func TestEngine_ResyncReReadsObjectWindows(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	windowed := newStoreModel(t, nil, 2, driftRows(5)...)
	sub, view := subscribeModel(t, eng, windowed, 30)
	unbounded := newStoreModel(t, nil, 0, driftRows(5)...)
	all, err := eng.Subscribe(SubConfig{Scope: driftScope, Refill: unbounded.refill()}, snapshotFromRows(unbounded.matching()))
	require.NoError(t, err)
	other := newStoreModel(t, nil, 2, driftRows(5)...)
	otherSub, err := eng.Subscribe(SubConfig{Scope: Scope{ObjectId: "obj2", Dataset: "chat"}, Sort: other.sort, Limit: 2, Refill: other.refill()}, snapshotFromRows(other.window()))
	require.NoError(t, err)
	shared := newStoreModel(t, nil, 2, driftRows(5)...)
	sharedSub, err := eng.Subscribe(SubConfig{Scope: Scope{Shared: true}, Sort: shared.sort, Limit: 2, Refill: shared.refill()}, snapshotFromRows(shared.window()))
	require.NoError(t, err)

	// Storage moved without events, as a rebuild's wipe and replay do.
	for _, m := range []*storeModel{windowed, unbounded} {
		m.delete("r0")
		m.set(makeRow(arena, "x", -1, nil))
	}
	src := &struct{ name string }{"obj1"}
	eng.Resync("obj1", src)

	evs := sub.Events().GetAll()
	require.Len(t, evs, 1)
	assert.Equal(t, crdt.VersionId(""), evs[0].VersionId)
	assert.Equal(t, []string{"x"}, idsOf(evs[0].Added))
	assert.Equal(t, []space.RemovedRecord{{Id: "r0", Reason: space.RemoveDeleted}}, evs[0].Removed)
	view.apply(evs)
	agrees(t, sub, view, windowed)
	assert.Equal(t, []any{src}, windowed.sources)
	assert.Equal(t, 0, sub.lost)

	evs = all.Events().GetAll()
	require.Len(t, evs, 1)
	assert.Equal(t, []string{"x"}, idsOf(evs[0].Added))
	assert.Equal(t, []space.RemovedRecord{{Id: "r0", Reason: space.RemoveDeleted}}, evs[0].Removed)
	assert.Equal(t, []any{src}, unbounded.sources)

	assert.Equal(t, 0, other.reads)
	assert.Equal(t, 0, shared.reads)
	assert.Empty(t, otherSub.Events().GetAll())
	assert.Empty(t, sharedSub.Events().GetAll())
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
