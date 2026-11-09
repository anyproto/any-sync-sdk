package subscribe

import (
	"bytes"
	"errors"
	"sort"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/space"
)

// storeModel is what storage holds for one subscription: the rows a
// refill reads, in the order and up to the count the real read
// returns. Tests change it alongside the events they fire, or without
// an event to stand for an apply whose event is still queued.
type storeModel struct {
	rows   map[string]*anyenc.Value
	filter query.Filter
	sort   query.Sort
	limit  int
	reads  int
	fail   error
	buf    *syncpool.DocBuffer
}

func newStoreModel(t *testing.T, filter query.Filter, limit int, rows ...row) *storeModel {
	t.Helper()
	m := &storeModel{rows: map[string]*anyenc.Value{}, filter: filter, sort: mustParseSort(t, "n"), limit: limit, buf: docBufFor()}
	for _, r := range rows {
		m.rows[r.id] = r.d
	}
	return m
}

func (m *storeModel) set(r row)        { m.rows[r.id] = r.d }
func (m *storeModel) delete(id string) { delete(m.rows, id) }

// matching returns every stored row the filter accepts, in sort
// order with ties broken by id.
func (m *storeModel) matching() []row {
	var out []row
	for id, d := range m.rows {
		if m.filter != nil && !m.filter.Ok(d, m.buf) {
			continue
		}
		out = append(out, row{id: id, d: d, n: d.GetInt("n")})
	}
	sort.Slice(out, func(i, j int) bool {
		if c := bytes.Compare(m.sort.AppendKey(nil, out[i].d), m.sort.AppendKey(nil, out[j].d)); c != 0 {
			return c < 0
		}
		return out[i].id < out[j].id
	})
	return out
}

// window returns the first limit+1 matching rows: the snapshot and
// refill read.
func (m *storeModel) window() []row {
	rows := m.matching()
	if m.limit > 0 && len(rows) > m.limit+1 {
		rows = rows[:m.limit+1]
	}
	return rows
}

// visible returns the ids a fresh query of the window's body shows.
func (m *storeModel) visible() []string {
	rows := m.matching()
	if m.limit > 0 && len(rows) > m.limit {
		rows = rows[:m.limit]
	}
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.id)
	}
	return ids
}

func (m *storeModel) refill() SnapshotFn {
	return func(yield func(id string, doc *anyenc.Value)) error {
		m.reads++
		if m.fail != nil {
			return m.fail
		}
		for _, r := range m.window() {
			yield(r.id, r.d)
		}
		return nil
	}
}

// subscribeModel opens a windowed sub over the model with the model as
// its refill, and returns the client's folded view of it.
func subscribeModel(t *testing.T, eng *Engine, m *storeModel, budget int) (*Sub, *clientView) {
	t.Helper()
	sub, err := eng.Subscribe(SubConfig{
		Scope:       driftScope,
		Filter:      m.filter,
		Sort:        m.sort,
		Limit:       m.limit,
		MailboxCap:  minMailboxCap,
		DriftBudget: budget,
		Refill:      m.refill(),
	}, snapshotFromRows(m.window()))
	require.NoError(t, err)
	view := &clientView{docs: map[string]*anyenc.Value{}}
	for _, id := range m.visible() {
		view.docs[id] = m.rows[id]
	}
	return sub, view
}

// clientView folds subscription events the way a consumer does: Added
// and Updated set the row, Removed drops it.
type clientView struct {
	docs map[string]*anyenc.Value
}

func (v *clientView) fold(t *testing.T, sub *Sub) {
	t.Helper()
	for _, ev := range sub.Events().GetAll() {
		for _, r := range ev.Added {
			require.NotNil(t, r.Doc, "added %s ships a doc", r.Id)
			v.docs[r.Id] = r.Doc
		}
		for _, r := range ev.Updated {
			_, shown := v.docs[r.Id]
			require.True(t, shown, "updated %s is in view", r.Id)
			require.NotNil(t, r.Doc, "updated %s ships a doc", r.Id)
			v.docs[r.Id] = r.Doc
		}
		for _, r := range ev.Removed {
			_, shown := v.docs[r.Id]
			require.True(t, shown, "removed %s was in view", r.Id)
			delete(v.docs, r.Id)
		}
	}
}

// apply folds events a test already drained.
func (v *clientView) apply(evs []space.SubscriptionEvent) {
	for _, ev := range evs {
		for _, r := range ev.Added {
			v.docs[r.Id] = r.Doc
		}
		for _, r := range ev.Updated {
			v.docs[r.Id] = r.Doc
		}
		for _, r := range ev.Removed {
			delete(v.docs, r.Id)
		}
	}
}

// ids returns the view's ids in the model's order.
func (v *clientView) ids(m *storeModel) []string {
	var ids []string
	for id := range v.docs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		di, dj := v.docs[ids[i]], v.docs[ids[j]]
		if c := bytes.Compare(m.sort.AppendKey(nil, di), m.sort.AppendKey(nil, dj)); c != 0 {
			return c < 0
		}
		return ids[i] < ids[j]
	})
	return ids
}

// agrees asserts the client's view equals a fresh query of the window
// and the engine's view equals the client's.
func agrees(t *testing.T, sub *Sub, view *clientView, m *storeModel) {
	t.Helper()
	view.fold(t, sub)
	require.Equal(t, m.visible(), view.ids(m), "client view vs fresh query")
	var held []string
	sent := sub.sentinel()
	for id, e := range sub.entries {
		if e != sent {
			held = append(held, id)
		}
	}
	sort.Strings(held)
	want := append([]string(nil), m.visible()...)
	sort.Strings(want)
	require.Equal(t, want, held, "engine view vs fresh query")
	for id, d := range view.docs {
		require.Equal(t, string(m.rows[id].MarshalTo(nil)), string(d.MarshalTo(nil)), "doc of %s", id)
	}
}

func activeRow(arena *anyenc.Arena, id string, n int) row {
	return makeRow(arena, id, n, map[string]any{"status": "active"})
}

func readRow(arena *anyenc.Arena, id string, n int) row {
	return makeRow(arena, id, n, map[string]any{"status": "read"})
}

// Rows that leave a window with matches past its edge are replaced
// from storage: an unread list of 20 over a window of 5 stays equal to
// a fresh query through 15 departures, deletes and filter-outs alike,
// with a budget that would have closed it after two.
func TestRefill_DeparturesRefillFromStorage(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	var rows []row
	for i := 0; i < 20; i++ {
		rows = append(rows, activeRow(arena, "r"+itoa(10+i), i))
	}
	m := newStoreModel(t, activeOnly, 5, rows...)
	sub, view := subscribeModel(t, eng, m, 30)
	require.False(t, sub.complete)

	for i, r := range rows[:15] {
		if i%2 == 0 {
			m.delete(r.id)
			fireEvent(eng, "obj1", "chat", r.id, nil, true, nil)
		} else {
			read := readRow(arena, r.id, r.n)
			m.set(read)
			fireEvent(eng, "obj1", "chat", r.id, read.d, false, nil)
		}
		agrees(t, sub, view, m)
		require.NoError(t, sub.Err())
		assert.Equal(t, 0, sub.lost)
		assert.Equal(t, i+1, m.reads, "one read per departure")
	}
	assert.True(t, sub.complete, "five rows remain: the window holds every match")

	// Complete: further departures read nothing.
	m.delete("r25")
	fireEvent(eng, "obj1", "chat", "r25", nil, true, nil)
	agrees(t, sub, view, m)
	assert.Equal(t, 15, m.reads)
	assert.NoError(t, sub.Err())
}

// A refill that fails leaves the departure as a loss; the budget then
// closes the sub as before.
func TestRefill_FailureCountsAsLoss(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 3, driftRows(6)...)
	sub, view := subscribeModel(t, eng, m, 50) // lostLimit 150: two losses close
	m.fail = errors.New("busy")

	m.delete("r0")
	fireEvent(eng, "obj1", "chat", "r0", nil, true, nil)
	view.fold(t, sub)
	assert.Equal(t, 1, sub.lost)
	assert.NoError(t, sub.Err())
	assert.Len(t, sub.entries, 3, "a failed refill changes nothing")
	assert.Equal(t, []string{"r1", "r2", "r3"}, view.ids(m), "the promoted sentinel; nothing read")

	// A refill that recovers settles the window.
	m.fail = nil
	m.set(activeRow(arena, "r9", 9))
	fireEvent(eng, "obj1", "chat", "r9", m.rows["r9"], false, nil)
	agrees(t, sub, view, m)
	assert.Len(t, sub.entries, 4)
	assert.NoError(t, sub.Err())

	m.fail = errors.New("busy")
	m.delete("r1")
	fireEvent(eng, "obj1", "chat", "r1", nil, true, nil)
	drain(t, sub)
	assert.Equal(t, 2, sub.lost)
	assert.ErrorIs(t, sub.Err(), space.ErrSubscriptionDrifted)
}

// A held row whose own sort move puts it past the edge is ranked
// against the rows past the edge before the next promotion lifts a
// row into view.
func TestRefill_OwnSortMovePastEdgeRanksTheSentinel(t *testing.T) {
	t.Run("two moves", func(t *testing.T) {
		eng := New("test")
		defer eng.Close()
		arena := newArena()
		m := newStoreModel(t, nil, 2, driftRows(5)...)
		sub, view := subscribeModel(t, eng, m, 30)

		moved := makeRow(arena, "r0", 10, nil)
		m.set(moved)
		fireEvent(eng, "obj1", "chat", "r0", moved.d, false, nil)
		agrees(t, sub, view, m)
		assert.Equal(t, 1, m.reads)

		moved = makeRow(arena, "r1", 11, nil)
		m.set(moved)
		fireEvent(eng, "obj1", "chat", "r1", moved.d, false, nil)
		agrees(t, sub, view, m)
		assert.Equal(t, []string{"r2", "r3"}, view.ids(m))
		assert.NoError(t, sub.Err())
	})
	t.Run("move then delete", func(t *testing.T) {
		eng := New("test")
		defer eng.Close()
		arena := newArena()
		m := newStoreModel(t, nil, 10, driftRows(20)...)
		sub, view := subscribeModel(t, eng, m, 30)

		moved := makeRow(arena, "r0", 100, nil)
		m.set(moved)
		fireEvent(eng, "obj1", "chat", "r0", moved.d, false, nil)
		agrees(t, sub, view, m)

		m.delete("r1")
		fireEvent(eng, "obj1", "chat", "r1", nil, true, nil)
		agrees(t, sub, view, m)
		assert.NotContains(t, view.ids(m), "r0")
		assert.Equal(t, 0, sub.lost)
	})
	t.Run("move within the window reads nothing", func(t *testing.T) {
		eng := New("test")
		defer eng.Close()
		arena := newArena()
		m := newStoreModel(t, nil, 3, driftRows(6)...)
		sub, view := subscribeModel(t, eng, m, 30)

		moved := makeRow(arena, "r0", 2, nil) // still ahead of the sentinel r3
		m.set(moved)
		fireEvent(eng, "obj1", "chat", "r0", moved.d, false, nil)
		agrees(t, sub, view, m)
		assert.Equal(t, 0, m.reads)
	})
}

// A row that enters below capacity after a loss the window could not
// refill is ranked by the next refill.
func TestRefill_InsertBelowCapacityIsRanked(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 10, driftRows(20)...)
	sub, view := subscribeModel(t, eng, m, 100)

	m.fail = errors.New("busy")
	m.delete("r0")
	fireEvent(eng, "obj1", "chat", "r0", nil, true, nil)
	view.fold(t, sub)
	require.Len(t, sub.entries, 10)
	require.Equal(t, 1, sub.lost)

	m.fail = nil
	reads := m.reads
	rewritten := makeRow(arena, "r14", 14, map[string]any{"touched": true})
	m.set(rewritten)
	fireEvent(eng, "obj1", "chat", "r14", rewritten.d, false, nil)
	agrees(t, sub, view, m)
	assert.Equal(t, reads+1, m.reads, "the insert below capacity ranks the window at once")
	require.Len(t, sub.entries, 11)
	assert.False(t, sub.unranked)

	m.delete("r1")
	fireEvent(eng, "obj1", "chat", "r1", nil, true, nil)
	agrees(t, sub, view, m)
	assert.Equal(t, []string{"r10", "r11", "r2", "r3", "r4", "r5", "r6", "r7", "r8", "r9"}, sortedCopy(view.ids(m)))
	assert.NoError(t, sub.Err())
}

func sortedCopy(ids []string) []string {
	out := append([]string(nil), ids...)
	sort.Strings(out)
	return out
}

// A refill picks up what storage changed without an event the sub saw:
// a held row with a new value ships as updated, a held row storage no
// longer returns is removed.
func TestRefill_ReconcilesHeldRowsWithStorage(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 3, driftRows(8)...)
	sub, view := subscribeModel(t, eng, m, 30)

	// Storage moved on: r1 rewritten, r2 gone, without events yet.
	m.set(makeRow(arena, "r1", 1, map[string]any{"edited": true}))
	m.delete("r2")

	m.delete("r0")
	fireEvent(eng, "obj1", "chat", "r0", nil, true, nil)
	evs := sub.Events().GetAll()
	require.Len(t, evs, 1)
	assert.Equal(t, []string{"r1"}, idsOf(evs[0].Updated))
	assert.ElementsMatch(t, []string{"r0", "r2"}, removedIdsOf(evs[0].Removed))
	for _, r := range evs[0].Removed {
		switch r.Id {
		case "r0":
			assert.Equal(t, space.RemoveDeleted, r.Reason)
		case "r2":
			assert.Equal(t, space.RemoveFilteredOut, r.Reason, "gone from storage within the window")
		}
	}
	assert.ElementsMatch(t, []string{"r3", "r4"}, idsOf(evs[0].Added))
	for _, r := range evs[0].Added {
		assert.Nil(t, r.Ops, "a refilled row carries no ops")
	}
	view.apply(evs)
	agrees(t, sub, view, m)

	// The queued events arrive: nothing left to do.
	fireEvent(eng, "obj1", "chat", "r2", nil, true, nil)
	fireEvent(eng, "obj1", "chat", "r1", m.rows["r1"], false, nil)
	evs = sub.Events().GetAll()
	require.Len(t, evs, 1, "the update re-ships; the delete of an unheld row is silent")
	assert.Equal(t, []string{"r1"}, idsOf(evs[0].Updated))
	agrees(t, sub, view, m)
}

// A held row absent from the refill leaves as displaced when its last
// known position is at or past the refilled window's edge, and as
// filtered out when it sorted inside it: the engine knows the row's
// last tuple, not what storage did to it.
func TestRefill_AbsentHeldRowReasonFollowsItsLastPosition(t *testing.T) {
	t.Run("past the new edge", func(t *testing.T) {
		eng := New("test")
		defer eng.Close()
		arena := newArena()
		m := newStoreModel(t, nil, 2, driftRows(6)...)
		sub, view := subscribeModel(t, eng, m, 30)

		// Without an event: three rows landed ahead of the window and
		// r1 is gone.
		m.set(makeRow(arena, "a", -3, nil))
		m.set(makeRow(arena, "b", -2, nil))
		m.set(makeRow(arena, "c", -1, nil))
		m.delete("r1")

		m.delete("r0")
		fireEvent(eng, "obj1", "chat", "r0", nil, true, nil)
		evs := sub.Events().GetAll()
		require.Len(t, evs, 1)
		assert.Equal(t, []string{"a", "b"}, idsOf(evs[0].Added))
		assert.Equal(t, []space.RemovedRecord{
			{Id: "r0", Reason: space.RemoveDeleted},
			{Id: "r1", Reason: space.RemoveDisplaced},
		}, evs[0].Removed)
		view.apply(evs)
		agrees(t, sub, view, m)
	})
	t.Run("inside the new edge", func(t *testing.T) {
		eng := New("test")
		defer eng.Close()
		m := newStoreModel(t, nil, 2, driftRows(6)...)
		sub, view := subscribeModel(t, eng, m, 30)

		m.delete("r1") // without an event

		m.delete("r0")
		fireEvent(eng, "obj1", "chat", "r0", nil, true, nil)
		evs := sub.Events().GetAll()
		require.Len(t, evs, 1)
		assert.Equal(t, []string{"r2", "r3"}, idsOf(evs[0].Added))
		assert.Equal(t, []space.RemovedRecord{
			{Id: "r0", Reason: space.RemoveDeleted},
			{Id: "r1", Reason: space.RemoveFilteredOut},
		}, evs[0].Removed)
		view.apply(evs)
		agrees(t, sub, view, m)
	})
}

// A window past an offset is never refilled: its rows shift with rows
// before it, which no re-read of its own rows shows. Departures count.
func TestRefill_OffsetWindowCountsLosses(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	m := newStoreModel(t, nil, 3, driftRows(8)...)
	sub, err := eng.Subscribe(SubConfig{
		Scope:  driftScope,
		Sort:   m.sort,
		Limit:  3,
		Offset: 1,
		Refill: m.refill(),
	}, snapshotFromRows(m.matching()[1:5]))
	require.NoError(t, err)

	fireEvent(eng, "obj1", "chat", "r1", nil, true, nil)
	drain(t, sub)
	assert.Equal(t, 0, m.reads)
	assert.Equal(t, 1, sub.lost)
	assert.ErrorIs(t, sub.Err(), space.ErrSubscriptionDrifted)
}

// A row the apply could not read settles from storage: the window
// shows what storage holds, and the sub stays open.
func TestUnreadable_WindowRefillsFromStorage(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 3, driftRows(6)...)
	sub, view := subscribeModel(t, eng, m, 30)

	// The unreadable row was deleted in storage.
	m.delete("r1")
	fireUnreadable(eng, "obj1", "chat", "r1")
	agrees(t, sub, view, m)
	assert.Equal(t, 1, m.reads)
	assert.NoError(t, sub.Err())
	assert.Equal(t, 0, sub.lost)

	// The unreadable row was rewritten in storage.
	m.set(makeRow(arena, "r2", 2, map[string]any{"edited": true}))
	fireUnreadable(eng, "obj1", "chat", "r2")
	evs := sub.Events().GetAll()
	require.Len(t, evs, 1)
	assert.Equal(t, []string{"r2"}, idsOf(evs[0].Updated))
	assert.Nil(t, evs[0].Updated[0].Ops, "the record's op is dropped: whether it landed is unknown")
	view.docs["r2"] = evs[0].Updated[0].Doc
	agrees(t, sub, view, m)

	// The unreadable row is not among the matches and was not held:
	// nothing changes, nothing ships.
	fireUnreadable(eng, "obj1", "chat", "zzz")
	waitNone(t, sub)
	agrees(t, sub, view, m)
	assert.NoError(t, sub.Err())

	// A complete window settles from storage too.
	m2 := newStoreModel(t, nil, 10, driftRows(4)...)
	sub2, view2 := subscribeModel(t, eng, m2, 30)
	require.True(t, sub2.complete)
	m2.delete("r3")
	fireUnreadable(eng, "obj1", "chat", "r3")
	agrees(t, sub2, view2, m2)
	assert.NoError(t, sub2.Err())
}

// Without a refill an unreadable row closes the sub: an unbounded
// sub, a window with no refill, and a change with no post-value
// lookup at all.
func TestUnreadable_ClosesWithoutRefill(t *testing.T) {
	t.Run("unbounded", func(t *testing.T) {
		eng := New("test")
		defer eng.Close()
		sub, err := eng.Subscribe(SubConfig{Scope: driftScope}, snapshotFromRows(driftRows(3)))
		require.NoError(t, err)
		fireUnreadable(eng, "obj1", "chat", "r1")
		assert.ErrorIs(t, sub.Err(), space.ErrSubscriptionDrifted)
	})
	t.Run("window without refill", func(t *testing.T) {
		eng := New("test")
		defer eng.Close()
		sub := subscribeSorted(t, eng, driftScope, nil, 3, driftRows(2))
		fireUnreadable(eng, "obj1", "chat", "r1")
		assert.ErrorIs(t, sub.Err(), space.ErrSubscriptionDrifted)
	})
	t.Run("nil post value", func(t *testing.T) {
		eng := New("test")
		defer eng.Close()
		sub := subscribeSorted(t, eng, driftScope, nil, 3, driftRows(2))
		other := subscribeSorted(t, eng, Scope{ObjectId: "obj2", Dataset: "chat"}, nil, 3, driftRows(2))
		eng.OnApply(Event{SpaceId: "test", ObjectId: "obj1", Dataset: "chat",
			Records: []EventRecord{{Id: "r1"}}}, nil)
		assert.ErrorIs(t, sub.Err(), space.ErrSubscriptionDrifted)
		assert.NoError(t, other.Err(), "a sub out of scope is untouched")
	})
	t.Run("nil post value with deleted records only", func(t *testing.T) {
		eng := New("test")
		defer eng.Close()
		sub := subscribeSorted(t, eng, driftScope, nil, 3, driftRows(2))
		eng.OnApply(Event{SpaceId: "test", ObjectId: "obj1", Dataset: "chat",
			Records: []EventRecord{{Id: "r1", Deleted: true}}}, nil)
		ev, err := waitOne(t, sub)
		require.NoError(t, err)
		assert.Equal(t, []string{"r1"}, removedIdsOf(ev.Removed))
		assert.NoError(t, sub.Err())
	})
	t.Run("refill fails", func(t *testing.T) {
		eng := New("test")
		defer eng.Close()
		m := newStoreModel(t, nil, 3, driftRows(6)...)
		sub, _ := subscribeModel(t, eng, m, 30)
		m.fail = errors.New("busy")
		fireUnreadable(eng, "obj1", "chat", "r1")
		assert.ErrorIs(t, sub.Err(), space.ErrSubscriptionDrifted)
	})
}

// An apply that mixes readable and unreadable records settles as a
// whole: the readable records' transitions and the refill's fold into
// one event that moves the client from the view before the apply to
// the view after it.
func TestUnreadable_MixedApplySettlesAsOne(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 3, driftRows(6)...)
	sub, view := subscribeModel(t, eng, m, 30)

	// r0 deleted (readable), r2 rewritten (unreadable), in one change.
	m.delete("r0")
	edited := makeRow(arena, "r2", 2, map[string]any{"edited": true})
	m.set(edited)
	ev := Event{SpaceId: "test", ObjectId: "obj1", Dataset: "chat", VersionId: "v",
		Records: []EventRecord{{Id: "r0", Deleted: true}, {Id: "r2"}}}
	eng.OnApply(ev, func(i int) (*anyenc.Value, error) {
		if i == 1 {
			return nil, errors.New("read failed")
		}
		return nil, nil
	})
	evs := sub.Events().GetAll()
	require.Len(t, evs, 1)
	assert.Equal(t, []string{"r3"}, idsOf(evs[0].Added))
	assert.Equal(t, []string{"r2"}, idsOf(evs[0].Updated))
	assert.Equal(t, []space.RemovedRecord{{Id: "r0", Reason: space.RemoveDeleted}}, evs[0].Removed)
	view.apply(evs)
	agrees(t, sub, view, m)
	assert.NoError(t, sub.Err())
}

// A complete window never refills, and a snapshot read that returns
// at most limit rows after a refill marks the window complete.
func TestRefill_CompletenessFollowsTheRead(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 3, driftRows(4)...)
	sub, view := subscribeModel(t, eng, m, 30)
	require.False(t, sub.complete)

	m.delete("r0")
	fireEvent(eng, "obj1", "chat", "r0", nil, true, nil)
	agrees(t, sub, view, m)
	assert.True(t, sub.complete)
	assert.Equal(t, 1, m.reads)

	m.delete("r1")
	fireEvent(eng, "obj1", "chat", "r1", nil, true, nil)
	agrees(t, sub, view, m)
	assert.Equal(t, 1, m.reads, "a complete window reads nothing")

	// Arrivals that overflow the window end completeness; the next
	// departure refills.
	for i := 0; i < 3; i++ {
		r := makeRow(arena, "x"+itoa(i), 10+i, nil)
		m.set(r)
		fireEvent(eng, "obj1", "chat", r.id, r.d, false, nil)
	}
	agrees(t, sub, view, m)
	assert.False(t, sub.complete)
	m.delete("r2")
	fireEvent(eng, "obj1", "chat", "r2", nil, true, nil)
	agrees(t, sub, view, m)
	assert.Equal(t, 2, m.reads)
}

// A sentinel whose own key grows is re-ranked: matches past the edge
// may now sort below it, and the next own move of a visible row must
// not keep that row in view ahead of them.
func TestRefill_SentinelOwnKeyGrowthIsRanked(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 2, driftRows(6)...)
	sub, view := subscribeModel(t, eng, m, 30)

	grown := makeRow(arena, "r2", 10, nil) // the sentinel
	m.set(grown)
	fireEvent(eng, "obj1", "chat", "r2", grown.d, false, nil)
	agrees(t, sub, view, m)
	assert.Equal(t, 1, m.reads, "the sentinel's growth is ranked at once")
	assert.Equal(t, "r3", sub.sentinel().id)

	moved := makeRow(arena, "r1", 6, nil)
	m.set(moved)
	fireEvent(eng, "obj1", "chat", "r1", moved.d, false, nil)
	agrees(t, sub, view, m)
	assert.Equal(t, []string{"r0", "r3"}, view.ids(m))
	assert.NoError(t, sub.Err())
}

// A refill that fails on an unranked sentinel with no departure still
// costs a loss: the window cannot be trusted, and repeated failures
// close it.
func TestRefill_FailedRankingIsALoss(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 2, driftRows(6)...)
	sub, _ := subscribeModel(t, eng, m, 100)
	m.fail = errors.New("busy")

	grown := makeRow(arena, "r2", 10, nil)
	m.set(grown)
	fireEvent(eng, "obj1", "chat", "r2", grown.d, false, nil)
	drain(t, sub)
	assert.Equal(t, 1, sub.lost)
	assert.False(t, sub.unranked, "not retried on every apply")
	assert.NoError(t, sub.Err())
}

// A row the window held past the edge that a record moved far beyond
// it leaves as displaced when the refill no longer returns it: its
// last known position is past the refilled edge.
func TestRefill_MovedRowAbsentFromRefillIsDisplaced(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 2, driftRows(6)...)
	sub, view := subscribeModel(t, eng, m, 30)

	moved := makeRow(arena, "r0", 100, nil)
	m.set(moved)
	fireEvent(eng, "obj1", "chat", "r0", moved.d, false, nil)
	evs := sub.Events().GetAll()
	require.Len(t, evs, 1)
	assert.Equal(t, []string{"r2"}, idsOf(evs[0].Added))
	assert.Equal(t, []space.RemovedRecord{{Id: "r0", Reason: space.RemoveDisplaced}}, evs[0].Removed)
	view.apply(evs)
	agrees(t, sub, view, m)
}

// A window past an offset counts departures only: arrivals below
// capacity and own sort moves are not losses there, since no refill
// can rank them.
func TestRefill_OffsetWindowArrivalsAreNotLosses(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 3, driftRows(3)...)
	sub, err := eng.Subscribe(SubConfig{
		Scope:  driftScope,
		Sort:   m.sort,
		Limit:  3,
		Offset: 1,
		Refill: m.refill(),
	}, snapshotFromRows(m.matching()[1:]))
	require.NoError(t, err)
	require.False(t, sub.complete)

	fireEvent(eng, "obj1", "chat", "a", makeRow(arena, "a", 10, nil).d, false, nil)
	fireEvent(eng, "obj1", "chat", "a", makeRow(arena, "a", 20, nil).d, false, nil)
	drain(t, sub)
	assert.Equal(t, 0, sub.lost)
	assert.Equal(t, 0, m.reads)
	assert.NoError(t, sub.Err())

	fireEvent(eng, "obj1", "chat", "r1", nil, true, nil)
	drain(t, sub)
	assert.Equal(t, 1, sub.lost)
	assert.ErrorIs(t, sub.Err(), space.ErrSubscriptionDrifted)
}

// A row promoted from sentinel and then removed within one apply ships
// nothing: it was never in the client's view. A purge of two held rows
// where one is the sentinel removes only the visible one.
func TestApply_PromotedThenRemovedInOneApply(t *testing.T) {
	t.Run("with refill", func(t *testing.T) {
		eng := New("test")
		defer eng.Close()
		m := newStoreModel(t, nil, 2, driftRows(6)...)
		sub, view := subscribeModel(t, eng, m, 30)

		m.delete("r1")
		m.delete("r2")
		eng.NotifyRecordsDeleted("test", "chat", "obj1", []string{"r1", "r2"})
		evs := sub.Events().GetAll()
		require.Len(t, evs, 1)
		assert.Equal(t, []string{"r3"}, idsOf(evs[0].Added))
		assert.Equal(t, []space.RemovedRecord{{Id: "r1", Reason: space.RemoveDeleted}}, evs[0].Removed)
		view.apply(evs)
		agrees(t, sub, view, m)
	})
	t.Run("without refill", func(t *testing.T) {
		eng := New("test")
		defer eng.Close()
		sub := subscribeSorted(t, eng, driftScope, nil, 2, driftRows(3))
		eng.NotifyRecordsDeleted("test", "chat", "obj1", []string{"r1", "r2"})
		evs := sub.Events().GetAll()
		require.Len(t, evs, 1)
		assert.Empty(t, evs[0].Added)
		assert.Equal(t, []space.RemovedRecord{{Id: "r1", Reason: space.RemoveDeleted}}, evs[0].Removed)
	})
}

// Several records of one apply that touch the sentinel slot ship the
// net transition per row: a sentinel updated in place and promoted by
// a later delete is added, a visible row demoted by a later arrival is
// displaced, a visible row that moves past the edge and is promoted
// back by a later delete is updated.
func TestApply_ImplicitTransitionsWithinOneApply(t *testing.T) {
	arena := newArena()
	fullComplete := func(t *testing.T, eng *Engine) *Sub {
		// Limit 2 over r0, r1; r2 arrives: at capacity and complete.
		sub := subscribeSorted(t, eng, driftScope, nil, 2, driftRows(2))
		fireEvent(eng, "obj1", "chat", "r2", makeRow(arena, "r2", 2, nil).d, false, nil)
		drain(t, sub)
		require.True(t, sub.complete)
		require.Equal(t, "r2", sub.sentinel().id)
		return sub
	}
	t.Run("sentinel updated then promoted", func(t *testing.T) {
		eng := New("test")
		defer eng.Close()
		sub := fullComplete(t, eng)
		edited := makeRow(arena, "r2", 2, map[string]any{"edited": true})
		fireRecords(eng, "obj1", "chat",
			[]EventRecord{{Id: "r2"}, {Id: "r0", Deleted: true}},
			[]*anyenc.Value{edited.d, nil})
		ev, err := waitOne(t, sub)
		require.NoError(t, err)
		assert.Equal(t, []string{"r2"}, idsOf(ev.Added))
		assert.True(t, ev.Added[0].Doc.GetBool("edited"))
		assert.Empty(t, ev.Updated)
		assert.Equal(t, []space.RemovedRecord{{Id: "r0", Reason: space.RemoveDeleted}}, ev.Removed)
	})
	t.Run("visible row updated then demoted", func(t *testing.T) {
		eng := New("test")
		defer eng.Close()
		sub := subscribeSorted(t, eng, driftScope, nil, 2, driftRows(3))
		fireRecords(eng, "obj1", "chat",
			[]EventRecord{{Id: "r1"}, {Id: "w"}},
			[]*anyenc.Value{makeRow(arena, "r1", 1, map[string]any{"edited": true}).d, makeRow(arena, "w", -1, nil).d})
		ev, err := waitOne(t, sub)
		require.NoError(t, err)
		assert.Equal(t, []string{"w"}, idsOf(ev.Added))
		assert.Empty(t, ev.Updated)
		assert.Equal(t, []space.RemovedRecord{{Id: "r1", Reason: space.RemoveDisplaced}}, ev.Removed)
	})
	t.Run("own move past the edge then promoted back", func(t *testing.T) {
		eng := New("test")
		defer eng.Close()
		sub := fullComplete(t, eng)
		fireRecords(eng, "obj1", "chat",
			[]EventRecord{{Id: "r0"}, {Id: "r1", Deleted: true}},
			[]*anyenc.Value{makeRow(arena, "r0", 10, nil).d, nil})
		ev, err := waitOne(t, sub)
		require.NoError(t, err)
		assert.Equal(t, []string{"r2"}, idsOf(ev.Added))
		assert.Equal(t, []string{"r0"}, idsOf(ev.Updated))
		assert.Equal(t, 10, ev.Updated[0].Doc.GetInt("n"))
		assert.Equal(t, []space.RemovedRecord{{Id: "r1", Reason: space.RemoveDeleted}}, ev.Removed)
	})
}

// Two records of one apply on the same id ship one record whose ops
// are both records' ops in order.
func TestApply_RepeatedIdConcatenatesOps(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	sub := subscribeSorted(t, eng, driftScope, nil, 3, driftRows(2))
	op := func(path string) []space.EventOp { return []space.EventOp{{Type: crdt.OpSet, Path: []string{path}}} }
	fireRecords(eng, "obj1", "chat",
		[]EventRecord{{Id: "r0", Ops: op("a")}, {Id: "r0", Ops: op("b")}},
		[]*anyenc.Value{makeRow(arena, "r0", 0, nil).d, makeRow(arena, "r0", 0, map[string]any{"b": 1}).d})
	ev, err := waitOne(t, sub)
	require.NoError(t, err)
	require.Len(t, ev.Updated, 1)
	assert.Equal(t, []string{"a", "b"}, []string{ev.Updated[0].Ops[0].Path[0], ev.Updated[0].Ops[1].Path[0]})
	assert.Equal(t, 1, ev.Updated[0].Doc.GetInt("b"))
}

// An unbounded sub settles an unreadable row by re-reading its
// membership: rows storage no longer returns leave, rows it returns
// enter, and the unreadable row ships its stored value.
func TestUnreadable_UnboundedRefillsMembership(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 0, driftRows(3)...)
	sub, err := eng.Subscribe(SubConfig{Scope: driftScope, Refill: m.refill()}, snapshotFromRows(m.matching()))
	require.NoError(t, err)

	m.delete("r1")                     // without an event
	m.set(makeRow(arena, "z", 9, nil)) // without an event
	m.set(makeRow(arena, "r2", 2, map[string]any{"edited": true}))
	fireUnreadable(eng, "obj1", "chat", "r2")
	ev, err := waitOne(t, sub)
	require.NoError(t, err)
	assert.Equal(t, []string{"z"}, idsOf(ev.Added))
	assert.Equal(t, []string{"r2"}, idsOf(ev.Updated))
	assert.True(t, ev.Updated[0].Doc.GetBool("edited"))
	assert.Nil(t, ev.Updated[0].Ops)
	assert.Equal(t, []string{"r1"}, removedIdsOf(ev.Removed))
	assert.NoError(t, sub.Err())
	assert.Len(t, sub.entries, 3)
	for _, e := range sub.entries {
		assert.Nil(t, e.raw, "an unbounded sub keeps no values")
	}
}

// A refill whose read returns a row without an id fills the window as
// far as it can and marks it complete, rather than refilling on every
// following apply.
func TestRefill_RowWithoutIdDoesNotLoop(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	m := newStoreModel(t, nil, 2, driftRows(5)...)
	reads := 0
	sub, err := eng.Subscribe(SubConfig{
		Scope: driftScope, Sort: m.sort, Limit: 2, MailboxCap: minMailboxCap, DriftBudget: 30,
		Refill: func(yield func(id string, doc *anyenc.Value)) error {
			reads++
			rows := m.window()
			yield(rows[0].id, rows[0].d)
			yield(rows[1].id, rows[1].d)
			yield("", nil)
			return nil
		},
	}, snapshotFromRows(m.window()))
	require.NoError(t, err)

	m.delete("r0")
	fireEvent(eng, "obj1", "chat", "r0", nil, true, nil)
	drain(t, sub)
	assert.Equal(t, 1, reads)
	assert.True(t, sub.complete)
	assert.Len(t, sub.entries, 2)

	for i := 0; i < 3; i++ {
		fireEvent(eng, "obj1", "chat", "r1", m.rows["r1"], false, nil)
	}
	drain(t, sub)
	assert.Equal(t, 1, reads, "a complete window reads nothing")
	assert.NoError(t, sub.Err())
}
