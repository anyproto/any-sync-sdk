package subscribe

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
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
	reads  int // refills opened
	scans  int // window reads
	closed int
	fail   error
	buf    *syncpool.DocBuffer

	sources    []any            // Event.Source of each refill
	lookups    []string         // ids the refills looked up
	lookupFail map[string]error // ids whose lookup fails
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
	if m.limit > 0 && len(rows)-1 > m.limit {
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

// refill is the model's RefillFn: the window read, and a lookup of
// any stored row by id. fail makes every read fail; lookups records
// the ids looked up.
func (m *storeModel) refill() RefillFn {
	return func(source any) (RefillRead, error) {
		m.reads++
		m.sources = append(m.sources, source)
		if m.fail != nil {
			return RefillRead{}, m.fail
		}
		return RefillRead{
			Rows: func(yield func(id string, doc *anyenc.Value)) error {
				m.scans++
				for _, r := range m.window() {
					yield(r.id, r.d)
				}
				return nil
			},
			Lookup: func(id string) (*anyenc.Value, error) {
				m.lookups = append(m.lookups, id)
				if err := m.lookupFail[id]; err != nil {
					return nil, err
				}
				return m.rows[id], nil
			},
			Close: func() { m.closed++ },
		}, nil
	}
}

// rowsOnly wraps a window read as a RefillFn with no lookup.
func rowsOnly(rows func(yield func(id string, doc *anyenc.Value)) error) RefillFn {
	return func(any) (RefillRead, error) {
		return RefillRead{Rows: rows, Close: func() {}}, nil
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
		v.foldChecked(t, ev)
	}
}

// foldChecked folds one event and asserts the client's contract: an id
// at most once per event, Added only for ids not shown, Updated and
// Removed only for ids shown, a doc on every Added and Updated.
func (v *clientView) foldChecked(t *testing.T, ev space.SubscriptionEvent) {
	t.Helper()
	seen := map[string]struct{}{}
	once := func(id string) {
		_, dup := seen[id]
		require.False(t, dup, "%s twice in one event", id)
		seen[id] = struct{}{}
	}
	for _, r := range ev.Added {
		once(r.Id)
		_, shown := v.docs[r.Id]
		require.False(t, shown, "added %s is already in view", r.Id)
		require.NotNil(t, r.Doc, "added %s ships a doc", r.Id)
		v.docs[r.Id] = r.Doc
	}
	for _, r := range ev.Updated {
		once(r.Id)
		_, shown := v.docs[r.Id]
		require.True(t, shown, "updated %s is in view", r.Id)
		require.NotNil(t, r.Doc, "updated %s ships a doc", r.Id)
		v.docs[r.Id] = r.Doc
	}
	for _, r := range ev.Removed {
		once(r.Id)
		_, shown := v.docs[r.Id]
		require.True(t, shown, "removed %s was in view", r.Id)
		delete(v.docs, r.Id)
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
	ids := make([]string, 0, len(v.docs))
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

// ownAndResync drains the sub after one apply with a VersionId that
// refilled: the change's own event, then the refill's. Either may be
// empty when the apply had nothing of that kind to say; the refill's
// never carries a VersionId or ops.
func ownAndResync(t *testing.T, sub *Sub) (own, resync space.SubscriptionEvent) {
	t.Helper()
	evs := sub.Events().GetAll()
	require.LessOrEqual(t, len(evs), 2, "%v", evs)
	for _, ev := range evs {
		if ev.VersionId != "" {
			own = ev
			continue
		}
		resync = ev
		for _, r := range append(ev.Added, ev.Updated...) {
			assert.Nil(t, r.Ops, "resync ships no ops")
		}
	}
	return own, resync
}

// agrees asserts the client's view equals a fresh query of the window
// and the engine's view equals the client's.
func agrees(t *testing.T, sub *Sub, view *clientView, m *storeModel) {
	t.Helper()
	view.fold(t, sub)
	require.Equal(t, m.visible(), view.ids(m), "client view vs fresh query")
	agreesWithEngine(t, sub, view, m)
	for id, d := range view.docs {
		require.Equal(t, string(m.rows[id].MarshalTo(nil)), string(d.MarshalTo(nil)), "doc of %s", id)
	}
}

// agreesWithEngine asserts the engine's visible rows are the client's:
// what holds whether or not refills work.
func agreesWithEngine(t *testing.T, sub *Sub, view *clientView, m *storeModel) {
	t.Helper()
	held := make([]string, 0, len(sub.entries))
	sent := sub.sentinel()
	for id, e := range sub.entries {
		if e != sent {
			held = append(held, id)
		}
	}
	sort.Strings(held)
	want := make([]string, 0, len(view.docs))
	for id := range view.docs {
		want = append(want, id)
	}
	sort.Strings(want)
	require.Equal(t, want, held, "engine view vs client view")
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
	own, resync := ownAndResync(t, sub)
	// The change's own event: its delete and the sentinel it promoted.
	assert.Equal(t, crdt.VersionId("vr0"), own.VersionId)
	assert.Equal(t, []string{"r3"}, idsOf(own.Added))
	assert.Equal(t, []space.RemovedRecord{{Id: "r0", Reason: space.RemoveDeleted}}, own.Removed)
	// The refill's: what storage held that the change did not say.
	assert.Equal(t, []string{"r4"}, idsOf(resync.Added))
	assert.Equal(t, []string{"r1"}, idsOf(resync.Updated))
	assert.Equal(t, []space.RemovedRecord{{Id: "r2", Reason: space.RemoveDeleted}}, resync.Removed, "gone from storage")
	view.apply([]space.SubscriptionEvent{own, resync})
	agrees(t, sub, view, m)

	// The queued events arrive: nothing left to do.
	fireEvent(eng, "obj1", "chat", "r2", nil, true, nil)
	fireEvent(eng, "obj1", "chat", "r1", m.rows["r1"], false, nil)
	evs := sub.Events().GetAll()
	require.Len(t, evs, 1, "the update re-ships; the delete of an unheld row is silent")
	assert.Equal(t, []string{"r1"}, idsOf(evs[0].Updated))
	agrees(t, sub, view, m)
}

// A held row a refill no longer returns is classified by a lookup of
// storage in the same read: absent, it was deleted; stored but failing
// the filter, it stopped matching; stored and matching, it sorts past
// the window. The change that moved it, arriving later, finds the row
// unheld and ships nothing more.
func TestRefill_AbsentHeldRowReasonComesFromStorage(t *testing.T) {
	t.Run("deleted past the new edge", func(t *testing.T) {
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
		own, resync := ownAndResync(t, sub)
		assert.Equal(t, []string{"r2"}, idsOf(own.Added), "the sentinel the delete promoted")
		assert.Equal(t, []space.RemovedRecord{{Id: "r0", Reason: space.RemoveDeleted}}, own.Removed)
		assert.Equal(t, []string{"a", "b"}, idsOf(resync.Added))
		assert.ElementsMatch(t, []space.RemovedRecord{
			{Id: "r2", Reason: space.RemoveDisplaced},
			{Id: "r1", Reason: space.RemoveDeleted},
		}, resync.Removed)
		assert.ElementsMatch(t, []string{"r1", "r2"}, m.lookups, "the held rows the read did not return")
		view.apply([]space.SubscriptionEvent{own, resync})
		agrees(t, sub, view, m)

		fireEvent(eng, "obj1", "chat", "r1", nil, true, nil)
		assert.Empty(t, sub.Events().GetAll(), "the delete of an unheld row is silent")
	})
	t.Run("deleted inside the new edge", func(t *testing.T) {
		eng := New("test")
		defer eng.Close()
		m := newStoreModel(t, nil, 2, driftRows(6)...)
		sub, view := subscribeModel(t, eng, m, 30)

		m.delete("r1") // without an event

		m.delete("r0")
		fireEvent(eng, "obj1", "chat", "r0", nil, true, nil)
		own, resync := ownAndResync(t, sub)
		assert.Equal(t, []string{"r2"}, idsOf(own.Added))
		assert.Equal(t, []space.RemovedRecord{{Id: "r0", Reason: space.RemoveDeleted}}, own.Removed)
		assert.Equal(t, []string{"r3"}, idsOf(resync.Added))
		assert.Equal(t, []space.RemovedRecord{{Id: "r1", Reason: space.RemoveDeleted}}, resync.Removed)
		view.apply([]space.SubscriptionEvent{own, resync})
		agrees(t, sub, view, m)
	})
	t.Run("filtered out", func(t *testing.T) {
		eng := New("test")
		defer eng.Close()
		arena := newArena()
		m := newStoreModel(t, activeOnly, 2, driftRows(6)...)
		sub, view := subscribeModel(t, eng, m, 30)

		m.set(readRow(arena, "r1", 1)) // without an event

		m.delete("r0")
		fireEvent(eng, "obj1", "chat", "r0", nil, true, nil)
		own, resync := ownAndResync(t, sub)
		assert.Equal(t, []space.RemovedRecord{{Id: "r0", Reason: space.RemoveDeleted}}, own.Removed)
		assert.Equal(t, []string{"r3"}, idsOf(resync.Added))
		assert.Equal(t, []space.RemovedRecord{{Id: "r1", Reason: space.RemoveFilteredOut}}, resync.Removed)
		view.apply([]space.SubscriptionEvent{own, resync})
		agrees(t, sub, view, m)
	})
	t.Run("moved past the edge", func(t *testing.T) {
		eng := New("test")
		defer eng.Close()
		arena := newArena()
		m := newStoreModel(t, nil, 2, driftRows(6)...)
		sub, view := subscribeModel(t, eng, m, 30)

		m.set(activeRow(arena, "r1", 100)) // without an event

		m.delete("r0")
		fireEvent(eng, "obj1", "chat", "r0", nil, true, nil)
		own, resync := ownAndResync(t, sub)
		assert.Equal(t, []space.RemovedRecord{{Id: "r0", Reason: space.RemoveDeleted}}, own.Removed)
		assert.Equal(t, []string{"r3"}, idsOf(resync.Added))
		assert.Equal(t, []space.RemovedRecord{{Id: "r1", Reason: space.RemoveDisplaced}}, resync.Removed)
		view.apply([]space.SubscriptionEvent{own, resync})
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
	own, resync := ownAndResync(t, sub)
	assert.Equal(t, crdt.VersionId("v"), own.VersionId)
	assert.Equal(t, []string{"r3"}, idsOf(own.Added))
	assert.Equal(t, []space.RemovedRecord{{Id: "r0", Reason: space.RemoveDeleted}}, own.Removed)
	assert.Empty(t, own.Updated, "the unreadable record ships nothing of its own")
	assert.Equal(t, crdt.VersionId(""), resync.VersionId)
	assert.Equal(t, []string{"r2"}, idsOf(resync.Updated), "storage settles it")
	view.apply([]space.SubscriptionEvent{own, resync})
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

// A refill that fails on an unranked sentinel with no departure costs
// one loss: the window cannot be trusted. Later applies retry the
// refill at no further cost, and the one that succeeds settles the
// window.
func TestRefill_FailedRankingIsALoss(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 2, driftRows(6)...)
	sub, view := subscribeModel(t, eng, m, 100)
	m.fail = errors.New("busy")

	grown := makeRow(arena, "r2", 10, nil)
	m.set(grown)
	fireEvent(eng, "obj1", "chat", "r2", grown.d, false, nil)
	view.fold(t, sub)
	assert.Equal(t, 1, sub.lost)
	assert.Equal(t, 1, m.reads)
	assert.True(t, sub.unranked, "still to be ranked")

	edited := makeRow(arena, "r0", 0, map[string]any{"edited": true})
	m.set(edited)
	fireEvent(eng, "obj1", "chat", "r0", edited.d, false, nil)
	view.fold(t, sub)
	assert.Equal(t, 1, sub.lost, "a retry costs nothing")
	assert.Equal(t, 2, m.reads, "retried")
	assert.NoError(t, sub.Err())

	m.fail = nil
	fireEvent(eng, "obj1", "chat", "r0", edited.d, false, nil)
	agrees(t, sub, view, m)
	assert.Equal(t, "r3", sub.sentinel().id)
	assert.False(t, sub.unranked)
}

// A window below capacity after a failed refill retries on every apply
// and costs nothing more until a row leaves it: one departure is one
// loss however many applies follow.
func TestRefill_RetriesCostNoExtraLoss(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 10, driftRows(20)...)
	sub, view := subscribeModel(t, eng, m, 30) // three losses close
	m.fail = errors.New("busy")

	m.delete("r0")
	fireEvent(eng, "obj1", "chat", "r0", nil, true, nil)
	view.fold(t, sub)
	require.Equal(t, 1, sub.lost)

	for i := 0; i < 5; i++ {
		edited := makeRow(arena, "r5", 5, map[string]any{"edit": i})
		m.set(edited)
		fireEvent(eng, "obj1", "chat", "r5", edited.d, false, nil)
		view.fold(t, sub)
	}
	assert.Equal(t, 1, sub.lost)
	assert.Equal(t, 6, m.reads, "every apply retried")
	assert.NoError(t, sub.Err())

	m.fail = nil
	m.delete("r1")
	fireEvent(eng, "obj1", "chat", "r1", nil, true, nil)
	agrees(t, sub, view, m)
	assert.Equal(t, 1, sub.lost)
	assert.True(t, sub.atCapacity())
}

// A refill that fails after yielding rows changes nothing: the rows it
// yielded are not held, and the departure counts as a loss.
func TestRefill_FailureMidwayChangesNothing(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	m := newStoreModel(t, nil, 3, driftRows(8)...)
	reads := 0
	sub, err := eng.Subscribe(SubConfig{
		Scope: driftScope, Sort: m.sort, Limit: 3, MailboxCap: minMailboxCap, DriftBudget: 100,
		Refill: rowsOnly(func(yield func(id string, doc *anyenc.Value)) error {
			reads++
			for _, r := range m.window()[:2] {
				yield(r.id, r.d)
			}
			return errors.New("torn")
		}),
	}, snapshotFromRows(m.window()))
	require.NoError(t, err)
	held := func() []string {
		var ids []string
		for id := range sub.entries {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		return ids
	}

	m.delete("r0")
	fireEvent(eng, "obj1", "chat", "r0", nil, true, nil)
	ev, err := waitOne(t, sub)
	require.NoError(t, err)
	assert.Equal(t, []string{"r3"}, idsOf(ev.Added), "the promoted sentinel")
	assert.Equal(t, []string{"r0"}, removedIdsOf(ev.Removed))
	assert.Equal(t, []string{"r1", "r2", "r3"}, held())
	assert.Equal(t, 1, reads)
	assert.Equal(t, 1, sub.lost)
	assert.False(t, sub.complete)
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
		require.Len(t, evs, 2)
		assert.Empty(t, evs[0].Added, "r2 was promoted and deleted within the change")
		assert.Equal(t, []space.RemovedRecord{{Id: "r1", Reason: space.RemoveDeleted}}, evs[0].Removed)
		assert.Equal(t, []string{"r3"}, idsOf(evs[1].Added), "the refill's event")
		assert.Empty(t, evs[1].Removed)
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
		Refill: rowsOnly(func(yield func(id string, doc *anyenc.Value)) error {
			reads++
			rows := m.window()
			yield(rows[0].id, rows[0].d)
			yield(rows[1].id, rows[1].d)
			yield("", nil)
			return nil
		}),
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

// One apply with a readable and an unreadable record on the same
// visible id ships the readable record's value: the refill finds
// storage equal to what the record wrote, and the update still goes
// out, with the ops dropped.
func TestUnreadable_ReadableAndUnreadableRecordsOnOneId(t *testing.T) {
	for _, order := range []string{"readable first", "unreadable first"} {
		t.Run(order, func(t *testing.T) {
			eng := New("test")
			defer eng.Close()
			arena := newArena()
			m := newStoreModel(t, nil, 3, driftRows(6)...)
			sub, view := subscribeModel(t, eng, m, 30)

			edited := makeRow(arena, "r1", 1, map[string]any{"edited": true})
			m.set(edited)
			op := []space.EventOp{{Type: crdt.OpSet, Path: []string{"edited"}}}
			records := []EventRecord{{Id: "r1", Ops: op}, {Id: "r1", Ops: op}}
			readable := 0
			if order == "unreadable first" {
				readable = 1
			}
			eng.OnApply(Event{SpaceId: "test", ObjectId: "obj1", Dataset: "chat", Records: records},
				func(i int) (*anyenc.Value, error) {
					if i == readable {
						return edited.d, nil
					}
					return nil, errors.New("read failed")
				})
			ev, err := waitOne(t, sub)
			require.NoError(t, err)
			require.Equal(t, []string{"r1"}, idsOf(ev.Updated))
			assert.True(t, ev.Updated[0].Doc.GetBool("edited"))
			assert.Nil(t, ev.Updated[0].Ops)
			view.foldChecked(t, ev)
			agrees(t, sub, view, m)
		})
	}
}

// A visible row demoted to sentinel by one arrival and evicted by the
// next in the same apply still matches: displaced.
func TestApply_DemotedThenEvictedIsDisplaced(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	sub := subscribeSorted(t, eng, driftScope, nil, 2, driftRows(3))
	fireRecords(eng, "obj1", "chat",
		[]EventRecord{{Id: "n1"}, {Id: "n2"}},
		[]*anyenc.Value{makeRow(arena, "n1", -2, nil).d, makeRow(arena, "n2", -1, nil).d})
	ev, err := waitOne(t, sub)
	require.NoError(t, err)
	assert.Equal(t, []string{"n1", "n2"}, idsOf(ev.Added))
	reasons := map[string]space.RemoveReason{}
	for _, r := range ev.Removed {
		reasons[r.Id] = r.Reason
	}
	assert.Equal(t, map[string]space.RemoveReason{"r0": space.RemoveDisplaced, "r1": space.RemoveDisplaced}, reasons)
}

// A row that moved past the edge, was evicted, and moved again in the
// same apply is fast-rejected on its last record; it still matches, so
// it leaves as displaced, not filtered out.
func TestApply_FastRejectedAfterVisibleIsDisplaced(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	sub := subscribeSorted(t, eng, driftScope, nil, 2, driftRows(3))
	fireRecords(eng, "obj1", "chat",
		[]EventRecord{{Id: "r1"}, {Id: "new"}, {Id: "r1"}},
		[]*anyenc.Value{makeRow(arena, "r1", 10, nil).d, makeRow(arena, "new", -1, nil).d, makeRow(arena, "r1", 11, nil).d})
	ev, err := waitOne(t, sub)
	require.NoError(t, err)
	assert.Equal(t, []string{"new"}, idsOf(ev.Added))
	assert.Equal(t, []space.RemovedRecord{{Id: "r1", Reason: space.RemoveDisplaced}}, ev.Removed)
}

// Random applies over windows of every size, with repeated ids,
// creates, edits, sort moves, filter-outs, deletes and unreadable
// records, in four regimes: storage in step with the events; storage
// a batch ahead of them, as when a later change commits before an
// earlier one's event runs; refills that fail for a while; and churn,
// small windows under long changes of mostly moves. After
// every apply the client's folded view and the engine's held set are
// compared, and both against a fresh query of the model whenever
// refills work or the engine certifies its window; every event is
// checked against the client's contract and each removal's reason
// against storage, and no loss is counted while refills work.
func TestRefill_RandomizedAgreesWithStorage(t *testing.T) {
	for _, regime := range []string{"in step", "storage ahead", "refills failing", "churn"} {
		t.Run(regime, func(t *testing.T) {
			for seed := int64(0); seed < 1000; seed++ {
				t.Run(fmt.Sprint(seed), func(t *testing.T) {
					randomizedRun(t, regime, seed)
				})
			}
		})
	}
}

func randomizedRun(t *testing.T, regime string, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	churn := regime == "churn"
	limit := rng.Intn(5)
	if churn {
		limit = 1 + rng.Intn(3)
	}
	var filter query.Filter
	if rng.Intn(2) == 0 {
		filter = activeOnly
	}
	// Sort keys are unique: ties at the edge are a known limit.
	usedN := map[int]struct{}{}
	freshN := func() int {
		for {
			n := rng.Intn(100000)
			if _, used := usedN[n]; !used {
				usedN[n] = struct{}{}
				return n
			}
		}
	}
	status := func() string {
		if rng.Intn(4) == 0 {
			return "read"
		}
		return "active"
	}
	var rows []row
	initial := 3 + rng.Intn(8)
	if churn {
		initial = 8 + rng.Intn(7)
	}
	for i := 0; i < initial; i++ {
		rows = append(rows, makeRow(arena, "r"+itoa(i), freshN(), map[string]any{"status": status()}))
	}
	m := newStoreModel(t, filter, limit, rows...)
	sub, view := subscribeModel(t, eng, m, 100)
	known := func() []string {
		ids := make([]string, 0, len(m.rows))
		for id := range m.rows {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		return ids
	}

	// batch builds one apply: it moves the model and returns the event
	// with its records' readability.
	type batch struct {
		ev         Event
		unreadable []bool
		after      map[string]*anyenc.Value // the model after this batch
	}
	build := func(step int, tag string) batch {
		var b batch
		count := 1 + rng.Intn(6)
		if churn {
			count = 3 + rng.Intn(6)
		}
		for j := 0; j < count; j++ {
			ids := known()
			var id string
			if len(ids) == 0 || (!churn && rng.Intn(4) == 0) || (churn && rng.Intn(7) == 0) {
				id = fmt.Sprintf("x%d%s_%d", step, tag, j)
			} else {
				id = ids[rng.Intn(len(ids))]
			}
			del := rng.Intn(5) == 0
			if churn {
				del = rng.Intn(100) < 15
			}
			switch {
			case del:
				m.delete(id)
				b.ev.Records = append(b.ev.Records, EventRecord{Id: id, Deleted: true})
				b.unreadable = append(b.unreadable, false)
			default:
				n := freshN()
				if cur, ok := m.rows[id]; ok && !churn && rng.Intn(2) == 0 {
					n = cur.GetInt("n")
				}
				m.set(makeRow(arena, id, n, map[string]any{"status": status()}))
				b.ev.Records = append(b.ev.Records, EventRecord{Id: id})
				b.unreadable = append(b.unreadable, rng.Intn(6) == 0)
			}
		}
		b.ev.SpaceId, b.ev.ObjectId, b.ev.Dataset = "test", "obj1", "chat"
		b.ev.VersionId = crdt.VersionId(fmt.Sprintf("v%d", step))
		b.after = make(map[string]*anyenc.Value, len(m.rows))
		for id, d := range m.rows {
			b.after[id] = d
		}
		return b
	}
	fire := func(b batch) {
		eng.OnApply(b.ev, func(i int) (*anyenc.Value, error) {
			if b.unreadable[i] {
				return nil, errors.New("read failed")
			}
			return m.rows[b.ev.Records[i].Id], nil
		})
	}
	matches := func(d *anyenc.Value) bool {
		return d != nil && (m.filter == nil || m.filter.Ok(d, m.buf))
	}
	// check folds the events of one apply and verifies each against the
	// client's contract and against storage: a deleted row is absent
	// from storage now; a row filtered out failed the filter after the
	// change that removed it or fails it now; a displaced row matched
	// then or matches now. The last two hold only while refills work:
	// a window that cannot re-read reports a row it lost sight of as
	// displaced.
	check := func(step int, b batch) {
		refills := m.fail == nil
		evs := sub.Events().GetAll()
		require.LessOrEqual(t, len(evs), 2, "step %d: %v", step, evs)
		for i, ev := range evs {
			if i == 0 && ev.VersionId != "" {
				require.Equal(t, b.ev.VersionId, ev.VersionId, "step %d", step)
			} else {
				require.Equal(t, crdt.VersionId(""), ev.VersionId, "step %d: the resync carries no version", step)
				for _, r := range append(ev.Added, ev.Updated...) {
					require.Nil(t, r.Ops, "step %d: the resync ships no ops", step)
				}
			}
			view.foldChecked(t, ev)
			for _, r := range ev.Removed {
				now, then := m.rows[r.Id], b.after[r.Id]
				switch r.Reason {
				case space.RemoveDeleted:
					require.Nil(t, now, "step %d: %s removed as deleted but stored", step, r.Id)
				case space.RemoveFilteredOut:
					require.True(t, (then != nil && !matches(then)) || (now != nil && !matches(now)),
						"step %d: %s removed as filtered out but matching", step, r.Id)
				default:
					require.NotNil(t, then, "step %d: %s removed as %s but never stored after its change", step, r.Id, r.Reason)
					if refills {
						require.True(t, matches(then) || matches(now), "step %d: %s removed as displaced but not matching", step, r.Id)
					}
				}
			}
		}
	}

	for step := 0; step < 25; step++ {
		switch regime {
		case "storage ahead":
			// Two changes commit, then their events run in order: the
			// first apply's refill sees the second's rows.
			a := build(step, "a")
			b := build(step, "b")
			b.ev.VersionId += "b"
			fire(a)
			check(step, a)
			require.NoError(t, sub.Err(), "step %d", step)
			fire(b)
			check(step, b)
		case "refills failing":
			if step%7 == 0 {
				m.fail = errors.New("busy")
			} else if step%7 == 4 {
				m.fail = nil
			}
			b := build(step, "")
			fire(b)
			check(step, b)
		default:
			b := build(step, "")
			fire(b)
			check(step, b)
		}
		if sub.Err() != nil {
			require.NotNil(t, m.fail, "step %d: closed while refills work", step)
			return
		}
		agreesWithEngine(t, sub, view, m)
		// A window the engine certifies (it holds every match, or is at
		// capacity with every row ranked) equals a fresh query whether
		// or not refills work.
		certified := sub.complete || (sub.atCapacity() && !sub.unranked)
		if m.fail == nil || certified {
			agrees(t, sub, view, m)
		}
		if regime != "refills failing" {
			assert.Equal(t, 0, sub.lost, "step %d: a loss while refills work", step)
		}
	}
}
