package subscribe

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// sameWindow asserts the client, the engine and a fresh query show the
// same rows.
func sameWindow(t *testing.T, sub *Sub, view *clientView, m *storeModel) {
	t.Helper()
	view.fold(t, sub)
	require.Equal(t, sortedCopy(m.visible()), sortedCopy(view.ids(m)), "client view vs fresh query")
	agreesWithEngine(t, sub, view, m)
}

// A sort-key tie across the window's edge: the engine hides the row the
// snapshot returned last, the one Initial leaves out, in whichever
// order storage returns ties.
func TestTie_EdgeHidesTheRowStorageReturnedLast(t *testing.T) {
	for _, tieDesc := range []bool{false, true} {
		eng := New("test")
		arena := newArena()
		m := newStoreModel(t, nil, 2, makeRow(arena, "a", 1, nil), makeRow(arena, "b", 2, nil), makeRow(arena, "c", 2, nil))
		m.tieDesc = tieDesc
		sub, view := subscribeModel(t, eng, m, 30)
		window := m.window()
		shown, hidden := window[1].id, window[2].id
		require.Equal(t, hidden, sub.sentinel().id, "tieDesc=%v", tieDesc)

		// The hidden row changes: nothing the consumer holds did.
		upd := makeRow(arena, hidden, 2, map[string]any{"x": 1})
		m.set(upd)
		fireEvent(eng, "obj1", "chat", hidden, upd.d, false, nil)
		waitNone(t, sub)

		// The shown tied row leaves: the hidden one takes its place.
		m.delete(shown)
		fireEvent(eng, "obj1", "chat", shown, nil, true, nil)
		own, _ := ownAndResync(t, sub)
		assert.Equal(t, []space.RemovedRecord{{Id: shown, Reason: space.RemoveDeleted}}, own.Removed)
		assert.Equal(t, []string{hidden}, idsOf(own.Added))
		view.apply([]space.SubscriptionEvent{own})
		sameWindow(t, sub, view, m)
		assert.NoError(t, sub.Err())
		assert.Equal(t, 0, sub.lost)
		eng.Close()
	}
}

// A boundary recomputed over tied rows picks the same row every time:
// the last placed.
func TestTie_RecomputedSentinelIsTheLastPlaced(t *testing.T) {
	for i := 0; i < 50; i++ {
		eng := New("test")
		arena := newArena()
		sub := subscribeSorted(t, eng, driftScope, nil, 3, []row{
			makeRow(arena, "a", 1, nil), makeRow(arena, "b", 2, nil),
			makeRow(arena, "c", 2, nil), makeRow(arena, "d", 2, nil),
		})
		// The sentinel moves into view; b and c tie for its slot.
		fireEvent(eng, "obj1", "chat", "d", makeRow(arena, "d", 0, nil).d, false, nil)
		ev, err := waitOne(t, sub)
		require.NoError(t, err)
		assert.Equal(t, []string{"d"}, idsOf(ev.Added))
		assert.Equal(t, []space.RemovedRecord{{Id: "c", Reason: space.RemoveDisplaced}}, ev.Removed)
		eng.Close()
	}
}

// A refill places tied rows in the order its read returned them, also
// when that order differs from the one the window held them in.
func TestTie_RefillTakesTheReadsOrder(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 2,
		makeRow(arena, "a", 1, nil), makeRow(arena, "b", 2, nil),
		makeRow(arena, "c", 2, nil), makeRow(arena, "d", 2, nil))
	sub, view := subscribeModel(t, eng, m, 30)
	require.Equal(t, "c", sub.sentinel().id)

	m.tieDesc = true
	m.delete("a")
	fireEvent(eng, "obj1", "chat", "a", nil, true, nil)
	sameWindow(t, sub, view, m)
	assert.Equal(t, []string{"c", "d"}, sortedCopy(view.ids(m)))
	assert.Equal(t, "b", sub.sentinel().id, "the read returned d, c, b")
	assert.NoError(t, sub.Err())
}

// An arrival that ties with the sentinel sorts after it: the window
// keeps its rows.
func TestTie_ArrivalAtTheEdgeStaysOut(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	m := newStoreModel(t, nil, 2, makeRow(arena, "a", 1, nil), makeRow(arena, "b", 2, nil), makeRow(arena, "c", 2, nil))
	sub, view := subscribeModel(t, eng, m, 30)

	r := makeRow(arena, "aa", 2, nil)
	m.set(r)
	fireEvent(eng, "obj1", "chat", "aa", r.d, false, nil)
	waitNone(t, sub)
	agreesWithEngine(t, sub, view, m)
	assert.Equal(t, "c", sub.sentinel().id)
}
