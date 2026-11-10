package subscribe

import (
	"math"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

var (
	driftScope = Scope{ObjectId: "obj1", Dataset: "chat"}
	activeOnly = query.Key{Path: []string{"status"}, Filter: query.NewComp(query.CompOpEq, "active")}
)

func driftRows(n int) []row {
	arena := newArena()
	rows := make([]row, n)
	for i := range rows {
		rows[i] = makeRow(arena, "r"+itoa(i), i, map[string]any{"status": "active"})
	}
	return rows
}

// drain discards every queued event. Events are queued synchronously
// by OnApply, so nothing is in flight.
func drain(t *testing.T, sub *Sub) {
	t.Helper()
	sub.Events().GetAll()
}

// A window whose snapshot holds every match stays open however many
// rows leave it, deletes and filter-outs alike, interleaved with
// arrivals that stay below capacity.
func TestDrift_CompleteWindowNeverDrifts(t *testing.T) {
	eng := New("test")
	defer eng.Close()

	sub := subscribeSorted(t, eng, driftScope, activeOnly, 10, driftRows(5))
	require.True(t, sub.complete)

	arena := newArena()
	for i := 0; i < 50; i++ {
		id := "x" + itoa(i)
		fireEvent(eng, "obj1", "chat", id, makeRow(arena, id, 100+i, map[string]any{"status": "active"}).d, false, nil)
		if i%2 == 0 {
			fireEvent(eng, "obj1", "chat", id, nil, true, nil)
		} else {
			fireEvent(eng, "obj1", "chat", id, makeRow(arena, id, 100+i, map[string]any{"status": "read"}).d, false, nil)
		}
		drain(t, sub)
	}
	for _, r := range driftRows(5) {
		fireEvent(eng, "obj1", "chat", r.id, nil, true, nil)
	}
	drain(t, sub)

	assert.NoError(t, sub.Err())
	assert.True(t, sub.complete)
	assert.Equal(t, 0, sub.lost)
	assert.Empty(t, sub.entries)
}

// A complete window that fills to capacity still holds every match;
// the first arrival that evicts the sentinel leaves a match past the
// edge, and from then on departures count against the budget.
func TestDrift_EvictionPastEdgeEndsCompleteness(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	// Limit 3, budget 30: one loss closes the sub once incomplete.
	sub := subscribeSorted(t, eng, driftScope, nil, 3, driftRows(2))
	require.True(t, sub.complete)

	arena := newArena()
	fireEvent(eng, "obj1", "chat", "a", makeRow(arena, "a", 10, nil).d, false, nil)
	fireEvent(eng, "obj1", "chat", "b", makeRow(arena, "b", 11, nil).d, false, nil)
	drain(t, sub)
	require.Len(t, sub.entries, 4)
	assert.True(t, sub.complete, "a full window that holds every match is still complete")

	// Departure while complete: no loss; the window refills below capacity.
	fireEvent(eng, "obj1", "chat", "b", nil, true, nil)
	fireEvent(eng, "obj1", "chat", "b", makeRow(arena, "b", 11, nil).d, false, nil)
	drain(t, sub)
	require.NoError(t, sub.Err())
	require.Equal(t, 0, sub.lost)

	// Sorts inside the window at capacity: the sentinel b is evicted.
	fireEvent(eng, "obj1", "chat", "c", makeRow(arena, "c", 5, nil).d, false, nil)
	drain(t, sub)
	assert.False(t, sub.complete)

	fireEvent(eng, "obj1", "chat", "r0", nil, true, nil)
	drain(t, sub)
	assert.ErrorIs(t, sub.Err(), space.ErrSubscriptionDrifted)
}

// An arrival past the sentinel of a full window is fast-rejected. A
// non-match there leaves the window complete; a match is left past
// the edge and ends completeness.
func TestDrift_MatchPastFullWindowEndsCompleteness(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	sub := subscribeSorted(t, eng, driftScope, activeOnly, 2, driftRows(2))
	arena := newArena()
	fireEvent(eng, "obj1", "chat", "a", makeRow(arena, "a", 10, map[string]any{"status": "active"}).d, false, nil)
	drain(t, sub)
	require.Len(t, sub.entries, 3)
	require.True(t, sub.complete)

	fireEvent(eng, "obj1", "chat", "y", makeRow(arena, "y", 98, map[string]any{"status": "read"}).d, false, nil)
	assert.True(t, sub.complete, "a non-match past the edge")

	fireEvent(eng, "obj1", "chat", "z", makeRow(arena, "z", 99, map[string]any{"status": "active"}).d, false, nil)
	waitNone(t, sub)
	assert.False(t, sub.complete, "a match past the edge")

	fireEvent(eng, "obj1", "chat", "r0", nil, true, nil)
	drain(t, sub)
	assert.ErrorIs(t, sub.Err(), space.ErrSubscriptionDrifted)
}

// A snapshot that returned limit+1 rows, or that skipped rows by
// offset, leaves rows outside the window: departures count.
func TestDrift_IncompleteSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name   string
		rows   int
		offset int
	}{
		{"rows past the edge", 4, 0},
		{"offset", 2, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng := New("test")
			defer eng.Close()
			sub, err := eng.Subscribe(SubConfig{
				Scope:  driftScope,
				Sort:   mustParseSort(t, "n"),
				Limit:  3,
				Offset: tc.offset,
			}, snapshotFromRows(driftRows(tc.rows)))
			require.NoError(t, err)
			require.False(t, sub.complete)

			fireEvent(eng, "obj1", "chat", "r0", nil, true, nil)
			drain(t, sub)
			assert.ErrorIs(t, sub.Err(), space.ErrSubscriptionDrifted)
		})
	}
}

// In a window with rows past its edge, a held row the filter rejects
// counts as a loss just like a delete.
func TestDrift_FilterOutCountsInIncompleteWindow(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	sub := subscribeSorted(t, eng, driftScope, activeOnly, 3, driftRows(4))
	require.False(t, sub.complete)

	arena := newArena()
	fireEvent(eng, "obj1", "chat", "r0", makeRow(arena, "r0", 0, map[string]any{"status": "read"}).d, false, nil)
	drain(t, sub)
	assert.Equal(t, 1, sub.lost)
	assert.ErrorIs(t, sub.Err(), space.ErrSubscriptionDrifted)
}

// A row the snapshot read but the engine skipped (no id) takes no
// slot: a window that holds at most limit rows holds every match it
// can, the same rule a refill applies.
func TestDrift_SkippedSnapshotRowTakesNoSlot(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	rows := driftRows(3)
	sub, err := eng.Subscribe(SubConfig{
		Scope: driftScope,
		Sort:  mustParseSort(t, "n"),
		Limit: 2,
	}, func(yield func(id string, doc *anyenc.Value)) error {
		yield(rows[0].id, rows[0].d)
		yield("", nil)
		yield(rows[1].id, rows[1].d)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, sub.entries, 2)
	assert.True(t, sub.complete)
}

// A budget whose product with the limit overflows int disables the
// drift close instead of wrapping negative and closing on the first
// apply.
func TestDrift_BudgetOverflowSaturates(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	sub, err := eng.Subscribe(SubConfig{
		Scope:       driftScope,
		Sort:        mustParseSort(t, "n"),
		Limit:       10,
		DriftBudget: math.MaxInt / 3,
	}, snapshotFromRows(driftRows(11)))
	require.NoError(t, err)
	require.False(t, sub.complete)
	assert.Equal(t, math.MaxInt, sub.lostLimit)

	for _, r := range driftRows(11)[:5] {
		fireEvent(eng, "obj1", "chat", r.id, nil, true, nil)
	}
	drain(t, sub)
	assert.NoError(t, sub.Err())
	assert.Equal(t, 5, sub.lost)
}
