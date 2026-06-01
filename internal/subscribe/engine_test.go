package subscribe

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
	"github.com/cheggaaa/mb/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/space"
)

// ---------- helpers ----------

// row is a tiny test fixture: id + sort key (we sort by "n" ascending
// across tests) + arbitrary other fields.
type row struct {
	id string
	n  int
	d  *anyenc.Value
}

func newArena() *anyenc.Arena { return &anyenc.Arena{} }

func makeRow(arena *anyenc.Arena, id string, n int, extra map[string]any) row {
	obj := arena.NewObject()
	obj.Set("id", arena.NewString(id))
	obj.Set("n", arena.NewNumberInt(n))
	for k, v := range extra {
		switch x := v.(type) {
		case string:
			obj.Set(k, arena.NewString(x))
		case int:
			obj.Set(k, arena.NewNumberInt(x))
		case bool:
			if x {
				obj.Set(k, arena.NewTrue())
			} else {
				obj.Set(k, arena.NewFalse())
			}
		}
	}
	return row{id: id, n: n, d: obj}
}

// snapshotFromRows returns a SnapshotFn that yields the given rows in
// order — simulates the any-store query result the real path supplies.
func snapshotFromRows(rows []row) SnapshotFn {
	return func(yield func(id string, doc *anyenc.Value)) error {
		for _, r := range rows {
			yield(r.id, r.d)
		}
		return nil
	}
}

// subscribeSorted parses "n asc" + caller's filter, builds the sub.
func subscribeSorted(t *testing.T, eng *Engine, scope Scope, filter query.Filter, limit int, initial []row) *Sub {
	t.Helper()
	sort, err := query.ParseSort("n")
	require.NoError(t, err)
	cfg := SubConfig{
		Scope:       scope,
		Filter:      filter,
		Sort:        sort,
		Limit:       limit,
		MailboxCap:  minMailboxCap,
		DriftBudget: defaultDriftBudgetPercent,
	}
	sub, err := eng.Subscribe(cfg, snapshotFromRows(initial))
	require.NoError(t, err)
	return sub
}

// fireEvent invokes engine.OnApply with a single-record event built
// from the given (id, postDoc, deleted, ops, sourceObjectId).
func fireEvent(eng *Engine, objectId, dataset, id string, postDoc *anyenc.Value, deleted bool, ops []space.EventOp) {
	ev := Event{
		SpaceId:  "test",
		ObjectId: objectId,
		Dataset:  dataset,
		VersionId: crdt.VersionId("v" + id),
		Records: []EventRecord{{Id: id, Deleted: deleted, Ops: ops}},
	}
	postValue := func(i int) *anyenc.Value {
		if i == 0 {
			return postDoc
		}
		return nil
	}
	eng.OnApply(ev, postValue)
}

func waitOne(t *testing.T, sub *Sub) (space.SubscriptionEvent, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	return sub.Events().WaitOne(ctx)
}

func waitNone(t *testing.T, sub *Sub) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := sub.Events().WaitOne(ctx)
	if err == nil {
		t.Fatalf("unexpected event delivered: should have been silent")
	}
}

// idsOf collects ids from a SubRecord slice.
func idsOf(rs []space.SubRecord) []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.Id)
	}
	return out
}

// removedIdsOf extracts the ids from a Removed slice, for assertions
// that only care about which ids left (not the cause).
func removedIdsOf(rs []space.RemovedRecord) []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.Id)
	}
	return out
}

// ---------- tests ----------

// Insert into an empty window yields a single Added emit with the new
// doc + ops.
func TestApply_AddToEmptyWindowEmitsAdded(t *testing.T) {
	eng := New("test")
	defer eng.Close()

	arena := newArena()
	sub := subscribeSorted(t, eng, Scope{Shared: false, ObjectId: "obj1", Dataset: "chat"}, nil, 5, nil)

	r := makeRow(arena, "m1", 10, nil)
	fireEvent(eng, "obj1", "chat", "m1", r.d, false, []space.EventOp{{Type: crdt.OpSet, Path: []string{"text"}, Payload: arena.NewString("hi")}})

	ev, err := waitOne(t, sub)
	require.NoError(t, err)
	assert.Equal(t, []string{"m1"}, idsOf(ev.Added))
	assert.Empty(t, ev.Updated)
	assert.Empty(t, ev.Removed)
}

// Update of an already-held record emits Updated with the new doc.
func TestApply_UpdateHeldRecordEmitsUpdated(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	initial := []row{makeRow(arena, "m1", 10, nil)}
	sub := subscribeSorted(t, eng, Scope{ObjectId: "obj1", Dataset: "chat"}, nil, 5, initial)

	r2 := makeRow(arena, "m1", 10, map[string]any{"edit": "yes"})
	fireEvent(eng, "obj1", "chat", "m1", r2.d, false, nil)
	ev, err := waitOne(t, sub)
	require.NoError(t, err)
	assert.Empty(t, ev.Added)
	assert.Equal(t, []string{"m1"}, idsOf(ev.Updated))
	assert.Empty(t, ev.Removed)
}

// Update that flips a record out of the filter's match set emits a
// Removed (the record is still in the database, just no longer in the
// query's window). lost is incremented so accumulated filter-evictions
// can trip the drift bound.
func TestApply_UpdateOutOfFilterEmitsRemoved(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()

	// Filter: only records with status="active".
	filter, err := query.ParseCondition(map[string]any{"status": "active"})
	require.NoError(t, err)

	r1 := makeRow(arena, "m1", 10, map[string]any{"status": "active"})
	sub := subscribeSorted(t, eng, Scope{ObjectId: "obj1", Dataset: "chat"}, filter, 5, []row{r1})
	require.Equal(t, 1, len(sub.entries))
	require.Equal(t, 0, sub.lost)

	// Update m1 with status="archived" — no longer matches the filter.
	r2 := makeRow(arena, "m1", 10, map[string]any{"status": "archived"})
	fireEvent(eng, "obj1", "chat", "m1", r2.d, false, nil)

	ev, err := waitOne(t, sub)
	require.NoError(t, err)
	assert.Empty(t, ev.Added)
	assert.Empty(t, ev.Updated)
	assert.Equal(t, []space.RemovedRecord{{Id: "m1", Reason: space.RemoveFilteredOut}}, ev.Removed, "filter-rejected update must emit Removed with RemoveFilteredOut")

	// Engine state: entry dropped, lost bumped (counts toward drift).
	assert.Equal(t, 0, len(sub.entries), "entry must leave the held set when the filter rejects the post-apply doc")
	assert.Equal(t, 1, sub.lost, "filter-rejected updates count toward the drift budget")
}

// Same record updating BACK to a filter-matching state re-enters the
// window as Added — from the engine's view it's a fresh insert (the
// prior membership was already shed by the filter-rejecting update).
func TestApply_UpdateBackIntoFilterEmitsAdded(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()

	filter, err := query.ParseCondition(map[string]any{"status": "active"})
	require.NoError(t, err)

	r1 := makeRow(arena, "m1", 10, map[string]any{"status": "active"})
	sub := subscribeSorted(t, eng, Scope{ObjectId: "obj1", Dataset: "chat"}, filter, 5, []row{r1})

	// Out of filter.
	r2 := makeRow(arena, "m1", 10, map[string]any{"status": "archived"})
	fireEvent(eng, "obj1", "chat", "m1", r2.d, false, nil)
	ev1, err := waitOne(t, sub)
	require.NoError(t, err)
	assert.Equal(t, []space.RemovedRecord{{Id: "m1", Reason: space.RemoveFilteredOut}}, ev1.Removed)

	// Back into filter.
	r3 := makeRow(arena, "m1", 10, map[string]any{"status": "active"})
	fireEvent(eng, "obj1", "chat", "m1", r3.d, false, nil)
	ev2, err := waitOne(t, sub)
	require.NoError(t, err)
	assert.Equal(t, []string{"m1"}, idsOf(ev2.Added), "re-entry into the filter match set emits Added")
	assert.Empty(t, ev2.Removed)
}

// Delete of a held record emits Removed.
func TestApply_DeleteHeldRecordEmitsRemoved(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	initial := []row{makeRow(arena, "m1", 10, nil)}
	sub := subscribeSorted(t, eng, Scope{ObjectId: "obj1", Dataset: "chat"}, nil, 5, initial)

	fireEvent(eng, "obj1", "chat", "m1", nil, true, nil)
	ev, err := waitOne(t, sub)
	require.NoError(t, err)
	assert.Empty(t, ev.Added)
	assert.Empty(t, ev.Updated)
	assert.Equal(t, []space.RemovedRecord{{Id: "m1", Reason: space.RemoveDeleted}}, ev.Removed)
}

// Limit+1 sentinel: a new arrival with a smaller sort key than every
// held row enters the window; the sentinel (largest tuple, formerly
// invisible) demotes the previous bottom-visible row out of view.
func TestSentinel_DemotePreviousBottomVisible(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	// Limit=2, snapshot holds 3: a(n=1), b(n=2), c(n=3) — sentinel=c.
	initial := []row{
		makeRow(arena, "a", 1, nil),
		makeRow(arena, "b", 2, nil),
		makeRow(arena, "c", 3, nil),
	}
	sub := subscribeSorted(t, eng, Scope{ObjectId: "obj1", Dataset: "chat"}, nil, 2, initial)

	// Sentinel-bookkeeping invariant: held set is 3, sentinel = c, visible = {a, b}.
	require.Equal(t, 3, len(sub.entries))
	require.NotNil(t, sub.maxRef)
	assert.Equal(t, "c", sub.maxRef.id)

	// New row e with n=0 (smaller than all) → evict c (sentinel),
	// insert e. New maxRef = b (largest of {a, b, e} is b). New sentinel = b.
	// Client view shift: e Added (visible), b Removed (now sentinel).
	e := makeRow(arena, "e", 0, nil)
	fireEvent(eng, "obj1", "chat", "e", e.d, false, nil)

	ev, err := waitOne(t, sub)
	require.NoError(t, err)
	assert.Equal(t, []string{"e"}, idsOf(ev.Added))
	assert.Empty(t, ev.Updated)
	assert.Equal(t, []space.RemovedRecord{{Id: "b", Reason: space.RemoveDisplaced}}, ev.Removed)
}

// Sentinel promotion: delete a visible record; the sentinel (formerly
// invisible) promotes into the visible window, no any-store query
// needed.
func TestSentinel_PromoteOnVisibleDelete(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	initial := []row{
		makeRow(arena, "a", 1, nil),
		makeRow(arena, "b", 2, nil),
		makeRow(arena, "c", 3, nil), // sentinel
	}
	sub := subscribeSorted(t, eng, Scope{ObjectId: "obj1", Dataset: "chat"}, nil, 2, initial)

	// Delete a (visible).
	fireEvent(eng, "obj1", "chat", "a", nil, true, nil)
	ev, err := waitOne(t, sub)
	require.NoError(t, err)
	// Visibility transitions: a was visible → gone (Removed). c was
	// sentinel (not visible) → promoted to visible (Added).
	assert.ElementsMatch(t, []string{"c"}, idsOf(ev.Added))
	assert.Empty(t, ev.Updated)
	assert.ElementsMatch(t, []space.RemovedRecord{{Id: "a", Reason: space.RemoveDeleted}}, ev.Removed)
}

// The three RemoveReason causes are reported distinctly: a tombstone is
// RemoveDeleted, a filter-breaking update is RemoveFilteredOut, and a
// higher-priority arrival pushing a visible row to the sentinel is
// RemoveDisplaced. Clients branch on RemoveDeleted to tell "gone" from
// "left my window but still exists".
func TestApply_RemoveReasonsAreDistinct(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()

	filter, err := query.ParseCondition(map[string]any{"status": "active"})
	require.NoError(t, err)
	sort, err := query.ParseSort("n")
	require.NoError(t, err)

	// Limit=3, snapshot holds 4 active rows: a(1)..d(4) — sentinel=d,
	// visible {a,b,c}. DriftBudget is relaxed so the filter/delete losses
	// below don't trip drift before we observe all three reasons.
	initial := []row{
		makeRow(arena, "a", 1, map[string]any{"status": "active"}),
		makeRow(arena, "b", 2, map[string]any{"status": "active"}),
		makeRow(arena, "c", 3, map[string]any{"status": "active"}),
		makeRow(arena, "d", 4, map[string]any{"status": "active"}),
	}
	sub, err := eng.Subscribe(SubConfig{
		Scope:       Scope{ObjectId: "obj1", Dataset: "chat"},
		Filter:      filter,
		Sort:        sort,
		Limit:       3,
		MailboxCap:  minMailboxCap,
		DriftBudget: 100,
	}, snapshotFromRows(initial))
	require.NoError(t, err)

	// Displaced: z(0) arrives smaller than all → evicts sentinel d,
	// demotes c out of the visible window. c still matches the filter.
	z := makeRow(arena, "z", 0, map[string]any{"status": "active"})
	fireEvent(eng, "obj1", "chat", "z", z.d, false, nil)
	ev, err := waitOne(t, sub)
	require.NoError(t, err)
	assert.Equal(t, []space.RemovedRecord{{Id: "c", Reason: space.RemoveDisplaced}}, ev.Removed)

	// FilteredOut: a updates to status=archived → fails the filter, but
	// the record still exists in the database.
	aArchived := makeRow(arena, "a", 1, map[string]any{"status": "archived"})
	fireEvent(eng, "obj1", "chat", "a", aArchived.d, false, nil)
	ev, err = waitOne(t, sub)
	require.NoError(t, err)
	assert.Contains(t, ev.Removed, space.RemovedRecord{Id: "a", Reason: space.RemoveFilteredOut})

	// Deleted: tombstone z → gone from the database.
	fireEvent(eng, "obj1", "chat", "z", nil, true, nil)
	ev, err = waitOne(t, sub)
	require.NoError(t, err)
	assert.Contains(t, ev.Removed, space.RemovedRecord{Id: "z", Reason: space.RemoveDeleted})
}

// Drift: loss without replacement crosses the budget → sub closes
// with ErrSubscriptionDrifted.
func TestDrift_LossCrossesBudgetClosesWithDrifted(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	// Limit=10, default budget=30 → close after lost*100 >= 300 → lost >= 3.
	var initial []row
	for i := 0; i < 11; i++ {
		initial = append(initial, makeRow(arena, "m"+string(rune('a'+i)), i, nil))
	}
	sub := subscribeSorted(t, eng, Scope{ObjectId: "obj1", Dataset: "chat"}, nil, 10, initial)

	// Delete 3 visible records — budget reached on the 3rd.
	for _, id := range []string{"ma", "mb", "mc"} {
		fireEvent(eng, "obj1", "chat", id, nil, true, nil)
	}
	// Drain whatever events arrived; mailbox should be closed after the
	// third delete.
	_, _ = sub.Events().Wait(context.Background())
	// Closed mailbox: Wait returns ErrClosed; Err() carries Drifted.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := sub.Events().Wait(ctx)
	require.Error(t, err)
	assert.True(t, errors.Is(err, mb.ErrClosed))
	assert.True(t, errors.Is(sub.Err(), space.ErrSubscriptionDrifted))
}

// Fast-reject: at capacity, an incoming record with postKey >= maxKey
// AND not held should NOT trigger filter evaluation.
type spyFilter struct {
	calls int
}

func (f *spyFilter) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	f.calls++
	return true
}

func (f *spyFilter) IndexBounds(_ string, bs query.Bounds) query.Bounds { return bs }
func (f *spyFilter) String() string                                     { return "spy" }

func TestFastReject_SkipsFilterEval(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	initial := []row{
		makeRow(arena, "a", 1, nil),
		makeRow(arena, "b", 2, nil),
		makeRow(arena, "c", 3, nil), // sentinel; maxKey holds "n=3".
	}
	spy := &spyFilter{}
	sub := subscribeSorted(t, eng, Scope{ObjectId: "obj1", Dataset: "chat"}, spy, 2, initial)
	// Snapshot ran the spy 3 times (one per initial row, when fast-reject
	// hadn't kicked in yet — we filter ids in the snapshot loop too).
	// Reset.
	spy.calls = 0

	// New record z with n=10 (> sentinel's 3). Fast-reject — filter
	// must NOT be invoked.
	z := makeRow(arena, "z", 10, nil)
	fireEvent(eng, "obj1", "chat", "z", z.d, false, nil)
	waitNone(t, sub)
	assert.Equal(t, 0, spy.calls, "filter should NOT have been called on fast-rejected event")
}

// Overflow: when the mailbox fills, the next TryAdd fails and the sub
// closes with ErrSubscriptionOverflow.
func TestOverflow_ClosesWithOverflow(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	// MailboxCap = minMailboxCap = 16. Fire enough events to saturate
	// without anyone draining.
	cfg := SubConfig{
		Scope:       Scope{ObjectId: "obj1", Dataset: "chat"},
		Sort:        mustParseSort(t, "n"),
		Limit:       0, // unbounded — every event is an Add
		MailboxCap:  minMailboxCap,
		DriftBudget: defaultDriftBudgetPercent,
	}
	sub, err := eng.Subscribe(cfg, snapshotFromRows(nil))
	require.NoError(t, err)

	for i := 0; i < minMailboxCap*2; i++ {
		r := makeRow(arena, "m"+string(rune('a'+i%26)), i, nil)
		// Ensure unique ids per fire so we always emit Added.
		// Tweak: rebuild row id manually per i.
		obj := arena.NewObject()
		obj.Set("id", arena.NewString("id"+itoa(i)))
		obj.Set("n", arena.NewNumberInt(i))
		_ = r
		fireEvent(eng, "obj1", "chat", "id"+itoa(i), obj, false, nil)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	// Drain whatever is in the mailbox.
	_, _ = sub.Events().Wait(ctx)
	// At least one further Wait should see Closed.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()
	_, err = sub.Events().Wait(ctx2)
	require.Error(t, err)
	assert.True(t, errors.Is(err, mb.ErrClosed))
	assert.True(t, errors.Is(sub.Err(), space.ErrSubscriptionOverflow))
}

// Scope routing: a shared-objects sub gets events with
// dataset=ObjectsDataset from any object; an explicit (obj1, "objects")
// sub does NOT see events for obj2.
func TestScope_SharedAndExplicitDoNotCrossFire(t *testing.T) {
	eng := New("test")
	defer eng.Close()

	shared := subscribeSorted(t, eng, Scope{Shared: true}, nil, 0, nil)
	explicit := subscribeSorted(t, eng, Scope{Shared: false, ObjectId: "obj1", Dataset: ObjectsDataset}, nil, 0, nil)

	arena := newArena()
	// Event for obj1 → shared and explicit both fire.
	r1 := makeRow(arena, "x", 1, nil)
	fireEvent(eng, "obj1", ObjectsDataset, "x", r1.d, false, nil)

	ev, err := waitOne(t, shared)
	require.NoError(t, err)
	assert.Equal(t, []string{"x"}, idsOf(ev.Added))
	ev2, err := waitOne(t, explicit)
	require.NoError(t, err)
	assert.Equal(t, []string{"x"}, idsOf(ev2.Added))

	// Event for obj2 → only shared fires.
	r2 := makeRow(arena, "y", 2, nil)
	fireEvent(eng, "obj2", ObjectsDataset, "y", r2.d, false, nil)

	ev3, err := waitOne(t, shared)
	require.NoError(t, err)
	assert.Equal(t, []string{"y"}, idsOf(ev3.Added))
	waitNone(t, explicit) // explicit on obj1 — no event for obj2
}

// Engine.Close cascades — every live sub gets its mailbox closed.
func TestEngineClose_CascadesToSubs(t *testing.T) {
	eng := New("test")
	sub := subscribeSorted(t, eng, Scope{ObjectId: "obj1", Dataset: "chat"}, nil, 0, nil)
	require.NoError(t, eng.Close())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := sub.Events().Wait(ctx)
	require.Error(t, err)
	assert.True(t, errors.Is(err, mb.ErrClosed))
}

// ---------- small helpers ----------

func mustParseSort(t *testing.T, sort string) query.Sort {
	t.Helper()
	s, err := query.ParseSort(sort)
	require.NoError(t, err)
	return s
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var out []byte
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	for i > 0 {
		out = append([]byte{byte('0' + i%10)}, out...)
		i /= 10
	}
	if neg {
		out = append([]byte{'-'}, out...)
	}
	return string(out)
}
