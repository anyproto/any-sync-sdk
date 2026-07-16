package history

import (
	"context"
	"fmt"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

const testSpaceId = "space-1"

func newTestIndex(t *testing.T, skip ...string) *Index {
	t.Helper()
	ctx := context.Background()
	db, err := anystore.Open(ctx, ":memory:", &anystore.Config{InMemory: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ix, err := OpenIndex(ctx, db, testSpaceId, skip)
	require.NoError(t, err)
	return ix
}

// idxChange builds a DAG-borne change ready for IndexChange.
func idxChange(seq int, dataset, recordId string, opts ...func(*crdt.Change)) *crdt.Change {
	ch := &crdt.Change{
		ObjectId:    "obj-1",
		Dataset:     dataset,
		ChangeId:    fmt.Sprintf("cid-%03d", seq),
		VersionId:   crdt.VersionId(fmt.Sprintf("o%03d", seq)),
		DataVersion: "v1",
		Timestamp:   1700000000 + int64(seq),
		Creator:     "author-a",
		Records: []crdt.RecordChange{{
			Id: recordId, Upsert: true,
			Ops: []crdt.Op{{Type: crdt.OpSet}},
		}},
	}
	for _, o := range opts {
		o(ch)
	}
	return ch
}

func indexOne(t *testing.T, ix *Index, ch *crdt.Change) {
	t.Helper()
	recordIds, err := crdt.ResolveRecordIds(*ch)
	require.NoError(t, err)
	require.NoError(t, ix.IndexChange(context.Background(), ch, recordIds))
}

func TestIndexChangeAndListDescending(t *testing.T) {
	ctx := context.Background()
	ix := newTestIndex(t)
	for i := 1; i <= 5; i++ {
		indexOne(t, ix, idxChange(i, "notes", fmt.Sprintf("n%d", i)))
	}

	out, cur, err := ix.ListChanges(ctx, Filter{ObjectId: "obj-1"}, 10, "")
	require.NoError(t, err)
	assert.Empty(t, cur)
	require.Len(t, out, 5)
	assert.Equal(t, "cid-005", out[0].Version) // descending OrderId
	assert.Equal(t, "cid-001", out[4].Version)
	assert.Equal(t, "author-a", out[0].Author)
	assert.Equal(t, "notes", out[0].Dataset)
	assert.Equal(t, 1, out[0].GroupSize)
	require.Len(t, out[0].Touched, 1)
	assert.Equal(t, "n5", out[0].Touched[0].RecordId)
	assert.Equal(t, []string{"$set"}, out[0].Touched[0].Ops)
}

func TestListPagination(t *testing.T) {
	ctx := context.Background()
	ix := newTestIndex(t)
	for i := 1; i <= 7; i++ {
		indexOne(t, ix, idxChange(i, "notes", "n1"))
	}

	page1, cur, err := ix.ListChanges(ctx, Filter{ObjectId: "obj-1"}, 3, "")
	require.NoError(t, err)
	require.Len(t, page1, 3)
	require.NotEmpty(t, cur)
	assert.Equal(t, "cid-007", page1[0].Version)

	page2, cur, err := ix.ListChanges(ctx, Filter{ObjectId: "obj-1"}, 3, cur)
	require.NoError(t, err)
	require.Len(t, page2, 3)
	assert.Equal(t, "cid-004", page2[0].Version)
	require.NotEmpty(t, cur)

	page3, cur, err := ix.ListChanges(ctx, Filter{ObjectId: "obj-1"}, 3, cur)
	require.NoError(t, err)
	require.Len(t, page3, 1)
	assert.Equal(t, "cid-001", page3[0].Version)
	assert.Empty(t, cur)
}

func TestListFilters(t *testing.T) {
	ctx := context.Background()
	ix := newTestIndex(t)
	indexOne(t, ix, idxChange(1, "notes", "n1"))
	indexOne(t, ix, idxChange(2, "tasks", "t1"))
	indexOne(t, ix, idxChange(3, "notes", "n2", func(c *crdt.Change) { c.Creator = "author-b" }))
	indexOne(t, ix, idxChange(4, "notes", "n1", func(c *crdt.Change) { c.TraceIds = []string{"trace-x"} }))

	// dataset filter
	out, _, err := ix.ListChanges(ctx, Filter{ObjectId: "obj-1", Dataset: "tasks"}, 10, "")
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, "cid-002", out[0].Version)

	// author filter
	out, _, err = ix.ListChanges(ctx, Filter{ObjectId: "obj-1", Author: "author-b"}, 10, "")
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, "cid-003", out[0].Version)

	// record filter (via _history_recs join)
	out, _, err = ix.ListChanges(ctx, Filter{ObjectId: "obj-1", Dataset: "notes", RecordId: "n1"}, 10, "")
	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.Equal(t, "cid-004", out[0].Version)
	assert.Equal(t, "cid-001", out[1].Version)

	// trace filter (via _history_traces join), object-scoped and space-wide
	for _, f := range []Filter{
		{ObjectId: "obj-1", TraceId: "trace-x"},
		{TraceId: "trace-x"},
	} {
		out, _, err = ix.ListChanges(ctx, f, 10, "")
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.Equal(t, "cid-004", out[0].Version)
		assert.Equal(t, []string{"trace-x"}, out[0].TraceIds)
	}

	// record filter requires dataset
	_, _, err = ix.ListChanges(ctx, Filter{ObjectId: "obj-1", RecordId: "n1"}, 10, "")
	require.Error(t, err)

	// no object and no trace is an error
	_, _, err = ix.ListChanges(ctx, Filter{}, 10, "")
	require.Error(t, err)
}

// Regression: an author-filtered record/trace listing whose over-fetch
// window contains fewer than `limit` author matches must return a
// continuation cursor, not "" — older matching changes live beyond the
// window (review finding: early termination + unreachable lastO branch).
func TestListViaJoinAuthorFilterPagination(t *testing.T) {
	ctx := context.Background()
	ix := newTestIndex(t)

	// 30 changes touching rec "n1": only the 3 OLDEST are by author-a,
	// the 27 newest by author-b. With limit=2 the first window
	// (fetch=8) is all author-b.
	for i := 1; i <= 3; i++ {
		indexOne(t, ix, idxChange(i, "notes", "n1"))
	}
	for i := 4; i <= 30; i++ {
		indexOne(t, ix, idxChange(i, "notes", "n1", func(c *crdt.Change) { c.Creator = "author-b" }))
	}

	f := Filter{ObjectId: "obj-1", Dataset: "notes", RecordId: "n1", Author: "author-a"}
	var (
		got    []ChangeMeta
		cursor string
		pages  int
	)
	for {
		page, next, err := ix.ListChanges(ctx, f, 2, cursor)
		require.NoError(t, err)
		got = append(got, page...)
		pages++
		require.Less(t, pages, 30, "pagination must terminate")
		if next == "" {
			break
		}
		cursor = next
	}
	require.Len(t, got, 3, "all author-a changes reachable across windows")
	assert.Equal(t, "cid-003", got[0].Version)
	assert.Equal(t, "cid-001", got[2].Version)

	// Sanity: unfiltered join listing still paginates exactly.
	all, next, err := ix.ListChanges(ctx, Filter{ObjectId: "obj-1", Dataset: "notes", RecordId: "n1"}, 30, "")
	require.NoError(t, err)
	assert.Empty(t, next)
	assert.Len(t, all, 30)
}

// OrderIds are only unique per tree: two objects easily mint identical
// early lexids. The trace route's cursor carries the object tail so
// paginating a space-wide trace never skips a same-o row from another
// object.
func TestListByTraceCursorAcrossObjects(t *testing.T) {
	ctx := context.Background()
	ix := newTestIndex(t)

	// Same o ("o001") on two different objects, same trace.
	for _, obj := range []string{"obj-a", "obj-b", "obj-c"} {
		ch := idxChange(1, "notes", "n1", func(c *crdt.Change) {
			c.ObjectId = obj
			c.ChangeId = "cid-" + obj
			c.TraceIds = []string{"tr-x"}
		})
		indexOne(t, ix, ch)
	}

	var got []string
	cursor := ""
	for {
		page, next, err := ix.ListChanges(ctx, Filter{TraceId: "tr-x"}, 1, cursor)
		require.NoError(t, err)
		for _, m := range page {
			got = append(got, m.Version)
		}
		if next == "" {
			break
		}
		cursor = next
		require.Less(t, len(got), 10, "must terminate")
	}
	assert.ElementsMatch(t, []string{"cid-obj-a", "cid-obj-b", "cid-obj-c"}, got,
		"no same-o row skipped across objects")
}

func TestIndexSkips(t *testing.T) {
	ctx := context.Background()
	ix := newTestIndex(t, "presence")

	// SkipHistory dataset
	indexOne(t, ix, idxChange(1, "presence", "p1"))
	// local / injected / no DAG identity
	local := idxChange(2, "notes", "n1", func(c *crdt.Change) { c.Local = true })
	indexOne(t, ix, local)
	injected := idxChange(3, "notes", "n2", func(c *crdt.Change) { c.Injected = true })
	indexOne(t, ix, injected)
	noId := idxChange(4, "notes", "n3", func(c *crdt.Change) { c.ChangeId = "" })
	indexOne(t, ix, noId)

	out, _, err := ix.ListChanges(ctx, Filter{ObjectId: "obj-1"}, 10, "")
	require.NoError(t, err)
	assert.Empty(t, out)
	assert.True(t, ix.SkipsDataset("presence"))
	assert.False(t, ix.SkipsDataset("notes"))
}

func TestIndexChangeIdempotent(t *testing.T) {
	ctx := context.Background()
	ix := newTestIndex(t)
	ch := idxChange(1, "notes", "n1", func(c *crdt.Change) { c.TraceIds = []string{"tr"} })
	indexOne(t, ix, ch)
	indexOne(t, ix, ch) // re-apply / backfill race

	out, _, err := ix.ListChanges(ctx, Filter{ObjectId: "obj-1"}, 10, "")
	require.NoError(t, err)
	assert.Len(t, out, 1)
}

func TestStaleLifecycleAndBackfill(t *testing.T) {
	ctx := context.Background()
	ix := newTestIndex(t)

	stale, err := ix.IsStale(ctx, testObjectId)
	require.NoError(t, err)
	assert.False(t, stale)

	require.NoError(t, ix.MarkStale(ctx, testObjectId))
	stale, err = ix.IsStale(ctx, testObjectId)
	require.NoError(t, err)
	assert.True(t, stale)

	// Backfill from a tree walk (fake tree serves codec-encoded payloads).
	b := newTreeBuilder(t)
	b.add("notes", upsert(t, "n1", `{"title":"a"}`))
	b.add("notes", upsert(t, "n2", `{"title":"b"}`))
	b.add("tasks", upsert(t, "t1", `{"done":true}`))

	require.NoError(t, ix.Backfill(ctx, b.tree, testObjectId, 2)) // batch smaller than total

	stale, err = ix.IsStale(ctx, testObjectId)
	require.NoError(t, err)
	assert.False(t, stale)

	out, _, err := ix.ListChanges(ctx, Filter{ObjectId: testObjectId}, 10, "")
	require.NoError(t, err)
	require.Len(t, out, 3)
	assert.Equal(t, "cid-003", out[0].Version)
	require.Len(t, out[0].Touched, 1)
	assert.Equal(t, "t1", out[0].Touched[0].RecordId)
}

func TestListLimitClamps(t *testing.T) {
	ctx := context.Background()
	ix := newTestIndex(t)
	indexOne(t, ix, idxChange(1, "notes", "n1"))

	// limit<=0 falls back to the default; oversized limits clamp.
	out, _, err := ix.ListChanges(ctx, Filter{ObjectId: "obj-1"}, 0, "")
	require.NoError(t, err)
	assert.Len(t, out, 1)
	_, _, err = ix.ListChanges(ctx, Filter{ObjectId: "obj-1"}, MaxListLimit+500, "")
	require.NoError(t, err)
}

// Combined filters must AND across scan routes: the route picks the
// scan, the remaining fields apply as residual checks (regression:
// RecordId used to silently drop TraceId, and the trace route ignored
// Dataset).
func TestListCombinedFilters(t *testing.T) {
	ctx := context.Background()
	ix := newTestIndex(t)
	indexOne(t, ix, idxChange(1, "notes", "n1"))
	indexOne(t, ix, idxChange(2, "notes", "n1", func(c *crdt.Change) { c.TraceIds = []string{"trace-x"} }))
	indexOne(t, ix, idxChange(3, "notes", "n2", func(c *crdt.Change) { c.TraceIds = []string{"trace-x"} }))
	indexOne(t, ix, idxChange(4, "tasks", "t1", func(c *crdt.Change) { c.TraceIds = []string{"trace-x"} }))

	// record + trace: only n1's traced change.
	out, cur, err := ix.ListChanges(ctx, Filter{
		ObjectId: "obj-1", Dataset: "notes", RecordId: "n1", TraceId: "trace-x",
	}, 10, "")
	require.NoError(t, err)
	assert.Empty(t, cur)
	require.Len(t, out, 1)
	assert.Equal(t, "cid-002", out[0].Version)

	// trace + dataset: the tasks change carrying the trace is excluded.
	out, cur, err = ix.ListChanges(ctx, Filter{TraceId: "trace-x", Dataset: "notes"}, 10, "")
	require.NoError(t, err)
	assert.Empty(t, cur)
	require.Len(t, out, 2)
	assert.Equal(t, "cid-003", out[0].Version)
	assert.Equal(t, "cid-002", out[1].Version)

	// trace + dataset + record: all three routes' fields at once.
	out, _, err = ix.ListChanges(ctx, Filter{
		ObjectId: "obj-1", Dataset: "tasks", RecordId: "t1", TraceId: "trace-x",
	}, 10, "")
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, "cid-004", out[0].Version)
}

// A page window whose rows are all rejected by residual filters must
// keep paging via the returned cursor instead of terminating early.
func TestListCombinedFiltersPagesPastRejects(t *testing.T) {
	ctx := context.Background()
	ix := newTestIndex(t)
	// 30 untraced changes on n1, then 1 traced one at the oldest end.
	indexOne(t, ix, idxChange(1, "notes", "n1", func(c *crdt.Change) { c.TraceIds = []string{"trace-x"} }))
	for i := 2; i <= 31; i++ {
		indexOne(t, ix, idxChange(i, "notes", "n1"))
	}

	var got []ChangeMeta
	cur := ""
	for {
		page, next, err := ix.ListChanges(ctx, Filter{
			ObjectId: "obj-1", Dataset: "notes", RecordId: "n1", TraceId: "trace-x",
		}, 2, cur)
		require.NoError(t, err)
		got = append(got, page...)
		if next == "" {
			break
		}
		cur = next
	}
	require.Len(t, got, 1)
	assert.Equal(t, "cid-001", got[0].Version)
}

// TouchedChangeIds must never feed the RecordAt pre-filter for a
// SkipHistory dataset: its changes are absent from the index, so a
// non-nil set would exclude them all from the filtered replay
// (regression: recordId collisions across datasets returned the other
// dataset's set).
func TestTouchedChangeIdsSkipHistoryFallsBack(t *testing.T) {
	ctx := context.Background()
	ix := newTestIndex(t, "presence")
	// Indexed dataset shares the record id with the skipped one.
	indexOne(t, ix, idxChange(1, "notes", "shared-id"))

	touched, err := ix.TouchedChangeIds(ctx, "obj-1", "presence", "shared-id")
	require.NoError(t, err)
	assert.Nil(t, touched, "skip-history dataset must take the decode-and-filter fallback")

	// The indexed dataset still gets its pre-filter.
	touched, err = ix.TouchedChangeIds(ctx, "obj-1", "notes", "shared-id")
	require.NoError(t, err)
	assert.Equal(t, []string{"cid-001"}, touched)
}
