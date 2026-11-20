package e2e

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// labRow is one samples record a test writes and expects to read back.
type labRow struct {
	obj, id, label, kind string
	score                int
}

// sample builds a samples record; the label names the record and its
// score.
func sample(obj, id, kind string, score int) labRow {
	return labRow{obj: obj, id: id, kind: kind, score: score, label: fmt.Sprintf("%s-%d", id, score)}
}

// key is the id the record is stored and read under.
func (r labRow) key() string { return r.obj + "/" + r.id }

// putRows writes each row to its object's samples dataset.
func (l *sharedLab) putRows(t *testing.T, ctx context.Context, rows ...labRow) {
	t.Helper()
	for _, r := range rows {
		l.put(t, ctx, r.obj, l.samples, r.id, map[string]any{"label": r.label, "score": r.score, "kind": r.kind})
	}
}

// deleteSample deletes one record of objectId's samples.
func (l *sharedLab) deleteSample(t *testing.T, ctx context.Context, objectId, recordId string) {
	t.Helper()
	res, err := l.sp.Delete(ctx, space.DeleteBatch{ObjectId: objectId, Dataset: l.samples, RecordIds: []string{recordId}})
	require.NoError(t, err, "delete %s on %s", recordId, objectId)
	require.Empty(t, res.Rejections, "delete %s on %s", recordId, objectId)
}

// spaceRows reads every live record of a shared dataset across the
// space, by id.
func (l *sharedLab) spaceRows(t *testing.T, ctx context.Context, dataset string) map[string]*anyenc.Value {
	t.Helper()
	all, err := l.sp.QueryDataset(dataset).All(ctx)
	require.NoError(t, err)
	out := make(map[string]*anyenc.Value, len(all))
	for _, row := range all {
		out[row.GetString("id")] = row
	}
	require.Len(t, out, len(all), "every row of the space has its own id")
	return out
}

// keysOf lists the rows' ids.
func keysOf(rows []labRow) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.key())
	}
	return out
}

// sortedKeys lists the rows' ids in Sort("score", "id") order, or in
// Sort("-score", "id") order when desc.
func sortedKeys(rows []labRow, desc bool) []string {
	sorted := append([]labRow(nil), rows...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].score != sorted[j].score {
			return (sorted[i].score < sorted[j].score) != desc
		}
		return sorted[i].key() < sorted[j].key()
	})
	return keysOf(sorted)
}

// rowsWhere keeps the rows keep accepts.
func rowsWhere(rows []labRow, keep func(labRow) bool) []labRow {
	var out []labRow
	for _, r := range rows {
		if keep(r) {
			out = append(out, r)
		}
	}
	return out
}

// withoutKeys drops the rows with the given ids.
func withoutKeys(rows []labRow, keys ...string) []labRow {
	return rowsWhere(rows, func(r labRow) bool {
		for _, k := range keys {
			if r.key() == k {
				return false
			}
		}
		return true
	})
}

// assertRows checks that got holds exactly want, each row naming its
// object and carrying its fields.
func assertRows(t *testing.T, got map[string]*anyenc.Value, want []labRow) {
	t.Helper()
	gotIds := make([]string, 0, len(got))
	for id := range got {
		gotIds = append(gotIds, id)
	}
	assert.ElementsMatch(t, keysOf(want), gotIds)
	for _, r := range want {
		row := got[r.key()]
		if !assert.NotNil(t, row, "row %s", r.key()) {
			continue
		}
		assert.Equal(t, r.obj, row.GetString("_objectId"), "row %s names its object", r.key())
		assert.Equal(t, r.label, row.GetString("label"), "row %s label", r.key())
		assert.Equal(t, float64(r.score), row.GetFloat64("score"), "row %s score", r.key())
		assert.Equal(t, r.kind, row.GetString("kind"), "row %s kind", r.key())
	}
}

// groupTotal is one output row of a `{n: $count, total: $sum}` group.
type groupTotal struct {
	n     int
	total float64
}

// groupTotals runs a pipeline whose output rows carry `id`, `n` and
// `total`, by id.
func groupTotals(t *testing.T, ctx context.Context, agg space.Agg) map[string]groupTotal {
	t.Helper()
	rows, err := agg.All(ctx)
	require.NoError(t, err)
	out := make(map[string]groupTotal, len(rows))
	for _, r := range rows {
		out[r.GetString("id")] = groupTotal{n: r.GetInt("n"), total: r.GetFloat64("total")}
	}
	require.Len(t, out, len(rows), "one row per group")
	return out
}

// totalsBy groups rows by group and sums their count and score.
func totalsBy(rows []labRow, group func(labRow) string) map[string]groupTotal {
	out := map[string]groupTotal{}
	for _, r := range rows {
		g := out[group(r)]
		g.n++
		g.total += float64(r.score)
		out[group(r)] = g
	}
	return out
}

func byObject(r labRow) string { return r.obj }
func byKind(r labRow) string   { return r.kind }

// subLog folds a subscription's events by record id: the last doc an
// id was added or updated with, the reason it was removed, and every
// id any event carried.
type subLog struct {
	added   map[string]*anyenc.Value
	updated map[string]*anyenc.Value
	removed map[string]space.RemoveReason
	ids     []string
}

func newSubLog() *subLog {
	return &subLog{
		added:   map[string]*anyenc.Value{},
		updated: map[string]*anyenc.Value{},
		removed: map[string]space.RemoveReason{},
	}
}

func (l *subLog) fold(ev space.SubscriptionEvent) {
	for _, r := range ev.Added {
		l.added[r.Id] = r.Doc
		l.ids = append(l.ids, r.Id)
	}
	for _, r := range ev.Updated {
		l.updated[r.Id] = r.Doc
		l.ids = append(l.ids, r.Id)
	}
	for _, r := range ev.Removed {
		l.removed[r.Id] = r.Reason
		l.ids = append(l.ids, r.Id)
	}
}

// receiveUntil folds sub's events into the log until done holds; fails
// the test after timeout.
func (l *subLog) receiveUntil(t *testing.T, sub space.QuerySubscription, timeout time.Duration, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !done() {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("subscription never reached the expected state; event ids %v, removed %v", l.ids, l.removed)
		}
		wctx, cancel := context.WithTimeout(context.Background(), remaining)
		evs, err := sub.Events().Wait(wctx)
		cancel()
		if err != nil {
			t.Fatalf("subscription wait: %v (sub err %v); event ids %v", err, sub.Err(), l.ids)
		}
		for _, ev := range evs {
			l.fold(ev)
		}
	}
}

// mirrorOf seeds a client-side mirror of a subscription's window from
// its initial rows.
func mirrorOf(initial []*anyenc.Value) map[string]*anyenc.Value {
	m := make(map[string]*anyenc.Value, len(initial))
	for _, row := range initial {
		m[row.GetString("id")] = row
	}
	return m
}

// applyToMirror applies one subscription event to a mirror.
func applyToMirror(m map[string]*anyenc.Value, ev space.SubscriptionEvent) {
	for _, r := range ev.Added {
		m[r.Id] = r.Doc
	}
	for _, r := range ev.Updated {
		m[r.Id] = r.Doc
	}
	for _, r := range ev.Removed {
		delete(m, r.Id)
	}
}

func mirrorIds(m map[string]*anyenc.Value) []string {
	out := make([]string, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	return out
}

// TestE2E_SharedDatasetsSpace_Query: QueryDataset reads a shared
// dataset across every object that holds it — composite ids, each row
// naming its object — with filters, sorts and pages spanning objects,
// and skips tombstones.
func TestE2E_SharedDatasetsSpace_Query(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	lab := openSharedLab(t, ctx, "SharedSpaceQuery")
	sp := lab.sp

	a, b, c := lab.newObject(t, ctx), lab.newObject(t, ctx), lab.newObject(t, ctx)
	empty := lab.newObject(t, ctx)
	// The objects share plain record ids; a/r3 and b/r3 tie on score.
	seed := []labRow{
		sample(a, "r1", "x", 1), sample(a, "r2", "y", 4), sample(a, "r3", "x", 7),
		sample(b, "r1", "y", 2), sample(b, "r2", "x", 5), sample(b, "r3", "y", 7),
		sample(c, "r1", "x", 3), sample(c, "r2", "y", 6),
	}
	lab.putRows(t, ctx, seed...)
	// Rows of another shared dataset and of a per-object one stay out.
	lab.put(t, ctx, a, lab.events, "", map[string]any{"label": "event"})
	lab.put(t, ctx, b, lab.plain, "r1", map[string]any{"label": "plain"})

	t.Run("AllObjects", func(t *testing.T) {
		assertRows(t, lab.spaceRows(t, ctx, lab.samples), seed)

		n, err := sp.QueryDataset(lab.samples).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, len(seed), n, "Count sums every object's records")

		snap, err := sp.QueryDataset(lab.samples).Snapshot(ctx, space.QueryOpts{IncludeTotal: true})
		require.NoError(t, err)
		assert.Equal(t, len(seed), snap.Total)
		assert.ElementsMatch(t, keysOf(seed), idsOfInitial(snap.Initial))
		assert.False(t, snap.HasNext)

		it, err := sp.QueryDataset(lab.samples).Iter(ctx)
		require.NoError(t, err)
		var iterIds []string
		for it.Next() {
			doc, derr := it.Doc()
			require.NoError(t, derr)
			iterIds = append(iterIds, doc.GetString("id"))
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
		assert.ElementsMatch(t, keysOf(seed), iterIds, "Iter streams every object's records")

		evs, err := sp.QueryDataset(lab.events).All(ctx)
		require.NoError(t, err)
		require.Len(t, evs, 1, "the events dataset holds its own rows only")
		assert.Equal(t, a, evs[0].GetString("_objectId"))
		assert.True(t, strings.HasPrefix(evs[0].GetString("id"), a+"/"))
	})

	t.Run("FilterByObject", func(t *testing.T) {
		for _, obj := range []string{a, b, c} {
			want := rowsWhere(seed, func(r labRow) bool { return r.obj == obj })
			rows, err := sp.QueryDataset(lab.samples).Filter(map[string]any{"_objectId": obj}).All(ctx)
			require.NoError(t, err)
			got := mirrorOf(rows)
			assertRows(t, got, want)

			perObject := lab.rows(t, ctx, obj, lab.samples)
			assert.ElementsMatch(t, mirrorIds(perObject), mirrorIds(got), "the space read of %s equals its per-object read", obj)
			for id, row := range perObject {
				if other := got[id]; assert.NotNil(t, other, id) {
					assert.Equal(t, row.GetString("label"), other.GetString("label"), id)
					assert.Equal(t, row.GetFloat64("score"), other.GetFloat64("score"), id)
				}
			}
			n, err := sp.QueryDataset(lab.samples).Filter(map[string]any{"_objectId": obj}).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, len(want), n)
		}
		n, err := sp.QueryDataset(lab.samples).Filter(map[string]any{"_objectId": empty}).Count(ctx)
		require.NoError(t, err)
		assert.Zero(t, n, "an object that wrote nothing holds no rows")
	})

	t.Run("FilterAcrossObjects", func(t *testing.T) {
		rows, err := sp.QueryDataset(lab.samples).Filter(map[string]any{"kind": "x"}).All(ctx)
		require.NoError(t, err)
		assertRows(t, mirrorOf(rows), rowsWhere(seed, func(r labRow) bool { return r.kind == "x" }))

		rows, err = sp.QueryDataset(lab.samples).Filter(map[string]any{"score": map[string]any{"$gte": 5}}).All(ctx)
		require.NoError(t, err)
		assertRows(t, mirrorOf(rows), rowsWhere(seed, func(r labRow) bool { return r.score >= 5 }))

		// Two filters AND together.
		n, err := sp.QueryDataset(lab.samples).
			Filter(map[string]any{"kind": "y"}).
			Filter(map[string]any{"score": map[string]any{"$lt": 7}}).
			Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, len(rowsWhere(seed, func(r labRow) bool { return r.kind == "y" && r.score < 7 })), n)

		// A record is addressed by its composite id; a plain id names no row.
		row, err := sp.QueryDataset(lab.samples).Filter(map[string]any{"id": b + "/r2"}).One(ctx)
		require.NoError(t, err)
		assert.Equal(t, "r2-5", row.GetString("label"))
		assert.Equal(t, b, row.GetString("_objectId"))
		n, err = sp.QueryDataset(lab.samples).Filter(map[string]any{"id": "r1"}).Count(ctx)
		require.NoError(t, err)
		assert.Zero(t, n, "rows are stored under composite ids only")
		_, err = sp.QueryDataset(lab.samples).Filter(map[string]any{"kind": "none"}).One(ctx)
		assert.ErrorIs(t, err, space.ErrNotFound)
	})

	t.Run("SortAndPage", func(t *testing.T) {
		want := sortedKeys(seed, false)
		rows, err := sp.QueryDataset(lab.samples).Sort("score", "id").All(ctx)
		require.NoError(t, err)
		assert.Equal(t, want, idsOfInitial(rows), "Sort orders rows of every object together")

		var paged []string
		for off := 0; off < len(seed); off += 3 {
			rows, err = sp.QueryDataset(lab.samples).Sort("score", "id").Limit(3).Offset(off).All(ctx)
			require.NoError(t, err)
			end := min(off+3, len(want))
			assert.Equal(t, want[off:end], idsOfInitial(rows), "page at offset %d", off)
			paged = append(paged, idsOfInitial(rows)...)
		}
		assert.Equal(t, want, paged, "the pages cover every row once, in order")

		desc := sortedKeys(seed, true)
		rows, err = sp.QueryDataset(lab.samples).Sort("-score", "id").Limit(2).All(ctx)
		require.NoError(t, err)
		assert.Equal(t, desc[:2], idsOfInitial(rows), "the score tie across objects breaks on the id")

		row, err := sp.QueryDataset(lab.samples).Sort("-score", "id").One(ctx)
		require.NoError(t, err)
		assert.Equal(t, desc[0], row.GetString("id"))

		snap, err := sp.QueryDataset(lab.samples).Sort("score", "id").Limit(3).Offset(3).
			Snapshot(ctx, space.QueryOpts{IncludeTotal: true})
		require.NoError(t, err)
		assert.Equal(t, want[3:6], idsOfInitial(snap.Initial))
		assert.Equal(t, len(seed), snap.Total)
		assert.True(t, snap.HasNext)
	})

	// Runs last: it deletes records.
	t.Run("Tombstones", func(t *testing.T) {
		lab.deleteSample(t, ctx, b, "r2")
		lab.deleteSample(t, ctx, a, a+"/r1")
		live := withoutKeys(seed, b+"/r2", a+"/r1")

		assertRows(t, lab.spaceRows(t, ctx, lab.samples), live)
		n, err := sp.QueryDataset(lab.samples).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, len(live), n)
		snap, err := sp.QueryDataset(lab.samples).Sort("score", "id").Snapshot(ctx, space.QueryOpts{IncludeTotal: true})
		require.NoError(t, err)
		assert.Equal(t, len(live), snap.Total)
		assert.Equal(t, sortedKeys(live, false), idsOfInitial(snap.Initial))

		rows, err := sp.QueryDataset(lab.samples).Filter(map[string]any{"kind": "x"}).All(ctx)
		require.NoError(t, err)
		assertRows(t, mirrorOf(rows), rowsWhere(live, func(r labRow) bool { return r.kind == "x" }))
		_, err = sp.QueryDataset(lab.samples).Filter(map[string]any{"id": b + "/r2"}).One(ctx)
		assert.ErrorIs(t, err, space.ErrNotFound, "a tombstone is not returned")

		// IncludeDeleted surfaces the tombstones, each naming its object.
		n, err = sp.QueryDataset(lab.samples).Projection(space.ProjectionOpts{IncludeDeleted: true}).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, len(seed), n)
		tomb, err := sp.QueryDataset(lab.samples).
			Filter(map[string]any{"id": b + "/r2"}).
			Projection(space.ProjectionOpts{IncludeDeleted: true}).
			One(ctx)
		require.NoError(t, err)
		assert.NotNil(t, tomb.Get("_deletedAt"))
		assert.Equal(t, b, tomb.GetString("_objectId"))
	})
}

// TestE2E_SharedDatasetsSpace_Aggregate: AggregateDataset runs a
// pipeline over every object's records; grouped by `_objectId` it
// matches each object's own Aggregate, and tombstones stay out.
func TestE2E_SharedDatasetsSpace_Aggregate(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	lab := openSharedLab(t, ctx, "SharedSpaceAgg")
	sp := lab.sp

	a, b, c := lab.newObject(t, ctx), lab.newObject(t, ctx), lab.newObject(t, ctx)
	seed := []labRow{
		sample(a, "s1", "x", 1), sample(a, "s2", "x", 2), sample(a, "s3", "y", 3),
		sample(b, "s1", "x", 10), sample(b, "s2", "y", 20),
		sample(c, "s1", "y", 100), sample(c, "s2", "y", 200), sample(c, "s3", "z", 300), sample(c, "s4", "x", 400),
	}
	lab.putRows(t, ctx, seed...)

	const groupByObject = `[
		{"$group": {"_id": "$_objectId", "n": {"$count": {}}, "total": {"$sum": "$score"}}},
		{"$sort": {"id": 1}}
	]`
	const groupByKind = `[
		{"$group": {"_id": "$kind", "n": {"$count": {}}, "total": {"$sum": "$score"}}},
		{"$sort": {"id": 1}}
	]`
	const matchX = `[
		{"$match": {"kind": "x"}},
		{"$group": {"_id": "$kind", "n": {"$count": {}}, "total": {"$sum": "$score"}}}
	]`

	// checkPerObject compares the space-wide group by object with each
	// object's own Aggregate and with the written rows.
	checkPerObject := func(t *testing.T, live []labRow) {
		t.Helper()
		got := groupTotals(t, ctx, sp.AggregateDataset(lab.samples, groupByObject))
		assert.Equal(t, totalsBy(live, byObject), got)
		for obj, g := range got {
			own := groupTotals(t, ctx, sp.Aggregate(obj, lab.samples, groupByObject))
			assert.Equal(t, map[string]groupTotal{obj: g}, own, "the space group of %s equals its own aggregate", obj)
		}
		rows, err := sp.AggregateDataset(lab.samples, groupByObject).All(ctx)
		require.NoError(t, err)
		objs := make([]string, 0, len(got))
		for obj := range got {
			objs = append(objs, obj)
		}
		sort.Strings(objs)
		assert.Equal(t, objs, idsOfInitial(rows), "$sort orders the groups")
		n, err := sp.AggregateDataset(lab.samples, groupByObject).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, len(got), n, "Count counts the groups")
	}

	t.Run("GroupByObject", func(t *testing.T) {
		checkPerObject(t, seed)
	})

	t.Run("MatchThenGroup", func(t *testing.T) {
		got := groupTotals(t, ctx, sp.AggregateDataset(lab.samples, matchX))
		want := totalsBy(rowsWhere(seed, func(r labRow) bool { return r.kind == "x" }), byKind)
		assert.Equal(t, want, got, "one group over the whole space")

		// The space group is the sum of the objects' own groups.
		var sum groupTotal
		for _, obj := range []string{a, b, c} {
			own := groupTotals(t, ctx, sp.Aggregate(obj, lab.samples, matchX))
			sum.n += own["x"].n
			sum.total += own["x"].total
		}
		assert.Equal(t, got["x"], sum)

		got = groupTotals(t, ctx, sp.AggregateDataset(lab.samples, `[
			{"$match": {"score": {"$gte": 3}}},
			{"$group": {"_id": "$_objectId", "n": {"$count": {}}, "total": {"$sum": "$score"}}}
		]`))
		assert.Equal(t, totalsBy(rowsWhere(seed, func(r labRow) bool { return r.score >= 3 }), byObject), got)

		assert.Equal(t, totalsBy(seed, byKind), groupTotals(t, ctx, sp.AggregateDataset(lab.samples, groupByKind)),
			"a group by a field spans objects")
	})

	t.Run("CountAndMatchObject", func(t *testing.T) {
		rows, err := sp.AggregateDataset(lab.samples, `[{"$count": "n"}]`).All(ctx)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, len(seed), rows[0].GetInt("n"))

		rows, err = sp.AggregateDataset(lab.samples, `[{"$match": {"_objectId": "`+b+`"}}, {"$count": "n"}]`).All(ctx)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, 2, rows[0].GetInt("n"))

		// A nil pipeline returns every live row.
		n, err := sp.AggregateDataset(lab.samples, nil).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, len(seed), n)

		plan, err := sp.AggregateDataset(lab.samples, groupByObject).Explain(ctx)
		require.NoError(t, err)
		assert.NotEmpty(t, plan)
	})

	// Runs last: it deletes records.
	t.Run("Tombstones", func(t *testing.T) {
		lab.deleteSample(t, ctx, a, "s2")
		lab.deleteSample(t, ctx, c, "s3")
		live := withoutKeys(seed, a+"/s2", c+"/s3")
		checkPerObject(t, live)
		got := groupTotals(t, ctx, sp.AggregateDataset(lab.samples, groupByKind))
		assert.Equal(t, totalsBy(live, byKind), got)
		assert.NotContains(t, got, "z", "the only z record is deleted")
		rows, err := sp.AggregateDataset(lab.samples, `[{"$count": "n"}]`).All(ctx)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, len(live), rows[0].GetInt("n"))
	})
}

// TestE2E_SharedDatasetsSpace_NotShared: a read across objects of a
// dataset that is not shared, or that the space does not hold, fails
// every terminal with ErrDatasetNotShared; a shared dataset nothing
// wrote reads empty.
func TestE2E_SharedDatasetsSpace_NotShared(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	lab := openSharedLab(t, ctx, "SharedSpaceNotShared")
	sp := lab.sp

	a := lab.newObject(t, ctx)
	lab.putRows(t, ctx, sample(a, "r1", "x", 1))
	lab.put(t, ctx, a, lab.plain, "p1", map[string]any{"label": "plain"})
	other, err := lab.sdk.Spaces().Create(ctx, space.CreateRequest{Name: "SharedSpaceOther"})
	require.NoError(t, err)

	refused := func(t *testing.T, sp space.Space, dataset string) {
		t.Helper()
		_, err := sp.QueryDataset(dataset).All(ctx)
		assert.ErrorIs(t, err, space.ErrDatasetNotShared, "All")
		_, err = sp.QueryDataset(dataset).Count(ctx)
		assert.ErrorIs(t, err, space.ErrDatasetNotShared, "Count")
		_, err = sp.QueryDataset(dataset).Filter(map[string]any{"kind": "x"}).One(ctx)
		assert.ErrorIs(t, err, space.ErrDatasetNotShared, "One")
		it, err := sp.QueryDataset(dataset).Iter(ctx)
		assert.ErrorIs(t, err, space.ErrDatasetNotShared, "Iter")
		if it != nil {
			_ = it.Close()
		}
		_, err = sp.QueryDataset(dataset).Snapshot(ctx, space.QueryOpts{IncludeTotal: true})
		assert.ErrorIs(t, err, space.ErrDatasetNotShared, "Snapshot")
		for _, q := range []space.Query{
			sp.QueryDataset(dataset),
			sp.QueryDataset(dataset).Sort("id").Limit(5),
		} {
			res, err := q.Subscribe(ctx, space.QueryOpts{})
			assert.ErrorIs(t, err, space.ErrDatasetNotShared, "Subscribe")
			if res != nil && res.Sub != nil {
				_ = res.Sub.Close()
			}
		}

		agg := func() space.Agg { return sp.AggregateDataset(dataset, `[{"$count": "n"}]`) }
		ait, err := agg().Iter(ctx)
		assert.ErrorIs(t, err, space.ErrDatasetNotShared, "agg Iter")
		if ait != nil {
			_ = ait.Close()
		}
		_, err = agg().All(ctx)
		assert.ErrorIs(t, err, space.ErrDatasetNotShared, "agg All")
		_, err = agg().Count(ctx)
		assert.ErrorIs(t, err, space.ErrDatasetNotShared, "agg Count")
		_, err = agg().Explain(ctx)
		assert.ErrorIs(t, err, space.ErrDatasetNotShared, "agg Explain")
	}

	for _, tc := range []struct{ name, dataset string }{
		{"PerObjectRecords", lab.plain},
		{"Unknown", "no-such-dataset"},
		{"Empty", ""},
		{"Objects", "objects"},
		{"Module", "notes_body"},
		{"StorageName", sp.Id() + "_" + lab.samples},
	} {
		t.Run(tc.name, func(t *testing.T) { refused(t, sp, tc.dataset) })
	}
	t.Run("OtherSpace", func(t *testing.T) { refused(t, other, lab.samples) })

	// The refused reads changed nothing: the shared and per-object
	// datasets read back as written.
	assertRows(t, lab.spaceRows(t, ctx, lab.samples), []labRow{sample(a, "r1", "x", 1)})
	assert.Contains(t, lab.rows(t, ctx, a, lab.plain), "p1")

	// A shared dataset no object wrote reads empty, without an error.
	t.Run("SharedButEmpty", func(t *testing.T) {
		rows, err := sp.QueryDataset(lab.events).All(ctx)
		require.NoError(t, err)
		assert.Empty(t, rows)
		n, err := sp.QueryDataset(lab.events).Count(ctx)
		require.NoError(t, err)
		assert.Zero(t, n)
		n, err = sp.AggregateDataset(lab.events, nil).Count(ctx)
		require.NoError(t, err)
		assert.Zero(t, n)
		res, err := sp.QueryDataset(lab.events).Sort("id").Limit(5).Subscribe(ctx, space.QueryOpts{IncludeTotal: true})
		require.NoError(t, err)
		t.Cleanup(func() { _ = res.Sub.Close() })
		assert.Empty(t, res.Initial)
		assert.Zero(t, res.Total)

		// Its first row reaches the open subscription.
		wrote := lab.put(t, ctx, a, lab.events, "", map[string]any{"label": "first"})
		require.Len(t, wrote.RecordIds, 1)
		ev := receiveOne(t, res.Sub, 2*time.Second)
		rec, where := subRecordFor(ev, wrote.RecordIds[0])
		require.NotNil(t, rec, "event ids %v", eventIds(ev))
		assert.Equal(t, "added", where)
		assert.Equal(t, a, rec.Doc.GetString("_objectId"))
	})
}

// TestE2E_SharedDatasetsSpace_Subscribe: a space-wide subscription
// starts from every object's records and receives every object's
// writes under composite ids; a filter scopes it; a sorted window
// admits and displaces rows across objects. Writes to other datasets
// never reach it.
func TestE2E_SharedDatasetsSpace_Subscribe(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	lab := openSharedLab(t, ctx, "SharedSpaceSub")
	sp := lab.sp

	a, b, c := lab.newObject(t, ctx), lab.newObject(t, ctx), lab.newObject(t, ctx)
	lab.putRows(t, ctx,
		sample(a, "r1", "x", 1),
		sample(b, "r1", "y", 2),
		sample(c, "r1", "x", 3),
		sample(c, "r9", "y", 30),
	)

	t.Run("Window", func(t *testing.T) {
		res, err := sp.QueryDataset(lab.samples).Sort("score", "id").Limit(10).
			Subscribe(ctx, space.QueryOpts{IncludeTotal: true})
		require.NoError(t, err)
		defer func() { _ = res.Sub.Close() }()
		assert.Equal(t, []string{a + "/r1", b + "/r1", c + "/r1", c + "/r9"}, idsOfInitial(res.Initial),
			"the snapshot holds every object's records in sort order")
		assert.Equal(t, 4, res.Total)
		for _, row := range res.Initial {
			assert.True(t, strings.HasPrefix(row.GetString("id"), row.GetString("_objectId")+"/"), "row %s", row.GetString("id"))
		}
		mirror := mirrorOf(res.Initial)

		// A create on A, then one on B, each arrives as its own Added.
		lab.putRows(t, ctx, sample(a, "r2", "x", 4))
		ev := receiveOne(t, res.Sub, 2*time.Second)
		applyToMirror(mirror, ev)
		assert.Equal(t, []string{a + "/r2"}, addedIds(ev.Added))
		assert.Empty(t, ev.Updated)
		assert.Empty(t, ev.Removed)
		if len(ev.Added) == 1 {
			assert.Equal(t, a, ev.Added[0].Doc.GetString("_objectId"))
			assert.Equal(t, "r2-4", ev.Added[0].Doc.GetString("label"))
		}
		lab.putRows(t, ctx, sample(b, "r2", "y", 5))
		ev = receiveOne(t, res.Sub, 2*time.Second)
		applyToMirror(mirror, ev)
		assert.Equal(t, []string{b + "/r2"}, addedIds(ev.Added))
		if len(ev.Added) == 1 {
			assert.Equal(t, b, ev.Added[0].Doc.GetString("_objectId"))
		}

		// A field change on C is an Updated.
		lab.set(t, ctx, c, lab.samples, "r1", "label", "c1-edited")
		ev = receiveOne(t, res.Sub, 2*time.Second)
		applyToMirror(mirror, ev)
		assert.Empty(t, ev.Added)
		assert.Empty(t, ev.Removed)
		rec, where := subRecordFor(ev, c+"/r1")
		require.NotNil(t, rec, "event ids %v", eventIds(ev))
		assert.Equal(t, "updated", where)
		assert.Equal(t, "c1-edited", rec.Doc.GetString("label"))
		assert.Equal(t, c, rec.Doc.GetString("_objectId"))
		assert.NotEmpty(t, rec.Ops, "an update carries its ops")

		// A record delete on B is a Removed.
		lab.deleteSample(t, ctx, b, "r1")
		ev = receiveOne(t, res.Sub, 2*time.Second)
		applyToMirror(mirror, ev)
		assert.Empty(t, ev.Added)
		assert.Empty(t, ev.Updated)
		assert.Equal(t, []space.RemovedRecord{{Id: b + "/r1", Reason: space.RemoveDeleted}}, ev.Removed)

		// A device-local field write is an Updated too.
		local, err := sp.Modify(ctx, space.ModifyBatch{
			ObjectId: a, Dataset: lab.samples, Scope: space.ScopeLocal,
			Records: []space.RecordModify{{Id: "r1", Ops: []space.Op{{Type: space.OpSet, Path: "seen", Value: true}}}},
		})
		require.NoError(t, err)
		require.Empty(t, local.Rejections)
		ev = receiveOne(t, res.Sub, 2*time.Second)
		applyToMirror(mirror, ev)
		rec, where = subRecordFor(ev, a+"/r1")
		require.NotNil(t, rec, "event ids %v", eventIds(ev))
		assert.Equal(t, "updated", where)
		assert.True(t, rec.Doc.GetBool("seen"))

		// Writes to another shared dataset and to a per-object one stay
		// out.
		lab.put(t, ctx, a, lab.events, "", map[string]any{"label": "event"})
		lab.put(t, ctx, b, lab.plain, "r1", map[string]any{"label": "plain"})
		assertNoSubEvent(t, res.Sub, 200*time.Millisecond)

		// The mirror built from the snapshot and the events equals a
		// fresh read.
		rows, err := sp.QueryDataset(lab.samples).Sort("score", "id").All(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, idsOfInitial(rows), mirrorIds(mirror))
		for _, row := range rows {
			if m := mirror[row.GetString("id")]; assert.NotNil(t, m, row.GetString("id")) {
				assert.Equal(t, row.GetString("label"), m.GetString("label"), row.GetString("id"))
			}
		}
	})

	t.Run("Filter", func(t *testing.T) {
		res, err := sp.QueryDataset(lab.samples).
			Filter(map[string]any{"score": map[string]any{"$gte": 10}}).
			Sort("score", "id").
			Subscribe(ctx, space.QueryOpts{})
		require.NoError(t, err)
		defer func() { _ = res.Sub.Close() }()
		assert.Equal(t, []string{c + "/r9"}, idsOfInitial(res.Initial), "the snapshot holds matching rows only")

		// A row outside the filter, on any object, stays out.
		lab.putRows(t, ctx, sample(a, "r3", "x", 6))
		assertNoSubEvent(t, res.Sub, 200*time.Millisecond)

		lab.putRows(t, ctx, sample(b, "r3", "y", 15))
		ev := receiveOne(t, res.Sub, 2*time.Second)
		assert.Equal(t, []string{b + "/r3"}, addedIds(ev.Added))
		assert.Empty(t, ev.Removed)

		// An update moving a row into the filter adds it; one moving a
		// row out removes it as filtered out.
		lab.set(t, ctx, a, lab.samples, "r3", "score", 25)
		ev = receiveOne(t, res.Sub, 2*time.Second)
		assert.Equal(t, []string{a + "/r3"}, addedIds(ev.Added))
		lab.set(t, ctx, b, lab.samples, b+"/r3", "score", 1)
		ev = receiveOne(t, res.Sub, 2*time.Second)
		assert.Empty(t, ev.Added)
		assert.Equal(t, []space.RemovedRecord{{Id: b + "/r3", Reason: space.RemoveFilteredOut}}, ev.Removed)
		lab.set(t, ctx, c, lab.samples, "r9", "label", "c9-edited")
		ev = receiveOne(t, res.Sub, 2*time.Second)
		assert.Equal(t, []string{c + "/r9"}, addedIds(ev.Updated))

		// A filter on `_objectId` follows one object.
		byC, err := sp.QueryDataset(lab.samples).Filter(map[string]any{"_objectId": c}).Sort("score", "id").
			Subscribe(ctx, space.QueryOpts{})
		require.NoError(t, err)
		defer func() { _ = byC.Sub.Close() }()
		assert.Equal(t, []string{c + "/r1", c + "/r9"}, idsOfInitial(byC.Initial))
		lab.putRows(t, ctx, sample(a, "r4", "x", 7))
		assertNoSubEvent(t, byC.Sub, 200*time.Millisecond)
		lab.putRows(t, ctx, sample(c, "r2", "y", 12))
		ev = receiveOne(t, byC.Sub, 2*time.Second)
		assert.Equal(t, []string{c + "/r2"}, addedIds(ev.Added))
	})

	// Live now: a/r1 1, b/r3 1, c/r1 3, a/r2 4, b/r2 5, a/r4 7, c/r2 12,
	// a/r3 25, c/r9 30.
	t.Run("Displacement", func(t *testing.T) {
		top, err := sp.QueryDataset(lab.samples).Sort("-score", "id").Limit(3).All(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{c + "/r9", a + "/r3", c + "/r2"}, idsOfInitial(top))

		res, err := sp.QueryDataset(lab.samples).Sort("-score", "id").Limit(2).
			Subscribe(ctx, space.QueryOpts{IncludeTotal: true, DriftBudgetPercent: 100})
		require.NoError(t, err)
		defer func() { _ = res.Sub.Close() }()
		assert.Equal(t, []string{c + "/r9", a + "/r3"}, idsOfInitial(res.Initial))
		assert.Equal(t, 9, res.Total)
		assert.True(t, res.HasNext)

		// B's new row enters the window and displaces A's.
		lab.putRows(t, ctx, sample(b, "r4", "x", 28))
		ev := receiveOne(t, res.Sub, 2*time.Second)
		assert.Equal(t, []string{b + "/r4"}, addedIds(ev.Added))
		assert.Equal(t, []space.RemovedRecord{{Id: a + "/r3", Reason: space.RemoveDisplaced}}, ev.Removed)

		// A row below the window changes nothing.
		lab.putRows(t, ctx, sample(c, "r3", "x", 2))
		assertNoSubEvent(t, res.Sub, 200*time.Millisecond)

		// C's top row deleted: A's row returns to the window.
		lab.deleteSample(t, ctx, c, "r9")
		ev = receiveOne(t, res.Sub, 2*time.Second)
		assert.Equal(t, []space.RemovedRecord{{Id: c + "/r9", Reason: space.RemoveDeleted}}, ev.Removed)
		assert.Equal(t, []string{a + "/r3"}, addedIds(ev.Added))
		assert.NoError(t, res.Sub.Err())
	})

	// Runs last: it changes the samples schema. A subscription open
	// across the change keeps receiving every object's writes, the new
	// field included.
	t.Run("SchemaChange", func(t *testing.T) {
		res, err := sp.QueryDataset(lab.samples).Filter(map[string]any{"kind": "x"}).Subscribe(ctx, space.QueryOpts{})
		require.NoError(t, err)
		defer func() { _ = res.Sub.Close() }()
		before := len(res.Initial)

		defs, err := sp.Types().Datasets(ctx, lab.typeId)
		require.NoError(t, err)
		var defId string
		for _, d := range defs {
			if d.Key == "samples" {
				defId = d.Id
			}
		}
		require.NotEmpty(t, defId)
		_, err = sp.Types().AddDatasetField(ctx, lab.typeId, defId, space.DatasetFieldDraft{
			Key: "note", Kind: space.PropertyKindString, MutableBy: space.MutableByAnyone,
		})
		require.NoError(t, err)
		assertNoSubEvent(t, res.Sub, 200*time.Millisecond)

		lab.set(t, ctx, a, lab.samples, "r1", "note", "after")
		ev := receiveOne(t, res.Sub, 2*time.Second)
		rec, where := subRecordFor(ev, a+"/r1")
		require.NotNil(t, rec, "event ids %v", eventIds(ev))
		assert.Equal(t, "updated", where)
		assert.Equal(t, "after", rec.Doc.GetString("note"))
		lab.put(t, ctx, b, lab.samples, "r5", map[string]any{"label": "r5", "score": 50, "kind": "x", "note": "fresh"})
		ev = receiveOne(t, res.Sub, 2*time.Second)
		assert.Equal(t, []string{b + "/r5"}, addedIds(ev.Added))

		rows, err := sp.QueryDataset(lab.samples).Filter(map[string]any{"kind": "x"}).All(ctx)
		require.NoError(t, err)
		assert.Len(t, rows, before+1)
		row, err := sp.QueryDataset(lab.samples).Filter(map[string]any{"note": "fresh"}).One(ctx)
		require.NoError(t, err)
		assert.Equal(t, b+"/r5", row.GetString("id"))
	})
}

// TestE2E_SharedDatasetsSpace_ObjectDelete: deleting an object purges
// its rows from the per-space collection and every live query on the
// dataset — space-wide or the object's own — receives a Removed for
// each of its live rows, and nothing for the other objects.
func TestE2E_SharedDatasetsSpace_ObjectDelete(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	lab := openSharedLab(t, ctx, "SharedSpaceObjDelete")
	sp := lab.sp

	a, b, c := lab.newObject(t, ctx), lab.newObject(t, ctx), lab.newObject(t, ctx)
	seed := []labRow{
		sample(a, "r1", "x", 1), sample(a, "r2", "y", 2), sample(a, "r3", "x", 3),
		sample(b, "r1", "y", 4), sample(b, "r2", "x", 5),
		sample(c, "r1", "x", 6),
	}
	lab.putRows(t, ctx, seed...)
	lab.deleteSample(t, ctx, a, "r3")
	live := withoutKeys(seed, a+"/r3")
	eventOf := map[string]string{}
	for _, obj := range []string{a, b} {
		res := lab.put(t, ctx, obj, lab.events, "", map[string]any{"label": "event"})
		require.Len(t, res.RecordIds, 1)
		eventOf[obj] = res.RecordIds[0]
	}

	all, err := sp.QueryDataset(lab.samples).Sort("score", "id").Limit(20).
		Subscribe(ctx, space.QueryOpts{IncludeTotal: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = all.Sub.Close() })
	assert.Equal(t, sortedKeys(live, false), idsOfInitial(all.Initial))
	assert.Equal(t, len(live), all.Total)

	events, err := sp.QueryDataset(lab.events).Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = events.Sub.Close() })
	assert.ElementsMatch(t, []string{eventOf[a], eventOf[b]}, idsOfInitial(events.Initial))

	ownA, err := sp.Query(a, lab.samples).Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ownA.Sub.Close() })
	assert.ElementsMatch(t, []string{a + "/r1", a + "/r2"}, idsOfInitial(ownA.Initial))

	onlyB, err := sp.QueryDataset(lab.samples).Filter(map[string]any{"_objectId": b}).Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = onlyB.Sub.Close() })
	assert.ElementsMatch(t, []string{b + "/r1", b + "/r2"}, idsOfInitial(onlyB.Initial))

	require.NoError(t, sp.Objects().Delete(ctx, a))

	removedA := []space.RemovedRecord{
		{Id: a + "/r1", Reason: space.RemoveDeleted},
		{Id: a + "/r2", Reason: space.RemoveDeleted},
	}
	ev := receiveOne(t, all.Sub, 2*time.Second)
	assert.ElementsMatch(t, removedA, ev.Removed, "the space-wide query drops A's live rows; event ids %v", eventIds(ev))
	assert.Empty(t, ev.Added)
	assert.Empty(t, ev.Updated)

	ev = receiveOne(t, events.Sub, 2*time.Second)
	assert.Equal(t, []space.RemovedRecord{{Id: eventOf[a], Reason: space.RemoveDeleted}}, ev.Removed,
		"the purge reaches every shared dataset's queries")

	ev = receiveOne(t, ownA.Sub, 2*time.Second)
	assert.ElementsMatch(t, removedA, ev.Removed, "A's own query drops its rows; event ids %v", eventIds(ev))

	assertNoSubEvent(t, onlyB.Sub, 200*time.Millisecond)
	assertNoSubEvent(t, all.Sub, 200*time.Millisecond)

	// The space reads B's and C's rows only, tombstones included.
	rest := rowsWhere(live, func(r labRow) bool { return r.obj != a })
	assertRows(t, lab.spaceRows(t, ctx, lab.samples), rest)
	n, err := sp.QueryDataset(lab.samples).Projection(space.ProjectionOpts{IncludeDeleted: true}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, len(rest), n, "A's tombstone is purged too")
	n, err = sp.QueryDataset(lab.samples).Filter(map[string]any{"_objectId": a}).Count(ctx)
	require.NoError(t, err)
	assert.Zero(t, n)
	evRows, err := sp.QueryDataset(lab.events).All(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{eventOf[b]}, idsOfInitial(evRows))
	assert.Equal(t, totalsBy(rest, byObject), groupTotals(t, ctx, sp.AggregateDataset(lab.samples,
		`[{"$group": {"_id": "$_objectId", "n": {"$count": {}}, "total": {"$sum": "$score"}}}]`)))

	// The space-wide query stays live; a fresh one starts without A.
	lab.putRows(t, ctx, sample(c, "r2", "y", 7))
	ev = receiveOne(t, all.Sub, 2*time.Second)
	assert.Equal(t, []string{c + "/r2"}, addedIds(ev.Added))
	assert.NoError(t, all.Sub.Err())
	fresh, err := sp.QueryDataset(lab.samples).Sort("score", "id").Snapshot(ctx, space.QueryOpts{})
	require.NoError(t, err)
	assert.Equal(t, sortedKeys(append(rest, sample(c, "r2", "y", 7)), false), idsOfInitial(fresh.Initial))
}

// TestE2E_SharedDatasetsSpace_SecondDevice: a second device of the
// account reads the shared dataset across objects with the same
// composite ids and values, and a space-wide subscription opened there
// receives the first device's later writes and its object delete.
func TestE2E_SharedDatasetsSpace_SecondDevice(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("cold-sync e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	provider := newFixedSeedProvider(t)
	open := func() *anysyncsdk.SDK {
		sdk, oerr := anysyncsdk.Open(ctx, config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
		}, provider)
		require.NoError(t, oerr)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk
	}

	sdkA := open()
	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "SharedSpaceSync"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatal(err)
	}
	labA := &sharedLab{sdk: sdkA, sp: spA}
	labA.declare(t, ctx)
	o1, o2, o3 := labA.newObject(t, ctx), labA.newObject(t, ctx), labA.newObject(t, ctx)
	seed := []labRow{
		sample(o1, "r1", "x", 1), sample(o1, "r2", "y", 4),
		sample(o2, "r1", "y", 2), sample(o2, "r2", "x", 5),
		sample(o3, "r1", "x", 3),
	}
	labA.putRows(t, ctx, seed...)
	labA.deleteSample(t, ctx, o2, "r2")
	live := withoutKeys(seed, o2+"/r2")

	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = spA.SyncHeads(ctx)

	sdkB := open()
	var spB space.Space
	require.True(t, waitFor(ctx, 90*time.Second, 250*time.Millisecond, func() bool {
		_ = sdkB.Spaces().SyncSpaceList(ctx)
		got, gerr := sdkB.Spaces().Get(ctx, spA.Id())
		if gerr != nil {
			return false
		}
		spB = got
		return true
	}), "device B must see the space")
	labB := &sharedLab{sdk: sdkB, sp: spB, typeId: labA.typeId, partId: labA.partId,
		samples: labA.samples, events: labA.events, plain: labA.plain}

	// Device B reads every object's rows across the space, with no
	// per-object read.
	require.True(t, waitFor(ctx, 150*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		n, qerr := spB.QueryDataset(labA.samples).Count(ctx)
		return qerr == nil && n == len(live)
	}), "device B must converge on the shared rows through QueryDataset")

	rowsA := labA.spaceRows(t, ctx, labA.samples)
	rowsB := labB.spaceRows(t, ctx, labA.samples)
	assertRows(t, rowsB, live)
	assert.ElementsMatch(t, mirrorIds(rowsA), mirrorIds(rowsB), "both devices read the same composite ids")
	tombs, err := spB.QueryDataset(labA.samples).Projection(space.ProjectionOpts{IncludeDeleted: true}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, len(seed), tombs, "the tombstone syncs under its composite id")
	const groupByObject = `[{"$group": {"_id": "$_objectId", "n": {"$count": {}}, "total": {"$sum": "$score"}}}]`
	assert.Equal(t, totalsBy(live, byObject), groupTotals(t, ctx, spB.AggregateDataset(labA.samples, groupByObject)))

	// Space-wide subscriptions on device B, opened before device A's
	// next writes.
	subB, err := spB.QueryDataset(labA.samples).Sort("score", "id").Subscribe(ctx, space.QueryOpts{IncludeTotal: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = subB.Sub.Close() })
	assert.Equal(t, sortedKeys(live, false), idsOfInitial(subB.Initial))
	assert.Equal(t, len(live), subB.Total)
	onlyO1, err := spB.QueryDataset(labA.samples).Filter(map[string]any{"_objectId": o1}).Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = onlyO1.Sub.Close() })
	assert.ElementsMatch(t, []string{o1 + "/r1", o1 + "/r2"}, idsOfInitial(onlyO1.Initial))

	// Device A: a create on o1, an update on o2, a delete and a create
	// on o3.
	labA.putRows(t, ctx, sample(o1, "r3", "x", 9))
	labA.set(t, ctx, o2, labA.samples, "r1", "label", "o2-edited")
	labA.deleteSample(t, ctx, o3, "r1")
	labA.putRows(t, ctx, sample(o3, "r2", "y", 6))
	want := []labRow{
		sample(o1, "r1", "x", 1), sample(o1, "r2", "y", 4), sample(o1, "r3", "x", 9),
		{obj: o2, id: "r1", kind: "y", score: 2, label: "o2-edited"},
		sample(o3, "r2", "y", 6),
	}

	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spA.SyncHeads(ctx)
		_ = spB.SyncHeads(ctx)
		rows, qerr := spB.QueryDataset(labA.samples).All(ctx)
		if qerr != nil || len(rows) != len(want) {
			return false
		}
		got := mirrorOf(rows)
		edited := got[o2+"/r1"]
		return got[o1+"/r3"] != nil && got[o3+"/r2"] != nil && edited != nil && edited.GetString("label") == "o2-edited"
	}), "device B must converge on device A's writes")
	assertRows(t, labB.spaceRows(t, ctx, labA.samples), want)

	log := newSubLog()
	log.receiveUntil(t, subB.Sub, 30*time.Second, func() bool {
		edited := log.updated[o2+"/r1"]
		_, removed := log.removed[o3+"/r1"]
		return log.added[o1+"/r3"] != nil && log.added[o3+"/r2"] != nil &&
			edited != nil && edited.GetString("label") == "o2-edited" && removed
	})
	assert.Equal(t, space.RemoveDeleted, log.removed[o3+"/r1"])
	for _, id := range log.ids {
		assert.True(t, strings.HasPrefix(id, o1+"/") || strings.HasPrefix(id, o2+"/") || strings.HasPrefix(id, o3+"/"),
			"device B's subscription received %q", id)
	}
	assert.Equal(t, o1, log.added[o1+"/r3"].GetString("_objectId"))
	assert.Equal(t, o3, log.added[o3+"/r2"].GetString("_objectId"))

	o1Log := newSubLog()
	o1Log.receiveUntil(t, onlyO1.Sub, 10*time.Second, func() bool { return o1Log.added[o1+"/r3"] != nil })
	for _, id := range o1Log.ids {
		assert.True(t, strings.HasPrefix(id, o1+"/"), "the o1 subscription received %q", id)
	}

	// Device A deletes o3: device B's space-wide subscription drops its
	// live row and the space reads o1's and o2's rows only.
	require.NoError(t, spA.Objects().Delete(ctx, o3))
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spA.SyncHeads(ctx)
		_ = spB.SyncHeads(ctx)
		return labB.storedRows(t, ctx, labA.samples, o3) == 0
	}), "device B must purge o3's shared rows")
	log.receiveUntil(t, subB.Sub, 10*time.Second, func() bool {
		_, gone := log.removed[o3+"/r2"]
		return gone
	})
	assert.Equal(t, space.RemoveDeleted, log.removed[o3+"/r2"])
	assertRows(t, labB.spaceRows(t, ctx, labA.samples), rowsWhere(want, func(r labRow) bool { return r.obj != o3 }))
	assert.NoError(t, subB.Sub.Err())
}

// TestE2E_SharedDatasetsSpace_Restart: after an SDK restart on the same
// data dir, the shared dataset reads across objects — records,
// tombstones, aggregation, a subscription's snapshot — before any
// per-object access.
func TestE2E_SharedDatasetsSpace_Restart(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)

	dataDir := t.TempDir()
	provider := newFixedSeedProvider(t)
	cfg := config.Config{
		Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// First boot: declare, write on three objects, delete one record.
	sdk1, err := anysyncsdk.Open(ctx, cfg, provider)
	require.NoError(t, err)
	sp1, err := sdk1.Spaces().Create(ctx, space.CreateRequest{Name: "SharedSpaceRestart"})
	if err != nil {
		_ = sdk1.Close()
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatal(err)
	}
	lab1 := &sharedLab{sdk: sdk1, sp: sp1}
	lab1.declare(t, ctx)
	a, b, c := lab1.newObject(t, ctx), lab1.newObject(t, ctx), lab1.newObject(t, ctx)
	seed := []labRow{
		sample(a, "r1", "x", 1), sample(a, "r2", "y", 2),
		sample(b, "r1", "y", 3), sample(b, "r2", "x", 4),
		sample(c, "r1", "x", 5),
	}
	lab1.putRows(t, ctx, seed...)
	lab1.deleteSample(t, ctx, a, "r2")
	live := withoutKeys(seed, a+"/r2")
	spaceId := sp1.Id()
	require.NoError(t, sdk1.Close())

	// Second boot: the first reads are across objects.
	sdk2, err := anysyncsdk.Open(ctx, cfg, provider)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk2.Close() })
	sp2, err := sdk2.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)
	lab := &sharedLab{sdk: sdk2, sp: sp2, typeId: lab1.typeId, partId: lab1.partId,
		samples: lab1.samples, events: lab1.events, plain: lab1.plain}

	rows, err := sp2.QueryDataset(lab.samples).Sort("score", "id").All(ctx)
	require.NoError(t, err, "a shared dataset reads across objects right after a restart")
	assert.Equal(t, sortedKeys(live, false), idsOfInitial(rows))
	assertRows(t, mirrorOf(rows), live)
	n, err := sp2.QueryDataset(lab.samples).Projection(space.ProjectionOpts{IncludeDeleted: true}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, len(seed), n, "the tombstone survives the restart")
	assert.Equal(t, totalsBy(live, byObject), groupTotals(t, ctx, sp2.AggregateDataset(lab.samples,
		`[{"$group": {"_id": "$_objectId", "n": {"$count": {}}, "total": {"$sum": "$score"}}}]`)))

	sub, err := sp2.QueryDataset(lab.samples).Sort("score", "id").Limit(10).Subscribe(ctx, space.QueryOpts{IncludeTotal: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Sub.Close() })
	assert.Equal(t, sortedKeys(live, false), idsOfInitial(sub.Initial))
	assert.Equal(t, len(live), sub.Total)

	// A write after the restart reaches the space-wide subscription.
	lab.putRows(t, ctx, sample(b, "r3", "x", 6))
	ev := receiveOne(t, sub.Sub, 5*time.Second)
	assert.Equal(t, []string{b + "/r3"}, addedIds(ev.Added))
	assertRows(t, lab.spaceRows(t, ctx, lab.samples), append(live, sample(b, "r3", "x", 6)))
}
