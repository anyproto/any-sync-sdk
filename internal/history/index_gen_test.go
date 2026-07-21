package history

import (
	"context"
	"errors"
	"fmt"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedLegacyGen1 fakes the pre-merge on-disk state for testObjectId:
// `__history` rows WITHOUT recIds, a populated `__history_recs`
// collection, and a stale=false meta row with no gen field.
func seedLegacyGen1(t *testing.T, ix *Index, changes int) {
	t.Helper()
	ctx := context.Background()
	hist, err := ix.db.Collection(ctx, testObjectId+HistoryCollectionSuffix)
	require.NoError(t, err)
	recs, err := ix.db.Collection(ctx, testObjectId+HistoryRecsCollectionSuffix)
	require.NoError(t, err)
	a := &anyenc.Arena{}
	for i := 1; i <= changes; i++ {
		o := fmt.Sprintf("o%03d", i)
		row := a.NewObject()
		row.Set("id", a.NewString(o))
		row.Set("c", a.NewString(fmt.Sprintf("cid-%03d", i)))
		row.Set("ds", a.NewString("notes"))
		row.Set("author", a.NewString("author-a"))
		row.Set("ts", a.NewNumberFloat64(float64(1700000000+i)))
		row.Set("n", a.NewNumberInt(1))
		require.NoError(t, hist.UpsertOne(ctx, row))
		rrow := a.NewObject()
		rrow.Set("id", a.NewString("n1\x00"+o))
		rrow.Set("o", a.NewString(o))
		rrow.Set("c", a.NewString(fmt.Sprintf("cid-%03d", i)))
		require.NoError(t, recs.UpsertOne(ctx, rrow))
	}
	row := a.NewObject()
	row.Set("id", a.NewString(testObjectId))
	row.Set("sp", a.NewString(testSpaceId))
	row.Set("stale", a.NewFalse())
	require.NoError(t, ix.meta.UpsertOne(ctx, row))
}

// TestGenUpgradeFromLegacyLayout pins the gen-1 → gen-2 migration: a
// gen-less meta row fails the freshness gate, the upgrade backfill
// rewrites rows with recIds, stamps the current gen, and drops the
// legacy `__history_recs` collection.
func TestGenUpgradeFromLegacyLayout(t *testing.T) {
	ctx := context.Background()
	ix := newTestIndex(t)
	seedLegacyGen1(t, ix, 2)

	// Gate: legacy state must read as needing a backfill.
	stale, err := ix.IsStale(ctx, testObjectId)
	require.NoError(t, err)
	assert.True(t, stale, "gen-less meta row must fail the freshness gate")
	// And it has to: gen-1 rows carry no recIds, so the record filter
	// finds nothing pre-upgrade.
	out, _, err := ix.ListChanges(ctx, Filter{ObjectId: testObjectId, Dataset: "notes", RecordId: "n1"}, 10, "")
	require.NoError(t, err)
	assert.Empty(t, out, "record filter cannot serve gen-1 rows")

	// Upgrade = the ordinary backfill.
	b := newTreeBuilder(t)
	b.add("notes", upsert(t, "n1", `{"title":"a"}`))
	b.add("notes", upsert(t, "n1", `{"title":"b"}`))
	require.NoError(t, ix.Backfill(ctx, b.tree, testObjectId, 0))

	stale, err = ix.IsStale(ctx, testObjectId)
	require.NoError(t, err)
	assert.False(t, stale, "backfill must stamp the current gen")

	out, _, err = ix.ListChanges(ctx, Filter{ObjectId: testObjectId, Dataset: "notes", RecordId: "n1"}, 10, "")
	require.NoError(t, err)
	assert.Len(t, out, 2, "record filter must serve rewritten rows")

	_, err = ix.db.OpenCollection(ctx, testObjectId+HistoryRecsCollectionSuffix)
	assert.True(t, errors.Is(err, anystore.ErrCollectionNotFound),
		"legacy __history_recs must be dropped by the upgrade backfill")
}

// TestWarmObjectFreshWithoutBackfill pins that an object born under
// the current gen is fresh from its first warm IndexChange — the
// birth meta stamp spares the first history query a redundant
// backfill.
func TestWarmObjectFreshWithoutBackfill(t *testing.T) {
	ctx := context.Background()
	ix := newTestIndex(t)
	indexOne(t, ix, idxChange(1, "notes", "n1"))

	has, err := ix.HasState(ctx, "obj-1")
	require.NoError(t, err)
	assert.True(t, has, "warm-created object must have a meta stamp")
	stale, err := ix.IsStale(ctx, "obj-1")
	require.NoError(t, err)
	assert.False(t, stale)

	out, _, err := ix.ListChanges(ctx, Filter{ObjectId: "obj-1", Dataset: "notes", RecordId: "n1"}, 10, "")
	require.NoError(t, err)
	assert.Len(t, out, 1)
}

// TestLegacyWarmRowsNotFresh pins the dangerous corner: rows exist
// (written warm under gen 1) but no meta row — HasState must NOT
// treat bare rows as state, or the recIds-less rows would serve a
// silently empty record filter forever.
func TestLegacyWarmRowsNotFresh(t *testing.T) {
	ctx := context.Background()
	ix := newTestIndex(t)
	hist, err := ix.db.Collection(ctx, testObjectId+HistoryCollectionSuffix)
	require.NoError(t, err)
	a := &anyenc.Arena{}
	row := a.NewObject()
	row.Set("id", a.NewString("o001"))
	row.Set("c", a.NewString("cid-001"))
	row.Set("ds", a.NewString("notes"))
	require.NoError(t, hist.UpsertOne(ctx, row))

	has, err := ix.HasState(ctx, testObjectId)
	require.NoError(t, err)
	assert.False(t, has, "bare gen-1 rows must not count as index state")
}

// TestRecIdsDeduped: a change with several ops on the same record
// stores the record id once in recIds and lists once per change.
func TestRecIdsDeduped(t *testing.T) {
	ctx := context.Background()
	ix := newTestIndex(t)
	ch := idxChange(1, "notes", "n1")
	ch.Records = append(ch.Records, ch.Records[0]) // second op batch on n1
	indexOne(t, ix, ch)

	out, _, err := ix.ListChanges(ctx, Filter{ObjectId: "obj-1", Dataset: "notes", RecordId: "n1"}, 10, "")
	require.NoError(t, err)
	require.Len(t, out, 1)

	hist, err := ix.historyColl(ctx, "obj-1", false)
	require.NoError(t, err)
	doc, err := hist.FindId(ctx, "o001")
	require.NoError(t, err)
	assert.Len(t, doc.Value().GetArray("recIds"), 1, "recIds must be deduped")
}
