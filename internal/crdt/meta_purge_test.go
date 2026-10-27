package crdt

import (
	"fmt"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type observedMetaCollection struct {
	anystore.Collection
	onFind func(anystore.Query)
}

func (c observedMetaCollection) Find(filter any) anystore.Query {
	q := c.Collection.Find(filter)
	c.onFind(q)
	return q
}

func TestPurgeSpaceMeta_SpaceIndexAfterUpgrade(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	db, err := anystore.Open(ctx, dbPath, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if db != nil {
			_ = db.Close()
		}
	})
	coll, err := db.Collection(ctx, MetaCollectionName)
	require.NoError(t, err)
	// Reproduce a persisted database with only the pre-upgrade indexes.
	ensurePreSpaceMetaIndexes(t, coll)
	seedUnrelatedMetaRows(t, db, coll, 1000)
	for _, doc := range []string{
		`{"id":"current","sp":"deleted-space","q":10,"as":20}`,
		`{"id":"add-only","sp":"deleted-space","q":10}`,
		`{"id":"apply-only","sp":"deleted-space","as":20}`,
		`{"id":"no-watermarks","sp":"deleted-space"}`,
		`{"id":"null-watermarks","sp":"deleted-space","q":null,"as":null}`,
		`{"id":"legacy","q":10}`,
		`{"id":"unrelated-legacy","q":10}`,
	} {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(doc)))
	}
	require.NoError(t, PersistSpaceMaxAddSeq(ctx, coll, "deleted-space", 20))
	require.NoError(t, PersistSpaceMaxAddSeq(ctx, coll, "live-space", 30))
	require.NoError(t, db.Close())

	db, err = anystore.Open(ctx, dbPath, nil)
	require.NoError(t, err)
	// Controller construction installs metadata indexes before startup GC.
	// A second controller exercises idempotent opens on the shared DB.
	_, err = NewController(ctx, "first-controller", db)
	require.NoError(t, err)
	_, err = NewController(ctx, "second-controller", db)
	require.NoError(t, err)
	coll, err = db.Collection(ctx, MetaCollectionName)
	require.NoError(t, err)

	// Repeat after deletion: clean tombstones are swept on every startup too.
	for pass := range 2 {
		var plans []anystore.Explain
		observed := observedMetaCollection{Collection: coll, onFind: func(q anystore.Query) {
			plan, explainErr := q.Explain(ctx)
			require.NoError(t, explainErr)
			plans = append(plans, plan)
		}}
		require.NoError(t, PurgeSpaceMeta(ctx, observed, "deleted-space", []string{"legacy"}))
		require.Len(t, plans, 1)
		assert.NotContains(t, plans[0].Sql, "FullScan", "purge pass %d: %s", pass, plans[0].Plan)
		usedSpaceIndex := false
		for _, index := range plans[0].Indexes {
			usedSpaceIndex = usedSpaceIndex || (index.Used && index.Name == "idx__meta_sp")
		}
		assert.True(t, usedSpaceIndex, "purge pass %d: %s", pass, plans[0].Plan)
		for _, id := range []string{"current", "add-only", "apply-only", "no-watermarks", "null-watermarks", "legacy", SpaceMetaKey("deleted-space")} {
			_, findErr := coll.FindId(ctx, id)
			assert.ErrorIs(t, findErr, anystore.ErrDocNotFound, id)
		}
		count, countErr := coll.Find(nil).Count(ctx)
		require.NoError(t, countErr)
		assert.Equal(t, 1002, count, "other spaces and unscoped rows survive")
	}
}

// Already-purged spaces remain tombstoned in the tech-space registry.
// Their startup cleanup should not scan every live object's metadata again.
func BenchmarkPurgeSpaceMeta_AlreadyPurged(b *testing.B) {
	for _, rows := range []int{1000, 100000} {
		b.Run(fmt.Sprintf("live_rows_%d", rows), func(b *testing.B) {
			db, err := anystore.Open(ctx, filepath.Join(b.TempDir(), "meta.db"), nil)
			require.NoError(b, err)
			b.Cleanup(func() { _ = db.Close() })
			coll, err := db.Collection(ctx, MetaCollectionName)
			require.NoError(b, err)
			seedUnrelatedMetaRows(b, db, coll, rows)
			require.NoError(b, ensureMetaIndexes(ctx, coll))
			require.NoError(b, PurgeSpaceMeta(ctx, coll, "deleted-space", nil))
			b.ReportAllocs()
			for b.Loop() {
				if err := PurgeSpaceMeta(ctx, coll, "deleted-space", nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Measure the one-time index installation separately from steady-state cleanup.
// Each iteration reopens a fresh database containing only the old indexes.
func BenchmarkEnsureMetaIndexes_Upgrade(b *testing.B) {
	for _, rows := range []int{1000, 100000} {
		b.Run(fmt.Sprintf("live_rows_%d", rows), func(b *testing.B) {
			b.ReportAllocs()
			b.StopTimer()
			for range b.N {
				func() {
					dbPath := filepath.Join(b.TempDir(), "meta.db")
					db, err := anystore.Open(ctx, dbPath, nil)
					require.NoError(b, err)
					defer func() {
						if db != nil {
							_ = db.Close()
						}
					}()
					coll, err := db.Collection(ctx, MetaCollectionName)
					require.NoError(b, err)
					seedUnrelatedMetaRows(b, db, coll, rows)
					ensurePreSpaceMetaIndexes(b, coll)
					require.NoError(b, db.Close())
					db, err = anystore.Open(ctx, dbPath, nil)
					require.NoError(b, err)
					coll, err = db.Collection(ctx, MetaCollectionName)
					require.NoError(b, err)
					b.StartTimer()
					err = ensureMetaIndexes(ctx, coll)
					b.StopTimer()
					require.NoError(b, err)
				}()
			}
		})
	}
}

func ensurePreSpaceMetaIndexes(tb testing.TB, coll anystore.Collection) {
	tb.Helper()
	require.NoError(tb, coll.EnsureIndex(ctx,
		anystore.IndexInfo{Name: "idx__meta_sp_q", Fields: []string{metaSpaceIdKey, metaAddSeqKey}, Sparse: true},
		anystore.IndexInfo{Name: "idx__meta_sp_as", Fields: []string{metaSpaceIdKey, metaApplySeqKey}, Sparse: true},
	))
}

func seedUnrelatedMetaRows(tb testing.TB, db anystore.DB, coll anystore.Collection, count int) {
	tb.Helper()
	tx, err := db.WriteTx(ctx)
	require.NoError(tb, err)
	defer func() { _ = tx.Rollback() }()
	for i := range count {
		require.NoError(tb, PersistMeta(tx.Context(), coll, fmt.Sprintf("live-%d", i), uint64(i+1), uint64(i+1), nil, "live-space"))
	}
	require.NoError(tb, tx.Commit())
}
