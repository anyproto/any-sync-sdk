package crdt

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/require"
)

// Version-history feasibility benchmarks: "state at version X" is planned as
// an on-demand replay of DAG changes through a fresh Controller into an
// in-memory any-store scratch DB (docs/version-history-proposal.md). These
// benchmarks measure the Controller-apply half of that path (decrypt+decode
// are excluded) so the proposal's latency claims are grounded.

// genHistoryWorkload builds a deterministic change sequence in ascending
// versionId order: numRecords creates (5-field $set upserts) interleaved
// with single-field $set modifies on random existing records, plus
// occasional deletes when withDeletes is set.
func genHistoryWorkload(numChanges, numRecords int, withDeletes bool) []Change {
	g := newVersionGen()
	rng := rand.New(rand.NewSource(42))
	arena := &anyenc.Arena{}
	changes := make([]Change, 0, numChanges)
	created := 0
	for i := 0; i < numChanges; i++ {
		var ch Change
		switch {
		case created < numRecords && (created == 0 || rng.Intn(numChanges/numRecords) == 0):
			id := fmt.Sprintf("rec-%d", created)
			created++
			ch = makeUpsert(g.Next(), id, Op{
				Type: OpSet,
				Payload: recordPayload(arena, map[string]any{
					"name":   "record " + id,
					"count":  rng.Intn(1000),
					"author": "peer-a",
					"tags":   []string{"x", "y"},
					"meta":   map[string]any{"color": "red", "pinned": false},
				}),
			})
		case withDeletes && rng.Intn(200) == 0:
			id := fmt.Sprintf("rec-%d", rng.Intn(created))
			ch = makeChange(g.Next(), id, Op{Type: OpDelete})
		default:
			id := fmt.Sprintf("rec-%d", rng.Intn(created))
			var op Op
			switch rng.Intn(4) {
			case 0:
				op = Op{Type: OpSet, Path: []string{"count"}, Payload: arena.NewNumberInt(rng.Intn(1000))}
			case 1:
				op = Op{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString(fmt.Sprintf("edit %d", i))}
			case 2:
				op = Op{Type: OpInc, Path: []string{"count"}, Payload: arena.NewNumberInt(1)}
			default:
				op = Op{Type: OpSet, Path: []string{"meta", "color"}, Payload: arena.NewString("blue")}
			}
			ch = makeChange(g.Next(), id, op)
		}
		ch.ChangeId = fmt.Sprintf("cid-%d", i)
		ch.AddSeq = uint64(i + 1)
		changes = append(changes, ch)
	}
	return changes
}

func newScratchController(tb testing.TB) (*Controller, func()) {
	tb.Helper()
	st, _, done := newScratchControllerDB(tb)
	return st, done
}

func newScratchControllerDB(tb testing.TB) (*Controller, anystore.DB, func()) {
	tb.Helper()
	return newScratchControllerMode(tb, true)
}

// newScratchControllerMode opens the scratch store either in memory
// (transient view) or on disk (persisted materialization / cache).
func newScratchControllerMode(tb testing.TB, inMemory bool) (*Controller, anystore.DB, func()) {
	tb.Helper()
	var (
		db  anystore.DB
		err error
	)
	if inMemory {
		db, err = anystore.Open(ctx, ":memory:", &anystore.Config{InMemory: true})
	} else {
		db, err = anystore.Open(ctx, filepath.Join(tb.TempDir(), "scratch.db"), nil)
	}
	require.NoError(tb, err)
	st, err := NewController(ctx, "obj1", db, HandlerReg{Name: testDS, Handler: DefaultHandler{}, Schema: dynSchema})
	require.NoError(tb, err)
	return st, db, func() { _ = db.Close() }
}

// BenchmarkHistoryReplay_FullObject measures reconstructing a whole object
// at a past version: fresh in-memory Controller, apply N changes.
func BenchmarkHistoryReplay_FullObject(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("changes=%d", n), func(b *testing.B) {
			changes := genHistoryWorkload(n, n/100+1, false)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				st, done := newScratchController(b)
				for _, ch := range changes {
					if err := st.ApplyChange(ctx, ch); err != nil {
						b.Fatal(err)
					}
				}
				done()
			}
			b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "changes/s")
		})
	}
}

// BenchmarkHistoryReplay_FullObjectBatchedTx is the same replay wrapped in
// one outer WriteTx (any-store reuses a context-carried tx), which is how a
// history replay engine would actually run — one commit for the whole
// reconstruction instead of one per change.
func BenchmarkHistoryReplay_FullObjectBatchedTx(b *testing.B) {
	for _, mode := range []struct {
		name     string
		inMemory bool
	}{{"mem", true}, {"disk", false}} {
		for _, n := range []int{1_000, 10_000, 100_000} {
			b.Run(fmt.Sprintf("%s/changes=%d", mode.name, n), func(b *testing.B) {
				changes := genHistoryWorkload(n, n/100+1, false)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					st, db, done := newScratchControllerMode(b, mode.inMemory)
					tx, err := db.WriteTx(ctx)
					if err != nil {
						b.Fatal(err)
					}
					for _, ch := range changes {
						if err := st.ApplyChange(tx.Context(), ch); err != nil {
							b.Fatal(err)
						}
					}
					if err := tx.Commit(); err != nil {
						b.Fatal(err)
					}
					if !mode.inMemory {
						// Persisted-cache scenario: count the WAL checkpoint
						// into the file, not just the commit.
						if err := db.Flush(ctx, 0, anystore.FlushModeCheckpointFull); err != nil {
							b.Fatal(err)
						}
					}
					done()
				}
				b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "changes/s")
			})
		}
	}
}

// BenchmarkHistoryReplay_SingleRecordFiltered measures the record-filtered
// fast path: from a 100k-change object, reconstruct one record by applying
// only the changes that touch it (index lookup assumed done).
func BenchmarkHistoryReplay_SingleRecordFiltered(b *testing.B) {
	all := genHistoryWorkload(100_000, 1_000, false)
	const target = "rec-500"
	var filtered []Change
	for _, ch := range all {
		for _, rc := range ch.Records {
			if rc.Id == target {
				filtered = append(filtered, ch)
				break
			}
		}
	}
	b.Logf("changes touching %s: %d of %d", target, len(filtered), len(all))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st, done := newScratchController(b)
		for _, ch := range filtered {
			if err := st.ApplyChange(ctx, ch); err != nil {
				b.Fatal(err)
			}
		}
		done()
	}
}

// BenchmarkHistoryCache_HitOpenAndRead measures the cache-hit path for a
// persisted materialization: the 100k-change view was already replayed to a
// disk file; a hit = open the file, read one record, close. Compare against
// the cold replay cost in BenchmarkHistoryReplay_FullObjectBatchedTx.
func BenchmarkHistoryCache_HitOpenAndRead(b *testing.B) {
	changes := genHistoryWorkload(100_000, 1_000, false)
	path := filepath.Join(b.TempDir(), "view-cache.db")
	db, err := anystore.Open(ctx, path, nil)
	require.NoError(b, err)
	st, err := NewController(ctx, "obj1", db, HandlerReg{Name: testDS, Handler: DefaultHandler{}, Schema: dynSchema})
	require.NoError(b, err)
	tx, err := db.WriteTx(ctx)
	require.NoError(b, err)
	for _, ch := range changes {
		require.NoError(b, st.ApplyChange(tx.Context(), ch))
	}
	require.NoError(b, tx.Commit())
	require.NoError(b, db.Close())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cdb, err := anystore.Open(ctx, path, nil)
		if err != nil {
			b.Fatal(err)
		}
		coll, err := cdb.OpenCollection(ctx, "obj1_"+testDS)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := coll.FindId(ctx, "rec-500"); err != nil {
			b.Fatal(err)
		}
		if err := cdb.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// stripLocalSeqs removes the peer-local bookkeeping stamps whose values
// depend on how many applies the controller has executed (_applySeq is
// allocated per apply), which legitimately differ between a full replay
// and a filtered one. Everything else must match byte-for-byte.
func stripLocalSeqs(rec *anyenc.Value) *anyenc.Value {
	if rec == nil {
		return nil
	}
	if o, _ := rec.Object(); o != nil {
		o.Del(ApplySeqField)
	}
	return rec
}

// TestFilteredReplayMatchesFullReplay is the soundness proof for the
// record-filtered fast path in the version-history proposal: because CRDT
// gating is strictly record-local (per-record _ver, sticky tombstones,
// _ver.id min-rule), applying only the changes that touch record X — in the
// same ascending order — must produce a record byte-identical to the full
// replay, including its _ver map.
func TestFilteredReplayMatchesFullReplay(t *testing.T) {
	changes := genHistoryWorkload(20_000, 200, true)

	full, doneFull := newScratchController(t)
	defer doneFull()
	for _, ch := range changes {
		require.NoError(t, full.ApplyChange(ctx, ch))
	}

	for _, target := range []string{"rec-0", "rec-42", "rec-100", "rec-199"} {
		var filtered []Change
		for _, ch := range changes {
			for _, rc := range ch.Records {
				if rc.Id == target {
					filtered = append(filtered, ch)
					break
				}
			}
		}
		part, donePart := newScratchController(t)
		for _, ch := range filtered {
			require.NoError(t, part.ApplyChange(ctx, ch))
		}
		want := stripLocalSeqs(full.Get(ctx, testDS, target))
		got := stripLocalSeqs(part.Get(ctx, testDS, target))
		require.NotNil(t, want, target)
		require.NotNil(t, got, target)
		require.Equal(t, want.String(), got.String(), "record %s diverged under filtered replay", target)
		donePart()
	}
}
