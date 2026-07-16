package e2e

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/history"
	"github.com/anyproto/any-sync-sdk/space"
)

// Real-life version-history benchmarks. Unlike the Controller-apply
// micro benchmarks (internal/crdt/history_replay_bench_test.go), these
// run the WHOLE stack through the public Space.History() surface: real
// sp.Modify writes (sign + encrypt + tree storage + warm index hook),
// then listing, view/record reconstruction (tree build + decrypt +
// decode + replay), diffs, cold backfill, and the write-path overhead
// of the index hook itself. Offline (dead loopback nodeconf) — history
// is a purely local feature, so no network cost pollutes the numbers.
//
// Two workload profiles mirror the micro benches:
//   - document: a page-shaped object — a few dozen block records,
//     edits dominate (keystroke-grained single-field sets);
//   - chat: creates dominate — most changes add a new message record
//     with a ~120-char body, the rest edit/react to recent messages.
//
// Setup writes real changes at real (crypto-bound) speed, so sizes are
// modest by default; -short shrinks them further.

// histBench is the shared per-profile fixture: one SDK, one space, one
// object, `versions[i]` = ChangeId of the i-th write.
type histBench struct {
	ctx      context.Context
	cfg      config.Config
	provider *fixedSeedProvider
	sdk      *anysyncsdk.SDK
	sp       space.Space
	spaceId  string
	objId    string
	versions []space.Version
	hotRec   string // most-touched record of the workload
}

func newHistBench(b *testing.B) *histBench {
	b.Helper()
	yaml, err := loadLocalNetwork()
	require.NoError(b, err)
	_, accPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(b, err)
	_, devPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(b, err)

	e := &histBench{
		ctx:      context.Background(),
		provider: &fixedSeedProvider{account: accPriv, device: devPriv},
		cfg: config.Config{
			Storage: config.Storage{DataDir: b.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
			Types:   []handler.Type{newHistoryType()},
		},
	}
	sdk, err := anysyncsdk.Open(e.ctx, e.cfg, e.provider)
	require.NoError(b, err)
	e.sdk = sdk
	b.Cleanup(func() { _ = e.sdk.Close() })

	sp, err := sdk.Spaces().Create(e.ctx, space.CreateRequest{Name: "HistoryBench"})
	require.NoError(b, err)
	e.sp = sp
	e.spaceId = sp.Id()
	objId, err := sp.Objects().Create(e.ctx, space.CreateObjectOpts{Types: []string{histTypeId}})
	require.NoError(b, err)
	e.objId = objId
	return e
}

func (e *histBench) write(b *testing.B, recId string, upsert bool, ops []space.Op, traces ...string) {
	b.Helper()
	res, err := e.sp.Modify(e.ctx, space.ModifyBatch{
		ObjectId: e.objId,
		Dataset:  histDataset,
		TraceIds: traces,
		Records:  []space.RecordModify{{Id: recId, Upsert: upsert, Ops: ops}},
	})
	require.NoError(b, err)
	require.NotEmpty(b, res.ChangeId)
	e.versions = append(e.versions, res.ChangeId)
}

const benchBody = "lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna"

// writeDocHistory builds a page-editing history: blocks created early,
// then keystroke-grained single-field edits on random blocks; ~5% of
// edits carry a trace id (AI-assist style).
func (e *histBench) writeDocHistory(b *testing.B, n int) {
	b.Helper()
	rng := mrand.New(mrand.NewSource(42))
	blocks := n/100 + 4
	touches := map[string]int{}
	created := 0
	for i := 0; i < n; i++ {
		if created < blocks && (created == 0 || rng.Intn(n/blocks+1) == 0) {
			id := fmt.Sprintf("blk-%d", created)
			created++
			e.write(b, id, true, []space.Op{
				{Type: space.OpSet, Path: "title", Value: fmt.Sprintf("Block %d", created)},
				{Type: space.OpSet, Path: "text", Value: benchBody},
				{Type: space.OpSet, Path: "checked", Value: false},
				{Type: space.OpSet, Path: "weight", Value: 0},
			})
			touches[id]++
			continue
		}
		id := fmt.Sprintf("blk-%d", rng.Intn(created))
		var op space.Op
		switch rng.Intn(4) {
		case 0:
			op = space.Op{Type: space.OpSet, Path: "title", Value: fmt.Sprintf("Block edit %d", i)}
		case 1:
			op = space.Op{Type: space.OpSet, Path: "text", Value: fmt.Sprintf("%s (rev %d)", benchBody, i)}
		case 2:
			op = space.Op{Type: space.OpInc, Path: "weight", Value: 1}
		default:
			op = space.Op{Type: space.OpSet, Path: "checked", Value: i%2 == 0}
		}
		var traces []string
		if rng.Intn(20) == 0 {
			traces = []string{fmt.Sprintf("trace-%d", rng.Intn(8))}
		}
		e.write(b, id, false, []space.Op{op}, traces...)
		touches[id]++
	}
	e.hotRec = hottest(touches)
}

// writeChatHistory builds a chat: ~75% of changes create a new message,
// the rest edit or react to one of the last 50.
func (e *histBench) writeChatHistory(b *testing.B, n int) {
	b.Helper()
	rng := mrand.New(mrand.NewSource(43))
	touches := map[string]int{}
	created := 0
	for i := 0; i < n; i++ {
		if created == 0 || rng.Intn(4) != 0 {
			id := fmt.Sprintf("msg-%d", created)
			created++
			e.write(b, id, true, []space.Op{
				{Type: space.OpSet, Path: "text", Value: fmt.Sprintf("message %d: %s", i, benchBody)},
				{Type: space.OpSet, Path: "createdAt", Value: 1700000000 + i},
			})
			touches[id]++
			continue
		}
		id := fmt.Sprintf("msg-%d", created-1-rng.Intn(minInt(created, 50)))
		var op space.Op
		if rng.Intn(2) == 0 {
			op = space.Op{Type: space.OpSet, Path: "text", Value: fmt.Sprintf("message %d (edited)", i)}
		} else {
			op = space.Op{Type: space.OpAddToSet, Path: "reactions", Value: fmt.Sprintf("u%d:+1", rng.Intn(8))}
		}
		e.write(b, id, false, []space.Op{op})
		touches[id]++
	}
	e.hotRec = hottest(touches)
}

func hottest(touches map[string]int) string {
	var rec string
	best := -1
	for id, c := range touches {
		if c > best || (c == best && id < rec) {
			rec, best = id, c
		}
	}
	return rec
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// reopen closes and reopens the SDK on the same DataDir/keys —
// the "app restart" boundary. When markStale is set, the object's
// index is flagged stale in between (via the on-disk meta row, the
// same state a cold restore leaves behind), so the next history call
// pays the full lazy backfill.
func (e *histBench) reopen(b *testing.B, markStale bool) {
	b.Helper()
	require.NoError(b, e.sdk.Close())
	if markStale {
		db, err := anystore.Open(e.ctx, filepath.Join(e.cfg.Storage.DataDir, "sdk.db"), nil)
		require.NoError(b, err)
		coll, err := db.Collection(e.ctx, history.HistoryMetaCollection)
		require.NoError(b, err)
		a := &anyenc.Arena{}
		row := a.NewObject()
		row.Set("id", a.NewString(e.objId))
		row.Set("sp", a.NewString(e.spaceId))
		row.Set("stale", a.NewTrue())
		require.NoError(b, coll.UpsertOne(e.ctx, row))
		require.NoError(b, db.Close())
	}
	sdk, err := anysyncsdk.Open(e.ctx, e.cfg, e.provider)
	require.NoError(b, err)
	e.sdk = sdk
	sp, err := sdk.Spaces().Get(e.ctx, e.spaceId)
	require.NoError(b, err)
	e.sp = sp
}

func docSizes() []int {
	if testing.Short() {
		return []int{1_000}
	}
	return []int{1_000, 10_000}
}

// BenchmarkHistory_Document: page-shaped object, edits dominate.
func BenchmarkHistory_Document(b *testing.B) {
	for _, n := range docSizes() {
		b.Run(fmt.Sprintf("changes=%d", n), func(b *testing.B) {
			e := newHistBench(b)
			e.writeDocHistory(b, n)
			hist := e.sp.History()
			ctx := e.ctx
			dsFilter := space.HistoryFilter{Dataset: histDataset}
			head := e.versions[len(e.versions)-1]
			mid := e.versions[len(e.versions)/2]

			b.Run("ListChanges/firstPage", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					page, err := hist.ListChanges(ctx, e.objId, dsFilter, 50, "")
					if err != nil {
						b.Fatal(err)
					}
					if len(page.Changes) == 0 {
						b.Fatal("empty page")
					}
				}
			})

			b.Run("ListChanges/fullWalk", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					total, cursor := 0, ""
					for {
						page, err := hist.ListChanges(ctx, e.objId, dsFilter, 200, cursor)
						if err != nil {
							b.Fatal(err)
						}
						total += len(page.Changes)
						if page.Cursor == "" {
							break
						}
						cursor = page.Cursor
					}
					if total != len(e.versions) {
						b.Fatalf("walked %d of %d changes", total, len(e.versions))
					}
				}
				b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
			})

			b.Run("ListChanges/recordFiltered", func(b *testing.B) {
				f := space.HistoryFilter{Dataset: histDataset, RecordId: e.hotRec}
				for i := 0; i < b.N; i++ {
					page, err := hist.ListChanges(ctx, e.objId, f, 50, "")
					if err != nil {
						b.Fatal(err)
					}
					if len(page.Changes) == 0 {
						b.Fatal("empty page")
					}
				}
			})

			b.Run("ListChanges/byTrace", func(b *testing.B) {
				f := space.HistoryFilter{TraceId: "trace-0"}
				for i := 0; i < b.N; i++ {
					page, err := hist.ListChanges(ctx, e.objId, f, 50, "")
					if err != nil {
						b.Fatal(err)
					}
					if len(page.Changes) == 0 {
						b.Fatal("empty page")
					}
				}
			})

			// One rapid single-author session coalesces into few (often
			// one) groups, so a first-page call may scan deep into the
			// raw history — this measures that worst case honestly.
			b.Run("ListChanges/coalescedFirstPage", func(b *testing.B) {
				f := space.HistoryFilter{Dataset: histDataset, Coalesce: &space.CoalesceOpts{}}
				for i := 0; i < b.N; i++ {
					page, err := hist.ListChanges(ctx, e.objId, f, 20, "")
					if err != nil {
						b.Fatal(err)
					}
					if len(page.Changes) == 0 {
						b.Fatal("empty page")
					}
				}
			})

			// ViewAt = tree build + decrypt + decode + full causal
			// replay into a scratch store, then one dataset read.
			for _, tc := range []struct {
				name string
				ver  space.Version
			}{{"head", head}, {"mid", mid}} {
				b.Run("ViewAt/"+tc.name, func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						view, err := hist.ViewAt(ctx, e.objId, tc.ver)
						if err != nil {
							b.Fatal(err)
						}
						recs, err := view.Records(ctx, histDataset)
						if err != nil {
							b.Fatal(err)
						}
						if len(recs) == 0 {
							b.Fatal("empty view")
						}
						if err := view.Close(); err != nil {
							b.Fatal(err)
						}
					}
					b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "changes/s")
				})
			}

			// RecordAt = index-fed filtered replay of one record.
			b.Run("RecordAt/hotRecord", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					rec, err := hist.RecordAt(ctx, e.objId, histDataset, e.hotRec, head)
					if err != nil {
						b.Fatal(err)
					}
					if rec == nil {
						b.Fatal("record missing at head")
					}
				}
			})

			b.Run("Diff/halfSpan", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					res, err := hist.Diff(ctx, e.objId, mid, head, space.DiffFilter{Dataset: histDataset})
					if err != nil {
						b.Fatal(err)
					}
					if len(res.Datasets) == 0 {
						b.Fatal("empty diff")
					}
				}
			})

			b.Run("Diff/effect", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if _, err := hist.Diff(ctx, e.objId, "", head, space.DiffFilter{Dataset: histDataset}); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// BenchmarkHistory_Chat: creates dominate, records are small and
// numerous — RecordAt is the product-critical path ("show this
// message's edit history"), ViewAt the worst case (whole chat).
func BenchmarkHistory_Chat(b *testing.B) {
	n := 10_000
	if testing.Short() {
		n = 2_000
	}
	b.Run(fmt.Sprintf("changes=%d", n), func(b *testing.B) {
		e := newHistBench(b)
		e.writeChatHistory(b, n)
		hist := e.sp.History()
		ctx := e.ctx
		head := e.versions[len(e.versions)-1]

		// A message touched once: filtered replay applies ~1 change.
		coldRec := "msg-0"

		b.Run("RecordAt/hotMessage", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				rec, err := hist.RecordAt(ctx, e.objId, histDataset, e.hotRec, head)
				if err != nil {
					b.Fatal(err)
				}
				if rec == nil {
					b.Fatal("record missing")
				}
			}
		})

		b.Run("RecordAt/coldMessage", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				rec, err := hist.RecordAt(ctx, e.objId, histDataset, coldRec, head)
				if err != nil {
					b.Fatal(err)
				}
				if rec == nil {
					b.Fatal("record missing")
				}
			}
		})

		b.Run("ListChanges/messageHistory", func(b *testing.B) {
			f := space.HistoryFilter{Dataset: histDataset, RecordId: e.hotRec}
			for i := 0; i < b.N; i++ {
				page, err := hist.ListChanges(ctx, e.objId, f, 50, "")
				if err != nil {
					b.Fatal(err)
				}
				if len(page.Changes) == 0 {
					b.Fatal("empty page")
				}
			}
		})

		b.Run("ViewAt/head", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				view, err := hist.ViewAt(ctx, e.objId, head)
				if err != nil {
					b.Fatal(err)
				}
				recs, err := view.Records(ctx, histDataset)
				if err != nil {
					b.Fatal(err)
				}
				if len(recs) == 0 {
					b.Fatal("empty view")
				}
				if err := view.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "changes/s")
		})
	})
}

// BenchmarkHistory_ColdBackfill: the first history query after a cold
// restore (or on an object predating the feature) pays a synchronous
// full-tree backfill — walk + decrypt + decode + index writes. The
// restart/stale-flip happens off-timer; only the triggering
// ListChanges is measured.
func BenchmarkHistory_ColdBackfill(b *testing.B) {
	n := 5_000
	if testing.Short() {
		n = 1_000
	}
	e := newHistBench(b)
	e.writeDocHistory(b, n)
	f := space.HistoryFilter{Dataset: histDataset}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		e.reopen(b, true)
		hist := e.sp.History()
		b.StartTimer()
		page, err := hist.ListChanges(e.ctx, e.objId, f, 1, "")
		if err != nil {
			b.Fatal(err)
		}
		if len(page.Changes) == 0 {
			b.Fatal("empty page")
		}
	}
	b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "changes/s")
}

// BenchmarkHistory_WarmReopen is the control for ColdBackfill: same
// restart boundary, index intact — first ListChanges should cost an
// object load plus one page scan, no backfill.
func BenchmarkHistory_WarmReopen(b *testing.B) {
	n := 5_000
	if testing.Short() {
		n = 1_000
	}
	e := newHistBench(b)
	e.writeDocHistory(b, n)
	f := space.HistoryFilter{Dataset: histDataset}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		e.reopen(b, false)
		hist := e.sp.History()
		b.StartTimer()
		page, err := hist.ListChanges(e.ctx, e.objId, f, 1, "")
		if err != nil {
			b.Fatal(err)
		}
		if len(page.Changes) == 0 {
			b.Fatal("empty page")
		}
	}
}

// BenchmarkHistory_WriteOverhead measures what the warm index hook adds
// to every write: the same single-field Modify against an indexed
// dataset vs a SkipHistory one. The delta is the per-write history tax.
func BenchmarkHistory_WriteOverhead(b *testing.B) {
	for _, tc := range []struct{ name, dataset string }{
		{"indexed", histDataset},
		{"skipHistory", histSkipDataset},
	} {
		b.Run(tc.name, func(b *testing.B) {
			e := newHistBench(b)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := e.sp.Modify(e.ctx, space.ModifyBatch{
					ObjectId: e.objId,
					Dataset:  tc.dataset,
					Records: []space.RecordModify{{
						Id: fmt.Sprintf("r-%d", i%64), Upsert: true,
						Ops: []space.Op{{Type: space.OpSet, Path: "v", Value: i}},
					}},
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
