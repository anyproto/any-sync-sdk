package crdt

import (
	"path/filepath"
	"strconv"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
)

// recordWith builds a record with the supplied _ver tree using the test
// helper from versions_test.go.
func recordWith(arena *anyenc.Arena, ver map[string]any) *anyenc.Value {
	rec := arena.NewObject()
	rec.Set(IdField, arena.NewString("rec"))
	rec.Set(VersionsKey, treeToValue(arena, ver))
	return rec
}

// BenchmarkCompact_NoopCanonical: record already in the {id, *} shape
// produced by factor — compactVersions must do nothing and allocate
// nothing.
func BenchmarkCompact_NoopCanonical(b *testing.B) {
	arena := &anyenc.Arena{}
	rec := recordWith(arena, map[string]any{
		IdField:    "v1",
		defaultKey: "v1",
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compactVersions(arena, rec)
	}
}

// BenchmarkCompact_NoopFlatUnique: typical "single-field update on an
// existing record" — all versions distinct, no subtree, no work.
func BenchmarkCompact_NoopFlatUnique(b *testing.B) {
	arena := &anyenc.Arena{}
	rec := recordWith(arena, map[string]any{
		IdField:     "v1",
		"name":      "v2",
		"count":     "v3",
		"createdAt": "v4",
		"author":    "v5",
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compactVersions(arena, rec)
	}
}

// BenchmarkCompact_NoopFlatLarge: 12 distinct-version siblings (within
// the rootFactorBufSize fast-path capacity).
func BenchmarkCompact_NoopFlatLarge(b *testing.B) {
	arena := &anyenc.Arena{}
	ver := map[string]any{IdField: "v0"}
	versions := []string{"v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "vA", "vB"}
	names := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"}
	for i, n := range names {
		ver[n] = versions[i]
	}
	rec := recordWith(arena, ver)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compactVersions(arena, rec)
	}
}

// BenchmarkCompact_Tombstone: tombstone short-circuit.
func BenchmarkCompact_Tombstone(b *testing.B) {
	arena := &anyenc.Arena{}
	rec := arena.NewObject()
	rec.Set(IdField, arena.NewString("rec"))
	rec.Set(DeletedAtField, arena.NewNumberInt(1715000000))
	ver := arena.NewObject()
	ver.Set(IdField, arena.NewString("v0"))
	ver.Set(defaultKey, arena.NewString("v9"))
	rec.Set(VersionsKey, ver)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compactVersions(arena, rec)
	}
}

// BenchmarkCompact_RootFactor: multi-field create case — 4 non-id
// siblings all at the same version, should factor into `*`.
func BenchmarkCompact_RootFactor(b *testing.B) {
	arena := &anyenc.Arena{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Build fresh each iteration so we measure the cost on the
		// shape that actually needs work (Del + Set).
		b.StopTimer()
		rec := recordWith(arena, map[string]any{
			IdField:    "v1",
			"changeId": "v1",
			"kind":     "v1",
			"propId":   "v1",
		})
		b.StartTimer()
		compactVersions(arena, rec)
	}
}

// newBenchController is the *testing.B equivalent of newTestController —
// opens a fresh any-store DB under a temp dir and returns a Controller
// with the default "blocks" handler.
func newBenchController(b *testing.B) *Controller {
	b.Helper()
	return newBenchControllerWith(b, nil)
}

// newBenchControllerWith opens with an explicit any-store config — used
// by the _FixedCache benches to test the global page-buffer slab path.
func newBenchControllerWith(b *testing.B, cfg *anystore.Config) *Controller {
	b.Helper()
	db, err := anystore.Open(ctx, filepath.Join(b.TempDir(), "bench.db"), cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	st, err := NewController(ctx, "obj1", db, DefaultHandler{DatasetName: testDS})
	if err != nil {
		b.Fatal(err)
	}
	return st
}

// BenchmarkApply_MultiFieldCreate measures end-to-end ApplyChange
// throughput against a real any-store DB. Each iteration applies one
// multi-field create on a fresh record id — the shape that triggers
// the root-factor compaction step.
//
// Run with:
//
//	go test -bench=BenchmarkApply_MultiFieldCreate -benchtime=1000x   ./internal/crdt/...
//	go test -bench=BenchmarkApply_MultiFieldCreate -benchtime=100000x ./internal/crdt/...
//
// to get 1k- and 100k-change wall-clock numbers.
func BenchmarkApply_MultiFieldCreate(b *testing.B) {
	st := newBenchController(b)
	g := newVersionGen()

	// Per-iteration arena that gets Reset, mirroring production where
	// the change's payload lives on the caller's transient arena and
	// can be discarded once ApplyChange returns. Pre-allocating one
	// arena and holding 100k payloads on it would inflate live memory
	// without reflecting real apply cost.
	arena := &anyenc.Arena{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		arena.Reset()
		payload := recordPayload(arena, map[string]any{
			"changeId": "abc-" + strconv.Itoa(i),
			"kind":     "string",
			"propId":   "p-" + strconv.Itoa(i),
		})
		ch := makeUpsert(g.Next(), "r"+strconv.Itoa(i), Op{
			Type:    OpSet,
			Payload: payload,
		})
		if err := st.ApplyChange(ctx, ch); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkApply_MultiFieldCreate_FixedCache runs the same workload as
// BenchmarkApply_MultiFieldCreate but with any-store's global page
// buffer slab — anystore.InitPageBuffer + Config.UseGlobalPageBuffer.
// Mirrors the mobile / memory-bounded deployment shape: total page
// cache memory is capped at a fixed slab size regardless of how many
// DBs are open.
func BenchmarkApply_MultiFieldCreate_FixedCache(b *testing.B) {
	// 4 KiB pages × 5000 = 20 MiB slab (same as default per-DB cache,
	// but now process-wide and pre-allocated). Init is idempotent
	// across the process — first call wins, subsequent are no-ops.
	anystore.InitPageBuffer(4096, 5000)
	st := newBenchControllerWith(b, &anystore.Config{UseGlobalPageBuffer: true})
	g := newVersionGen()

	arena := &anyenc.Arena{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		arena.Reset()
		payload := recordPayload(arena, map[string]any{
			"changeId": "abc-" + strconv.Itoa(i),
			"kind":     "string",
			"propId":   "p-" + strconv.Itoa(i),
		})
		ch := makeUpsert(g.Next(), "r"+strconv.Itoa(i), Op{
			Type:    OpSet,
			Payload: payload,
		})
		if err := st.ApplyChange(ctx, ch); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkApply_SingleFieldUpdate_FixedCache: steady-state update
// against a slab-backed DB. The single-record hot loop barely
// touches the cache (one btree leaf), so this is a sanity check
// rather than a stress test.
func BenchmarkApply_SingleFieldUpdate_FixedCache(b *testing.B) {
	anystore.InitPageBuffer(4096, 5000)
	st := newBenchControllerWith(b, &anystore.Config{UseGlobalPageBuffer: true})
	g := newVersionGen()

	{
		a := &anyenc.Arena{}
		if err := st.ApplyChange(ctx, makeUpsert(g.Next(), "r1", Op{
			Type: OpSet,
			Payload: recordPayload(a, map[string]any{
				"name":  "init",
				"count": 0,
			}),
		})); err != nil {
			b.Fatal(err)
		}
	}

	arena := &anyenc.Arena{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		arena.Reset()
		ch := makeChange(g.Next(), "r1", Op{
			Type:    OpSet,
			Path:    []string{"name"},
			Payload: arena.NewString("v" + strconv.Itoa(i)),
		})
		if err := st.ApplyChange(ctx, ch); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkApply_SingleFieldUpdate measures the steady-state update
// path: a pre-populated record gets a sequence of single-field $sets
// (the hottest no-op-compaction path).
func BenchmarkApply_SingleFieldUpdate(b *testing.B) {
	st := newBenchController(b)
	g := newVersionGen()

	// Seed one record with an initial multi-field create on a throwaway arena.
	{
		a := &anyenc.Arena{}
		if err := st.ApplyChange(ctx, makeUpsert(g.Next(), "r1", Op{
			Type: OpSet,
			Payload: recordPayload(a, map[string]any{
				"name":  "init",
				"count": 0,
			}),
		})); err != nil {
			b.Fatal(err)
		}
	}

	arena := &anyenc.Arena{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		arena.Reset()
		ch := makeChange(g.Next(), "r1", Op{
			Type:    OpSet,
			Path:    []string{"name"},
			Payload: arena.NewString("v" + strconv.Itoa(i)),
		})
		if err := st.ApplyChange(ctx, ch); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCompact_SubtreeCollapse: nav-style nested subtree where
// all children share a version.
func BenchmarkCompact_SubtreeCollapse(b *testing.B) {
	arena := &anyenc.Arena{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		rec := recordWith(arena, map[string]any{
			IdField:   "v1",
			"author":  "v1",
			"spaceId": "v1",
			"nav": map[string]any{
				"type":     "v5",
				"parentId": "v5",
				"pos":      "v5",
			},
		})
		b.StartTimer()
		compactVersions(arena, rec)
	}
}
