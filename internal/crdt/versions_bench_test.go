package crdt

import (
	"testing"

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
