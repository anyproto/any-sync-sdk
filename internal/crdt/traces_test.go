package crdt

import (
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readTraces extracts `_traces` into a Go map[verId][]traceId for assertion.
func readTraces(rec *anyenc.Value) map[string][]string {
	if rec == nil {
		return nil
	}
	t := rec.Get(TracesKey)
	if t == nil || t.Type() != anyenc.TypeObject {
		return nil
	}
	out := map[string][]string{}
	obj, _ := t.Object()
	obj.Visit(func(k []byte, v *anyenc.Value) {
		if v.Type() != anyenc.TypeArray {
			return
		}
		items, _ := v.Array()
		got := make([]string, 0, len(items))
		for _, it := range items {
			if it.Type() == anyenc.TypeString {
				got = append(got, string(it.GetStringBytes()))
			}
		}
		out[string(k)] = got
	})
	return out
}

// TestTraces_WriteAndReadBack verifies traces are stamped on create and
// readable via `_traces[versionId]`.
func TestTraces_WriteAndReadBack(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	ch := makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "Alice"}),
	})
	ch.TraceIds = []string{"session-A", "op-42"}
	require.NoError(t, st.ApplyChange(ctx, ch))

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	tr := readTraces(rec)
	assert.Equal(t, []string{"session-A", "op-42"}, tr["v1"])
}

// TestTraces_EmptyTraceIdsNoEntry verifies a change with no TraceIds doesn't
// add an entry.
func TestTraces_EmptyTraceIdsNoEntry(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	ch := makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "Alice"}),
	})
	require.NoError(t, st.ApplyChange(ctx, ch))

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	assert.Nil(t, readTraces(rec))
	assert.Nil(t, rec.Get(TracesKey))
}

// TestTraces_SupersededVersionGCed verifies that when a later write
// overwrites a field, the older versionId drops out of `_traces`.
func TestTraces_SupersededVersionGCed(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	first := makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "Alice"}),
	})
	first.TraceIds = []string{"first-trace"}
	require.NoError(t, st.ApplyChange(ctx, first))

	// Overwrite name at v2 with a new trace — v1 is no longer referenced
	// in `_ver` (name moves to v2, and _ver.id keeps v1 only if min-rule
	// applies; the record's _ver.id stays at v1 because it's the creation
	// marker, so trace for v1 is kept via the marker).
	second := makeChange("v2", "r1", Op{
		Type:    OpSet,
		Path:    []string{"name"},
		Payload: arena.NewString("Bob"),
	})
	second.TraceIds = []string{"second-trace"}
	require.NoError(t, st.ApplyChange(ctx, second))

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	tr := readTraces(rec)
	assert.Equal(t, []string{"first-trace"}, tr["v1"], "v1 kept via creation marker")
	assert.Equal(t, []string{"second-trace"}, tr["v2"])
}

// TestTraces_CreationMarkerSupersededByMinRule verifies that when a second
// upsert lowers _ver.id, the dropped higher versionId's traces are GC'd.
func TestTraces_CreationMarkerSupersededByMinRule(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// Upsert at the higher versionId first.
	high := makeUpsert("v9", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "Alice"}),
	})
	high.TraceIds = []string{"high-trace"}
	require.NoError(t, st.ApplyChange(ctx, high))

	// Concurrent upsert at a lower versionId — all field ops gated out,
	// but min-rule lowers _ver.id from v9 to v1.
	low := makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "Bob"}),
	})
	low.TraceIds = []string{"low-trace"}
	require.NoError(t, st.ApplyChange(ctx, low))

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	tr := readTraces(rec)
	// v9 still referenced by _ver.name. v1 now referenced by _ver.id.
	assert.Equal(t, []string{"high-trace"}, tr["v9"])
	assert.Equal(t, []string{"low-trace"}, tr["v1"])
}

// TestTraces_TombstonePreservesCreation verifies that traces for the
// creation versionId survive a delete (since `_ver.id` is preserved).
func TestTraces_TombstonePreservesCreation(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	create := makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "Alice"}),
	})
	create.TraceIds = []string{"create-trace"}
	require.NoError(t, st.ApplyChange(ctx, create))

	del := Change{
		ObjectId:    "obj1",
		Dataset:     testDS,
		VersionId:   "v2",
		DataVersion: testDataVersion,
		Timestamp:   1000,
		TraceIds:    []string{"delete-trace"},
		Records: []RecordChange{
			{Id: "r1", Ops: []Op{{Type: OpDelete}}},
		},
	}
	require.NoError(t, st.ApplyChange(ctx, del))

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	tr := readTraces(rec)
	// v1 stays via _ver.id; v2 appears via _ver.* (default).
	assert.Equal(t, []string{"create-trace"}, tr["v1"])
	assert.Equal(t, []string{"delete-trace"}, tr["v2"])
}

// TestTraces_TombstoneDropsSupersededFields verifies that when a delete
// collapses `_ver`, traces for versionIds that only keyed per-field entries
// (no longer present in the shrunken _ver) are GC'd.
func TestTraces_TombstoneDropsSupersededFields(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	create := makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "Alice"}),
	})
	create.TraceIds = []string{"create-trace"}
	require.NoError(t, st.ApplyChange(ctx, create))

	update := makeChange("v2", "r1", Op{
		Type:    OpSet,
		Path:    []string{"name"},
		Payload: arena.NewString("Bob"),
	})
	update.TraceIds = []string{"update-trace"}
	require.NoError(t, st.ApplyChange(ctx, update))

	del := Change{
		ObjectId:    "obj1",
		Dataset:     testDS,
		VersionId:   "v3",
		DataVersion: testDataVersion,
		Timestamp:   1000,
		TraceIds:    []string{"delete-trace"},
		Records: []RecordChange{
			{Id: "r1", Ops: []Op{{Type: OpDelete}}},
		},
	}
	require.NoError(t, st.ApplyChange(ctx, del))

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	tr := readTraces(rec)
	// v1 stays (creation marker). v3 stays (_ver.*). v2 has no reference
	// in the tombstone's _ver — its trace is GC'd.
	assert.Equal(t, []string{"create-trace"}, tr["v1"])
	assert.Equal(t, []string{"delete-trace"}, tr["v3"])
	_, hasV2 := tr["v2"]
	assert.False(t, hasV2, "v2 trace should be GC'd after delete")
}

// TestTraces_EmptyTraceIdsClears verifies that an explicit empty TraceIds
// removes any existing trace for that versionId.
func TestTraces_EmptyTraceIdsClears(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// Apply a change with traces.
	first := makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "Alice"}),
	})
	first.TraceIds = []string{"trace-a"}
	require.NoError(t, st.ApplyChange(ctx, first))

	// Replay the same logical change (same versionId) without traces.
	// Real-world: a later code path re-delivers without the metadata.
	replay := makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "Alice"}),
	})
	require.NoError(t, st.ApplyChange(ctx, replay))

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	tr := readTraces(rec)
	// The replay's upsert lowered _ver.id (no-op since equal) but
	// updateTraces saw empty TraceIds and cleared v1's entry.
	_, has := tr["v1"]
	assert.False(t, has, "empty TraceIds should clear existing entry")
}

// TestTraces_MultipleFieldsShareEntry verifies the versionId-keyed
// compaction: one change writing many fields creates exactly one _traces
// entry.
func TestTraces_MultipleFieldsShareEntry(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	ch := makeUpsert("v1", "r1", Op{
		Type: OpSet,
		Payload: recordPayload(arena, map[string]any{
			"a": 1, "b": 2, "c": 3, "d": 4,
		}),
	})
	ch.TraceIds = []string{"shared-trace"}
	require.NoError(t, st.ApplyChange(ctx, ch))

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	tr := readTraces(rec)
	assert.Len(t, tr, 1, "one change → one _traces entry regardless of field count")
	assert.Equal(t, []string{"shared-trace"}, tr["v1"])
}

// TestTraces_CommutativeOpsNotTraced verifies that $addToSet/$pull/$inc —
// which don't update `_ver` — don't create `_traces` entries (their
// versionId doesn't land in `_ver`, so GC drops them immediately).
func TestTraces_CommutativeOpsNotTraced(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// Seed record with tags at v1.
	seed := makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"tags": []any{"x"}}),
	})
	seed.TraceIds = []string{"seed-trace"}
	require.NoError(t, st.ApplyChange(ctx, seed))

	// $addToSet at v2 — does not update _ver.tags.
	add := makeChange("v2", "r1", Op{
		Type:    OpAddToSet,
		Path:    []string{"tags"},
		Payload: arena.NewString("y"),
	})
	add.TraceIds = []string{"add-trace"}
	require.NoError(t, st.ApplyChange(ctx, add))

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	tr := readTraces(rec)
	// v1 survives via creation marker + per-field _ver.tags.
	assert.Equal(t, []string{"seed-trace"}, tr["v1"])
	// v2 never landed in _ver (commutative op) → no trace retained.
	_, hasV2 := tr["v2"]
	assert.False(t, hasV2, "commutative ops don't stamp _ver → no trace entry")
}

// TestTraces_StrictNoopDoesNotAdd verifies that a strict modify on an
// absent record (no-op) doesn't create trace entries.
func TestTraces_StrictNoopDoesNotAdd(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// Strict (no Upsert) on a non-existent id — silent no-op.
	ch := makeChange("v1", "ghost", Op{
		Type:    OpSet,
		Path:    []string{"name"},
		Payload: arena.NewString("nope"),
	})
	ch.TraceIds = []string{"would-not-appear"}
	require.NoError(t, st.ApplyChange(ctx, ch))

	assert.Nil(t, st.Get(ctx, testDS, "ghost"))
}

// TestTraces_StickyTombstoneUpsertLowersAndStamps verifies that an upsert
// against a sticky tombstone, which lowers _ver.id, also writes its trace.
func TestTraces_StickyTombstoneUpsertLowersAndStamps(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// Delete at v5 (no prior record — tombstone seeded with _ver.id=v5).
	del := Change{
		ObjectId:    "obj1",
		Dataset:     testDS,
		VersionId:   "v5",
		DataVersion: testDataVersion,
		Timestamp:   1000,
		TraceIds:    []string{"delete-trace"},
		Records: []RecordChange{
			{Id: "r1", Ops: []Op{{Type: OpDelete}}},
		},
	}
	require.NoError(t, st.ApplyChange(ctx, del))

	// Concurrent upsert at v1 arrives after — field ops gated out, but
	// min-rule lowers _ver.id from v5 to v1 and we want its trace.
	upsert := makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "lost"}),
	})
	upsert.TraceIds = []string{"upsert-trace"}
	require.NoError(t, st.ApplyChange(ctx, upsert))

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	tr := readTraces(rec)
	assert.Equal(t, []string{"upsert-trace"}, tr["v1"])
	assert.Equal(t, []string{"delete-trace"}, tr["v5"])
}
