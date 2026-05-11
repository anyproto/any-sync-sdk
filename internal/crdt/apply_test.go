package crdt

import (
	"context"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

const (
	testDS          = "blocks"
	testDataVersion = "test-v1"
)

// newTestController opens a temp any-store DB and returns a Controller with
// one DefaultHandler for the "blocks" dataset.
func newTestController(t *testing.T) *Controller {
	t.Helper()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	st, err := NewController(ctx, "obj1", db, DefaultHandler{DatasetName: testDS})
	require.NoError(t, err)
	return st
}

// recordPayload constructs an anyenc object from a Go map. Strings, ints,
// floats, bools, nested maps, and []any are supported.
func recordPayload(arena *anyenc.Arena, m map[string]any) *anyenc.Value {
	obj := arena.NewObject()
	for k, v := range m {
		obj.Set(k, goToAnyenc(arena, v))
	}
	return obj
}

func goToAnyenc(arena *anyenc.Arena, v any) *anyenc.Value {
	switch x := v.(type) {
	case nil:
		return arena.NewNull()
	case string:
		return arena.NewString(x)
	case int:
		return arena.NewNumberInt(x)
	case float64:
		return arena.NewNumberFloat64(x)
	case bool:
		if x {
			return arena.NewTrue()
		}
		return arena.NewFalse()
	case map[string]any:
		return recordPayload(arena, x)
	case []any:
		arr := arena.NewArray()
		for i, it := range x {
			arr.SetArrayItem(i, goToAnyenc(arena, it))
		}
		return arr
	case []string:
		arr := arena.NewArray()
		for i, it := range x {
			arr.SetArrayItem(i, arena.NewString(it))
		}
		return arr
	}
	return nil
}

// makeChange is a small DSL to build a Change targeting one record.
// makeChange builds a strict (Upsert=false) change targeting one record.
// Most tests use this for "modify an existing record" scenarios.
func makeChange(version VersionId, recordId string, ops ...Op) Change {
	return Change{
		ObjectId:    "obj1",
		Dataset:     testDS,
		VersionId:   version,
		DataVersion: testDataVersion,
		Records: []RecordChange{
			{Id: recordId, Ops: ops},
		},
	}
}

// makeUpsert builds an Upsert=true change. Used as the explicit "create"
// path: the first time a test wants a record to exist, it goes through
// makeUpsert, and subsequent modifies use makeChange.
func makeUpsert(version VersionId, recordId string, ops ...Op) Change {
	return Change{
		ObjectId:    "obj1",
		Dataset:     testDS,
		VersionId:   version,
		DataVersion: testDataVersion,
		Records: []RecordChange{
			{Id: recordId, Upsert: true, Ops: ops},
		},
	}
}

// ----------------------------------------------------------------------------
// insert
// ----------------------------------------------------------------------------

// A multi-field $set on a non-existent record is the equivalent of "insert"
// in the old model: the record is auto-created and each payload field is
// applied as an independent gated $set sharing the change's versionId.
func TestSet_AutoCreatesRecordWithMultiField(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	ch := makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "hello", "count": 7}),
	})
	require.NoError(t, st.ApplyChange(ctx, ch))

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	assert.Equal(t, "r1", rec.GetString(IdField))
	assert.Equal(t, "hello", rec.GetString("name"))
	assert.Equal(t, 7, rec.GetInt("count"))
	// Each enumerated field is gated at v1.
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, "name"))
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, "count"))
	// Unmentioned fields have NO recorded version (auto-create leaves _ver
	// empty for unspecified paths). This is a known divergence from the old
	// insert form which collapsed _ver to a single defaultKey entry.
	assert.Equal(t, VersionId(""), GetRecordVersion(rec, "anything"))
}

// Two multi-field $sets racing on the same id merge per-field via gating —
// the higher version wins for every overlapping field. There is no "skip if
// exists" rule to drop one of them.
func TestSet_TwoMultiFieldChangesMerge(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "first", "color": "red"}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange("v9", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "second"}),
	})))

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	// Newer name wins; color from v1 is preserved (not overwritten).
	assert.Equal(t, "second", rec.GetString("name"))
	assert.Equal(t, "red", rec.GetString("color"))
	assert.Equal(t, VersionId("v9"), GetRecordVersion(rec, "name"))
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, "color"))
}

// id is immutable per spec §3.1: any `id` key in a multi-field $set payload
// causes the whole change to be rejected at pre-apply validation.
func TestSet_RejectsIdInMultiFieldPayload(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	err := st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type: OpSet,
		Payload: recordPayload(arena, map[string]any{
			"id":   "wrong",
			"name": "hello",
		}),
	}))
	require.ErrorIs(t, err, ErrValidation)
	require.ErrorIs(t, err, ErrInvalidPath)
	// State untouched: no record was created because the whole change
	// dropped at the pre-apply validation pass.
	assert.Nil(t, st.Get(ctx, testDS, "r1"))
}

// Same protection for single-path writes targeting `id`.
func TestSet_RejectsExplicitIdPath(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "hi"}),
	})))
	err := st.ApplyChange(ctx, makeChange("v2", "r1", Op{
		Type:    OpSet,
		Path:    []string{"id"},
		Payload: arena.NewString("rewritten"),
	}))
	require.ErrorIs(t, err, ErrInvalidPath)

	// Existing record untouched.
	rec := st.Get(ctx, testDS, "r1")
	assert.Equal(t, "r1", rec.GetString(IdField))
	assert.Equal(t, "hi", rec.GetString("name"))
}

// ----------------------------------------------------------------------------
// $set
// ----------------------------------------------------------------------------

func TestSet_NewerWins(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "old"}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange("v3", "r1", Op{
		Type:    OpSet,
		Path:    []string{"name"},
		Payload: arena.NewString("new"),
	})))

	rec := st.Get(ctx, testDS, "r1")
	assert.Equal(t, "new", rec.GetString("name"))
	assert.Equal(t, VersionId("v3"), GetRecordVersion(rec, "name"))
}

func TestSet_OlderSkipped(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v5", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "winner"}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange("v3", "r1", Op{
		Type:    OpSet,
		Path:    []string{"name"},
		Payload: arena.NewString("loser"),
	})))

	rec := st.Get(ctx, testDS, "r1")
	assert.Equal(t, "winner", rec.GetString("name"))
	assert.Equal(t, VersionId("v5"), GetRecordVersion(rec, "name"))
}

func TestSet_NestedPath(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange("v2", "r1", Op{
		Type:    OpSet,
		Path:    []string{"meta", "color"},
		Payload: arena.NewString("red"),
	})))

	rec := st.Get(ctx, testDS, "r1")
	assert.Equal(t, "red", rec.GetString("meta", "color"))
	assert.Equal(t, VersionId("v2"), GetRecordVersion(rec, "meta", "color"))
}

func TestSet_SubtreeReplaceGated(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// insert + set meta.color at v5, then a $set replacing the whole meta at
	// v3 should be skipped (v3 < v5, max in subtree = v5).
	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange("v5", "r1", Op{
		Type:    OpSet,
		Path:    []string{"meta", "color"},
		Payload: arena.NewString("red"),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange("v3", "r1", Op{
		Type:    OpSet,
		Path:    []string{"meta"},
		Payload: recordPayload(arena, map[string]any{"color": "blue"}),
	})))

	rec := st.Get(ctx, testDS, "r1")
	assert.Equal(t, "red", rec.GetString("meta", "color"))
}

// ----------------------------------------------------------------------------
// $unset
// ----------------------------------------------------------------------------

func TestUnset_RemovesFieldAndUpdatesOrder(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "hi"}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange("v2", "r1", Op{
		Type: OpUnset,
		Path: []string{"name"},
	})))

	rec := st.Get(ctx, testDS, "r1")
	assert.Nil(t, rec.Get("name"))
	assert.Equal(t, VersionId("v2"), GetRecordVersion(rec, "name"))
}

func TestUnset_OlderSkipped(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v5", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "keep"}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange("v3", "r1", Op{
		Type: OpUnset,
		Path: []string{"name"},
	})))

	rec := st.Get(ctx, testDS, "r1")
	assert.Equal(t, "keep", rec.GetString("name"))
}

// ----------------------------------------------------------------------------
// $addToSet / $pull
// ----------------------------------------------------------------------------

func TestAddToSet_AddsAndDedups(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"tags": []any{}}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange("v2", "r1", Op{
		Type:    OpAddToSet,
		Path:    []string{"tags"},
		Payload: arena.NewString("urgent"),
	})))
	// duplicate
	require.NoError(t, st.ApplyChange(ctx, makeChange("v3", "r1", Op{
		Type:    OpAddToSet,
		Path:    []string{"tags"},
		Payload: arena.NewString("urgent"),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange("v4", "r1", Op{
		Type:    OpAddToSet,
		Path:    []string{"tags"},
		Payload: arena.NewString("done"),
	})))

	rec := st.Get(ctx, testDS, "r1")
	tags := rec.GetArray("tags")
	require.Len(t, tags, 2)
	got := []string{string(tags[0].GetStringBytes()), string(tags[1].GetStringBytes())}
	assert.Equal(t, []string{"urgent", "done"}, got)
}

func TestAddToSet_DoesNotUpdateOrder(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"tags": []any{}}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange("v5", "r1", Op{
		Type:    OpAddToSet,
		Path:    []string{"tags"},
		Payload: arena.NewString("a"),
	})))

	rec := st.Get(ctx, testDS, "r1")
	// Spec §5.3: $addToSet does NOT update _ver[field]. The field's stored
	// order remains at the insert default v1.
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, "tags"))
}

func TestAddToSet_SkipsIfNewerSetAlready(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{}),
	})))
	// $set tags at v9
	require.NoError(t, st.ApplyChange(ctx, makeChange("v9", "r1", Op{
		Type:    OpSet,
		Path:    []string{"tags"},
		Payload: goToAnyenc(arena, []string{"done"}),
	})))
	// $addToSet at v5 should be skipped (v5 < v9).
	require.NoError(t, st.ApplyChange(ctx, makeChange("v5", "r1", Op{
		Type:    OpAddToSet,
		Path:    []string{"tags"},
		Payload: arena.NewString("urgent"),
	})))

	rec := st.Get(ctx, testDS, "r1")
	tags := rec.GetArray("tags")
	require.Len(t, tags, 1)
	assert.Equal(t, "done", string(tags[0].GetStringBytes()))
}

func TestPull_RemovesElements(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"tags": []any{"a", "b", "c", "b"}}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange("v2", "r1", Op{
		Type:    OpPull,
		Path:    []string{"tags"},
		Payload: arena.NewString("b"),
	})))

	rec := st.Get(ctx, testDS, "r1")
	tags := rec.GetArray("tags")
	require.Len(t, tags, 2)
	assert.Equal(t, "a", string(tags[0].GetStringBytes()))
	assert.Equal(t, "c", string(tags[1].GetStringBytes()))
}

// ----------------------------------------------------------------------------
// $inc / $incGated
// ----------------------------------------------------------------------------

func TestInc_Commutative(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"count": 0}),
	})))
	for i, ver := range []VersionId{"v3", "v2", "v5", "v4"} {
		require.NoError(t, st.ApplyChange(ctx, makeChange(ver, "r1", Op{
			Type:    OpInc,
			Path:    []string{"count"},
			Payload: arena.NewNumberInt(1),
		})))
		_ = i
	}

	rec := st.Get(ctx, testDS, "r1")
	assert.Equal(t, 4, rec.GetInt("count"))
	// _ver.count untouched since insert.
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, "count"))
}

func TestInc_SkipsIfStaleAgainstSet(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"count": 0}),
	})))
	// $set count to 100 at v9
	require.NoError(t, st.ApplyChange(ctx, makeChange("v9", "r1", Op{
		Type:    OpSet,
		Path:    []string{"count"},
		Payload: arena.NewNumberInt(100),
	})))
	// $inc at v5 — stale, should be skipped (spec §5.5 edge case).
	require.NoError(t, st.ApplyChange(ctx, makeChange("v5", "r1", Op{
		Type:    OpInc,
		Path:    []string{"count"},
		Payload: arena.NewNumberInt(1),
	})))

	rec := st.Get(ctx, testDS, "r1")
	assert.Equal(t, 100, rec.GetInt("count"))
}

func TestIncGated_NewerWins(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"priority": 5}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange("v3", "r1", Op{
		Type:    OpIncGated,
		Path:    []string{"priority"},
		Payload: arena.NewNumberInt(2),
	})))
	// Concurrent stale incGated.
	require.NoError(t, st.ApplyChange(ctx, makeChange("v2", "r1", Op{
		Type:    OpIncGated,
		Path:    []string{"priority"},
		Payload: arena.NewNumberInt(10),
	})))

	rec := st.Get(ctx, testDS, "r1")
	assert.Equal(t, 7, rec.GetInt("priority"))
	assert.Equal(t, VersionId("v3"), GetRecordVersion(rec, "priority"))
}

// ----------------------------------------------------------------------------
// delete
// ----------------------------------------------------------------------------

func TestDelete_CreatesTombstone(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "hi"}),
	})))
	require.NoError(t, st.ApplyChange(ctx, Change{
		ObjectId:    "obj1",
		Dataset:     testDS,
		VersionId:   "v9",
		DataVersion: testDataVersion,
		Timestamp:   1715000000,
		Records: []RecordChange{
			{Id: "r1", Ops: []Op{{Type: OpDelete}}},
		},
	}))

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	assert.Nil(t, rec.Get("name"))
	assert.Equal(t, 1715000000, rec.GetInt(DeletedAtField))
	// Tombstone is excluded from Records().
	assert.Empty(t, st.Records(ctx, testDS))
}

func TestDelete_BlocksLaterInsert(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeChange("v1", "r1", Op{Type: OpDelete})))
	// Higher-version $set against a tombstone is still rejected — sticky.
	require.NoError(t, st.ApplyChange(ctx, makeChange("v9", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "resurrect"}),
	})))

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	assert.True(t, isTombstone(rec))
}

func TestDelete_StickyTombstoneRejectsAllOps(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "hi", "tags": []any{"x"}, "count": 5}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange("v3", "r1", Op{Type: OpDelete})))

	// Every kind of modify is rejected on a tombstone, regardless of version.
	cases := []Op{
		{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("Z")},
		{Type: OpUnset, Path: []string{"name"}},
		{Type: OpAddToSet, Path: []string{"tags"}, Payload: arena.NewString("y")},
		{Type: OpPull, Path: []string{"tags"}, Payload: arena.NewString("x")},
		{Type: OpInc, Path: []string{"count"}, Payload: arena.NewNumberInt(1)},
		{Type: OpIncGated, Path: []string{"count"}, Payload: arena.NewNumberInt(1)},
	}
	for _, op := range cases {
		require.NoError(t, st.ApplyChange(ctx, makeChange("v9", "r1", op)))
	}

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	assert.True(t, isTombstone(rec))
	// Original fields are gone (delete wiped them) and never came back.
	assert.Nil(t, rec.Get("name"))
	assert.Nil(t, rec.Get("tags"))
	assert.Nil(t, rec.Get("count"))
}

func TestSet_MultiFieldNestedPaths(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// Mongo-style dotted keys in a multi-field $set drill into nested
	// objects without touching siblings.
	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type: OpSet,
		Payload: recordPayload(arena, map[string]any{
			"name":       "hi",
			"meta.color": "red",
			"meta.size":  10,
		}),
	})))

	rec := st.Get(ctx, testDS, "r1")
	assert.Equal(t, "hi", rec.GetString("name"))
	assert.Equal(t, "red", rec.GetString("meta", "color"))
	assert.Equal(t, 10, rec.GetInt("meta", "size"))
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, "meta", "color"))
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, "meta", "size"))
}

// ----------------------------------------------------------------------------
// _ver.id creation marker
// ----------------------------------------------------------------------------

func TestVerIdMarker_StampedOnUpsertCreate(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "hi"}),
	})))

	rec := st.Get(ctx, testDS, "r1")
	// _ver.id records the creation version. Clients can read or query this
	// for sort/filter ("chat messages ordered by creation version DESC").
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, IdField))
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, "name"))
}

func TestVerIdMarker_StampedOnSinglePathUpsert(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Path:    []string{"name"},
		Payload: arena.NewString("hi"),
	})))

	rec := st.Get(ctx, testDS, "r1")
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, IdField))
}

func TestVerIdMarker_StampedOnUpsertInc(t *testing.T) {
	// $inc also auto-creates in upsert mode; the creation marker must land
	// even though $inc itself doesn't update _ver.
	st := newTestController(t)
	arena := &anyenc.Arena{}

	ch := Change{
		ObjectId: "obj1", Dataset: testDS, VersionId: "v1", DataVersion: testDataVersion,
		Records: []RecordChange{{Id: "r1", Upsert: true, Ops: []Op{{
			Type:    OpInc,
			Path:    []string{"count"},
			Payload: arena.NewNumberInt(5),
		}}}},
	}
	require.NoError(t, st.ApplyChange(ctx, ch))

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, IdField))
	assert.Equal(t, 5, rec.GetInt("count"))
	// $inc does NOT stamp _ver.count per spec §5.5.
	assert.Equal(t, VersionId(""), GetRecordVersion(rec, "count"))
}

func TestVerIdMarker_NotSetOnStrictUpdate(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// Create with upsert...
	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "first"}),
	})))
	// ...then strict update at a later version. The creation marker must
	// not move — it records *creation*, not *last touch*.
	require.NoError(t, st.ApplyChange(ctx, makeChange("v5", "r1", Op{
		Type:    OpSet,
		Path:    []string{"name"},
		Payload: arena.NewString("second"),
	})))

	rec := st.Get(ctx, testDS, "r1")
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, IdField))
	assert.Equal(t, VersionId("v5"), GetRecordVersion(rec, "name"))
}

func TestVerIdMarker_PreservedAcrossDelete(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "hi"}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange("v5", "r1", Op{Type: OpDelete})))

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	assert.True(t, isTombstone(rec))
	// Tombstone keeps the original creation version — sort by _ver.id still
	// places the deleted record where it was created, not where it died.
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, IdField))
	// Non-id fields fall back to the deletion version via the "*" default.
	assert.Equal(t, VersionId("v5"), GetRecordVersion(rec, "name"))
}

func TestVerIdMarker_ConcurrentUpsertsConverge(t *testing.T) {
	// Alice and Bob both upsert the same id with disjoint fields and
	// different versions. Regardless of delivery order, _ver.id must be the
	// minimum of the two versions on both peers.
	arena := &anyenc.Arena{}
	alice := makeUpsert("va", "x", Op{
		Type: OpSet, Path: []string{"a"}, Payload: arena.NewNumberInt(1),
	})
	bob := makeUpsert("vb", "x", Op{
		Type: OpSet, Path: []string{"b"}, Payload: arena.NewNumberInt(2),
	})

	// Peer 1: Alice first.
	p1 := newTestController(t)
	require.NoError(t, p1.ApplyChange(ctx, alice))
	require.NoError(t, p1.ApplyChange(ctx, bob))
	rec1 := p1.Get(ctx, testDS, "x")

	// Peer 2: Bob first.
	p2 := newTestController(t)
	require.NoError(t, p2.ApplyChange(ctx, bob))
	require.NoError(t, p2.ApplyChange(ctx, alice))
	rec2 := p2.Get(ctx, testDS, "x")

	// Both converge on the same record content AND on the same _ver.id
	// (the minimum of "va" and "vb", which is "va").
	assert.Equal(t, 1, rec1.GetInt("a"))
	assert.Equal(t, 2, rec1.GetInt("b"))
	assert.Equal(t, 1, rec2.GetInt("a"))
	assert.Equal(t, 2, rec2.GetInt("b"))
	assert.Equal(t, VersionId("va"), GetRecordVersion(rec1, IdField))
	assert.Equal(t, VersionId("va"), GetRecordVersion(rec2, IdField))
}

func TestVerIdMarker_LaterUpsertCannotRaiseMarker(t *testing.T) {
	// A newer-version upsert must NOT push _ver.id forward; the marker only
	// moves downward. "latest upsert sets it" would break the min semantics
	// and divergence would return.
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("va", "x", Op{
		Type: OpSet, Path: []string{"a"}, Payload: arena.NewNumberInt(1),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeUpsert("vz", "x", Op{
		Type: OpSet, Path: []string{"b"}, Payload: arena.NewNumberInt(2),
	})))

	rec := st.Get(ctx, testDS, "x")
	assert.Equal(t, VersionId("va"), GetRecordVersion(rec, IdField))
}

func TestVerIdMarker_TombstoneLoweredByLaterEarlierUpsert(t *testing.T) {
	// Delete-on-absent seeds _ver.id with the delete version. A subsequent
	// concurrent upsert with an older version arrives — the record stays
	// tombstoned (sticky), but _ver.id is lowered so sort order converges
	// with a peer that saw the upsert first.
	arena := &anyenc.Arena{}
	upsert := makeUpsert("va", "x", Op{
		Type: OpSet, Path: []string{"a"}, Payload: arena.NewNumberInt(1),
	})
	del := makeChange("vd", "x", Op{Type: OpDelete})

	// Peer 1: upsert then delete.
	p1 := newTestController(t)
	require.NoError(t, p1.ApplyChange(ctx, upsert))
	require.NoError(t, p1.ApplyChange(ctx, del))
	rec1 := p1.Get(ctx, testDS, "x")
	require.True(t, isTombstone(rec1))

	// Peer 2: delete then upsert (upsert hits sticky tombstone).
	p2 := newTestController(t)
	require.NoError(t, p2.ApplyChange(ctx, del))
	require.NoError(t, p2.ApplyChange(ctx, upsert))
	rec2 := p2.Get(ctx, testDS, "x")
	require.True(t, isTombstone(rec2))

	// Both peers end up with _ver.id = min("va", "vd") = "va".
	assert.Equal(t, VersionId("va"), GetRecordVersion(rec1, IdField))
	assert.Equal(t, VersionId("va"), GetRecordVersion(rec2, IdField))
}

func TestVerIdMarker_DeleteOnAbsentSeedsFromDeleteVersion(t *testing.T) {
	st := newTestController(t)

	// Delete arrives for an id we've never seen — tombstone is seeded from
	// the delete itself.
	require.NoError(t, st.ApplyChange(ctx, makeChange("v1", "r1", Op{Type: OpDelete})))

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	assert.True(t, isTombstone(rec))
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, IdField))
}

// ----------------------------------------------------------------------------
// AddSeq watermark
// ----------------------------------------------------------------------------

func TestAddSeq_StartsAtZero(t *testing.T) {
	st := newTestController(t)
	assert.Equal(t, uint64(0), st.MaxAddSeq())
}

func TestAddSeq_MonotonicBump(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	ch := makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "x"}),
	})
	ch.AddSeq = 5
	require.NoError(t, st.ApplyChange(ctx, ch))
	assert.Equal(t, uint64(5), st.MaxAddSeq())

	// Higher seq advances the watermark.
	ch2 := makeChange("v2", "r1", Op{
		Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("y"),
	})
	ch2.AddSeq = 9
	require.NoError(t, st.ApplyChange(ctx, ch2))
	assert.Equal(t, uint64(9), st.MaxAddSeq())
}

func TestAddSeq_LowerSeqDoesNotRegress(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// Seed watermark at 10.
	ch := makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "x"}),
	})
	ch.AddSeq = 10
	require.NoError(t, st.ApplyChange(ctx, ch))

	// Replay an older delivery — the CRDT is idempotent so apply succeeds,
	// but the watermark must NOT move backwards.
	replay := makeChange("v2", "r1", Op{
		Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("replayed"),
	})
	replay.AddSeq = 3
	require.NoError(t, st.ApplyChange(ctx, replay))
	assert.Equal(t, uint64(10), st.MaxAddSeq())

	// The CRDT did still apply the write (v2 > v1).
	assert.Equal(t, "replayed", st.Get(ctx, testDS, "r1").GetString("name"))
}

func TestAddSeq_FailedChangeDoesNotBump(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// Change for a missing dataset → ErrUnknownDataset before any mutation.
	ch := Change{
		ObjectId: "obj1", Dataset: "nonexistent", DataVersion: testDataVersion,
		ChangeId: "chA", VersionId: "v1", AddSeq: 7,
		Records: []RecordChange{{Id: "r1", Upsert: true, Ops: []Op{{Type: OpSet,
			Payload: recordPayload(arena, map[string]any{"name": "x"}),
		}}}},
	}
	require.Error(t, st.ApplyChange(ctx, ch))
	assert.Equal(t, uint64(0), st.MaxAddSeq())
}

func TestAddSeq_SetMaxAddSeqSeeds(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// Simulate "restore": space layer read persisted watermark and seeds us.
	st.SetMaxAddSeq(42)
	assert.Equal(t, uint64(42), st.MaxAddSeq())

	// A change with lower seq does not regress.
	ch := makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "x"}),
	})
	ch.AddSeq = 30
	require.NoError(t, st.ApplyChange(ctx, ch))
	assert.Equal(t, uint64(42), st.MaxAddSeq())

	// A change above the seed does advance.
	ch2 := makeChange("v2", "r1", Op{
		Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("y"),
	})
	ch2.AddSeq = 50
	require.NoError(t, st.ApplyChange(ctx, ch2))
	assert.Equal(t, uint64(50), st.MaxAddSeq())
}

// ----------------------------------------------------------------------------
// Empty id → ChangeId sugar
// ----------------------------------------------------------------------------

func TestEmptyId_ResolvesToChangeId(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	ch := Change{
		ObjectId: "obj1", Dataset: testDS, DataVersion: testDataVersion,
		ChangeId: "chA", VersionId: "v1",
		Records: []RecordChange{{
			Upsert: true, // empty Id — no record to target yet
			Ops: []Op{{
				Type:    OpSet,
				Payload: recordPayload(arena, map[string]any{"name": "anon"}),
			}},
		}},
	}
	require.NoError(t, st.ApplyChange(ctx, ch))

	derived := DeriveRecordId("chA")
	rec := st.Get(ctx, testDS, derived)
	require.NotNil(t, rec)
	assert.Equal(t, derived, rec.GetString(IdField))
	assert.Equal(t, "anon", rec.GetString("name"))
}

func TestEmptyId_MultipleRecordsGetSuffixed(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	ch := Change{
		ObjectId: "obj1", Dataset: testDS, DataVersion: testDataVersion,
		ChangeId: "chA", VersionId: "v1",
		Records: []RecordChange{
			{Upsert: true, Ops: []Op{{Type: OpSet,
				Payload: recordPayload(arena, map[string]any{"name": "a"}),
			}}},
			{Upsert: true, Ops: []Op{{Type: OpSet,
				Payload: recordPayload(arena, map[string]any{"name": "b"}),
			}}},
			{Upsert: true, Ops: []Op{{Type: OpSet,
				Payload: recordPayload(arena, map[string]any{"name": "c"}),
			}}},
		},
	}
	require.NoError(t, st.ApplyChange(ctx, ch))

	// First empty uses DeriveRecordId(ChangeId); subsequent get :1, :2, ...
	derived := DeriveRecordId("chA")
	a := st.Get(ctx, testDS, derived)
	require.NotNil(t, a)
	assert.Equal(t, "a", a.GetString("name"))

	b := st.Get(ctx, testDS, derived+":1")
	require.NotNil(t, b)
	assert.Equal(t, "b", b.GetString("name"))

	c := st.Get(ctx, testDS, derived+":2")
	require.NotNil(t, c)
	assert.Equal(t, "c", c.GetString("name"))
}

func TestEmptyId_MixedWithExplicitIdsSkipsExplicit(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// Explicit ids are NOT counted toward the emptySeen index — the suffix
	// reflects position among empty-id records only, not absolute index.
	ch := Change{
		ObjectId: "obj1", Dataset: testDS, DataVersion: testDataVersion,
		ChangeId: "chA", VersionId: "v1",
		Records: []RecordChange{
			{Id: "explicit1", Upsert: true, Ops: []Op{{Type: OpSet,
				Payload: recordPayload(arena, map[string]any{"name": "x"}),
			}}},
			{Upsert: true, Ops: []Op{{Type: OpSet, // first empty
				Payload: recordPayload(arena, map[string]any{"name": "a"}),
			}}},
			{Id: "explicit2", Upsert: true, Ops: []Op{{Type: OpSet,
				Payload: recordPayload(arena, map[string]any{"name": "y"}),
			}}},
			{Upsert: true, Ops: []Op{{Type: OpSet, // second empty → :1
				Payload: recordPayload(arena, map[string]any{"name": "b"}),
			}}},
		},
	}
	require.NoError(t, st.ApplyChange(ctx, ch))

	derived := DeriveRecordId("chA")
	assert.Equal(t, "x", st.Get(ctx, testDS, "explicit1").GetString("name"))
	assert.Equal(t, "a", st.Get(ctx, testDS, derived).GetString("name"))
	assert.Equal(t, "y", st.Get(ctx, testDS, "explicit2").GetString("name"))
	assert.Equal(t, "b", st.Get(ctx, testDS, derived+":1").GetString("name"))
}

func TestEmptyId_ErrorsWhenUpsertFalse(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// Strict modify (Upsert=false) on an empty id is nonsensical: the
	// resolved id would be freshly derived from ChangeId, so no record can
	// possibly exist at it. Reject the combination as a programming error.
	ch := Change{
		ObjectId: "obj1", Dataset: testDS, DataVersion: testDataVersion,
		ChangeId: "chA", VersionId: "v1",
		Records: []RecordChange{{
			// Upsert defaults to false
			Ops: []Op{{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("bad")}},
		}},
	}
	err := st.ApplyChange(ctx, ch)
	require.ErrorIs(t, err, ErrEmptyIdRequiresUpsert)

	// State untouched.
	assert.Nil(t, st.Get(ctx, testDS, DeriveRecordId("chA")))
}

func TestEmptyId_ErrorsWhenChangeIdAlsoEmpty(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	ch := Change{
		ObjectId: "obj1", Dataset: testDS, DataVersion: testDataVersion,
		VersionId: "v1", // no ChangeId
		Records: []RecordChange{{
			Upsert: true,
			Ops: []Op{{Type: OpSet,
				Payload: recordPayload(arena, map[string]any{"name": "doomed"}),
			}},
		}},
	}
	err := st.ApplyChange(ctx, ch)
	require.ErrorIs(t, err, ErrMissingRecordId)
}

func TestEmptyId_SubsequentModifiesByDerivedId(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// Create via empty-id sugar...
	require.NoError(t, st.ApplyChange(ctx, Change{
		ObjectId: "obj1", Dataset: testDS, DataVersion: testDataVersion,
		ChangeId: "chA", VersionId: "v1",
		Records: []RecordChange{{Upsert: true, Ops: []Op{{Type: OpSet,
			Payload: recordPayload(arena, map[string]any{"name": "init"}),
		}}}},
	}))
	// ...and modify the same record using the derived id.
	derived := DeriveRecordId("chA")
	require.NoError(t, st.ApplyChange(ctx, makeChange("v2", derived, Op{
		Type:    OpSet,
		Path:    []string{"name"},
		Payload: arena.NewString("renamed"),
	})))

	rec := st.Get(ctx, testDS, derived)
	assert.Equal(t, "renamed", rec.GetString("name"))
}

// ----------------------------------------------------------------------------
// Upsert flag
// ----------------------------------------------------------------------------

func TestUpsert_StrictSkipsAbsent(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// No record exists. A strict (Upsert=false) modify must be a silent
	// no-op — no record gets created from the attempt.
	require.NoError(t, st.ApplyChange(ctx, makeChange("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "ghost"}),
	})))
	assert.Nil(t, st.Get(ctx, testDS, "r1"))
}

func TestUpsert_StrictSkipSurfacesRejection(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// The skip is silent in terms of state (no record, no error), but a
	// rejection is surfaced via ApplyResult so callers can detect that
	// nothing landed in the projection — useful when middleware computes
	// "inserted ids" from RecordIds (resolved structurally pre-apply)
	// and would otherwise report a fully successful write.
	res, err := st.ApplyChangeWithResult(ctx, makeChange("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "ghost"}),
	}))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.ErrorIs(t, res.Rejections[0].Err, ErrStrictSkipAbsent)
	assert.Equal(t, "r1", res.Rejections[0].RecordId)
	assert.Nil(t, st.Get(ctx, testDS, "r1"))
}

func TestUpsert_TrueCreatesAbsent(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "real"}),
	})))
	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	assert.Equal(t, "real", rec.GetString("name"))
}

func TestUpsert_AllModifyOpsRespectStrict(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	cases := []Op{
		{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("x")},
		{Type: OpUnset, Path: []string{"name"}},
		{Type: OpAddToSet, Path: []string{"tags"}, Payload: arena.NewString("a")},
		{Type: OpPull, Path: []string{"tags"}, Payload: arena.NewString("a")},
		{Type: OpInc, Path: []string{"count"}, Payload: arena.NewNumberInt(1)},
		{Type: OpIncGated, Path: []string{"count"}, Payload: arena.NewNumberInt(1)},
	}
	for _, op := range cases {
		require.NoError(t, st.ApplyChange(ctx, makeChange("v1", "r1", op)))
		assert.Nilf(t, st.Get(ctx, testDS, "r1"), "op %s in strict mode must not auto-create", op.Type)
	}
}

func TestUpsert_DeleteIgnoresFlagAndCreatesTombstone(t *testing.T) {
	st := newTestController(t)

	// Strict delete on an absent record still writes a tombstone. This
	// preserves "delete wins absolutely" against a create that hasn't yet
	// been delivered to this peer.
	require.NoError(t, st.ApplyChange(ctx, makeChange("v1", "r1", Op{Type: OpDelete})))
	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	assert.True(t, isTombstone(rec))
}

func TestUpsert_StrictOnLiveRecordBehavesNormally(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// Create via upsert, then update via strict (default).
	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "first"}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange("v2", "r1", Op{
		Type:    OpSet,
		Path:    []string{"name"},
		Payload: arena.NewString("second"),
	})))

	rec := st.Get(ctx, testDS, "r1")
	assert.Equal(t, "second", rec.GetString("name"))
}

// ----------------------------------------------------------------------------
// Multi-record-per-batch semantics
// ----------------------------------------------------------------------------

// A batch with two RecordChanges targeting the same id applies sequentially
// in Records order. Second writes see the first's effects. The batch has a
// strict internal order — this is an intentional part of the contract,
// useful for generated code, split validation phases, and merged transport
// batches.
func TestBatch_DuplicateIdAppliesSequentially(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	ch := Change{
		ObjectId: "obj1", Dataset: testDS, VersionId: "v1", DataVersion: testDataVersion,
		Records: []RecordChange{
			{Id: "r1", Upsert: true, Ops: []Op{{Type: OpSet,
				Payload: recordPayload(arena, map[string]any{"name": "first"}),
			}}},
			{Id: "r1", Upsert: true, Ops: []Op{{Type: OpSet,
				Path: []string{"color"}, Payload: arena.NewString("red"),
			}}},
		},
	}
	require.NoError(t, st.ApplyChange(ctx, ch))

	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	assert.Equal(t, "first", rec.GetString("name"))
	assert.Equal(t, "red", rec.GetString("color"))
}

// Auto-resolved empty ids disambiguate against each other via the :N
// suffix, so two empty-id records in one batch are NOT duplicates —
// they produce distinct records.
func TestBatch_MultipleEmptyIdsProduceDistinctRecords(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	ch := Change{
		ObjectId: "obj1", Dataset: testDS, DataVersion: testDataVersion,
		ChangeId: "chA", VersionId: "v1",
		Records: []RecordChange{
			{Upsert: true, Ops: []Op{{Type: OpSet,
				Payload: recordPayload(arena, map[string]any{"name": "a"}),
			}}},
			{Upsert: true, Ops: []Op{{Type: OpSet,
				Payload: recordPayload(arena, map[string]any{"name": "b"}),
			}}},
		},
	}
	require.NoError(t, st.ApplyChange(ctx, ch))
	derived := DeriveRecordId("chA")
	assert.Equal(t, "a", st.Get(ctx, testDS, derived).GetString("name"))
	assert.Equal(t, "b", st.Get(ctx, testDS, derived+":1").GetString("name"))
}

// ----------------------------------------------------------------------------
// Path / reserved field validation
// ----------------------------------------------------------------------------

func TestValidate_RejectsUnderscorePrefixedTopPath(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// Simulate user trying to forge a tombstone via _deletedAt.
	err := st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Path:    []string{"_deletedAt"},
		Payload: arena.NewNumberInt(123),
	}))
	require.ErrorIs(t, err, ErrInvalidPath)
	assert.Nil(t, st.Get(ctx, testDS, "r1"))
}

func TestValidate_RejectsVerTopPath(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	err := st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Path:    []string{"_ver", "hacked"},
		Payload: arena.NewString("x"),
	}))
	require.ErrorIs(t, err, ErrInvalidPath)
}

func TestValidate_RejectsDotInPathElement(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// Callers must use nested path components, not literal dots, to avoid
	// confusion with the dotted-string form accepted by multi-field $set.
	err := st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Path:    []string{"meta.color"},
		Payload: arena.NewString("red"),
	}))
	require.ErrorIs(t, err, ErrInvalidPath)
}

func TestValidate_RejectsEmptyPathElement(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	err := st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Path:    []string{"meta", "", "color"},
		Payload: arena.NewString("red"),
	}))
	require.ErrorIs(t, err, ErrInvalidPath)
}

func TestValidate_RejectsReservedKeyInMultiFieldSet(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// A multi-field $set payload with a reserved key must drop the whole
	// change, not just silently skip the offending key.
	err := st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type: OpSet,
		Payload: recordPayload(arena, map[string]any{
			"name":        "hi",
			"_deletedAt":  1000,
		}),
	}))
	require.ErrorIs(t, err, ErrInvalidPath)
	assert.Nil(t, st.Get(ctx, testDS, "r1"))
}

func TestValidate_RejectsEmptySegmentInDottedKey(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	// "meta." splits to ["meta", ""] — empty segment is rejected.
	err := st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type: OpSet,
		Payload: recordPayload(arena, map[string]any{
			"meta.": "broken",
		}),
	}))
	require.ErrorIs(t, err, ErrInvalidPath)
}

// ----------------------------------------------------------------------------
// Type safety on $inc / $addToSet / $pull / $incGated
// ----------------------------------------------------------------------------

func TestInc_SkipsOnNonNumericExistingValue(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"count": "not a number"}),
	})))
	// Previously this would destructively overwrite count with a number.
	// New behavior: silent skip, field unchanged.
	require.NoError(t, st.ApplyChange(ctx, makeChange("v2", "r1", Op{
		Type: OpInc, Path: []string{"count"}, Payload: arena.NewNumberInt(5),
	})))

	rec := st.Get(ctx, testDS, "r1")
	assert.Equal(t, "not a number", rec.GetString("count"))
}

func TestIncGated_SkipsOnNonNumericExistingValue(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"priority": "high"}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange("v2", "r1", Op{
		Type: OpIncGated, Path: []string{"priority"}, Payload: arena.NewNumberInt(1),
	})))

	rec := st.Get(ctx, testDS, "r1")
	assert.Equal(t, "high", rec.GetString("priority"))
	// _ver.priority must also stay untouched since the op was skipped.
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, "priority"))
}

func TestAddToSet_SkipsOnNonArrayExistingValue(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"tags": "single-string"}),
	})))
	// Previously would have replaced with ["new"]. Now skip.
	require.NoError(t, st.ApplyChange(ctx, makeChange("v2", "r1", Op{
		Type: OpAddToSet, Path: []string{"tags"}, Payload: arena.NewString("new"),
	})))

	rec := st.Get(ctx, testDS, "r1")
	assert.Equal(t, "single-string", rec.GetString("tags"))
}

func TestPull_SkipsOnNonArrayExistingValue(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"tags": "single-string"}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange("v2", "r1", Op{
		Type: OpPull, Path: []string{"tags"}, Payload: arena.NewString("x"),
	})))

	rec := st.Get(ctx, testDS, "r1")
	assert.Equal(t, "single-string", rec.GetString("tags"))
}

func TestInc_OnAbsentFieldStartsFromZero(t *testing.T) {
	// Absent-field case stays unchanged: $inc on a missing field creates
	// it starting from zero. Only existing-but-wrong-type triggers the
	// type-safe skip.
	st := newTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "hi"}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange("v2", "r1", Op{
		Type: OpInc, Path: []string{"hits"}, Payload: arena.NewNumberInt(3),
	})))

	rec := st.Get(ctx, testDS, "r1")
	assert.Equal(t, 3, rec.GetInt("hits"))
}

// ----------------------------------------------------------------------------
// validation
// ----------------------------------------------------------------------------

type rejectingHandler struct {
	DefaultHandler
}

func (rejectingHandler) BeforeModify(_ *ChangeCtx, _ *RecordChange, op *Op, _ *Sink) error {
	if op.Type == OpUnset {
		return assert.AnError
	}
	return nil
}

// Per-op handler errors drop just the offending op; other ops in the same
// RecordChange still apply and ApplyChange returns nil. Path-syntax errors
// abort the whole Change (see TestValidate_* cases above).
func TestValidation_DropsOffendingOp(t *testing.T) {
	arena := &anyenc.Arena{}
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	st, err := NewController(ctx, "obj1", db, rejectingHandler{DefaultHandler{DatasetName: testDS}})
	require.NoError(t, err)

	// Auto-create via $set is allowed by the handler.
	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "hi"}),
	})))
	// $unset is rejected → dropped silently. $set color landed at v2.
	require.NoError(t, st.ApplyChange(ctx, makeChange("v2", "r1",
		Op{Type: OpSet, Path: []string{"color"}, Payload: arena.NewString("red")},
		Op{Type: OpUnset, Path: []string{"name"}},
	)))

	rec := st.Get(ctx, testDS, "r1")
	assert.Equal(t, "hi", rec.GetString("name"))   // $unset was dropped, name survives
	assert.Equal(t, "red", rec.GetString("color")) // $set landed
}

func TestUnknownDataset(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}
	err := st.ApplyChange(ctx, Change{
		ObjectId:    "obj1",
		Dataset:     "missing",
		VersionId:   "v1",
		DataVersion: testDataVersion,
		Records: []RecordChange{
			{Id: "r1", Ops: []Op{{Type: OpSet, Payload: recordPayload(arena, map[string]any{"name": "x"})}}},
		},
	})
	require.ErrorIs(t, err, ErrUnknownDataset)
}

// Empty DataVersion is rejected before any validation or apply work runs.
// See docs/types-properties-proposal.md § "Change-level DataVersion".
func TestMissingDataVersion(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}
	err := st.ApplyChange(ctx, Change{
		ObjectId:  "obj1",
		Dataset:   testDS,
		VersionId: "v1",
		// DataVersion intentionally omitted.
		Records: []RecordChange{{
			Id: "r1", Upsert: true,
			Ops: []Op{{Type: OpSet, Payload: recordPayload(arena, map[string]any{"name": "x"})}},
		}},
	})
	require.ErrorIs(t, err, ErrMissingDataVersion)
	// No side effects — watermark untouched, no record written.
	assert.Equal(t, uint64(0), st.MaxAddSeq())
	assert.Nil(t, st.Get(ctx, testDS, "r1"))
}
