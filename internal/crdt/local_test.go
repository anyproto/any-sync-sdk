package crdt

import (
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNextVersion_Monotonic(t *testing.T) {
	a := NextVersion("")
	b := NextVersion(a)
	c := NextVersion(b)
	assert.Less(t, string(a), string(b))
	assert.Less(t, string(b), string(c))
	// Strictly greater than the input, whatever the input was (incl. an
	// any-sync-style OrderId already on the field).
	assert.Greater(t, string(NextVersion("zzzz")), "zzzz")
}

func TestIsLocalPath(t *testing.T) {
	assert.True(t, IsLocalPath([]string{LocalFieldPrefix + "localStatus"}))
	assert.False(t, IsLocalPath([]string{"name"}))
	assert.False(t, IsLocalPath(nil))
}

// Synced changes must not write a local-namespace path — single-field
// and multi-field shapes both rejected before anything lands.
func TestApply_SyncedRejectsLocalPath(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}

	single := makeUpsert("v1", "rec1", Op{
		Type:    OpSet,
		Path:    []string{LocalFieldPrefix + "x"},
		Payload: arena.NewString("nope"),
	})
	require.Error(t, st.ApplyChange(ctx, single))

	multi := makeUpsert("v2", "rec2", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{LocalFieldPrefix + "x": "nope", "name": "ok"}),
	})
	require.Error(t, st.ApplyChange(ctx, multi))

	// Neither record should exist — the change aborts whole.
	assert.Nil(t, st.Get(ctx, testDS, "rec1"))
	assert.Nil(t, st.Get(ctx, testDS, "rec2"))
}

// A Change.Local must touch ONLY local paths; a non-local op is rejected.
func TestApply_LocalRejectsSyncedPath(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}
	ch := makeUpsert("v1", "rec1", Op{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("x")})
	ch.Local = true
	require.Error(t, st.ApplyChange(ctx, ch))
}

// A local write lands on a local path, is gated by the local version,
// and a subsequent synced write to a sibling synced field does not
// disturb it (per-field _ver isolation).
func TestApply_LocalSetIsolatedFromSynced(t *testing.T) {
	st := newTestController(t)
	arena := &anyenc.Arena{}
	const id = "rec1"
	localPath := LocalFieldPrefix + "localStatus"

	// Seed a synced field.
	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", id,
		Op{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("Alpha")})))

	// Local write to the local field, version allocated locally.
	lch := Change{
		ObjectId: "obj1", Dataset: testDS, DataVersion: testDataVersion, Local: true,
		Records: []RecordChange{{Id: id, Ops: []Op{
			{Type: OpSet, Path: []string{localPath}, Payload: arena.NewString("offloaded")},
		}}},
	}
	lch.VersionId = st.NextLocalVersion(ctx, &lch)
	require.NoError(t, st.ApplyChange(ctx, lch))

	rec := st.Get(ctx, testDS, id)
	require.NotNil(t, rec)
	assert.Equal(t, "offloaded", rec.GetString(localPath))
	assert.Equal(t, "Alpha", rec.GetString("name"))

	// Synced update to the synced field with a LOW version still applies
	// to its own field (its _ver entry is independent of the local field's
	// high local version).
	require.NoError(t, st.ApplyChange(ctx, makeChange("v2", id,
		Op{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("Beta")})))
	rec = st.Get(ctx, testDS, id)
	assert.Equal(t, "Beta", rec.GetString("name"))
	assert.Equal(t, "offloaded", rec.GetString(localPath))
}
