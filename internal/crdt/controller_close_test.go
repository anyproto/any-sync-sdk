package crdt

import (
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newCloseTestController builds a controller with one per-object dataset
// and one shared (per-space style) collection, returning both the
// controller and the DB for handle-liveness probes.
func newCloseTestController(t *testing.T) (*Controller, anystore.DB, anystore.Collection) {
	t.Helper()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "close.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	sharedColl, err := db.Collection(ctx, "spaceA_objects")
	require.NoError(t, err)
	st, err := NewControllerWithShared(ctx, "obj1", db, SharedCollections{"objects": sharedColl},
		HandlerReg{Name: testDS, Handler: DefaultHandler{}, Schema: dynSchema},
		HandlerReg{Name: "objects", Handler: DefaultHandler{}, Schema: dynSchema},
	)
	require.NoError(t, err)
	return st, db, sharedColl
}

// CloseOwnedCollections must close the per-object dataset handle (so its
// registry slot and caches are released) while leaving shared handles
// and the _meta collection alive.
func TestCloseOwnedCollections_ClosesPerObjectKeepsShared(t *testing.T) {
	st, db, sharedColl := newCloseTestController(t)
	arena := &anyenc.Arena{}

	// Materialise the per-object collection and a shared row.
	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1",
		Op{Type: OpSet, Payload: recordPayload(arena, map[string]any{"name": "hello"})})))
	require.NoError(t, st.ApplyChange(ctx, Change{
		ObjectId: "obj1", Dataset: "objects", VersionId: "v1", DataVersion: testDataVersion,
		Records: []RecordChange{{Id: "obj1", Upsert: true,
			Ops: []Op{{Type: OpSet, Payload: recordPayload(arena, map[string]any{"any": map[string]any{"name": "x"}})}}}},
	}))

	// Grab the live per-object handle out of the registry — same handle
	// the controller cached.
	perObj, err := db.OpenCollection(ctx, "obj1_"+testDS)
	require.NoError(t, err)

	require.NoError(t, st.CloseOwnedCollections())

	// Per-object handle is closed: direct ops on it fail.
	_, err = perObj.FindId(ctx, "r1")
	assert.ErrorIs(t, err, anystore.ErrCollectionClosed)

	// Shared handle stays alive.
	_, err = sharedColl.FindId(ctx, "obj1")
	assert.NoError(t, err)

	// _meta stays alive (watermark persist on the next apply must work).
	_, err = st.metaColl.FindId(ctx, "obj1")
	assert.NoError(t, err)
}

// After the close, reads and writes on the controller must re-open by
// name and see the persisted state — a stale cached pointer surviving
// eviction would surface ErrCollectionClosed here.
func TestCloseOwnedCollections_ReopensCleanly(t *testing.T) {
	st, _, _ := newCloseTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1",
		Op{Type: OpSet, Payload: recordPayload(arena, map[string]any{"name": "hello"})})))
	require.NoError(t, st.CloseOwnedCollections())

	// Read path re-opens by name.
	rec := st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	assert.Equal(t, "hello", string(rec.GetStringBytes("name")))

	// Write path re-opens by name.
	require.NoError(t, st.ApplyChange(ctx, makeChange("v2", "r1",
		Op{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("world")})))
	rec = st.Get(ctx, testDS, "r1")
	require.NotNil(t, rec)
	assert.Equal(t, "world", string(rec.GetStringBytes("name")))

	// Idempotent.
	require.NoError(t, st.CloseOwnedCollections())
}

// A released controller must not cache handles: a successor controller
// for the same object closes the shared-by-name registry handle on ITS
// eviction, and a stale cached pointer would then be permanently dead.
func TestCloseOwnedCollections_StaleControllerDoesNotCache(t *testing.T) {
	st, db, _ := newCloseTestController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "r1",
		Op{Type: OpSet, Payload: recordPayload(arena, map[string]any{"name": "hello"})})))
	require.NoError(t, st.CloseOwnedCollections())

	// Stale controller reads after release — opens by name, must not cache.
	require.NotNil(t, st.Get(ctx, testDS, "r1"))
	st.collMu.Lock()
	_, cached := st.collections[testDS]
	st.collMu.Unlock()
	assert.False(t, cached, "released controller must not re-cache handles")

	// Simulate the successor's eviction closing the registry handle,
	// then read again through the stale controller: it must re-open by
	// name and still succeed.
	coll, err := db.OpenCollection(ctx, "obj1_"+testDS)
	require.NoError(t, err)
	require.NoError(t, coll.Close())
	require.NotNil(t, st.Get(ctx, testDS, "r1"))
}
