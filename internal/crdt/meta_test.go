package crdt

import (
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openMetaColl(t *testing.T) (anystore.DB, anystore.Collection) {
	t.Helper()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "meta.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	coll, err := db.Collection(ctx, MetaCollectionName)
	require.NoError(t, err)
	return db, coll
}

func TestMeta_RoundTrip(t *testing.T) {
	_, coll := openMetaColl(t)

	hv := map[string]int{"blocks": 1, "chat": 2}
	require.NoError(t, PersistMeta(ctx, coll, "obj1", 42, hv))

	seq, hvOut, err := LoadMeta(ctx, coll, "obj1")
	require.NoError(t, err)
	assert.Equal(t, uint64(42), seq)
	assert.Equal(t, hv, hvOut)
}

func TestMeta_MissingReturnsZero(t *testing.T) {
	_, coll := openMetaColl(t)

	seq, hv, err := LoadMeta(ctx, coll, "nonexistent")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), seq)
	assert.Nil(t, hv)
}

func TestMeta_UpdateOverwrites(t *testing.T) {
	_, coll := openMetaColl(t)

	require.NoError(t, PersistMeta(ctx, coll, "obj1", 10, map[string]int{"blocks": 1}))
	require.NoError(t, PersistMeta(ctx, coll, "obj1", 20, map[string]int{"blocks": 2, "chat": 1}))

	seq, hv, err := LoadMeta(ctx, coll, "obj1")
	require.NoError(t, err)
	assert.Equal(t, uint64(20), seq)
	assert.Equal(t, map[string]int{"blocks": 2, "chat": 1}, hv)
}

func TestMeta_InsideSameTx(t *testing.T) {
	db, coll := openMetaColl(t)

	// PersistMeta inside a WriteTx alongside other writes.
	tx, err := db.WriteTx(ctx)
	require.NoError(t, err)
	txCtx := tx.Context()

	require.NoError(t, PersistMeta(txCtx, coll, "obj1", 99, map[string]int{"blocks": 3}))
	require.NoError(t, tx.Commit())

	seq, hv, err := LoadMeta(ctx, coll, "obj1")
	require.NoError(t, err)
	assert.Equal(t, uint64(99), seq)
	assert.Equal(t, 3, hv["blocks"])
}

func TestMeta_ControllerLoadAndSeed(t *testing.T) {
	db, coll := openMetaColl(t)

	// Persist some metadata.
	require.NoError(t, PersistMeta(ctx, coll, "obj1", 55, map[string]int{"blocks": 2}))

	// Create a Controller and load.
	ctrl, err := NewController(ctx, "obj1", db, DefaultHandler{DatasetName: "blocks", HandlerVersion: 3})
	require.NoError(t, err)
	storedHV, err := ctrl.LoadAndSeedMeta(ctx, coll)
	require.NoError(t, err)

	assert.Equal(t, uint64(55), ctrl.MaxAddSeq())
	assert.Equal(t, 2, storedHV["blocks"])

	// Current handler version is 3, stored is 2 → caller detects mismatch.
	currentHV := ctrl.HandlerVersions()
	assert.Equal(t, 3, currentHV["blocks"])
	assert.NotEqual(t, storedHV["blocks"], currentHV["blocks"])
}

func TestMeta_SpaceMaxAddSeq_RoundTrip(t *testing.T) {
	_, coll := openMetaColl(t)

	require.NoError(t, PersistSpaceMaxAddSeq(ctx, coll, "spaceA", 123))
	got, err := LoadSpaceMaxAddSeq(ctx, coll, "spaceA")
	require.NoError(t, err)
	assert.Equal(t, uint64(123), got)
}

func TestMeta_SpaceMaxAddSeq_MissingReturnsZero(t *testing.T) {
	_, coll := openMetaColl(t)

	got, err := LoadSpaceMaxAddSeq(ctx, coll, "never-written")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), got)
}

func TestMeta_SpaceMaxAddSeq_Overwrite(t *testing.T) {
	_, coll := openMetaColl(t)

	require.NoError(t, PersistSpaceMaxAddSeq(ctx, coll, "spaceA", 10))
	require.NoError(t, PersistSpaceMaxAddSeq(ctx, coll, "spaceA", 200))

	got, err := LoadSpaceMaxAddSeq(ctx, coll, "spaceA")
	require.NoError(t, err)
	assert.Equal(t, uint64(200), got)
}

// Per-object and per-space rows share the _meta collection. Verify
// they don't collide: writing to one must not change the other.
func TestMeta_SpaceMaxAddSeq_DoesNotCollideWithObjectRows(t *testing.T) {
	_, coll := openMetaColl(t)

	require.NoError(t, PersistMeta(ctx, coll, "objA", 7, map[string]int{"blocks": 1}))
	require.NoError(t, PersistSpaceMaxAddSeq(ctx, coll, "objA", 999))

	objSeq, hv, err := LoadMeta(ctx, coll, "objA")
	require.NoError(t, err)
	assert.Equal(t, uint64(7), objSeq, "object row untouched by space write")
	assert.Equal(t, 1, hv["blocks"])

	spaceSeq, err := LoadSpaceMaxAddSeq(ctx, coll, "objA")
	require.NoError(t, err)
	assert.Equal(t, uint64(999), spaceSeq, "space row untouched by object write")
}

func TestMeta_ControllerPersistMeta(t *testing.T) {
	db, coll := openMetaColl(t)
	arena := &anyenc.Arena{}

	ctrl, err := NewController(ctx, "obj1", db, DefaultHandler{DatasetName: testDS})
	require.NoError(t, err)

	// Apply a change to bump maxAddSeq.
	ch := makeUpsert("v1", "r1", Op{
		Type: OpSet, Payload: recordPayload(arena, map[string]any{"name": "hi"}),
	})
	ch.AddSeq = 7
	require.NoError(t, ctrl.ApplyChange(ctx, ch))

	// Persist and re-load.
	require.NoError(t, ctrl.PersistMeta(ctx, coll))
	seq, _, err := LoadMeta(ctx, coll, "obj1")
	require.NoError(t, err)
	assert.Equal(t, uint64(7), seq)
}
