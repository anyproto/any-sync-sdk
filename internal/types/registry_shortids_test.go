package types_test

import (
	"context"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/types"
)

// insertShortId materialises one row in `<typeId>_shortIds` shaped the
// way the apply loop writes it: record id = shortId, with `_ver.id`
// carrying the triggering change's VersionId (the field LatestShortId
// sorts on).
func insertShortId(t *testing.T, ctx context.Context, db anystore.DB, typeId, shortId, verId string) {
	t.Helper()
	coll, err := db.Collection(ctx, typeId+"_shortIds")
	require.NoError(t, err)
	a := &anyenc.Arena{}
	doc := a.NewObject()
	doc.Set("id", a.NewString(shortId))
	ver := a.NewObject()
	ver.Set("id", a.NewString(verId))
	doc.Set("_ver", ver)
	require.NoError(t, coll.UpsertOne(ctx, doc))
}

func TestLiveRegistry_KnownShortId(t *testing.T) {
	ctx := context.Background()

	t.Run("nil db: never known", func(t *testing.T) {
		r := types.NewLiveRegistry(nil, staticOverlay())
		known, err := r.KnownShortId(ctx, "typeT", "sA")
		require.NoError(t, err)
		assert.False(t, known)
	})

	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "t.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	r := types.NewLiveRegistry(db, staticOverlay())

	t.Run("type with no shortIds collection: false, no error", func(t *testing.T) {
		known, err := r.KnownShortId(ctx, "neverWritten", "sA")
		require.NoError(t, err)
		assert.False(t, known)
	})

	insertShortId(t, ctx, db, "typeT", "sA", "v001")

	t.Run("present shortId is known", func(t *testing.T) {
		known, err := r.KnownShortId(ctx, "typeT", "sA")
		require.NoError(t, err)
		assert.True(t, known)
	})

	t.Run("absent shortId on an existing type is not known", func(t *testing.T) {
		known, err := r.KnownShortId(ctx, "typeT", "sZ")
		require.NoError(t, err)
		assert.False(t, known)
	})
}

func TestLiveRegistry_LatestShortId(t *testing.T) {
	ctx := context.Background()

	t.Run("nil db: empty", func(t *testing.T) {
		r := types.NewLiveRegistry(nil, staticOverlay())
		got, err := r.LatestShortId(ctx, "typeT")
		require.NoError(t, err)
		assert.Equal(t, "", got)
	})

	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "t.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	r := types.NewLiveRegistry(db, staticOverlay())

	t.Run("type with no important change: empty", func(t *testing.T) {
		got, err := r.LatestShortId(ctx, "neverWritten")
		require.NoError(t, err)
		assert.Equal(t, "", got)
	})

	t.Run("single row", func(t *testing.T) {
		insertShortId(t, ctx, db, "typeOne", "only", "v001")
		got, err := r.LatestShortId(ctx, "typeOne")
		require.NoError(t, err)
		assert.Equal(t, "only", got)
	})

	t.Run("returns shortId of the greatest _ver.id, not insertion order", func(t *testing.T) {
		// Insert out of version order; latest by _ver.id is v003 → "sB".
		insertShortId(t, ctx, db, "typeT", "sA", "v001")
		insertShortId(t, ctx, db, "typeT", "sB", "v003")
		insertShortId(t, ctx, db, "typeT", "sC", "v002")
		got, err := r.LatestShortId(ctx, "typeT")
		require.NoError(t, err)
		assert.Equal(t, "sB", got)
	})
}
