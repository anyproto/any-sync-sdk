package spacesync

import (
	"context"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// TestPersistWatermarkIfAhead pins the monotonic guard shared by the
// boot catch-up and the close-time snapshot: the stored space
// watermark advances only when the candidate is strictly ahead and
// never regresses.
func TestPersistWatermarkIfAhead(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "wm.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	coll, err := db.Collection(ctx, crdt.MetaCollectionName)
	require.NoError(t, err)

	const spaceId = "space-a"
	read := func() uint64 {
		v, rerr := crdt.LoadSpaceMaxAddSeq(ctx, coll, spaceId)
		require.NoError(t, rerr)
		return v
	}

	// Fresh row: 0 → 10 advances.
	require.Equal(t, uint64(0), read())
	advanced, err := persistWatermarkIfAhead(ctx, coll, spaceId, 10)
	require.NoError(t, err)
	assert.True(t, advanced)
	assert.Equal(t, uint64(10), read())

	// Equal: no write, no regress.
	advanced, err = persistWatermarkIfAhead(ctx, coll, spaceId, 10)
	require.NoError(t, err)
	assert.False(t, advanced)
	assert.Equal(t, uint64(10), read())

	// Behind: never regresses.
	advanced, err = persistWatermarkIfAhead(ctx, coll, spaceId, 5)
	require.NoError(t, err)
	assert.False(t, advanced)
	assert.Equal(t, uint64(10), read())

	// Ahead again: advances.
	advanced, err = persistWatermarkIfAhead(ctx, coll, spaceId, 17)
	require.NoError(t, err)
	assert.True(t, advanced)
	assert.Equal(t, uint64(17), read())

	// Other spaces are untouched.
	other, err := crdt.LoadSpaceMaxAddSeq(ctx, coll, "space-b")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), other)
}
