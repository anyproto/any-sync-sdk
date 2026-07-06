package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestSDK_ChangeIndex exercises the consumer-side change-index surface:
//
//   - ChangedSince(0) lists every object that has applied a change,
//     ascending by ApplySeq, with _applySeq visible on the property record.
//   - ChangedSince(cursor) returns only objects that moved past it.
//   - Subscribe fires live on every applied change with (objectId,
//     addSeq).
func TestSDK_ChangeIndex(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Idx"})
	require.NoError(t, err)

	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Note"})
	require.NoError(t, err)
	titleProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Title", XKey: "title", Kind: space.PropertyKindString,
	})
	require.NoError(t, err)

	// Live feed: capture every notification.
	type ev = space.ObjectChange
	events := make(chan ev, 64)
	cancelSub := sp.Changes().Subscribe(func(e ev) { events <- e })
	defer cancelSub()

	// Two objects, each with a property write.
	idA, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	_, err = sp.Properties().Set(ctx, idA, typeId, map[string]any{titleProp: "alpha"})
	require.NoError(t, err)

	idB, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	_, err = sp.Properties().Set(ctx, idB, typeId, map[string]any{titleProp: "beta"})
	require.NoError(t, err)

	// Live feed saw both objects (among other system writes). Drain
	// what's buffered so far and assert both ids appeared.
	seen := map[string]uint64{}
	drainDeadline := time.After(2 * time.Second)
drain:
	for {
		select {
		case e := <-events:
			if e.ApplySeq > seen[e.ObjectId] {
				seen[e.ObjectId] = e.ApplySeq
			}
		case <-drainDeadline:
			break drain
		}
	}
	assert.Contains(t, seen, idA, "live feed observed object A")
	assert.Contains(t, seen, idB, "live feed observed object B")

	// Catch-up pull: ChangedSince(0) lists both, ascending by ApplySeq.
	all, err := sp.Changes().ChangedSince(ctx, 0, 0)
	require.NoError(t, err)
	idToSeq := map[string]uint64{}
	for i, c := range all {
		if i > 0 {
			assert.LessOrEqual(t, all[i-1].ApplySeq, c.ApplySeq, "ascending by ApplySeq")
		}
		idToSeq[c.ObjectId] = c.ApplySeq
	}
	require.Contains(t, idToSeq, idA)
	require.Contains(t, idToSeq, idB)

	// Cursor: ChangedSince(maxOfA) excludes A, still includes the later B.
	maxA := idToSeq[idA]
	if idToSeq[idB] > maxA {
		later, err := sp.Changes().ChangedSince(ctx, maxA, 0)
		require.NoError(t, err)
		laterIds := map[string]bool{}
		for _, c := range later {
			laterIds[c.ObjectId] = true
		}
		assert.False(t, laterIds[idA], "A is at or below the cursor")
		assert.True(t, laterIds[idB], "B is past the cursor")
	}

	// MaxApplySeq is at least the highest object we saw.
	maxSeq, err := sp.Changes().MaxApplySeq(ctx)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, maxSeq, idToSeq[idB])

	// _addSeq is visible on the property record.
	rec, err := sp.Properties().Get(ctx, idA)
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Greater(t, uint64(rec.GetInt("_addSeq")), uint64(0), "_addSeq stamped on property row")
}

// TestSDK_ObjectDelete_PurgesLocalState asserts the deletion model:
// deleting an object PURGES its local materialized state rather than
// leaving a tombstone. any-sync's head storage is the durable,
// cross-device record that the tree is deleted, so the SDK keeps no
// object-level `_deletedAt` row — a deleted object is simply gone, and
// IncludeDeleted (a record-level opt-in) surfaces nothing for it.
func TestSDK_ObjectDelete_PurgesLocalState(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Tomb"})
	require.NoError(t, err)

	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Note"})
	require.NoError(t, err)
	titleProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Title", XKey: "title", Kind: space.PropertyKindString,
	})
	require.NoError(t, err)

	id, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	_, err = sp.Properties().Set(ctx, id, typeId, map[string]any{titleProp: "doomed"})
	require.NoError(t, err)

	live, err := sp.QueryObjects().Filter(map[string]any{"id": id}).One(ctx)
	require.NoError(t, err)
	require.NotNil(t, live)

	require.NoError(t, sp.Objects().Delete(ctx, id))

	// The object is gone from the default find path.
	_, err = sp.QueryObjects().Filter(map[string]any{"id": id}).One(ctx)
	assert.ErrorIs(t, err, space.ErrNotFound, "deleted object gone from find path")
	n, err := sp.QueryObjects().Filter(map[string]any{"id": id}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, n, "Count excludes the deleted object")

	// No tombstone is retained: IncludeDeleted surfaces nothing either —
	// the row was purged, not tombstoned.
	_, err = sp.QueryObjects().
		Filter(map[string]any{"id": id}).
		Projection(space.ProjectionOpts{IncludeDeleted: true}).
		One(ctx)
	assert.ErrorIs(t, err, space.ErrNotFound, "purged object has no tombstone to surface")
	nDel, err := sp.QueryObjects().
		Filter(map[string]any{"id": id}).
		Projection(space.ProjectionOpts{IncludeDeleted: true}).
		Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, nDel, "IncludeDeleted surfaces no purged object")
}
