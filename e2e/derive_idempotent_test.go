package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestSDK_Objects_DeriveIdempotentInDAG pins the DAG-level idempotence
// contract of Objects().Derive: a derive whose membership is already
// on the row writes no change at all. Derive is the resolve primitive
// for well-known objects (general chat, agent brain, space index) and
// runs on hot read paths — a resolve must not append a no-op
// membership change that syncs to all peers, grows the DAG forever,
// and clobbers collections attached by other writers.
func TestSDK_Objects_DeriveIdempotentInDAG(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "DeriveIdempotent"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}

	typeA, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "A"})
	require.NoError(t, err)
	typeD, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "D"})
	require.NoError(t, err)
	collB, err := sp.Collections().Create(ctx, space.CollectionCreateParams{Name: "B"})
	require.NoError(t, err)

	seed := []byte("well-known/v1")

	// First materialization must name a type: every object has one.
	_, err = sp.Objects().Derive(ctx, space.DeriveObjectOpts{Seed: seed})
	require.ErrorIs(t, err, space.ErrTypeRequired)

	objectId, err := sp.Objects().Derive(ctx, space.DeriveObjectOpts{
		Seed: seed, Type: typeA,
	})
	require.NoError(t, err)
	baseline := treeLen(t, ctx, sp, objectId)

	// Re-derive with the same membership: same id, zero new changes.
	again, err := sp.Objects().Derive(ctx, space.DeriveObjectOpts{
		Seed: seed, Type: typeA,
	})
	require.NoError(t, err)
	assert.Equal(t, objectId, again)
	assert.Equal(t, baseline, treeLen(t, ctx, sp, objectId),
		"derive with the membership already on the row must not append a change")

	// A row that already has a type needs none: the resolve is a no-op.
	again, err = sp.Objects().Derive(ctx, space.DeriveObjectOpts{Seed: seed})
	require.NoError(t, err)
	assert.Equal(t, objectId, again)
	assert.Equal(t, baseline, treeLen(t, ctx, sp, objectId),
		"a typeless re-derive of a typed row must not append a change")
	assert.Equal(t, typeA, objectTypeOf(t, ctx, sp, objectId))

	// A derive naming another type never replaces the one the row has.
	_, err = sp.Objects().Derive(ctx, space.DeriveObjectOpts{
		Seed: seed, Type: typeD,
	})
	require.NoError(t, err)
	assert.Equal(t, baseline, treeLen(t, ctx, sp, objectId),
		"a type the row already has is never replaced")
	assert.Equal(t, typeA, objectTypeOf(t, ctx, sp, objectId))

	// A collection attached by another writer survives a subset
	// re-derive (a whole-array $set would clobber it).
	_, err = sp.Properties().AttachCollection(ctx, objectId, collB)
	require.NoError(t, err)
	afterAttach := treeLen(t, ctx, sp, objectId)

	_, err = sp.Objects().Derive(ctx, space.DeriveObjectOpts{
		Seed: seed, Type: typeA,
	})
	require.NoError(t, err)
	assert.Equal(t, afterAttach, treeLen(t, ctx, sp, objectId),
		"subset re-derive must not write")
	assert.ElementsMatch(t, []string{collB}, objectCollectionsOf(t, ctx, sp, objectId),
		"subset re-derive must not clobber independently attached collections")

	// Superset derive adds only the missing collection: exactly one new
	// change, existing attachments intact.
	collC, err := sp.Collections().Create(ctx, space.CollectionCreateParams{Name: "C"})
	require.NoError(t, err)
	_, err = sp.Objects().Derive(ctx, space.DeriveObjectOpts{
		Seed: seed, Type: typeA, Collections: []string{collB, collC},
	})
	require.NoError(t, err)
	assert.Equal(t, afterAttach+1, treeLen(t, ctx, sp, objectId),
		"one missing collection ⇒ exactly one attach change")
	assert.Equal(t, typeA, objectTypeOf(t, ctx, sp, objectId))
	assert.ElementsMatch(t, []string{collB, collC}, objectCollectionsOf(t, ctx, sp, objectId))
}

// treeLen reads the object's change count via the debug surface.
func treeLen(t *testing.T, ctx context.Context, sp space.Space, objectId string) int {
	t.Helper()
	got, err := sp.Debug().Object(ctx, objectId)
	require.NoError(t, err)
	return got.TreeLen
}

// objectRowOf reads the object's shared `objects` row.
func objectRowOf(t *testing.T, ctx context.Context, sp space.Space, objectId string) *anyenc.Value {
	t.Helper()
	rows, err := sp.QueryObjects().All(ctx)
	require.NoError(t, err)
	for _, row := range rows {
		if string(row.GetStringBytes("id")) == objectId {
			return row
		}
	}
	t.Fatalf("object %s not found in QueryObjects", objectId)
	return nil
}

// objectTypeOf reads any.type off the object's shared `objects` row.
func objectTypeOf(t *testing.T, ctx context.Context, sp space.Space, objectId string) string {
	t.Helper()
	return objectRowOf(t, ctx, sp, objectId).GetString("any", "type")
}

// objectCollectionsOf reads any.collections off the object's shared
// `objects` row.
func objectCollectionsOf(t *testing.T, ctx context.Context, sp space.Space, objectId string) []string {
	t.Helper()
	arr := objectRowOf(t, ctx, sp, objectId).GetArray("any", "collections")
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		out = append(out, string(e.GetStringBytes()))
	}
	return out
}
