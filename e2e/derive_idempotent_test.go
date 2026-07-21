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

// TestSDK_Objects_DeriveIdempotentInDAG pins the DAG-level idempotence
// contract of Objects().Derive: a derive whose Types are already
// attached writes no change at all. Derive is the resolve primitive
// for well-known objects (general chat, agent brain, space index) and
// runs on hot read paths — before this contract, every resolve
// appended a no-op `$set any.types` change that synced to all peers,
// grew the DAG forever, and could clobber types attached by other
// writers (whole-array $set vs the sanctioned $addToSet).
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
	typeB, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "B"})
	require.NoError(t, err)

	seed := []byte("well-known/v1")
	objectId, err := sp.Objects().Derive(ctx, space.DeriveObjectOpts{
		Seed: seed, Types: []string{typeA},
	})
	require.NoError(t, err)
	baseline := treeLen(t, ctx, sp, objectId)

	// Re-derive with the same types: same id, zero new changes.
	again, err := sp.Objects().Derive(ctx, space.DeriveObjectOpts{
		Seed: seed, Types: []string{typeA},
	})
	require.NoError(t, err)
	assert.Equal(t, objectId, again)
	assert.Equal(t, baseline, treeLen(t, ctx, sp, objectId),
		"derive with already-attached types must not append a change")

	// A type attached by another writer survives a subset re-derive
	// (the old whole-array $set clobbered it).
	_, err = sp.Properties().AttachType(ctx, objectId, typeB)
	require.NoError(t, err)
	afterAttach := treeLen(t, ctx, sp, objectId)

	_, err = sp.Objects().Derive(ctx, space.DeriveObjectOpts{
		Seed: seed, Types: []string{typeA},
	})
	require.NoError(t, err)
	assert.Equal(t, afterAttach, treeLen(t, ctx, sp, objectId),
		"subset re-derive must not write")
	assert.ElementsMatch(t, []string{typeA, typeB}, objectTypesOf(t, ctx, sp, objectId),
		"subset re-derive must not clobber independently attached types")

	// Superset derive attaches only the missing type: exactly one new
	// change, existing attachments intact.
	typeC, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "C"})
	require.NoError(t, err)
	_, err = sp.Objects().Derive(ctx, space.DeriveObjectOpts{
		Seed: seed, Types: []string{typeA, typeC},
	})
	require.NoError(t, err)
	assert.Equal(t, afterAttach+1, treeLen(t, ctx, sp, objectId),
		"one missing type ⇒ exactly one attach change")
	assert.ElementsMatch(t, []string{typeA, typeB, typeC}, objectTypesOf(t, ctx, sp, objectId))
}

// treeLen reads the object's change count via the debug surface.
func treeLen(t *testing.T, ctx context.Context, sp space.Space, objectId string) int {
	t.Helper()
	got, err := sp.Debug().Object(ctx, objectId)
	require.NoError(t, err)
	return got.TreeLen
}

// objectTypesOf reads any.types off the object's shared `objects` row.
func objectTypesOf(t *testing.T, ctx context.Context, sp space.Space, objectId string) []string {
	t.Helper()
	rows, err := sp.QueryObjects().All(ctx)
	require.NoError(t, err)
	for _, row := range rows {
		if string(row.GetStringBytes("id")) != objectId {
			continue
		}
		arr := row.GetArray("any", "types")
		out := make([]string, 0, len(arr))
		for _, e := range arr {
			out = append(out, string(e.GetStringBytes()))
		}
		return out
	}
	t.Fatalf("object %s not found in QueryObjects", objectId)
	return nil
}
