package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestSDK_Objects_DeriveUnderParent covers SYN-20: deriving a child object
// bound to a parent, and the cascade reclaim + local cleanup that fires
// when the parent is deleted.
//
//   - the derived id is idempotent and encodes the parent (same seed under a
//     different parent → a different id);
//   - deleting the parent cascade-reclaims the bound child and the child
//     disappears from the SDK's query surface (any-sync drives the deletion
//     callback for the child; the store tombstones its materialized row).
func TestSDK_Objects_DeriveUnderParent(t *testing.T) {
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

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "DeriveChild"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}

	typeId, _ := setupMovieType(t, ctx, sp)

	// The parent is a regular object: any-sync forbids deleting derived
	// objects directly, so a cascade parent must be a normal object.
	parentId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)

	seed := []byte("payloads")

	// Derive is idempotent for the same (seed, parent).
	childId, err := sp.Objects().Derive(ctx, space.DeriveObjectOpts{
		Seed: seed, ParentId: parentId, Types: []string{typeId},
	})
	require.NoError(t, err)
	require.NotEmpty(t, childId)

	childIdAgain, err := sp.Objects().Derive(ctx, space.DeriveObjectOpts{Seed: seed, ParentId: parentId})
	require.NoError(t, err)
	assert.Equal(t, childId, childIdAgain, "Derive must be idempotent for the same seed+parent")

	// The parent is hashed into the derived id: same seed, different
	// parent → different id.
	otherParent, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	childUnderOther, err := sp.Objects().Derive(ctx, space.DeriveObjectOpts{Seed: seed, ParentId: otherParent})
	require.NoError(t, err)
	assert.NotEqual(t, childId, childUnderOther, "same seed under a different parent must derive a different id")

	// The bound child is materialized and visible before the parent delete.
	require.True(t, queryHasObject(t, ctx, sp, childId), "child must be queryable before parent delete")

	// Subscribe so we can assert the cascade fires a Removed event for the
	// child — the child carries no CRDT tombstone (derived objects can't be
	// deleted directly), so this event comes from the delete-callback's
	// local cleanup (Store.reflectDeleted → Engine.NotifyDeleted), not an
	// apply of a tombstone change.
	res, err := sp.QueryObjects().Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = res.Sub.Close() })

	require.NoError(t, sp.Objects().Delete(ctx, parentId))

	// The cascade runs asynchronously via any-sync's deletion loop. Drain
	// events until we observe the child removed with RemoveDeleted (the
	// parent's own Removed may arrive first, in the same or an earlier
	// event).
	require.True(t, waitForRemoved(t, res.Sub, childId, 20*time.Second),
		"parent delete must cascade a Removed{RemoveDeleted} for the bound child")

	// And it is gone from the query surface.
	assert.False(t, queryHasObject(t, ctx, sp, childId), "cascade-reclaimed child must not be queryable")
}

// waitForRemoved drains subscription events until one reports objectId
// removed with RemoveDeleted, or the timeout elapses.
func waitForRemoved(t *testing.T, sub space.QuerySubscription, objectId string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		ev, err := sub.Events().WaitOne(ctx)
		cancel()
		if err != nil {
			return false
		}
		for _, r := range ev.Removed {
			if r.Id == objectId && r.Reason == space.RemoveDeleted {
				return true
			}
		}
	}
	return false
}

// queryHasObject reports whether objectId is currently returned by
// QueryObjects — i.e. it has a live, non-tombstoned row.
func queryHasObject(t *testing.T, ctx context.Context, sp space.Space, objectId string) bool {
	t.Helper()
	rows, err := sp.QueryObjects().All(ctx)
	require.NoError(t, err)
	for _, row := range rows {
		if id := row.Get(crdt.IdField); id != nil && string(id.GetStringBytes()) == objectId {
			return true
		}
	}
	return false
}
