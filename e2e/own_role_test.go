package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestOwnRole_MirroredToSpaceRow: creating a space must (async, via
// the ACL mirror watcher) land this account's ACL permission on the
// tech-space row, surfaced as SpaceInfo.OwnRole — so List reports the
// caller's role without loading every space. The creator is the ACL
// owner, so the mirrored value must be PermissionOwner; the
// authoritative per-space read (Members().Me) must agree.
func TestOwnRole_MirroredToSpaceRow(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	cfg := config.Config{
		Storage: config.Storage{
			DataDir:  t.TempDir(),
			Topology: config.StorageShared,
		},
		Network: config.Network{NodeConfYAML: yaml},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{
		Name:      "own-role",
		SpaceType: space.SpaceTypeRegular,
	})
	require.NoError(t, err)

	// The mirror is primed at wiring time but runs on its own
	// goroutine — poll the space list until the row carries it.
	require.Eventually(t, func() bool {
		list, lerr := sdk.Spaces().List(ctx)
		if lerr != nil {
			return false
		}
		for _, info := range list {
			if info.Id == sp.Id() {
				return info.OwnRole == space.PermissionOwner
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond, "own role should be mirrored onto the space row as owner")

	// The row mirror and the authoritative ACL read must agree.
	me, err := sp.Members().Me(ctx)
	require.NoError(t, err)
	require.Equal(t, space.PermissionOwner, me.Permission)
}
