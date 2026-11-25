package anysyncsdk

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

func spaceInfoById(t *testing.T, ctx context.Context, sdk *SDK, spaceId string) space.SpaceInfo {
	t.Helper()
	list, err := sdk.Spaces().List(ctx)
	require.NoError(t, err)
	for _, info := range list {
		if info.Id == spaceId {
			return info
		}
	}
	t.Fatalf("space %s is not in the list", spaceId)
	return space.SpaceInfo{}
}

// A space carries the account's use of it on the account's row: given
// at Create or once afterwards, with no network, and kept across a
// restart. The space's own type tag is untouched.
func TestSDK_AnySpaceTypeOffline(t *testing.T) {
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sdk, err := Open(ctx, offlineConfig(t, dataDir), wmProvider(t, dataDir))
	require.NoError(t, err)
	closed := false
	defer func() {
		if !closed {
			_ = sdk.Close()
		}
	}()

	bao, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Bao", AnySpaceType: "bao"})
	require.NoError(t, err)
	assert.Equal(t, "bao", bao.Info().AnySpaceType)
	assert.Equal(t, space.SpaceTypeAny, bao.Info().SpaceType)
	assert.Equal(t, "bao", spaceInfoById(t, ctx, sdk, bao.Id()).AnySpaceType)

	plain, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Plain"})
	require.NoError(t, err)
	assert.Empty(t, plain.Info().AnySpaceType)

	require.NoError(t, sdk.Spaces().SetAnySpaceType(ctx, plain.Id(), "notes"))
	assert.Equal(t, "notes", plain.Info().AnySpaceType)
	require.NoError(t, sdk.Spaces().SetAnySpaceType(ctx, plain.Id(), "notes"), "the same value again")
	assert.ErrorIs(t, sdk.Spaces().SetAnySpaceType(ctx, plain.Id(), "other"), space.ErrAnySpaceTypeSet)
	assert.ErrorIs(t, sdk.Spaces().SetAnySpaceType(ctx, bao.Id(), "other"), space.ErrAnySpaceTypeSet)
	assert.ErrorIs(t, sdk.Spaces().SetAnySpaceType(ctx, "no-such-space", "bao"), space.ErrSpaceUnknown)
	assert.Error(t, sdk.Spaces().SetAnySpaceType(ctx, plain.Id(), ""))

	gone, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Gone"})
	require.NoError(t, err)
	require.NoError(t, sdk.Spaces().Delete(ctx, gone.Id()))
	assert.ErrorIs(t, sdk.Spaces().SetAnySpaceType(ctx, gone.Id(), "notes"), space.ErrSpaceDeleted)
	assert.Equal(t, "notes", plain.Info().AnySpaceType)
	assert.Equal(t, "bao", bao.Info().AnySpaceType)

	baoId, plainId := bao.Id(), plain.Id()
	require.NoError(t, sdk.Close())
	closed = true

	sdk2, err := Open(ctx, offlineConfig(t, dataDir), wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk2.Close() }()
	assert.Equal(t, "bao", spaceInfoById(t, ctx, sdk2, baoId).AnySpaceType)
	assert.Equal(t, "notes", spaceInfoById(t, ctx, sdk2, plainId).AnySpaceType)
}
