package anysyncsdk

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

// offlineConfig is wmConfig with every node address moved to a closed
// port and no LAN peer to find.
func offlineConfig(t *testing.T, dataDir string) config.Config {
	t.Helper()
	cfg := wmConfig(t, dataDir)
	cfg.Network.NodeConfYAML = bytes.ReplaceAll(cfg.Network.NodeConfYAML, []byte("127.0.0.1:"), []byte("127.0.0.1:1"))
	p2pOff := false
	cfg.P2P.Enabled = &p2pOff
	return cfg
}

// The index of a space is on the device that created the space at
// once, and not on a device that holds only another device's row.
func TestSDK_IndexSeededOffline(t *testing.T) {
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sdk, err := Open(ctx, offlineConfig(t, dataDir), wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()

	created, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Created"})
	require.NoError(t, err)
	assert.True(t, created.IndexSeeded(ctx), "a created space has its index at once")

	tech, err := sdk.Spaces().Get(ctx, sdk.TechSpaceId())
	require.NoError(t, err)
	assert.True(t, tech.IndexSeeded(ctx))

	// A derived space as another device of the account left it: a row,
	// and storage this device builds from the seed.
	req := space.DeriveRequest{Seed: []byte("test/space/derived/v1")}
	spaceId, err := sdk.Spaces().DeriveId(ctx, req)
	require.NoError(t, err)
	_, err = sdk.tsp.Add(ctx, techspace.SpaceIndexRecord{
		Id:           spaceId,
		Type:         space.SpaceTypeAny,
		SpaceType:    space.SpaceTypeAny,
		RemoteStatus: techspace.StatusActive,
		Derived:      true,
	})
	require.NoError(t, err)
	getCtx, cancelGet := context.WithTimeout(ctx, 10*time.Second)
	defer cancelGet()
	downloaded, err := sdk.Spaces().Get(getCtx, spaceId)
	require.NoError(t, err)
	assert.False(t, downloaded.IndexSeeded(ctx), "nothing of the space's index is here")
}
