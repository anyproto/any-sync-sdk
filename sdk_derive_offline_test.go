package anysyncsdk

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

// A derived space another device of the account created reaches this
// device as a row with no storage. With no node reachable, Get builds
// the storage from the seed and the space takes writes.
func TestSDK_GetDerivedWithoutStorageOffline(t *testing.T) {
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cfg := wmConfig(t, dataDir)
	// Node addresses moved to closed ports, so every dial is refused,
	// and no LAN peer to find.
	cfg.Network.NodeConfYAML = bytes.ReplaceAll(cfg.Network.NodeConfYAML, []byte("127.0.0.1:"), []byte("127.0.0.1:1"))
	p2pOff := false
	cfg.P2P.Enabled = &p2pOff

	sdk, err := Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()

	req := space.DeriveRequest{Seed: []byte("test/space/derived/v1")}
	spaceId, err := sdk.Spaces().DeriveId(ctx, req)
	require.NoError(t, err)
	// The row as the other device's Derive wrote it.
	_, err = sdk.tsp.Add(ctx, techspace.SpaceIndexRecord{
		Id:           spaceId,
		Type:         space.SpaceTypeAny,
		SpaceType:    space.SpaceTypeAny,
		RemoteStatus: techspace.StatusActive,
		Derived:      true,
	})
	require.NoError(t, err)
	require.False(t, sdk.app.SpaceExists(spaceId))

	getCtx, cancelGet := context.WithTimeout(ctx, 10*time.Second)
	defer cancelGet()
	sp, err := sdk.Spaces().Get(getCtx, spaceId)
	require.NoError(t, err)
	assert.Equal(t, spaceId, sp.Id())
	assert.True(t, sdk.app.SpaceExists(spaceId))

	wmWrite(t, ctx, sp, 1)

	// Derive on this device lands on the same space.
	derived, err := sdk.Spaces().Derive(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, spaceId, derived.Id())
}

// The index follower loads a row with no storage as soon as the index
// names it. With the seed known it builds the storage too, so no pull
// is left in flight for a later Get to wait on.
func TestSDK_IndexFollowerBuildsDerivedStorageOffline(t *testing.T) {
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cfg := wmConfig(t, dataDir)
	// Nodes unreachable; p2p stays on, which is what runs the follower.
	cfg.Network.NodeConfYAML = bytes.ReplaceAll(cfg.Network.NodeConfYAML, []byte("127.0.0.1:"), []byte("127.0.0.1:1"))

	sdk, err := Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()

	spaceId, err := sdk.Spaces().DeriveId(ctx, space.DeriveRequest{Seed: []byte("test/space/derived/v1")})
	require.NoError(t, err)
	select {
	case <-sdk.BootstrapDone():
	case <-ctx.Done():
		t.Fatal("boot pass did not finish")
	}
	_, err = sdk.tsp.Add(ctx, techspace.SpaceIndexRecord{
		Id:           spaceId,
		Type:         space.SpaceTypeAny,
		SpaceType:    space.SpaceTypeAny,
		RemoteStatus: techspace.StatusActive,
		Derived:      true,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return sdk.app.SpaceExists(spaceId) },
		20*time.Second, 50*time.Millisecond, "the follower builds the storage")
	getCtx, cancelGet := context.WithTimeout(ctx, 10*time.Second)
	defer cancelGet()
	sp, err := sdk.Spaces().Get(getCtx, spaceId)
	require.NoError(t, err)
	wmWrite(t, ctx, sp, 1)
}

// A row with no storage whose seed this process never saw is pulled
// from the nodes: with none reachable Get waits for its ctx.
func TestSDK_GetUnknownSeedWithoutStorageOffline(t *testing.T) {
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cfg := wmConfig(t, dataDir)
	cfg.Network.NodeConfYAML = bytes.ReplaceAll(cfg.Network.NodeConfYAML, []byte("127.0.0.1:"), []byte("127.0.0.1:1"))
	p2pOff := false
	cfg.P2P.Enabled = &p2pOff

	sdk, err := Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()

	const foreign = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.2a5s8kx60jkm"
	require.NoError(t, sdk.Spaces().Track(ctx, foreign))

	getCtx, cancelGet := context.WithTimeout(ctx, 3*time.Second)
	defer cancelGet()
	_, err = sdk.Spaces().Get(getCtx, foreign)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.False(t, sdk.app.SpaceExists(foreign))
}
