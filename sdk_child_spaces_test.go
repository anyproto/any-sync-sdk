package anysyncsdk

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

// childTestConfig is wmConfig with every node address on a closed port
// and no LAN peer: the network is not there.
func childTestConfig(t *testing.T, dataDir string) config.Config {
	t.Helper()
	cfg := wmConfig(t, dataDir)
	cfg.Network.NodeConfYAML = bytes.ReplaceAll(cfg.Network.NodeConfYAML, []byte("127.0.0.1:"), []byte("127.0.0.1:2"))
	p2pOff := false
	cfg.P2P.Enabled = &p2pOff
	return cfg
}

func openChildTestSDK(t *testing.T, ctx context.Context, dataDir string) *SDK {
	t.Helper()
	sdk, err := Open(ctx, childTestConfig(t, dataDir), wmProvider(t, dataDir))
	require.NoError(t, err)
	return sdk
}

// A joined child space reaches the index as a row with no type and no
// parent. Its first load fills both from the space header.
func TestSDK_ChildSpace_JoinedRowTakesParentFromHeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()

	parent, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	assert.Empty(t, parent.Info().ParentSpaceId, "a top-level space has no parent")

	// The child's storage as a pull leaves it, and the row a join
	// registers before the header is readable.
	keys := sdk.app.AccountKeys()
	readKey, err := crypto.NewRandomAES()
	require.NoError(t, err)
	metadataKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	childId, err := sdk.app.SpaceService().CreateSpace(ctx, spacepayloads.SpaceCreatePayload{
		SigningKey:       keys.SignKey,
		MasterKey:        keys.SignKey,
		ReadKey:          readKey,
		MetadataKey:      metadataKey,
		SpaceType:        space.SpaceTypeAny,
		FileProtoVersion: spacesyncproto.SpaceFileProtoVersion_SpaceFileProtoVersionV2,
		ParentSpaceId:    parent.Id(),
	})
	require.NoError(t, err)
	_, err = sdk.tsp.Add(ctx, techspace.SpaceIndexRecord{Id: childId, RemoteStatus: techspace.StatusActive})
	require.NoError(t, err)

	child, err := sdk.Spaces().Get(ctx, childId)
	require.NoError(t, err)
	info := child.Info()
	assert.Equal(t, parent.Id(), info.ParentSpaceId)
	assert.Equal(t, space.SpaceTypeAny, info.Type)

	rec, ok := sdk.tsp.Get(ctx, childId)
	require.True(t, ok)
	assert.Equal(t, parent.Id(), rec.ParentSpaceId, "the row keeps the parent id")
	assert.Equal(t, space.SpaceTypeAny, rec.Type)

	list, err := sdk.Spaces().List(ctx)
	require.NoError(t, err)
	parents := map[string]string{}
	for _, in := range list {
		parents[in.Id] = in.ParentSpaceId
	}
	assert.Equal(t, parent.Id(), parents[childId])
	assert.Empty(t, parents[parent.Id()])
}
