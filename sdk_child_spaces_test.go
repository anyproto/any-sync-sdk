package anysyncsdk

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
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

func spaceHeaderOf(t *testing.T, ctx context.Context, sdk *SDK, spaceId string) *spacesyncproto.SpaceHeader {
	t.Helper()
	handle, err := sdk.app.GetSpace(ctx, spaceId)
	require.NoError(t, err)
	desc, err := handle.Inner().Description(ctx)
	require.NoError(t, err)
	hdr := anysyncx.DecodeSpaceHeader(desc.SpaceHeader.RawHeader)
	require.NotNil(t, hdr)
	return hdr
}

func replicationSuffix(spaceId string) string {
	return spaceId[strings.LastIndexByte(spaceId, '.'):]
}

// A child is created with no network. Its v1 header names the parent,
// its row carries the parent id, and the parent is not written to.
func TestSDK_CreateChild_Offline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	dataDir := t.TempDir()
	sdk := openChildTestSDK(t, ctx, dataDir)

	parent, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	parentHandle, err := sdk.app.GetSpace(ctx, parent.Id())
	require.NoError(t, err)
	aclHeadBefore := parentHandle.Inner().Acl().Head().Id
	treesBefore := parentHandle.Inner().StoredIds()

	child, err := sdk.Spaces().CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: parent.Id(), Name: "Child"})
	require.NoError(t, err)
	childId := child.Id()

	info := child.Info()
	assert.Equal(t, parent.Id(), info.ParentSpaceId)
	assert.Equal(t, space.StatusActive, info.Status)
	assert.Equal(t, space.SpaceTypeAny, info.Type)
	assert.Equal(t, "Child", info.Name)

	hdr := spaceHeaderOf(t, ctx, sdk, childId)
	assert.Equal(t, parent.Id(), hdr.ParentSpaceId)
	assert.Equal(t, spacesyncproto.SpaceHeaderVersion_SpaceHeaderVersion1, hdr.Version)
	assert.Equal(t, space.SpaceTypeAny, hdr.SpaceType)
	assert.Equal(t, replicationSuffix(parent.Id()), replicationSuffix(childId))

	assert.Equal(t, aclHeadBefore, parentHandle.Inner().Acl().Head().Id, "the parent's ACL is not written to")
	assert.ElementsMatch(t, treesBefore, parentHandle.Inner().StoredIds(), "the parent gets no tree")

	wmWrite(t, ctx, child, 1)
	require.NoError(t, sdk.Close())

	sdk = openChildTestSDK(t, ctx, dataDir)
	defer func() { _ = sdk.Close() }()
	child, err = sdk.Spaces().Get(ctx, childId)
	require.NoError(t, err)
	assert.Equal(t, parent.Id(), child.Info().ParentSpaceId)
	wmWrite(t, ctx, child, 1)
}

// A child lives on its parent's nodes: it takes the parent's
// replication key, not the account's.
func TestSDK_CreateChild_TakesParentReplicationKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()

	// A parent another account created, known here as a bare row.
	const foreignParent = foreignSpaceA
	require.NoError(t, sdk.Spaces().Track(ctx, foreignParent))
	require.NotEqual(t, replicationSuffix(foreignParent), replicationSuffix(sdk.TechSpaceId()))

	child, err := sdk.Spaces().CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: foreignParent})
	require.NoError(t, err)
	assert.Equal(t, replicationSuffix(foreignParent), replicationSuffix(child.Id()))
	assert.Equal(t, foreignParent, spaceHeaderOf(t, ctx, sdk, child.Id()).ParentSpaceId)
}

// CreateChild refuses a parent the index does not hold as an accepted
// space, and one the index already shows cannot be a parent. No row is
// added then.
func TestSDK_CreateChild_RefusedParents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	child, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: parent.Id()})
	require.NoError(t, err)
	derived, err := spaces.Derive(ctx, space.DeriveRequest{Seed: []byte("test/child/derived-parent")})
	require.NoError(t, err)
	deleted, err := spaces.Create(ctx, space.CreateRequest{Name: "Deleted"})
	require.NoError(t, err)
	require.NoError(t, spaces.Delete(ctx, deleted.Id()))

	const (
		oneToOne = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.1onetoone"
		pending  = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.2pending"
		foreign  = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.3foreign"
	)
	for _, rec := range []techspace.SpaceIndexRecord{
		{Id: oneToOne, Type: space.SpaceTypeOneToOne, RemoteStatus: techspace.StatusActive, OneToOnePeer: "peer"},
		{Id: pending, RemoteStatus: techspace.InvitePendingRemoteStatus},
		{Id: foreign, Type: "anytype.space", RemoteStatus: techspace.StatusActive},
	} {
		_, err := sdk.tsp.Add(ctx, rec)
		require.NoError(t, err)
	}

	before, err := spaces.List(ctx)
	require.NoError(t, err)

	cases := []struct {
		name   string
		parent string
		want   error
	}{
		{"no row", "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.4unknown", space.ErrSpaceUnknown},
		{"deleted", deleted.Id(), space.ErrSpaceDeleted},
		{"not accepted", pending, space.ErrSpaceNotAccepted},
		{"tech space", sdk.TechSpaceId(), space.ErrParentSpaceInvalid},
		{"child", child.Id(), space.ErrParentSpaceInvalid},
		{"derived", derived.Id(), space.ErrParentSpaceInvalid},
		{"1-1", oneToOne, space.ErrParentSpaceInvalid},
		{"another header type", foreign, space.ErrParentSpaceInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: tc.parent})
			require.ErrorIs(t, err, tc.want)
		})
	}

	after, err := spaces.List(ctx)
	require.NoError(t, err)
	assert.Len(t, after, len(before), "a refused CreateChild adds no row")
}

// A space id no account of the test holds.
const (
	foreignSpaceA = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.2a5s8kx60jkm"
)
