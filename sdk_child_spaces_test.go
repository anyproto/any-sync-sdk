package anysyncsdk

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/commonspace/acl/aclclient"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/list/listtest"
	"github.com/anyproto/any-sync/commonspace/object/acl/recordverifier"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/consensus/consensusproto/consensuserr"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
	anynet "github.com/anyproto/any-sync/net"
	"github.com/anyproto/any-sync/node/nodeclient"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

// fakeNetwork answers in place of a network that is not there: the
// coordinator, and the nodes the joining client posts ACL records
// through. A call it has no answer for goes to the real client, which
// reaches nobody.
type fakeNetwork struct {
	coord   fakeCoordinator
	joining fakeJoiningClient
	nodes   fakeNodeClient
}

// installFakeNetwork puts a fakeNetwork in front of the network clients
// of every SDK opened until the test ends.
func installFakeNetwork(t *testing.T) *fakeNetwork {
	t.Helper()
	n := &fakeNetwork{}
	t.Cleanup(anysyncx.WrapComponentsForTest(func(c app.Component) app.Component {
		switch real := c.(type) {
		case coordinatorclient.CoordinatorClient:
			n.coord.CoordinatorClient = real
			return &n.coord
		case aclclient.AclJoiningClient:
			n.joining.AclJoiningClient = real
			return &n.joining
		case nodeclient.NodeClient:
			n.nodes.NodeClient = real
			return &n.nodes
		}
		return c
	}))
	return n
}

type fakeCoordinator struct {
	coordinatorclient.CoordinatorClient

	mu sync.Mutex
	// sign answers SpaceSign: nil signs.
	sign  func() error
	signs int
	// status answers StatusCheckMany for one space.
	status func(spaceId string) *coordinatorproto.SpaceStatusPayload
	// children answers SpaceChildren.
	children func(spaceId string) ([]*coordinatorproto.SpaceChild, error)
	// del answers SpaceDelete; deleted lists the spaces it was asked
	// to delete.
	del     func(spaceId string) error
	deleted []string
	// shareable answers SpaceMakeShareable.
	shareable func(spaceId string) error
}

func (f *fakeCoordinator) set(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn()
}

func (f *fakeCoordinator) SpaceSign(ctx context.Context, payload coordinatorclient.SpaceSignPayload) (*coordinatorproto.SpaceReceiptWithSignature, error) {
	f.mu.Lock()
	sign := f.sign
	f.signs++
	f.mu.Unlock()
	if sign == nil {
		return f.CoordinatorClient.SpaceSign(ctx, payload)
	}
	if err := sign(); err != nil {
		return nil, err
	}
	return &coordinatorproto.SpaceReceiptWithSignature{}, nil
}

func (f *fakeCoordinator) StatusCheckMany(ctx context.Context, spaceIds []string) ([]*coordinatorproto.SpaceStatusPayload, *coordinatorproto.AccountLimits, error) {
	f.mu.Lock()
	status := f.status
	f.mu.Unlock()
	if status == nil {
		return f.CoordinatorClient.StatusCheckMany(ctx, spaceIds)
	}
	out := make([]*coordinatorproto.SpaceStatusPayload, len(spaceIds))
	for i, id := range spaceIds {
		out[i] = status(id)
	}
	return out, &coordinatorproto.AccountLimits{}, nil
}

func (f *fakeCoordinator) SpaceChildren(ctx context.Context, spaceId string) ([]*coordinatorproto.SpaceChild, error) {
	f.mu.Lock()
	children := f.children
	f.mu.Unlock()
	if children == nil {
		return f.CoordinatorClient.SpaceChildren(ctx, spaceId)
	}
	return children(spaceId)
}

func (f *fakeCoordinator) SpaceDelete(ctx context.Context, spaceId string, conf *coordinatorproto.DeletionConfirmPayloadWithSignature) error {
	f.mu.Lock()
	del := f.del
	if del != nil {
		f.deleted = append(f.deleted, spaceId)
	}
	f.mu.Unlock()
	if del == nil {
		return f.CoordinatorClient.SpaceDelete(ctx, spaceId, conf)
	}
	return del(spaceId)
}

func (f *fakeCoordinator) SpaceMakeShareable(ctx context.Context, spaceId string) error {
	f.mu.Lock()
	shareable := f.shareable
	f.mu.Unlock()
	if shareable == nil {
		return f.CoordinatorClient.SpaceMakeShareable(ctx, spaceId)
	}
	return shareable(spaceId)
}

type fakeJoiningClient struct {
	aclclient.AclJoiningClient

	mu sync.Mutex
	// remove answers RemoveChildMembers, owner ChangeChildOwner.
	remove func(spaceId string, identities []string) error
	owner  func(spaceId, newOwner string, oldOwnerPerm list.AclPermissions) error
}

func (f *fakeJoiningClient) set(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn()
}

func (f *fakeJoiningClient) RemoveChildMembers(ctx context.Context, spaceId string, identities []crypto.PubKey) (string, error) {
	f.mu.Lock()
	remove := f.remove
	f.mu.Unlock()
	if remove == nil {
		return f.AclJoiningClient.RemoveChildMembers(ctx, spaceId, identities)
	}
	accounts := make([]string, len(identities))
	for i, pk := range identities {
		accounts[i] = pk.Account()
	}
	return "", remove(spaceId, accounts)
}

func (f *fakeJoiningClient) ChangeChildOwner(ctx context.Context, spaceId string, newOwner crypto.PubKey, oldOwnerPermissions list.AclPermissions) (string, error) {
	f.mu.Lock()
	owner := f.owner
	f.mu.Unlock()
	if owner == nil {
		return f.AclJoiningClient.ChangeChildOwner(ctx, spaceId, newOwner, oldOwnerPermissions)
	}
	return "", owner(spaceId, newOwner.Account(), oldOwnerPermissions)
}

type fakeNodeClient struct {
	nodeclient.NodeClient

	mu sync.Mutex
	// add answers AclAddRecord, get AclGetRecords.
	add func(spaceId string, rec *consensusproto.RawRecord) (*consensusproto.RawRecordWithId, error)
	get func(spaceId string) ([]*consensusproto.RawRecordWithId, error)
}

func (f *fakeNodeClient) AclGetRecords(ctx context.Context, spaceId, aclHead string) ([]*consensusproto.RawRecordWithId, error) {
	f.mu.Lock()
	get := f.get
	f.mu.Unlock()
	if get == nil {
		return f.NodeClient.AclGetRecords(ctx, spaceId, aclHead)
	}
	return get(spaceId)
}

func (f *fakeNodeClient) set(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn()
}

func (f *fakeNodeClient) AclAddRecord(ctx context.Context, spaceId string, rec *consensusproto.RawRecord) (*consensusproto.RawRecordWithId, error) {
	f.mu.Lock()
	add := f.add
	f.mu.Unlock()
	if add == nil {
		return f.NodeClient.AclAddRecord(ctx, spaceId, rec)
	}
	return add(spaceId, rec)
}

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

// A child the coordinator refuses to sign stays a usable local space
// that names the failed check. The reason survives a restart and is
// cleared when the coordinator signs.
func TestSDK_ChildSpace_RefusalRecordedAndCleared(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	coord := &installFakeNetwork(t).coord
	dataDir := t.TempDir()
	sdk := openChildTestSDK(t, ctx, dataDir)
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	sdk.app.NodeAnsweredForTest(parent.Id())
	child, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: parent.Id()})
	require.NoError(t, err)
	childId := child.Id()
	assert.Equal(t, space.RefusalNone, child.Info().Refusal)

	// The coordinator out of reach is no refusal.
	require.ErrorIs(t, spaces.RetryPush(ctx, childId), space.ErrOffline)
	assert.Equal(t, space.RefusalNone, child.Info().Refusal)

	cases := []struct {
		answer error
		want   space.Refusal
		err    error
	}{
		{coordinatorproto.ErrParentSpaceNotAvailable, space.RefusalParentNotAvailable, space.ErrParentSpaceNotAvailable},
		{coordinatorproto.ErrParentSpaceInvalid, space.RefusalParentInvalid, space.ErrParentSpaceInvalid},
		{coordinatorproto.ErrParentPermissionDenied, space.RefusalParentPermissionDenied, space.ErrParentPermissionDenied},
		{coordinatorproto.ErrChildSpacesLimitReached, space.RefusalChildLimitReached, space.ErrChildSpacesLimitReached},
	}
	for _, tc := range cases {
		coord.set(func() { coord.sign = func() error { return tc.answer } })
		require.ErrorIs(t, spaces.RetryPush(ctx, childId), tc.err)
		info := child.Info()
		assert.Equal(t, tc.want, info.Refusal)
		assert.Equal(t, space.StatusActive, info.Status, "a refused child stays a local space")
	}
	wmWrite(t, ctx, child, 1)
	assert.Equal(t, space.RefusalNone, parent.Info().Refusal)

	// Out of reach again: the recorded reason stays.
	coord.set(func() { coord.sign = nil })
	require.ErrorIs(t, spaces.RetryPush(ctx, childId), space.ErrOffline)
	assert.Equal(t, space.RefusalChildLimitReached, child.Info().Refusal)
	require.NoError(t, sdk.Close())

	sdk = openChildTestSDK(t, ctx, dataDir)
	spaces = sdk.Spaces()
	list, err := spaces.List(ctx)
	require.NoError(t, err)
	refusals := map[string]space.Refusal{}
	for _, info := range list {
		refusals[info.Id] = info.Refusal
	}
	assert.Equal(t, space.RefusalChildLimitReached, refusals[childId], "the reason survives a restart")

	sdk.app.NodeAnsweredForTest(parent.Id())
	coord.set(func() { coord.sign = func() error { return nil } })
	require.NoError(t, spaces.RetryPush(ctx, childId))
	child, err = spaces.Get(ctx, childId)
	require.NoError(t, err)
	assert.Equal(t, space.RefusalNone, child.Info().Refusal)
	require.NoError(t, sdk.Close())

	sdk = openChildTestSDK(t, ctx, dataDir)
	defer func() { _ = sdk.Close() }()
	rec, ok := sdk.tsp.Get(ctx, childId)
	require.True(t, ok)
	assert.Equal(t, space.RefusalNone, rec.Refusal, "and stays cleared")
}

// RetryPush is for a child space the account holds.
func TestSDK_RetryPush_RefusedSpaces(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	coord := &installFakeNetwork(t).coord
	coord.sign = func() error { return nil }
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	child, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: parent.Id()})
	require.NoError(t, err)

	require.ErrorIs(t, spaces.RetryPush(ctx, parent.Id()), space.ErrNotChildSpace)
	require.ErrorIs(t, spaces.RetryPush(ctx, "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.4unknown"), space.ErrSpaceUnknown)
	require.NoError(t, spaces.Delete(ctx, child.Id()))
	require.ErrorIs(t, spaces.RetryPush(ctx, child.Id()), space.ErrSpaceDeleted)
	assert.Zero(t, coord.signs, "none of them reaches the coordinator")
}

// A refusal recorded on this device is over once the coordinator
// reports the space registered: another device pushed it.
func TestSDK_ChildSpace_RefusalClearedByCoordinatorStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	defer spaceimpl.SetDeletionReconcileIntervalForTest(50 * time.Millisecond)()
	coord := &installFakeNetwork(t).coord
	coord.sign = func() error { return coordinatorproto.ErrChildSpacesLimitReached }
	var registered atomic.Bool
	coord.status = func(string) *coordinatorproto.SpaceStatusPayload {
		if registered.Load() {
			return &coordinatorproto.SpaceStatusPayload{Status: coordinatorproto.SpaceStatus_SpaceStatusCreated}
		}
		return &coordinatorproto.SpaceStatusPayload{Status: coordinatorproto.SpaceStatus_SpaceStatusNotExists}
	}
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	sdk.app.NodeAnsweredForTest(parent.Id())
	child, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: parent.Id()})
	require.NoError(t, err)
	require.ErrorIs(t, spaces.RetryPush(ctx, child.Id()), space.ErrChildSpacesLimitReached)

	// Not registered: the refusal stands over any number of passes.
	passes := spaceimpl.DeletionReconcilePassesForTest()
	require.Eventually(t, func() bool { return spaceimpl.DeletionReconcilePassesForTest() >= passes+3 },
		10*time.Second, 10*time.Millisecond)
	assert.Equal(t, space.RefusalChildLimitReached, child.Info().Refusal)

	registered.Store(true)
	require.Eventually(t, func() bool { return child.Info().Refusal == space.RefusalNone },
		10*time.Second, 10*time.Millisecond)
	assert.Equal(t, space.StatusActive, child.Info().Status)
}

// A space id no account of the test holds.
const (
	foreignSpaceA = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.2a5s8kx60jkm"
	foreignSpaceB = "bafybeibwzifw52ttrkqlikfzext5akxu7lz4xiwjgwzmqcpdzmp3n5vnbe.2a5s8kx60jkm"
	foreignSpaceC = "bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku.2a5s8kx60jkm"
)

// Children comes from the coordinator and from nowhere else: no answer
// offline, a typed error for a caller without the role, and no row or
// storage for the spaces it lists.
func TestSDK_Children(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	coord := &installFakeNetwork(t).coord
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	_, err := spaces.Children(ctx, foreignSpaceA)
	require.ErrorIs(t, err, space.ErrOffline)

	_, err = spaces.Children(ctx, "not-a-space-id")
	require.ErrorIs(t, err, space.ErrBadSpaceId)

	coord.set(func() {
		coord.children = func(string) ([]*coordinatorproto.SpaceChild, error) {
			return nil, coordinatorproto.ErrParentPermissionDenied
		}
	})
	_, err = spaces.Children(ctx, foreignSpaceA)
	require.ErrorIs(t, err, space.ErrParentPermissionDenied)
	require.NotErrorIs(t, err, space.ErrOffline)

	var asked string
	coord.set(func() {
		coord.children = func(spaceId string) ([]*coordinatorproto.SpaceChild, error) {
			asked = spaceId
			return []*coordinatorproto.SpaceChild{
				{SpaceId: foreignSpaceB, Status: coordinatorproto.SpaceStatus_SpaceStatusCreated},
				{SpaceId: foreignSpaceC, Status: coordinatorproto.SpaceStatus_SpaceStatusPendingDeletion},
			}, nil
		}
	})
	before, err := spaces.List(ctx)
	require.NoError(t, err)
	children, err := spaces.Children(ctx, foreignSpaceA)
	require.NoError(t, err)
	assert.Equal(t, foreignSpaceA, asked)
	assert.Equal(t, []space.ChildSpace{
		{Id: foreignSpaceB, Status: space.ChildStatusActive},
		{Id: foreignSpaceC, Status: space.ChildStatusDeletionPending},
	}, children)

	after, err := spaces.List(ctx)
	require.NoError(t, err)
	assert.Len(t, after, len(before), "listing children adds no row")
	for _, id := range []string{foreignSpaceA, foreignSpaceB, foreignSpaceC} {
		assert.False(t, sdk.app.SpaceExists(id), "and no storage")
	}

	coord.set(func() {
		coord.children = func(string) ([]*coordinatorproto.SpaceChild, error) { return nil, nil }
	})
	children, err = spaces.Children(ctx, foreignSpaceA)
	require.NoError(t, err)
	assert.Empty(t, children)
}

// foreignChildId is the id of a child space of parentSpaceId that
// another account created: this device holds nothing of it.
func foreignChildId(t *testing.T, parentSpaceId string) string {
	t.Helper()
	keys, err := accountdata.NewRandom()
	require.NoError(t, err)
	metadataKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	payload, err := spacepayloads.StoragePayloadForSpaceCreateV1(spacepayloads.SpaceCreatePayload{
		SigningKey:    keys.SignKey,
		MasterKey:     keys.SignKey,
		ReadKey:       crypto.NewAES(),
		MetadataKey:   metadataKey,
		SpaceType:     space.SpaceTypeAny,
		ParentSpaceId: parentSpaceId,
	})
	require.NoError(t, err)
	return payload.SpaceHeaderWithId.Id
}

// foreignChild is a child space another account owns, as the network
// holds its ACL: this device holds nothing of it.
type foreignChild struct {
	id      string
	owner   *accountdata.AccountKeys
	records []*consensusproto.RawRecordWithId
	network testNetworkKey
	// storage is what a device creates the child's storage from.
	storage spacestorage.SpaceStorageCreatePayload
}

func newForeignChild(t *testing.T, network testNetworkKey, parentSpaceId string) *foreignChild {
	t.Helper()
	owner, err := accountdata.NewRandom()
	require.NoError(t, err)
	metadataKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	payload, err := spacepayloads.StoragePayloadForSpaceCreateV1(spacepayloads.SpaceCreatePayload{
		SigningKey:    owner.SignKey,
		MasterKey:     owner.SignKey,
		ReadKey:       crypto.NewAES(),
		MetadataKey:   metadataKey,
		SpaceType:     space.SpaceTypeAny,
		ParentSpaceId: parentSpaceId,
	})
	require.NoError(t, err)
	return &foreignChild{
		id:      payload.SpaceHeaderWithId.Id,
		owner:   owner,
		records: []*consensusproto.RawRecordWithId{payload.AclWithId},
		network: network,
		storage: payload,
	}
}

// heldBy makes the device of sdk hold the child with its account as a
// member of role perm: the storage a pull leaves, the ACL record that
// adds the account, and the row an accepted invite leaves. It returns
// the child's ACL on that device.
func (c *foreignChild) heldBy(t *testing.T, ctx context.Context, sdk *SDK, parentSpaceId string, perm list.AclPermissions) list.AclList {
	t.Helper()
	rec, err := c.ownerAcl(t).RecordBuilder().BuildAccountsAdd(list.AccountsAddPayload{Additions: []list.AccountAdd{
		{Identity: sdk.app.AccountKeys().SignKey.GetPublic(), Permissions: perm},
	}})
	require.NoError(t, err)
	added := c.accept(rec)

	create := context.WithValue(ctx, commonspace.AddSpaceCtxKey, commonspace.SpaceDescription{
		SpaceHeader:          c.storage.SpaceHeaderWithId,
		AclId:                c.storage.AclWithId.Id,
		AclPayload:           c.storage.AclWithId.Payload,
		SpaceSettingsId:      c.storage.SpaceSettingsWithId.Id,
		SpaceSettingsPayload: c.storage.SpaceSettingsWithId.RawChange,
	})
	handle, err := sdk.app.GetSpaceCreating(create, c.id)
	require.NoError(t, err)
	acl := handle.Inner().Acl()
	acl.Lock()
	err = acl.AddRawRecord(added)
	acl.Unlock()
	require.NoError(t, err)

	_, err = sdk.tsp.Add(ctx, techspace.SpaceIndexRecord{
		Id:            c.id,
		Type:          space.SpaceTypeAny,
		SpaceType:     space.SpaceTypeAny,
		RemoteStatus:  techspace.StatusActive,
		ParentSpaceId: parentSpaceId,
	})
	require.NoError(t, err)
	return acl
}

// ownerAcl is the child's ACL on a device of its owner.
func (c *foreignChild) ownerAcl(t *testing.T) list.AclList {
	t.Helper()
	st, err := list.NewInMemoryStorage(c.records[0].Id, c.records)
	require.NoError(t, err)
	acl, err := list.BuildAclListWithIdentity(c.owner, st, recordverifier.NewValidateAcceptedLog())
	require.NoError(t, err)
	return acl
}

// accept takes a record into the child's ACL as the network does.
func (c *foreignChild) accept(rec *consensusproto.RawRecord) *consensusproto.RawRecordWithId {
	accepted := c.network.accept(rec)
	c.records = append(c.records, accepted)
	return accepted
}

// invite adds a request-to-join invite and returns its key.
func (c *foreignChild) invite(t *testing.T) crypto.PrivKey {
	t.Helper()
	invite, err := c.ownerAcl(t).RecordBuilder().BuildInvite()
	require.NoError(t, err)
	c.accept(invite.InviteRec)
	return invite.InviteKey
}

// addMember makes a new account a member and returns its identity.
func (c *foreignChild) addMember(t *testing.T, perm list.AclPermissions) crypto.PubKey {
	t.Helper()
	member, err := accountdata.NewRandom()
	require.NoError(t, err)
	rec, err := c.ownerAcl(t).RecordBuilder().BuildAccountsAdd(list.AccountsAddPayload{Additions: []list.AccountAdd{
		{Identity: member.SignKey.GetPublic(), Permissions: perm},
	}})
	require.NoError(t, err)
	c.accept(rec)
	return member.SignKey.GetPublic()
}

// serve makes the fake nodes serve the child's ACL and take the
// records posted to it. posted receives each of them.
func (c *foreignChild) serve(t *testing.T, nodes *fakeNodeClient, posted func(*consensusproto.RawRecord) error) {
	t.Helper()
	nodes.set(func() {
		nodes.get = func(spaceId string) ([]*consensusproto.RawRecordWithId, error) {
			assert.Equal(t, c.id, spaceId)
			return c.records, nil
		}
		nodes.add = func(spaceId string, rec *consensusproto.RawRecord) (*consensusproto.RawRecordWithId, error) {
			assert.Equal(t, c.id, spaceId)
			if err := posted(rec); err != nil {
				return nil, err
			}
			return c.accept(rec), nil
		}
	})
}

func randomIdentity(t *testing.T) string {
	t.Helper()
	keys, err := accountdata.NewRandom()
	require.NoError(t, err)
	return keys.SignKey.GetPublic().Account()
}

// requireNothingHeld fails when the account holds a row or storage for
// spaceId.
func requireNothingHeld(t *testing.T, ctx context.Context, sdk *SDK, spaceId string) {
	t.Helper()
	_, held := sdk.tsp.Get(ctx, spaceId)
	require.False(t, held, "no row for %s", spaceId)
	require.False(t, sdk.app.SpaceExists(spaceId), "no storage for %s", spaceId)
}

// The Admins and the Owner of a parent act in a child they are not a
// member of. The child gets no row and no storage on their device, and
// the network's refusals come back typed.
func TestSDK_ChildGovernance_FromTheParentSide(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	net := installFakeNetwork(t)
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	childId := foreignChildId(t, parent.Id())
	member, newOwner := randomIdentity(t), randomIdentity(t)
	before, err := spaces.List(ctx)
	require.NoError(t, err)

	calls := map[string]func() error{
		"RemoveChildMembers": func() error { return spaces.RemoveChildMembers(ctx, childId, []string{member}) },
		"ChangeChildOwner": func() error {
			return spaces.ChangeChildOwner(ctx, childId, newOwner, space.PermissionAdmin)
		},
		"DeleteChild": func() error { return spaces.DeleteChild(ctx, childId) },
	}

	// No network: each call says so and changes nothing.
	for name, call := range calls {
		require.ErrorIs(t, call(), space.ErrOffline, name)
	}

	// The network's refusals, and the checks the record builder makes
	// on this device before anything is sent.
	answers := []struct {
		answer error
		want   error
	}{
		{coordinatorproto.ErrParentPermissionDenied, space.ErrParentPermissionDenied},
		{coordinatorproto.ErrParentSpaceNotAvailable, space.ErrParentSpaceNotAvailable},
		{list.ErrInsufficientPermissions, space.ErrInsufficientPermissions},
	}
	for _, tc := range answers {
		net.joining.set(func() {
			net.joining.remove = func(string, []string) error { return tc.answer }
			net.joining.owner = func(string, string, list.AclPermissions) error { return tc.answer }
		})
		net.coord.set(func() { net.coord.del = func(string) error { return tc.answer } })
		for name, call := range calls {
			err := call()
			require.ErrorIs(t, err, tc.want, name)
			require.NotErrorIs(t, err, space.ErrOffline, name)
		}
	}

	// Accepted: what was asked reaches the network as given.
	var removedIn, ownerIn, removed, owner string
	var ownerPerm list.AclPermissions
	net.joining.set(func() {
		net.joining.remove = func(spaceId string, identities []string) error {
			removedIn, removed = spaceId, strings.Join(identities, ",")
			return nil
		}
		net.joining.owner = func(spaceId, newOwner string, perm list.AclPermissions) error {
			ownerIn, owner, ownerPerm = spaceId, newOwner, perm
			return nil
		}
	})
	net.coord.set(func() {
		net.coord.deleted = nil
		net.coord.del = func(string) error { return nil }
	})
	for name, call := range calls {
		require.NoError(t, call(), name)
	}
	assert.Equal(t, childId, removedIn)
	assert.Equal(t, member, removed)
	assert.Equal(t, childId, ownerIn)
	assert.Equal(t, newOwner, owner)
	assert.Equal(t, list.AclPermissionsAdmin, ownerPerm)
	assert.Equal(t, []string{childId}, net.coord.deleted)

	requireNothingHeld(t, ctx, sdk, childId)
	after, err := spaces.List(ctx)
	require.NoError(t, err)
	assert.Len(t, after, len(before), "the child never enters the account's list")

	// Malformed input and a space the index shows is no child never
	// reach the network.
	net.joining.set(func() {
		net.joining.remove = func(string, []string) error { t.Error("RemoveChildMembers sent"); return nil }
		net.joining.owner = func(string, string, list.AclPermissions) error { t.Error("ChangeChildOwner sent"); return nil }
	})
	net.coord.set(func() {
		net.coord.del = func(string) error { t.Error("SpaceDelete sent"); return nil }
	})
	derived, err := spaces.Derive(ctx, space.DeriveRequest{Seed: []byte("test/child/governance")})
	require.NoError(t, err)
	for _, notChild := range []string{parent.Id(), derived.Id(), sdk.TechSpaceId()} {
		require.ErrorIs(t, spaces.RemoveChildMembers(ctx, notChild, []string{member}), space.ErrNotChildSpace)
		require.ErrorIs(t, spaces.ChangeChildOwner(ctx, notChild, newOwner, space.PermissionAdmin), space.ErrNotChildSpace)
		require.ErrorIs(t, spaces.DeleteChild(ctx, notChild), space.ErrNotChildSpace)
	}
	require.ErrorIs(t, spaces.RemoveChildMembers(ctx, "not-a-space-id", []string{member}), space.ErrBadSpaceId)
	require.ErrorIs(t, spaces.ChangeChildOwner(ctx, "not-a-space-id", newOwner, space.PermissionAdmin), space.ErrBadSpaceId)
	require.ErrorIs(t, spaces.DeleteChild(ctx, "not-a-space-id"), space.ErrBadSpaceId)
	require.ErrorIs(t, spaces.RemoveChildMembers(ctx, childId, []string{"not-an-identity"}), space.ErrBadIdentity)
	require.ErrorIs(t, spaces.ChangeChildOwner(ctx, childId, "not-an-identity", space.PermissionAdmin), space.ErrBadIdentity)
	require.Error(t, spaces.RemoveChildMembers(ctx, childId, nil))
}

// The Owner of a parent who also holds a child in its own index deletes
// it with DeleteChild: the coordinator answers first, then the row
// becomes a deleted one and the local copy goes.
func TestSDK_DeleteChild_HeldByTheAccount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	net := installFakeNetwork(t)
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	newChild := func() string {
		child, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: parent.Id()})
		require.NoError(t, err)
		wmWrite(t, ctx, child, 1)
		return child.Id()
	}
	requireDeleted := func(childId string) {
		rec, ok := sdk.tsp.Get(ctx, childId)
		require.True(t, ok)
		assert.Equal(t, techspace.StatusDeleted, rec.RemoteStatus)
		assert.False(t, sdk.app.SpaceExists(childId), "the local copy is removed")
		_, err := spaces.Get(ctx, childId)
		require.ErrorIs(t, err, space.ErrSpaceDeleted)
	}

	// Refused or out of reach: the account's copy is untouched.
	childId := newChild()
	require.ErrorIs(t, spaces.DeleteChild(ctx, childId), space.ErrOffline)
	net.coord.set(func() {
		net.coord.del = func(string) error { return coordinatorproto.ErrParentPermissionDenied }
	})
	require.ErrorIs(t, spaces.DeleteChild(ctx, childId), space.ErrParentPermissionDenied)
	child, err := spaces.Get(ctx, childId)
	require.NoError(t, err)
	assert.Equal(t, space.StatusActive, child.Info().Status)
	wmWrite(t, ctx, child, 1)

	net.coord.set(func() { net.coord.del = func(string) error { return nil } })
	require.NoError(t, spaces.DeleteChild(ctx, childId))
	requireDeleted(childId)
	require.NoError(t, spaces.DeleteChild(ctx, childId), "deleting it again is the same answer")

	// A deletion already under way on the network settles the row too.
	for _, underWay := range []error{coordinatorproto.ErrSpaceDeletionPending, coordinatorproto.ErrSpaceIsDeleted} {
		childId = newChild()
		net.coord.set(func() { net.coord.del = func(string) error { return underWay } })
		require.NoError(t, spaces.DeleteChild(ctx, childId))
		requireDeleted(childId)
	}
	parentSpace, err := spaces.Get(ctx, parent.Id())
	require.NoError(t, err)
	assert.Equal(t, space.StatusActive, parentSpace.Info().Status, "the parent stays")
}

// A child the coordinator holds as being deleted while the account's
// row is live: the state a DeleteChild leaves when the process dies
// after the coordinator accepted it, and the state after a delete from
// another device. The deletion reconciler marks the row deleted and
// removes the local copy.
func TestSDK_ChildInDeletionOnTheNetwork_IsRemovedByTheReconciler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	defer spaceimpl.SetDeletionReconcileIntervalForTest(50 * time.Millisecond)()
	net := installFakeNetwork(t)
	var pendingDeletion atomic.Pointer[string]
	net.coord.status = func(spaceId string) *coordinatorproto.SpaceStatusPayload {
		if id := pendingDeletion.Load(); id != nil && *id == spaceId {
			return &coordinatorproto.SpaceStatusPayload{Status: coordinatorproto.SpaceStatus_SpaceStatusPendingDeletion}
		}
		return &coordinatorproto.SpaceStatusPayload{Status: coordinatorproto.SpaceStatus_SpaceStatusCreated}
	}
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	child, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: parent.Id()})
	require.NoError(t, err)
	childId := child.Id()

	pendingDeletion.Store(&childId)
	require.Eventually(t, func() bool {
		rec, ok := sdk.tsp.Get(ctx, childId)
		return ok && rec.RemoteStatus == techspace.StatusDeleted && !sdk.app.SpaceExists(childId)
	}, 10*time.Second, 10*time.Millisecond)
	rec, ok := sdk.tsp.Get(ctx, parent.Id())
	require.True(t, ok)
	assert.False(t, rec.IsDeleted())
}

// testNetworkKey makes the test the network of cfg: ACL records are
// accepted by whoever signs them with the network key.
type testNetworkKey struct {
	t   *testing.T
	key crypto.PrivKey
}

func newTestNetworkKey(t *testing.T, cfg *config.Config) testNetworkKey {
	t.Helper()
	key, pub, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	const networkId = "networkId: N4N1wDHFpFpovXBqdbq2TDXE9tXdXbtV1eTJFpKJW4YeaJqR"
	require.Contains(t, string(cfg.Network.NodeConfYAML), networkId)
	cfg.Network.NodeConfYAML = bytes.Replace(cfg.Network.NodeConfYAML, []byte(networkId), []byte("networkId: "+pub.Network()), 1)
	return testNetworkKey{t: t, key: key}
}

// accept signs rec as the network's consensus does.
func (n testNetworkKey) accept(rec *consensusproto.RawRecord) *consensusproto.RawRecordWithId {
	n.t.Helper()
	identity, err := n.key.GetPublic().Marshall()
	require.NoError(n.t, err)
	rec.AcceptorIdentity = identity
	rec.AcceptorSignature, err = n.key.Sign(rec.Payload)
	require.NoError(n.t, err)
	return listtest.WrapAclRecord(rec)
}

// An Admin of the parent removes a member of a child without the read
// key. The Owner's device, which holds the key, sees the rotation
// pending when the record is applied and ends it by itself.
func TestSDK_ChildSpace_RotationCompletedByTheWatcher(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	net := installFakeNetwork(t)
	dataDir := t.TempDir()
	cfg := childTestConfig(t, dataDir)
	network := newTestNetworkKey(t, &cfg)
	sdk, err := Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	child, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: parent.Id()})
	require.NoError(t, err)
	handle, err := sdk.app.GetSpace(ctx, child.Id())
	require.NoError(t, err)
	acl := handle.Inner().Acl()
	apply := func(rec *consensusproto.RawRecord) {
		acl.Lock()
		defer acl.Unlock()
		require.NoError(t, acl.AddRawRecord(network.accept(rec)))
	}

	// The network takes the records the SDK posts and counts them,
	// once it is in reach.
	var posted, unreached atomic.Int32
	var inReach atomic.Bool
	net.nodes.set(func() {
		net.nodes.add = func(spaceId string, rec *consensusproto.RawRecord) (*consensusproto.RawRecordWithId, error) {
			assert.Equal(t, child.Id(), spaceId)
			if !inReach.Load() {
				unreached.Add(1)
				return nil, anynet.ErrUnableToConnect
			}
			posted.Add(1)
			return network.accept(rec), nil
		}
	})

	member, err := accountdata.NewRandom()
	require.NoError(t, err)
	acl.Lock()
	add, err := acl.RecordBuilder().BuildAccountsAdd(list.AccountsAddPayload{Additions: []list.AccountAdd{
		{Identity: member.SignKey.GetPublic(), Permissions: list.AclPermissionsWriter},
	}})
	acl.Unlock()
	require.NoError(t, err)
	apply(add)
	pending, err := child.ACL().RotationPending(ctx)
	require.NoError(t, err)
	require.False(t, pending)
	acl.RLock()
	keyBefore := acl.AclState().CurrentReadKeyId()
	acl.RUnlock()

	// The removal as an account outside the child writes it.
	parentAdmin, err := accountdata.NewRandom()
	require.NoError(t, err)
	records, err := acl.RecordsAfter(ctx, "")
	require.NoError(t, err)
	st, err := list.NewInMemoryStorage(records[0].Id, records)
	require.NoError(t, err)
	outside, err := list.BuildAclListWithIdentity(parentAdmin, st, recordverifier.NewValidateFull())
	require.NoError(t, err)
	removal, err := outside.RecordBuilder().BuildAccountRemoveNoRotate([]crypto.PubKey{member.SignKey.GetPublic()})
	require.NoError(t, err)
	apply(removal)

	// No network: the change is not published and the rotation stays
	// pending.
	require.Eventually(t, func() bool { return unreached.Load() == 1 },
		20*time.Second, 10*time.Millisecond, "the watcher tries at once")
	pending, err = child.ACL().RotationPending(ctx)
	require.NoError(t, err)
	require.True(t, pending)

	// The network is back: tree nodes answer the rounds of the space.
	inReach.Store(true)
	require.Eventually(t, func() bool {
		sdk.app.NodeAnsweredForTest(child.Id())
		pending, err := child.ACL().RotationPending(ctx)
		return err == nil && !pending && posted.Load() > 0
	}, 20*time.Second, 10*time.Millisecond, "the watcher rotates the read key")
	assert.EqualValues(t, 1, posted.Load(), "one read key change")
	acl.RLock()
	state := acl.AclState()
	assert.NotEqual(t, keyBefore, state.CurrentReadKeyId())
	assert.True(t, state.Permissions(member.SignKey.GetPublic()).NoPermissions())
	acl.RUnlock()

	// Nothing more is written once the rotation is done.
	time.Sleep(200 * time.Millisecond)
	assert.EqualValues(t, 1, posted.Load())
}

// A space the account owns is not deleted while the index holds a child
// of it: the coordinator would refuse the deletion, and Delete removes
// the local copy before the coordinator answers.
func TestSDK_Delete_OwnedSpaceWithHeldChild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return parent.Info().OwnRole == space.PermissionOwner },
		20*time.Second, 10*time.Millisecond, "the ACL mirror records the role")
	first, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: parent.Id()})
	require.NoError(t, err)
	second, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: parent.Id()})
	require.NoError(t, err)

	require.ErrorIs(t, spaces.Delete(ctx, parent.Id()), space.ErrSpaceHasChildren)
	require.NoError(t, spaces.Delete(ctx, first.Id()))
	require.ErrorIs(t, spaces.Delete(ctx, parent.Id()), space.ErrSpaceHasChildren, "one child is left")
	assert.Equal(t, space.StatusActive, parent.Info().Status)
	assert.True(t, sdk.app.SpaceExists(parent.Id()), "nothing of the parent is removed")
	wmWrite(t, ctx, parent, 1)

	require.NoError(t, spaces.Delete(ctx, second.Id()))
	require.NoError(t, spaces.Delete(ctx, parent.Id()))
	assert.Equal(t, space.StatusDeleted, parent.Info().Status)
}

// A child space admits only members of its parent. The coordinator's
// refusal reads ErrNotParentMember on every way in: a direct add, an
// accepted join request, a join by invite. A guest key is not a member
// and is added as before.
func TestSDK_ChildSpace_OnlyParentMembersAreAdmitted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	net := installFakeNetwork(t)
	dataDir := t.TempDir()
	cfg := childTestConfig(t, dataDir)
	network := newTestNetworkKey(t, &cfg)
	sdk, err := Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	child, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: parent.Id()})
	require.NoError(t, err)
	handle, err := sdk.app.GetSpace(ctx, child.Id())
	require.NoError(t, err)
	acl := handle.Inner().Acl()

	net.coord.set(func() { net.coord.shareable = func(string) error { return nil } })
	accept := func(_ string, rec *consensusproto.RawRecord) (*consensusproto.RawRecordWithId, error) {
		return network.accept(rec), nil
	}
	refuse := func(string, *consensusproto.RawRecord) (*consensusproto.RawRecordWithId, error) {
		return nil, coordinatorproto.ErrNotParentMember
	}

	// A direct add.
	net.nodes.set(func() { net.nodes.add = refuse })
	outsider := randomIdentity(t)
	err = child.ACL().AddAccounts(ctx, []space.MemberAdd{{Identity: outsider, Permission: space.PermissionWriter}})
	require.ErrorIs(t, err, space.ErrNotParentMember)
	rec, ok := sdk.tsp.Get(ctx, child.Id())
	require.True(t, ok)
	assert.Empty(t, rec.InviteNotifyPending, "nobody is notified of an add that did not happen")

	// An accepted join request.
	net.nodes.set(func() { net.nodes.add = accept })
	invite, err := child.ACL().CreateInvite(ctx)
	require.NoError(t, err)
	joiner, err := accountdata.NewRandom()
	require.NoError(t, err)
	records, err := acl.RecordsAfter(ctx, "")
	require.NoError(t, err)
	st, err := list.NewInMemoryStorage(records[0].Id, records)
	require.NoError(t, err)
	joinerAcl, err := list.BuildAclListWithIdentity(joiner, st, recordverifier.NewValidateFull())
	require.NoError(t, err)
	requestRec, err := joinerAcl.RecordBuilder().BuildRequestJoin(list.RequestJoinPayload{InviteKey: invite.InviteKey})
	require.NoError(t, err)
	request := network.accept(requestRec)
	acl.Lock()
	err = acl.AddRawRecord(request)
	acl.Unlock()
	require.NoError(t, err)

	net.nodes.set(func() { net.nodes.add = refuse })
	err = child.ACL().AcceptRequest(ctx, request.Id, space.PermissionWriter)
	require.ErrorIs(t, err, space.ErrNotParentMember)

	// A join by invite, from the joiner's side: the account reads the
	// ACL of another account's child off the nodes and posts its join
	// request, which the network refuses.
	foreign := newForeignChild(t, network, parent.Id())
	foreignInvite := foreign.invite(t)
	var requests atomic.Int32
	foreign.serve(t, &net.nodes, func(*consensusproto.RawRecord) error {
		requests.Add(1)
		return coordinatorproto.ErrNotParentMember
	})
	token, err := space.EncodeInvite(space.Invite{SpaceId: foreign.id, InviteKey: foreignInvite})
	require.NoError(t, err)
	_, err = spaces.Join(ctx, space.JoinRequest{Invite: token})
	require.ErrorIs(t, err, space.ErrNotParentMember)
	assert.EqualValues(t, 1, requests.Load(), "the request was posted and refused")
	requireNothingHeld(t, ctx, sdk, foreign.id)

	// A guest key.
	net.nodes.set(func() { net.nodes.add = accept })
	guest, err := child.ACL().CreateGuestKey(ctx)
	require.NoError(t, err)
	assert.Equal(t, space.InviteKindGuest, guest.Kind)
	acl.RLock()
	assert.True(t, acl.AclState().Permissions(guest.InviteKey.GetPublic()).IsGuest())
	acl.RUnlock()
}

// The refusal is on the synced row: a device of the account with no
// copy of the child reads why no node holds it, and is told that the
// push is another device's to retry.
func TestSDK_ChildSpace_RefusalOnADeviceWithoutTheSpace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	coord := &installFakeNetwork(t).coord
	coord.sign = func() error { return nil }
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	// The row as it syncs in from the device that created the child
	// and was refused.
	childId := foreignChildId(t, parent.Id())
	_, err = sdk.tsp.Add(ctx, techspace.SpaceIndexRecord{
		Id:            childId,
		Type:          space.SpaceTypeAny,
		SpaceType:     space.SpaceTypeAny,
		RemoteStatus:  techspace.StatusActive,
		ParentSpaceId: parent.Id(),
	})
	require.NoError(t, err)
	_, err = sdk.tsp.SetRefusal(ctx, childId, space.RefusalChildLimitReached)
	require.NoError(t, err)

	list, err := spaces.List(ctx)
	require.NoError(t, err)
	var info space.SpaceInfo
	for _, in := range list {
		if in.Id == childId {
			info = in
		}
	}
	assert.Equal(t, space.RefusalChildLimitReached, info.Refusal)
	assert.Equal(t, space.StatusActive, info.Status)

	require.ErrorIs(t, spaces.RetryPush(ctx, childId), space.ErrSpaceNotStored)
	assert.Zero(t, coord.signs, "the coordinator is not asked")
	assert.False(t, sdk.app.SpaceExists(childId), "and nothing is pulled")
}

// A parent and its child created with no network reach it together.
// The child is not asked for before the network holds the parent, so
// the coordinator's "parent not available" is never an answer nobody
// caused. A child of a parent the account does not own is asked for at
// once.
func TestSDK_ChildSpace_SignWaitsForItsOwnParent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	coord := &installFakeNetwork(t).coord
	coord.sign = func() error { return nil }
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	child, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: parent.Id()})
	require.NoError(t, err)

	require.ErrorIs(t, spaces.RetryPush(ctx, child.Id()), space.ErrParentSpaceNotAvailable)
	assert.Zero(t, coord.signs, "the coordinator is not asked")
	assert.Equal(t, space.RefusalNone, child.Info().Refusal, "and no refusal is recorded")

	// A tree node answered a head-sync round of the parent.
	sdk.app.NodeAnsweredForTest(parent.Id())
	require.NoError(t, spaces.RetryPush(ctx, child.Id()))
	assert.Equal(t, 1, coord.signs)

	// Someone else's parent, known here as a row with no storage.
	require.NoError(t, spaces.Track(ctx, foreignSpaceA))
	foreign, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: foreignSpaceA})
	require.NoError(t, err)
	coord.set(func() { coord.sign = func() error { return coordinatorproto.ErrParentSpaceNotAvailable } })
	require.ErrorIs(t, spaces.RetryPush(ctx, foreign.Id()), space.ErrParentSpaceNotAvailable)
	assert.Equal(t, 2, coord.signs, "asked at once")
	assert.Equal(t, space.RefusalParentNotAvailable, foreign.Info().Refusal, "there it is a refusal")
}

func spaceInfoOf(t *testing.T, ctx context.Context, sdk *SDK, spaceId string) space.SpaceInfo {
	t.Helper()
	list, err := sdk.Spaces().List(ctx)
	require.NoError(t, err)
	for _, info := range list {
		if info.Id == spaceId {
			return info
		}
	}
	t.Fatalf("no row for %s", spaceId)
	return space.SpaceInfo{}
}

// The owner of a space is usually no member of the children others
// created under it, so the index does not show them. With the
// coordinator in reach, Delete asks it before anything is written: a
// space with children is not deleted and stays as it is.
func TestSDK_Delete_OwnedSpaceAsksTheCoordinatorFirst(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	coord := &installFakeNetwork(t).coord
	coord.del = func(string) error { return coordinatorproto.ErrSpaceHasChildren }
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	// Right after Create the ACL mirror may not have written the role.
	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	require.ErrorIs(t, spaces.Delete(ctx, parent.Id()), space.ErrSpaceHasChildren)
	assert.Equal(t, []string{parent.Id()}, coord.deleted, "the coordinator was asked")
	info := parent.Info()
	assert.Equal(t, space.StatusActive, info.Status)
	assert.Equal(t, space.RefusalNone, info.Refusal)
	assert.True(t, sdk.app.SpaceExists(parent.Id()), "nothing of the space is removed")
	wmWrite(t, ctx, parent, 1)

	// The coordinator finished deleting the last child.
	coord.set(func() { coord.del = func(string) error { return nil } })
	require.NoError(t, spaces.Delete(ctx, parent.Id()))
	assert.Equal(t, space.StatusDeleted, spaceInfoOf(t, ctx, sdk, parent.Id()).Status)
	assert.False(t, sdk.app.SpaceExists(parent.Id()))

	// A child cannot have children: its delete does not ask first.
	other, err := spaces.Create(ctx, space.CreateRequest{Name: "Other"})
	require.NoError(t, err)
	child, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: other.Id()})
	require.NoError(t, err)
	asked := len(coord.deleted)
	coord.set(func() { coord.del = func(string) error { return anynet.ErrUnableToConnect } })
	require.NoError(t, spaces.Delete(ctx, child.Id()))
	assert.Len(t, coord.deleted, asked)
}

// parentDelete is a fake coordinator's side of the signed delete of one
// space, the parent: the test says what it answers. Every other delete
// is accepted, and the parent's status follows its own.
type parentDelete struct {
	id       atomic.Pointer[string]
	answer   atomic.Pointer[error]
	sent     atomic.Int32
	accepted atomic.Bool
}

func (p *parentDelete) answers(err error) { p.answer.Store(&err) }

func newParentDelete(coord *fakeCoordinator, first error) *parentDelete {
	p := &parentDelete{}
	p.answers(first)
	isParent := func(spaceId string) bool {
		id := p.id.Load()
		return id != nil && *id == spaceId
	}
	coord.status = func(spaceId string) *coordinatorproto.SpaceStatusPayload {
		st := &coordinatorproto.SpaceStatusPayload{
			Status:      coordinatorproto.SpaceStatus_SpaceStatusCreated,
			Permissions: coordinatorproto.SpacePermissions_SpacePermissionsOwner,
		}
		if isParent(spaceId) && p.accepted.Load() {
			st.Status = coordinatorproto.SpaceStatus_SpaceStatusPendingDeletion
		}
		return st
	}
	coord.del = func(spaceId string) error {
		if !isParent(spaceId) {
			return nil
		}
		p.sent.Add(1)
		err := *p.answer.Load()
		if err == nil {
			p.accepted.Store(true)
		}
		return err
	}
	return p
}

// With no coordinator in reach the delete is deferred: the space leaves
// the account at once. When the coordinator then refuses it for
// children, the deleted row says so. The owner deletes the children,
// and the delete goes through once the coordinator has finished
// deleting them: until then they are children still.
func TestSDK_Delete_DeferredDeleteWaitsForChildren(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	defer spaceimpl.SetDeletionReconcileIntervalForTest(50 * time.Millisecond)()
	coord := &installFakeNetwork(t).coord
	parentDelete := newParentDelete(coord, anynet.ErrUnableToConnect)
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	parentId := parent.Id()
	parentDelete.id.Store(&parentId)
	firstChild, lastChild := foreignChildId(t, parentId), foreignChildId(t, parentId)
	refusal := func() space.Refusal { return spaceInfoOf(t, ctx, sdk, parentId).Refusal }

	require.NoError(t, spaces.Delete(ctx, parentId), "out of reach: deferred")
	info := spaceInfoOf(t, ctx, sdk, parentId)
	assert.Equal(t, space.StatusDeleted, info.Status)
	assert.Equal(t, space.RefusalNone, info.Refusal)
	assert.False(t, sdk.app.SpaceExists(parentId))

	// The coordinator is back and holds two children of the space.
	parentDelete.answers(coordinatorproto.ErrSpaceHasChildren)
	require.Eventually(t, func() bool { return refusal() == space.RefusalHasChildren },
		10*time.Second, 10*time.Millisecond, "the deleted row says what its deletion waits for")
	assert.Equal(t, space.StatusDeleted, spaceInfoOf(t, ctx, sdk, parentId).Status)

	// Deleted children are children until their deletion is final.
	require.NoError(t, spaces.DeleteChild(ctx, firstChild))
	require.NoError(t, spaces.DeleteChild(ctx, lastChild))
	sent := parentDelete.sent.Load()
	require.Eventually(t, func() bool { return parentDelete.sent.Load() >= sent+2 },
		10*time.Second, 10*time.Millisecond, "the delete is sent again on every pass")
	assert.Equal(t, space.RefusalHasChildren, refusal())

	// The coordinator finished deleting them.
	parentDelete.answers(nil)
	require.Eventually(t, func() bool {
		return parentDelete.accepted.Load() && refusal() == space.RefusalNone
	}, 10*time.Second, 10*time.Millisecond, "the waiting delete goes through")
}

// An accepted DeleteChild starts a pass of the deletion reconciler: a
// delete of the parent that waits for its children is sent again
// without waiting for the next pass.
func TestSDK_DeleteChild_SendsAWaitingParentDeleteAgain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	// Only a kick starts a pass of the reconciler.
	defer spaceimpl.SetDeletionReconcileIntervalForTest(time.Hour)()
	coord := &installFakeNetwork(t).coord
	parentDelete := newParentDelete(coord, anynet.ErrUnableToConnect)
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	parentId := parent.Id()
	parentDelete.id.Store(&parentId)
	require.NoError(t, spaces.Delete(ctx, parentId), "out of reach: deferred")

	parentDelete.answers(coordinatorproto.ErrSpaceHasChildren)
	sent := parentDelete.sent.Load()
	require.NoError(t, spaces.DeleteChild(ctx, foreignChildId(t, parentId)))
	require.Eventually(t, func() bool {
		return parentDelete.sent.Load() > sent && spaceInfoOf(t, ctx, sdk, parentId).Refusal == space.RefusalHasChildren
	}, 10*time.Second, 10*time.Millisecond)
}

// An Admin or the Owner of a parent reads who is in a child it is no
// member of, removes one of them and hands the child to another: each
// over the ACL the nodes serve, with nothing of the child kept here.
func TestSDK_ChildMembers_AndTheRecordsBuiltOnThem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	net := installFakeNetwork(t)
	dataDir := t.TempDir()
	cfg := childTestConfig(t, dataDir)
	network := newTestNetworkKey(t, &cfg)
	sdk, err := Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	child := newForeignChild(t, network, parent.Id())

	_, err = spaces.ChildMembers(ctx, child.id)
	require.ErrorIs(t, err, space.ErrOffline)
	_, err = spaces.ChildMembers(ctx, parent.Id())
	require.ErrorIs(t, err, space.ErrNotChildSpace)
	_, err = spaces.ChildMembers(ctx, "not-a-space-id")
	require.ErrorIs(t, err, space.ErrBadSpaceId)

	admin := child.addMember(t, list.AclPermissionsAdmin)
	writer := child.addMember(t, list.AclPermissionsWriter)
	var posted []*consensusproto.RawRecord
	var answer error
	child.serve(t, &net.nodes, func(rec *consensusproto.RawRecord) error {
		if answer != nil {
			return answer
		}
		posted = append(posted, rec)
		return nil
	})
	owner := child.owner.SignKey.GetPublic().Account()

	members, err := spaces.ChildMembers(ctx, child.id)
	require.NoError(t, err)
	assert.ElementsMatch(t, []space.ChildMember{
		{Identity: owner, Permission: space.PermissionOwner, Status: space.MemberStatusActive},
		{Identity: admin.Account(), Permission: space.PermissionAdmin, Status: space.MemberStatusActive},
		{Identity: writer.Account(), Permission: space.PermissionWriter, Status: space.MemberStatusActive},
	}, members)

	// An identity the child's ACL does not hold, and the child's owner.
	stranger := randomIdentity(t)
	require.ErrorIs(t, spaces.RemoveChildMembers(ctx, child.id, []string{stranger}), space.ErrAccountNotFound)
	require.ErrorIs(t, spaces.ChangeChildOwner(ctx, child.id, stranger, space.PermissionAdmin), space.ErrAccountNotFound)
	require.ErrorIs(t, spaces.RemoveChildMembers(ctx, child.id, []string{owner}), space.ErrInsufficientPermissions)
	require.ErrorIs(t, spaces.ChangeChildOwner(ctx, child.id, admin.Account(), space.PermissionGuest), space.ErrInsufficientPermissions)
	assert.Empty(t, posted, "none of them is sent")

	// The child's ACL moved between the read and the post: the network
	// refuses the record, nothing is written, and the call is repeated.
	answer = consensuserr.ErrConflict
	err = spaces.RemoveChildMembers(ctx, child.id, []string{writer.Account()})
	require.ErrorIs(t, err, consensuserr.ErrConflict)
	require.NotErrorIs(t, err, space.ErrOffline)
	answer = nil

	require.NoError(t, spaces.RemoveChildMembers(ctx, child.id, []string{writer.Account()}))
	require.NoError(t, spaces.ChangeChildOwner(ctx, child.id, admin.Account(), space.PermissionWriter))
	require.Len(t, posted, 2)

	members, err = spaces.ChildMembers(ctx, child.id)
	require.NoError(t, err)
	assert.ElementsMatch(t, []space.ChildMember{
		{Identity: owner, Permission: space.PermissionWriter, Status: space.MemberStatusActive},
		{Identity: admin.Account(), Permission: space.PermissionOwner, Status: space.MemberStatusActive},
		{Identity: writer.Account(), Permission: space.PermissionNone, Status: space.MemberStatusRemoved},
	}, members)
	assert.True(t, child.ownerAcl(t).AclState().RotationPending(), "the removal left the read key as it was")
	requireNothingHeld(t, ctx, sdk, child.id)
}

// A row whose header was never read says neither what the space is nor
// whose. It can be a child of a space the account owns, and the
// coordinator takes a signed delete of a child from the parent's Owner:
// Delete only removes such a row from the account.
func TestSDK_Delete_RowWithUnreadHeaderSendsNoSignedDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	defer spaceimpl.SetDeletionReconcileIntervalForTest(50 * time.Millisecond)()
	coord := &installFakeNetwork(t).coord
	var asked atomic.Int32
	coord.del = func(string) error {
		asked.Add(1)
		return nil
	}
	// The coordinator tells the caller's own place in each space: no
	// owner of the child.
	coord.status = func(string) *coordinatorproto.SpaceStatusPayload {
		return &coordinatorproto.SpaceStatusPayload{Status: coordinatorproto.SpaceStatus_SpaceStatusCreated}
	}
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	childId := foreignChildId(t, parent.Id())
	require.NoError(t, spaces.Track(ctx, childId))
	rec, ok := sdk.tsp.Get(ctx, childId)
	require.True(t, ok)
	require.Empty(t, rec.Type)
	require.Empty(t, rec.ParentSpaceId)
	require.Equal(t, space.PermissionNone, rec.OwnRole)

	require.NoError(t, spaces.Delete(ctx, childId))
	assert.Equal(t, space.StatusDeleted, spaceInfoOf(t, ctx, sdk, childId).Status)

	passes := spaceimpl.DeletionReconcilePassesForTest()
	require.Eventually(t, func() bool { return spaceimpl.DeletionReconcilePassesForTest() >= passes+2 },
		10*time.Second, 10*time.Millisecond)
	assert.Zero(t, asked.Load(), "no signed delete, on the call path or from the reconciler")
	assert.Equal(t, space.StatusActive, spaceInfoOf(t, ctx, sdk, parent.Id()).Status)
}

// A space the coordinator does not know, and one it holds as deleted or
// in deletion, read as an unknown and as a deleted space on every call
// made from the parent side. The nodes pass the coordinator's answer to
// an ACL read or write on unchanged.
func TestSDK_ChildCalls_UnknownAndDeletedSpaces(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	net := installFakeNetwork(t)
	dataDir := t.TempDir()
	cfg := childTestConfig(t, dataDir)
	network := newTestNetworkKey(t, &cfg)
	sdk, err := Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	child := newForeignChild(t, network, parent.Id())
	member := child.addMember(t, list.AclPermissionsWriter).Account()

	cases := []struct {
		name   string
		answer error
		want   error
	}{
		{"not known", coordinatorproto.ErrSpaceNotExists, space.ErrSpaceUnknown},
		{"deleted or in deletion", coordinatorproto.ErrSpaceIsDeleted, space.ErrSpaceDeleted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			net.coord.set(func() {
				net.coord.children = func(string) ([]*coordinatorproto.SpaceChild, error) { return nil, tc.answer }
			})
			_, err := spaces.Children(ctx, parent.Id())
			require.ErrorIs(t, err, tc.want)

			// The ACL cannot be read.
			net.nodes.set(func() {
				net.nodes.get = func(string) ([]*consensusproto.RawRecordWithId, error) { return nil, tc.answer }
				net.nodes.add = func(string, *consensusproto.RawRecord) (*consensusproto.RawRecordWithId, error) {
					t.Error("a record was posted")
					return nil, tc.answer
				}
			})
			_, err = spaces.ChildMembers(ctx, child.id)
			require.ErrorIs(t, err, tc.want)
			require.ErrorIs(t, spaces.RemoveChildMembers(ctx, child.id, []string{member}), tc.want)
			require.ErrorIs(t, spaces.ChangeChildOwner(ctx, child.id, member, space.PermissionAdmin), tc.want)

			// The ACL is read, and the record posted to it is refused.
			child.serve(t, &net.nodes, func(*consensusproto.RawRecord) error { return tc.answer })
			require.ErrorIs(t, spaces.RemoveChildMembers(ctx, child.id, []string{member}), tc.want)
			require.ErrorIs(t, spaces.ChangeChildOwner(ctx, child.id, member, space.PermissionAdmin), tc.want)
		})
	}

	net.coord.set(func() { net.coord.del = func(string) error { return coordinatorproto.ErrSpaceNotExists } })
	require.ErrorIs(t, spaces.DeleteChild(ctx, child.id), space.ErrSpaceUnknown)
	// A child already in deletion, or deleted, is what DeleteChild asks
	// for.
	for _, underWay := range []error{coordinatorproto.ErrSpaceDeletionPending, coordinatorproto.ErrSpaceIsDeleted} {
		net.coord.set(func() { net.coord.del = func(string) error { return underWay } })
		require.NoError(t, spaces.DeleteChild(ctx, child.id))
	}
	requireNothingHeld(t, ctx, sdk, child.id)
}

// The parent side reads whether a child rotated its key after a
// removal, from the ACL the nodes serve.
func TestSDK_ChildRotationPending(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	net := installFakeNetwork(t)
	dataDir := t.TempDir()
	cfg := childTestConfig(t, dataDir)
	network := newTestNetworkKey(t, &cfg)
	sdk, err := Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent"})
	require.NoError(t, err)
	child := newForeignChild(t, network, parent.Id())

	_, err = spaces.ChildRotationPending(ctx, child.id)
	require.ErrorIs(t, err, space.ErrOffline)
	_, err = spaces.ChildRotationPending(ctx, parent.Id())
	require.ErrorIs(t, err, space.ErrNotChildSpace)
	_, err = spaces.ChildRotationPending(ctx, "not-a-space-id")
	require.ErrorIs(t, err, space.ErrBadSpaceId)

	member := child.addMember(t, list.AclPermissionsWriter)
	child.serve(t, &net.nodes, func(*consensusproto.RawRecord) error { return nil })
	pending, err := spaces.ChildRotationPending(ctx, child.id)
	require.NoError(t, err)
	assert.False(t, pending)

	require.NoError(t, spaces.RemoveChildMembers(ctx, child.id, []string{member.Account()}))
	pending, err = spaces.ChildRotationPending(ctx, child.id)
	require.NoError(t, err)
	assert.True(t, pending, "the removed member still holds the key")

	// A device of the child's owner changes the key.
	metadataKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	rotation, err := child.ownerAcl(t).RecordBuilder().BuildReadKeyChange(list.ReadKeyChangePayload{
		MetadataKey: metadataKey,
		ReadKey:     crypto.NewAES(),
	})
	require.NoError(t, err)
	child.accept(rotation)
	pending, err = spaces.ChildRotationPending(ctx, child.id)
	require.NoError(t, err)
	assert.False(t, pending)
	requireNothingHeld(t, ctx, sdk, child.id)
}

// Only the Owner of a space makes it shareable, and the coordinator
// refuses that call from anyone else. An Admin acts in a space that is
// shared already: it adds a member and creates an invite with no such
// call.
func TestSDK_ChildSpace_AdminAddsMembersWithoutMakeShareable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	net := installFakeNetwork(t)
	var makeShareable atomic.Int32
	net.coord.shareable = func(string) error {
		makeShareable.Add(1)
		return coordinatorproto.ErrForbidden
	}
	dataDir := t.TempDir()
	cfg := childTestConfig(t, dataDir)
	network := newTestNetworkKey(t, &cfg)
	sdk, err := Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	// A child another account owns, in which this account is an Admin.
	parent := foreignSpaceA
	require.NoError(t, spaces.Track(ctx, parent))
	child := newForeignChild(t, network, parent)
	acl := child.heldBy(t, ctx, sdk, parent, list.AclPermissionsAdmin)
	child.serve(t, &net.nodes, func(*consensusproto.RawRecord) error { return nil })
	sp, err := spaces.Get(ctx, child.id)
	require.NoError(t, err)
	me, err := sp.Members().Me(ctx)
	require.NoError(t, err)
	require.Equal(t, space.PermissionAdmin, me.Permission)

	added := randomIdentity(t)
	require.NoError(t, sp.ACL().AddAccounts(ctx, []space.MemberAdd{{Identity: added, Permission: space.PermissionWriter}}))
	invite, err := sp.ACL().CreateInvite(ctx)
	require.NoError(t, err)
	assert.Equal(t, child.id, invite.SpaceId)
	assert.Zero(t, makeShareable.Load(), "the coordinator is not asked to make the space shareable")

	acl.RLock()
	state := acl.AclState()
	addedKey, err := crypto.DecodeAccountAddress(added)
	require.NoError(t, err)
	assert.True(t, state.Permissions(addedKey).CanWrite(), "the member is added")
	assert.Len(t, state.InviteIds(), 1, "and the invite is on the ACL")
	acl.RUnlock()
}

// A device of an Admin of a child that starts with a rotation pending
// ends it by itself: the boot pass loads the child and starts the
// watcher that rotates, with no Get of the space.
func TestSDK_ChildSpace_RotationAfterRestartWithoutGet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	net := installFakeNetwork(t)
	dataDir := t.TempDir()
	cfg := childTestConfig(t, dataDir)
	network := newTestNetworkKey(t, &cfg)
	sdk, err := Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)

	// A child another account owns, held here by one of its Admins,
	// with a member that an Admin of the parent removes without the
	// read key. The space is never opened with Get, so nothing here
	// rotates in this run.
	parent := foreignSpaceA
	require.NoError(t, sdk.Spaces().Track(ctx, parent))
	child := newForeignChild(t, network, parent)
	acl := child.heldBy(t, ctx, sdk, parent, list.AclPermissionsAdmin)
	member := child.addMember(t, list.AclPermissionsWriter)
	parentAdmin, err := accountdata.NewRandom()
	require.NoError(t, err)
	st, err := list.NewInMemoryStorage(child.records[0].Id, child.records)
	require.NoError(t, err)
	outside, err := list.BuildAclListWithIdentity(parentAdmin, st, recordverifier.NewValidateAcceptedLog())
	require.NoError(t, err)
	removal, err := outside.RecordBuilder().BuildAccountRemoveNoRotate([]crypto.PubKey{member})
	require.NoError(t, err)
	child.accept(removal)
	acl.Lock()
	err = acl.AddRawRecords(child.records)
	pending := acl.AclState().RotationPending()
	acl.Unlock()
	require.NoError(t, err)
	require.True(t, pending)
	require.NoError(t, sdk.Close())

	var posted atomic.Int32
	child.serve(t, &net.nodes, func(*consensusproto.RawRecord) error {
		posted.Add(1)
		return nil
	})
	sdk, err = Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()

	require.Eventually(t, func() bool {
		handle, ok := sdk.app.PickSpace(ctx, child.id)
		if !ok {
			return false
		}
		acl := handle.Inner().Acl()
		acl.RLock()
		defer acl.RUnlock()
		return !acl.AclState().RotationPending() && posted.Load() == 1
	}, 20*time.Second, 10*time.Millisecond, "the device rotates the read key at start")
	assert.EqualValues(t, 1, posted.Load(), "one read key change")
}

// An id that is not a space id is refused as one by every child-space
// call, before anything is created or sent.
func TestSDK_ChildCalls_MalformedSpaceId(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	net := installFakeNetwork(t)
	sent := func(what string) func() { return func() { t.Errorf("%s reached the network", what) } }
	net.coord.sign = func() error { sent("SpaceSign")(); return nil }
	net.coord.children = func(string) ([]*coordinatorproto.SpaceChild, error) { sent("SpaceChildren")(); return nil, nil }
	net.coord.del = func(string) error { sent("SpaceDelete")(); return nil }
	net.nodes.get = func(string) ([]*consensusproto.RawRecordWithId, error) { sent("AclGetRecords")(); return nil, nil }
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()
	identity := randomIdentity(t)
	before, err := spaces.List(ctx)
	require.NoError(t, err)

	for _, id := range []string{"", "not-a-space-id", "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi", "nocid.2a5s8kx60jkm"} {
		calls := map[string]func() error{
			"CreateChild": func() error {
				_, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: id})
				return err
			},
			"RetryPush": func() error { return spaces.RetryPush(ctx, id) },
			"Children": func() error {
				_, err := spaces.Children(ctx, id)
				return err
			},
			"ChildMembers": func() error {
				_, err := spaces.ChildMembers(ctx, id)
				return err
			},
			"ChildRotationPending": func() error {
				_, err := spaces.ChildRotationPending(ctx, id)
				return err
			},
			"RemoveChildMembers": func() error { return spaces.RemoveChildMembers(ctx, id, []string{identity}) },
			"ChangeChildOwner": func() error {
				return spaces.ChangeChildOwner(ctx, id, identity, space.PermissionAdmin)
			},
			"DeleteChild": func() error { return spaces.DeleteChild(ctx, id) },
		}
		for name, call := range calls {
			require.ErrorIs(t, call(), space.ErrBadSpaceId, "%s(%q)", name, id)
		}
	}
	after, err := spaces.List(ctx)
	require.NoError(t, err)
	assert.Len(t, after, len(before), "nothing is created")
}
