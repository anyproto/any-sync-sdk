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
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/coordinator/coordinatorclient"
	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
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
// coordinator. A call it has no answer for goes to the real client,
// which reaches nobody.
type fakeNetwork struct {
	coord fakeCoordinator
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
