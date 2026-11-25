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
		case anysyncx.NodeClient:
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

// deletesOf counts the signed deletes of spaceId it was sent.
func (f *fakeCoordinator) deletesOf(spaceId string) (n int) {
	f.set(func() {
		for _, id := range f.deleted {
			if id == spaceId {
				n++
			}
		}
	})
	return n
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

// activeSpace is the coordinator's status of an active space of another
// account.
func activeSpace(string) *coordinatorproto.SpaceStatusPayload {
	return &coordinatorproto.SpaceStatusPayload{Status: coordinatorproto.SpaceStatus_SpaceStatusCreated}
}

// coordSpace is what a coordinatorModel holds of one space.
type coordSpace struct {
	status coordinatorproto.SpaceStatus
	// owned: the coordinator's record names the account the owner. It
	// is a copy of the ACL's owner, and can be a stale one. parent: the
	// space names one; parentOwned: the account owns that parent.
	owned, parent, parentOwned bool
	oneToOne                   bool
	// byParentOwner: the deletion is the one of the Owner of the parent.
	byParentOwner bool
}

// coordinatorModel answers the account's status checks, signed deletes
// and ACL calls as the coordinator does, for the spaces put in it.
type coordinatorModel struct {
	mu     sync.Mutex
	spaces map[string]coordSpace
}

func (m *coordinatorModel) put(spaceId string, sp coordSpace) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.spaces[spaceId] = sp
}

func (m *coordinatorModel) get(spaceId string) coordSpace {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.spaces[spaceId]
}

// status reports the owner as the coordinator does: never for a 1-1
// space.
func (m *coordinatorModel) status(spaceId string) *coordinatorproto.SpaceStatusPayload {
	m.mu.Lock()
	defer m.mu.Unlock()
	sp, known := m.spaces[spaceId]
	if !known {
		return &coordinatorproto.SpaceStatusPayload{Status: coordinatorproto.SpaceStatus_SpaceStatusNotExists}
	}
	status := &coordinatorproto.SpaceStatusPayload{Status: sp.status}
	if sp.owned && !sp.oneToOne {
		status.Permissions = coordinatorproto.SpacePermissions_SpacePermissionsOwner
	}
	return status
}

// delete takes the delete of an active space from its owner, and of a
// child from the Owner of its parent: an active one, and one in
// deletion by its own owner, whose deletion becomes the parent Owner's.
func (m *coordinatorModel) delete(spaceId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	sp, known := m.spaces[spaceId]
	switch {
	case !known:
		return coordinatorproto.ErrSpaceNotExists
	case sp.oneToOne:
		return coordinatorproto.ErrUnexpected
	case !sp.owned && sp.parent && !sp.parentOwned:
		return coordinatorproto.ErrParentPermissionDenied
	}
	byParentOwner := !sp.owned && sp.parentOwned
	from := coordinatorproto.SpaceStatus_SpaceStatusCreated
	if byParentOwner && sp.status == coordinatorproto.SpaceStatus_SpaceStatusPendingDeletion && !sp.byParentOwner {
		from = sp.status
	}
	if (sp.owned || byParentOwner) && sp.status == from {
		sp.status = coordinatorproto.SpaceStatus_SpaceStatusPendingDeletion
		sp.byParentOwner = sp.byParentOwner || byParentOwner
		m.spaces[spaceId] = sp
		return nil
	}
	switch sp.status {
	case coordinatorproto.SpaceStatus_SpaceStatusCreated:
		return coordinatorproto.ErrSpaceIsCreated
	case coordinatorproto.SpaceStatus_SpaceStatusPendingDeletion:
		return coordinatorproto.ErrSpaceDeletionPending
	}
	return coordinatorproto.ErrSpaceIsDeleted
}

// acl is the coordinator's answer to a read or a write of the ACL of a
// space it does not hold as an active one.
func (m *coordinatorModel) acl(spaceId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	sp, known := m.spaces[spaceId]
	switch {
	case !known:
		return coordinatorproto.ErrSpaceNotExists
	case sp.status != coordinatorproto.SpaceStatus_SpaceStatusCreated:
		return coordinatorproto.ErrSpaceIsDeleted
	}
	return nil
}

type fakeJoiningClient struct {
	aclclient.AclJoiningClient

	mu sync.Mutex
	// remove answers RemoveChildMembers.
	remove func(spaceId string, identities []string) error
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

type fakeNodeClient struct {
	anysyncx.NodeClient

	mu sync.Mutex
	// add answers AclAddRecord, get AclGetRecords, header SpaceHeader.
	add    func(spaceId string, rec *consensusproto.RawRecord) (*consensusproto.RawRecordWithId, error)
	get    func(spaceId string) ([]*consensusproto.RawRecordWithId, error)
	header func(spaceId string) (*spacesyncproto.RawSpaceHeaderWithId, error)
	// held lists the spaces the nodes hold, when serveHeaders says
	// which: the ACL of any other space is not on the network.
	held map[string]bool
}

func (f *fakeNodeClient) SpaceHeader(ctx context.Context, spaceId string) (*spacesyncproto.RawSpaceHeaderWithId, error) {
	f.mu.Lock()
	header := f.header
	f.mu.Unlock()
	if header == nil {
		return f.NodeClient.SpaceHeader(ctx, spaceId)
	}
	return header(spaceId)
}

// serveHeaders makes the fake nodes hold the spaces of these headers
// and no other.
func (f *fakeNodeClient) serveHeaders(headers ...*spacesyncproto.RawSpaceHeaderWithId) {
	held := make(map[string]bool, len(headers))
	for _, hdr := range headers {
		held[hdr.Id] = true
	}
	f.set(func() {
		f.held = held
		f.header = func(spaceId string) (*spacesyncproto.RawSpaceHeaderWithId, error) {
			for _, hdr := range headers {
				if hdr.Id == spaceId {
					return hdr, nil
				}
			}
			return nil, spacesyncproto.ErrSpaceMissing
		}
	})
}

// answerHeaders makes header answer SpaceHeader, the real client when
// it is nil, and leaves open which spaces the fake nodes hold.
func (f *fakeNodeClient) answerHeaders(header func(spaceId string) (*spacesyncproto.RawSpaceHeaderWithId, error)) {
	f.set(func() { f.header, f.held = header, nil })
}

// AclGetRecords answers as the coordinator behind the nodes does: the
// node that stores a space creates the log of its ACL, so a space the
// coordinator knows and no node holds has none.
func (f *fakeNodeClient) AclGetRecords(ctx context.Context, spaceId, aclHead string) ([]*consensusproto.RawRecordWithId, error) {
	f.mu.Lock()
	get, onNoNode := f.get, f.held != nil && !f.held[spaceId]
	f.mu.Unlock()
	if get == nil {
		return f.NodeClient.AclGetRecords(ctx, spaceId, aclHead)
	}
	recs, err := get(spaceId)
	if err == nil && onNoNode {
		return nil, consensuserr.ErrLogNotFound
	}
	return recs, err
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
// parent. Its first load fills both from the space header. A row can
// hold the type alone, written by a device whose SDK reads no parent
// from a header: the first load here fills the parent.
func TestSDK_ChildSpace_JoinedRowTakesParentFromHeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()

	parent, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
	require.NoError(t, err)
	assert.Empty(t, parent.Info().ParentSpaceId, "a top-level space has no parent")

	// The child's storage as a pull leaves it.
	keys := sdk.app.AccountKeys()
	pulled := func() string {
		readKey, err := crypto.NewRandomAES()
		require.NoError(t, err)
		metadataKey, _, err := crypto.GenerateRandomEd25519KeyPair()
		require.NoError(t, err)
		childId, err := sdk.app.SpaceService().CreateSpace(ctx, spacepayloads.SpaceCreatePayload{
			SigningKey:       keys.SignKey,
			MasterKey:        keys.SignKey,
			ReadKey:          readKey,
			MetadataKey:      metadataKey,
			SpaceType:        space.SpaceTypeOrganizationSpace,
			FileProtoVersion: spacesyncproto.SpaceFileProtoVersion_SpaceFileProtoVersionV2,
			ParentSpaceId:    parent.Id(),
		})
		require.NoError(t, err)
		return childId
	}

	rows := map[string]techspace.SpaceIndexRecord{
		"the row a join registers before the header is readable": {RemoteStatus: techspace.StatusActive},
		"the row with the type alone":                            {RemoteStatus: techspace.StatusActive, Type: space.SpaceTypeOrganizationSpace},
	}
	for name, row := range rows {
		childId := pulled()
		row.Id = childId
		_, err = sdk.tsp.Add(ctx, row)
		require.NoError(t, err)

		child, err := sdk.Spaces().Get(ctx, childId)
		require.NoError(t, err)
		info := child.Info()
		assert.Equal(t, parent.Id(), info.ParentSpaceId, name)
		assert.Equal(t, space.SpaceTypeOrganizationSpace, info.Type, name)

		rec, ok := sdk.tsp.Get(ctx, childId)
		require.True(t, ok)
		assert.Equal(t, parent.Id(), rec.ParentSpaceId, "%s: the row keeps the parent id", name)
		assert.Equal(t, space.SpaceTypeOrganizationSpace, rec.Type, name)

		list, err := sdk.Spaces().List(ctx)
		require.NoError(t, err)
		parents := map[string]string{}
		for _, in := range list {
			parents[in.Id] = in.ParentSpaceId
		}
		assert.Equal(t, parent.Id(), parents[childId], name)
		assert.Empty(t, parents[parent.Id()])
	}
}

// Create makes a space outside any organization, or an organization:
// the type goes into the header, the row and the app-level tag. The
// type of a child is not accepted: such a space comes from CreateChild.
func TestSDK_Create_SpaceTypes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	for _, tc := range []struct{ asked, want string }{
		{"", space.SpaceTypeAny},
		{space.SpaceTypeAny, space.SpaceTypeAny},
		{space.SpaceTypeOrganization, space.SpaceTypeOrganization},
	} {
		sp, err := spaces.Create(ctx, space.CreateRequest{Name: "Space", SpaceType: tc.asked})
		require.NoError(t, err, tc.asked)
		info := sp.Info()
		assert.Equal(t, tc.want, info.Type, tc.asked)
		assert.Equal(t, tc.want, info.SpaceType, tc.asked)
		assert.Empty(t, info.ParentSpaceId, tc.asked)
		hdr := spaceHeaderOf(t, ctx, sdk, sp.Id())
		assert.Equal(t, tc.want, hdr.SpaceType, tc.asked)
		assert.Empty(t, hdr.ParentSpaceId, tc.asked)
		assert.Equal(t, replicationSuffix(sdk.TechSpaceId()), replicationSuffix(sp.Id()), tc.asked)
		wmWrite(t, ctx, sp, 1)
	}

	before, err := spaces.List(ctx)
	require.NoError(t, err)
	for _, refused := range []string{space.SpaceTypeOrganizationSpace, space.SpaceTypeOneToOne, "anytype.space"} {
		_, err := spaces.Create(ctx, space.CreateRequest{Name: "Space", SpaceType: refused})
		require.ErrorIs(t, err, space.ErrBadSpaceType, refused)
	}
	after, err := spaces.List(ctx)
	require.NoError(t, err)
	assert.Len(t, after, len(before), "a refused Create adds no row")
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

// A child is created with no network. Its v1 header carries the child
// type and names the parent, its row carries both, and the parent is
// not written to.
func TestSDK_CreateChild_Offline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	dataDir := t.TempDir()
	sdk := openChildTestSDK(t, ctx, dataDir)

	parent, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
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
	assert.Equal(t, space.SpaceTypeOrganizationSpace, info.Type)
	assert.Equal(t, space.SpaceTypeOrganizationSpace, info.SpaceType)
	assert.Equal(t, "Child", info.Name)

	hdr := spaceHeaderOf(t, ctx, sdk, childId)
	assert.Equal(t, parent.Id(), hdr.ParentSpaceId)
	assert.Equal(t, spacesyncproto.SpaceHeaderVersion_SpaceHeaderVersion1, hdr.Version)
	assert.Equal(t, space.SpaceTypeOrganizationSpace, hdr.SpaceType)
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
// space, and one the index already shows is not an organization. No row
// is added then.
func TestSDK_CreateChild_RefusedParents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
	require.NoError(t, err)
	child, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: parent.Id()})
	require.NoError(t, err)
	derived, err := spaces.Derive(ctx, space.DeriveRequest{Seed: []byte("test/child/derived-parent")})
	require.NoError(t, err)
	plain, err := spaces.Create(ctx, space.CreateRequest{Name: "Plain"})
	require.NoError(t, err)
	deleted, err := spaces.Create(ctx, space.CreateRequest{Name: "Deleted", SpaceType: space.SpaceTypeOrganization})
	require.NoError(t, err)
	require.NoError(t, spaces.Delete(ctx, deleted.Id()))

	const (
		oneToOne   = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.1onetoone"
		pending    = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.2pending"
		foreign    = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.3foreign"
		typedChild = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.5child"
	)
	for _, rec := range []techspace.SpaceIndexRecord{
		{Id: oneToOne, Type: space.SpaceTypeOneToOne, RemoteStatus: techspace.StatusActive, OneToOnePeer: "peer"},
		{Id: pending, RemoteStatus: techspace.InvitePendingRemoteStatus},
		{Id: foreign, Type: "anytype.space", RemoteStatus: techspace.StatusActive},
		{Id: typedChild, Type: space.SpaceTypeOrganizationSpace, RemoteStatus: techspace.StatusActive},
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
		{"child whose row names no parent", typedChild, space.ErrParentSpaceInvalid},
		{"space outside any organization", plain.Id(), space.ErrParentSpaceInvalid},
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

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
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

// RetryPush is for a child space the account holds. A row with the type
// of a child and no parent id is the row of one.
func TestSDK_RetryPush_RefusedSpaces(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	coord := &installFakeNetwork(t).coord
	coord.sign = func() error { return nil }
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
	require.NoError(t, err)
	child, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: parent.Id()})
	require.NoError(t, err)

	require.ErrorIs(t, spaces.RetryPush(ctx, parent.Id()), space.ErrNotChildSpace)
	require.ErrorIs(t, spaces.RetryPush(ctx, "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.4unknown"), space.ErrSpaceUnknown)
	require.NoError(t, spaces.Delete(ctx, child.Id()))
	require.ErrorIs(t, spaces.RetryPush(ctx, child.Id()), space.ErrSpaceDeleted)

	const typedChild = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.5child"
	_, err = sdk.tsp.Add(ctx, techspace.SpaceIndexRecord{
		Id: typedChild, Type: space.SpaceTypeOrganizationSpace, RemoteStatus: techspace.StatusActive,
	})
	require.NoError(t, err)
	require.ErrorIs(t, spaces.RetryPush(ctx, typedChild), space.ErrSpaceNotStored)
	_, err = spaces.ChildMembers(ctx, typedChild)
	require.ErrorIs(t, err, space.ErrOffline, "the parent side takes it for a child too")
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

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
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

	// A parent that is not on the network yet: the coordinator does not
	// know it, then knows it while its ACL has not arrived.
	for _, tc := range []struct{ answer, want error }{
		{coordinatorproto.ErrSpaceNotExists, space.ErrSpaceUnknown},
		{coordinatorproto.ErrParentSpaceNotAvailable, space.ErrParentSpaceNotAvailable},
	} {
		coord.set(func() {
			coord.children = func(string) ([]*coordinatorproto.SpaceChild, error) { return nil, tc.answer }
		})
		_, err = spaces.Children(ctx, foreignSpaceA)
		require.ErrorIs(t, err, tc.want)
		require.NotErrorIs(t, err, space.ErrOffline)
	}

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
		SpaceType:     space.SpaceTypeOrganizationSpace,
		ParentSpaceId: parentSpaceId,
	})
	require.NoError(t, err)
	return payload.SpaceHeaderWithId.Id
}

// networkSpace is a space as the network holds it, its header and its
// ACL: this device holds nothing of it.
type networkSpace struct {
	id      string
	owner   *accountdata.AccountKeys
	records []*consensusproto.RawRecordWithId
	network testNetworkKey
	// storage is what a device creates the child's storage from.
	storage spacestorage.SpaceStorageCreatePayload
}

// newForeignChild is a child space of parentSpaceId another account
// owns.
func newForeignChild(t *testing.T, network testNetworkKey, parentSpaceId string) *networkSpace {
	t.Helper()
	owner, err := accountdata.NewRandom()
	require.NoError(t, err)
	return newNetworkSpace(t, network, owner, space.SpaceTypeOrganizationSpace, parentSpaceId)
}

// newNetworkSpace is a space of type spaceType that owner created, a
// top-level one when parentSpaceId is empty.
func newNetworkSpace(t *testing.T, network testNetworkKey, owner *accountdata.AccountKeys, spaceType, parentSpaceId string) *networkSpace {
	t.Helper()
	metadataKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	payload, err := spacepayloads.StoragePayloadForSpaceCreateV1(spacepayloads.SpaceCreatePayload{
		SigningKey:    owner.SignKey,
		MasterKey:     owner.SignKey,
		ReadKey:       crypto.NewAES(),
		MetadataKey:   metadataKey,
		SpaceType:     spaceType,
		ParentSpaceId: parentSpaceId,
	})
	require.NoError(t, err)
	return networkSpaceOf(network, owner, payload)
}

// networkSpaceOf is the space of payload. keys are the account the
// space's ACL is read as: the one that created it, or a member.
func networkSpaceOf(network testNetworkKey, keys *accountdata.AccountKeys, payload spacestorage.SpaceStorageCreatePayload) *networkSpace {
	return &networkSpace{
		id:      payload.SpaceHeaderWithId.Id,
		owner:   keys,
		records: []*consensusproto.RawRecordWithId{payload.AclWithId},
		network: network,
		storage: payload,
	}
}

// heldBy makes the device of sdk hold the child with its account as a
// member of role perm: the storage a pull leaves, the ACL record that
// adds the account, and the row an accepted invite leaves, which names
// rowParent as the parent. It returns the child's ACL on that device.
func (c *networkSpace) heldBy(t *testing.T, ctx context.Context, sdk *SDK, rowParent string, perm list.AclPermissions) list.AclList {
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
		Type:          space.SpaceTypeOrganizationSpace,
		SpaceType:     space.SpaceTypeOrganizationSpace,
		RemoteStatus:  techspace.StatusActive,
		ParentSpaceId: rowParent,
	})
	require.NoError(t, err)
	return acl
}

// ownerAcl is the space's ACL on a device of the account that created
// it.
func (c *networkSpace) ownerAcl(t *testing.T) list.AclList {
	t.Helper()
	st, err := list.NewInMemoryStorage(c.records[0].Id, c.records)
	require.NoError(t, err)
	acl, err := list.BuildAclListWithIdentity(c.owner, st, recordverifier.NewValidateAcceptedLog())
	require.NoError(t, err)
	return acl
}

// accept takes a record into the space's ACL as the network does.
func (c *networkSpace) accept(rec *consensusproto.RawRecord) *consensusproto.RawRecordWithId {
	accepted := c.network.accept(rec)
	c.records = append(c.records, accepted)
	return accepted
}

// invite adds a request-to-join invite and returns its key.
func (c *networkSpace) invite(t *testing.T) crypto.PrivKey {
	t.Helper()
	invite, err := c.ownerAcl(t).RecordBuilder().BuildInvite()
	require.NoError(t, err)
	c.accept(invite.InviteRec)
	return invite.InviteKey
}

// addMember makes a new account a member and returns its identity.
func (c *networkSpace) addMember(t *testing.T, perm list.AclPermissions) crypto.PubKey {
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

// handTo makes account a member of the space and then its owner.
func (c *networkSpace) handTo(t *testing.T, account crypto.PubKey) {
	t.Helper()
	add, err := c.ownerAcl(t).RecordBuilder().BuildAccountsAdd(list.AccountsAddPayload{Additions: []list.AccountAdd{
		{Identity: account, Permissions: list.AclPermissionsWriter},
	}})
	require.NoError(t, err)
	c.accept(add)
	change, err := c.ownerAcl(t).RecordBuilder().BuildOwnershipChange(list.OwnershipChangePayload{
		NewOwner:            account,
		OldOwnerPermissions: list.AclPermissionsAdmin,
	})
	require.NoError(t, err)
	c.accept(change)
}

// serve makes the fake nodes serve the space's ACL and take the
// records posted to it. posted receives each of them.
func (c *networkSpace) serve(t *testing.T, nodes *fakeNodeClient, posted func(*consensusproto.RawRecord) error) {
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
	dataDir := t.TempDir()
	cfg := childTestConfig(t, dataDir)
	network := newTestNetworkKey(t, &cfg)
	sdk, err := Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
	require.NoError(t, err)
	child := newForeignChild(t, network, parent.Id())
	childId := child.id
	member, newOwnerKey := randomIdentity(t), child.addMember(t, list.AclPermissionsWriter)
	newOwner := newOwnerKey.Account()
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

	// The coordinator holds the child as a space of another account,
	// and the nodes serve its ACL: ChangeChildOwner posts its record
	// there.
	net.coord.set(func() { net.coord.status = activeSpace })
	var ownerAnswer error
	child.serve(t, &net.nodes, func(*consensusproto.RawRecord) error { return ownerAnswer })

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
		})
		ownerAnswer = tc.answer
		net.coord.set(func() { net.coord.del = func(string) error { return tc.answer } })
		for name, call := range calls {
			err := call()
			require.ErrorIs(t, err, tc.want, name)
			require.NotErrorIs(t, err, space.ErrOffline, name)
		}
	}

	// Accepted: what was asked reaches the network as given.
	var removedIn, removed string
	net.joining.set(func() {
		net.joining.remove = func(spaceId string, identities []string) error {
			removedIn, removed = spaceId, strings.Join(identities, ",")
			return nil
		}
	})
	ownerAnswer = nil
	net.coord.set(func() {
		net.coord.deleted = nil
		net.coord.del = func(string) error { return nil }
	})
	for name, call := range calls {
		require.NoError(t, call(), name)
	}
	assert.Equal(t, childId, removedIn)
	assert.Equal(t, member, removed)
	acl := child.ownerAcl(t).AclState()
	assert.True(t, acl.Permissions(newOwnerKey).IsOwner())
	assert.Equal(t, list.AclPermissionsAdmin, acl.Permissions(child.owner.SignKey.GetPublic()))
	assert.Equal(t, []string{childId}, net.coord.deleted)

	requireNothingHeld(t, ctx, sdk, childId)
	after, err := spaces.List(ctx)
	require.NoError(t, err)
	assert.Len(t, after, len(before), "the child never enters the account's list")

	// Malformed input and a space the index shows is no child never
	// reach the network.
	net.joining.set(func() {
		net.joining.remove = func(string, []string) error { t.Error("RemoveChildMembers sent"); return nil }
	})
	net.nodes.set(func() {
		net.nodes.get = func(spaceId string) ([]*consensusproto.RawRecordWithId, error) {
			t.Errorf("the ACL of %s was read", spaceId)
			return nil, anynet.ErrUnableToConnect
		}
		net.nodes.add = func(string, *consensusproto.RawRecord) (*consensusproto.RawRecordWithId, error) {
			t.Error("ChangeChildOwner sent")
			return nil, anynet.ErrUnableToConnect
		}
	})
	net.coord.set(func() {
		net.coord.del = func(string) error { t.Error("SpaceDelete sent"); return nil }
	})
	derived, err := spaces.Derive(ctx, space.DeriveRequest{Seed: []byte("test/child/governance")})
	require.NoError(t, err)
	const oneToOne = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.1onetoone"
	_, err = sdk.tsp.Add(ctx, techspace.SpaceIndexRecord{
		Id: oneToOne, Type: space.SpaceTypeOneToOne, RemoteStatus: techspace.StatusActive, OneToOnePeer: "peer",
	})
	require.NoError(t, err)
	for _, notChild := range []string{parent.Id(), derived.Id(), sdk.TechSpaceId(), oneToOne} {
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

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
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

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
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

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
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
	// pending. A pass under way when the removal is applied tries as
	// well as the pass the removal starts: the tries are not counted.
	require.Eventually(t, func() bool { return unreached.Load() > 0 },
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

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
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

// A child space admits only members of its parent, and takes only one
// of them as its owner. The coordinator's refusal reads
// ErrNotParentMember on every way in: a direct add, an accepted join
// request, a join by invite, a change of the owner. It reads
// ErrParentSpaceNotAvailable when the coordinator cannot read the ACL
// of the parent.
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

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
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
	net.nodes.set(func() { net.nodes.add = accept })
	member := randomIdentity(t)
	require.NoError(t, child.ACL().AddAccounts(ctx, []space.MemberAdd{{Identity: member, Permission: space.PermissionWriter}}))
	request := postJoinRequest(t, ctx, network, child, acl)

	for _, tc := range []struct{ answer, want error }{
		{coordinatorproto.ErrNotParentMember, space.ErrNotParentMember},
		{coordinatorproto.ErrParentSpaceNotAvailable, space.ErrParentSpaceNotAvailable},
	} {
		net.nodes.set(func() {
			net.nodes.add = func(string, *consensusproto.RawRecord) (*consensusproto.RawRecordWithId, error) {
				return nil, tc.answer
			}
		})

		// A direct add.
		outsider := randomIdentity(t)
		err = child.ACL().AddAccounts(ctx, []space.MemberAdd{{Identity: outsider, Permission: space.PermissionWriter}})
		require.ErrorIs(t, err, tc.want)
		rec, ok := sdk.tsp.Get(ctx, child.Id())
		require.True(t, ok)
		assert.NotContains(t, rec.InviteNotifyPending, outsider, "nobody is notified of an add that did not happen")

		// An accepted join request.
		require.ErrorIs(t, child.ACL().AcceptRequest(ctx, request, space.PermissionWriter), tc.want)

		// A change of the owner.
		require.ErrorIs(t, child.ACL().OwnershipChange(ctx, member, space.PermissionAdmin), tc.want)

		// A join by invite, from the joiner's side: the account reads the
		// ACL of another account's child off the nodes and posts its join
		// request, which the network refuses.
		foreign := newForeignChild(t, network, parent.Id())
		foreignInvite := foreign.invite(t)
		var requests atomic.Int32
		foreign.serve(t, &net.nodes, func(*consensusproto.RawRecord) error {
			requests.Add(1)
			return tc.answer
		})
		token, err := space.EncodeInvite(space.Invite{SpaceId: foreign.id, InviteKey: foreignInvite})
		require.NoError(t, err)
		_, err = spaces.Join(ctx, space.JoinRequest{Invite: token})
		require.ErrorIs(t, err, tc.want)
		assert.EqualValues(t, 1, requests.Load(), "the request was posted and refused")
		requireNothingHeld(t, ctx, sdk, foreign.id)
		net.nodes.set(func() { net.nodes.get = nil })
	}
}

// postJoinRequest makes sp hold the join request of a new account, as
// the network accepted it, and returns its record id. acl is the ACL
// of sp on this device.
func postJoinRequest(t *testing.T, ctx context.Context, network testNetworkKey, sp space.Space, acl list.AclList) string {
	t.Helper()
	invite, err := sp.ACL().CreateInvite(ctx)
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
	return request.Id
}

// An organization and its spaces have no guest role. The calls that
// mint or revoke a guest key, or name the guest role for an account,
// return ErrNoGuestRole there: for any caller, with nothing sent and
// nothing stored. The type decides. It is the row's, whether or not the
// row names a parent, and the header's for a row with no type. A space
// outside any organization keeps its guest key and its guests.
func TestSDK_NoGuestRole_InAnOrganizationAndItsSpaces(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	net := installFakeNetwork(t)
	dataDir := t.TempDir()
	cfg := childTestConfig(t, dataDir)
	network := newTestNetworkKey(t, &cfg)
	var sent atomic.Int32
	net.coord.shareable = func(string) error {
		sent.Add(1)
		return nil
	}
	net.nodes.add = func(_ string, rec *consensusproto.RawRecord) (*consensusproto.RawRecordWithId, error) {
		sent.Add(1)
		return network.accept(rec), nil
	}
	sdk, err := Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	aclOf := func(spaceId string) list.AclList {
		handle, err := sdk.app.GetSpace(ctx, spaceId)
		require.NoError(t, err)
		return handle.Inner().Acl()
	}
	guestsIn := func(acl list.AclList) (guests int) {
		acl.RLock()
		defer acl.RUnlock()
		for _, acc := range acl.AclState().CurrentAccounts() {
			if acc.Permissions.IsGuest() {
				guests++
			}
		}
		return guests
	}
	// held makes the device hold a space the account owns with the row
	// rec: the storage a pull leaves, and a handle that has not loaded
	// the space, so the row stays as given.
	held := func(spaceType, parentSpaceId string, rec techspace.SpaceIndexRecord) space.Space {
		keys := sdk.app.AccountKeys()
		readKey, err := crypto.NewRandomAES()
		require.NoError(t, err)
		metadataKey, _, err := crypto.GenerateRandomEd25519KeyPair()
		require.NoError(t, err)
		rec.Id, err = sdk.app.SpaceService().CreateSpace(ctx, spacepayloads.SpaceCreatePayload{
			SigningKey:       keys.SignKey,
			MasterKey:        keys.SignKey,
			ReadKey:          readKey,
			MetadataKey:      metadataKey,
			SpaceType:        spaceType,
			FileProtoVersion: spacesyncproto.SpaceFileProtoVersion_SpaceFileProtoVersionV2,
			ParentSpaceId:    parentSpaceId,
		})
		require.NoError(t, err)
		rec.RemoteStatus = techspace.StatusActive
		_, err = sdk.tsp.Add(ctx, rec)
		require.NoError(t, err)
		return sdk.spaces.HandleForTest(rec.Id)
	}

	org, err := spaces.Create(ctx, space.CreateRequest{Name: "Organization", SpaceType: space.SpaceTypeOrganization})
	require.NoError(t, err)
	child, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: org.Id()})
	require.NoError(t, err)
	foreign := newForeignChild(t, network, org.Id())
	foreign.heldBy(t, ctx, sdk, org.Id(), list.AclPermissionsAdmin)
	asAdmin, err := spaces.Get(ctx, foreign.id)
	require.NoError(t, err)

	const childType = space.SpaceTypeOrganizationSpace
	for _, tc := range []struct {
		name string
		sp   space.Space
	}{
		{"an organization", org},
		{"a space of an organization", child},
		{"a space of an organization of which the account is an Admin", asAdmin},
		{"a row with the type and no parent id", held(childType, org.Id(), techspace.SpaceIndexRecord{Type: childType})},
		{"a row with no type, of a space of an organization", held(childType, org.Id(), techspace.SpaceIndexRecord{})},
		{"a row with no type, of an organization", held(space.SpaceTypeOrganization, "", techspace.SpaceIndexRecord{})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sp, id := tc.sp, tc.sp.Id()
			acl := aclOf(id)
			// A Reader and a join request: without the refusal the
			// calls below would go to the network.
			reader := randomIdentity(t)
			require.NoError(t, sp.ACL().AddAccounts(ctx, []space.MemberAdd{{Identity: reader, Permission: space.PermissionReader}}))
			request := postJoinRequest(t, ctx, network, sp, acl)
			rowBefore, ok := sdk.tsp.Get(ctx, id)
			require.True(t, ok)
			head, sentBefore := acl.Head().Id, sent.Load()

			member, guest := randomIdentity(t), randomIdentity(t)
			calls := map[string]func() error{
				"CreateGuestKey": func() error {
					_, err := sp.ACL().CreateGuestKey(ctx)
					return err
				},
				"RevokeGuestKey": func() error { return sp.ACL().RevokeGuestKey(ctx) },
				"AddAccounts": func() error {
					return sp.ACL().AddAccounts(ctx, []space.MemberAdd{
						{Identity: member, Permission: space.PermissionWriter},
						{Identity: guest, Permission: space.PermissionGuest},
					})
				},
				"ChangePermissions": func() error {
					return sp.ACL().ChangePermissions(ctx, []space.PermissionChange{{Identity: reader, Permission: space.PermissionGuest}})
				},
				"AcceptRequest": func() error { return sp.ACL().AcceptRequest(ctx, request, space.PermissionGuest) },
			}
			for name, call := range calls {
				require.ErrorIs(t, call(), space.ErrNoGuestRole, name)
			}

			assert.Equal(t, sentBefore, sent.Load(), "nothing is sent")
			assert.Equal(t, head, acl.Head().Id, "the ACL is not written to")
			assert.Zero(t, guestsIn(acl), "no account is a guest")
			row, ok := sdk.tsp.Get(ctx, id)
			require.True(t, ok)
			assert.Empty(t, row.IssuedInviteKey(techspace.IssuedKeyGuest), "no guest key is stored")
			assert.NotContains(t, row.InviteNotifyPending, member, "nobody of the batch is notified")
			assert.NotContains(t, row.InviteNotifyPending, guest, "nobody of the batch is notified")
			assert.Equal(t, rowBefore.Type, row.Type, "the row's type is as it was")
			assert.Equal(t, rowBefore.ParentSpaceId, row.ParentSpaceId, "the row's parent id is as it was")

			// The other roles are open.
			require.NoError(t, sp.ACL().ChangePermissions(ctx, []space.PermissionChange{{Identity: reader, Permission: space.PermissionWriter}}))
			require.NoError(t, sp.ACL().AcceptRequest(ctx, request, space.PermissionReader))
		})
	}

	// A row with no type of a space that does not load: the type is not
	// known, the call fails on the load and nothing is sent.
	t.Run("a space that does not load", func(t *testing.T) {
		require.NoError(t, spaces.Track(ctx, foreignSpaceB))
		sp := sdk.spaces.HandleForTest(foreignSpaceB)
		short, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		sentBefore := sent.Load()
		_, err := sp.ACL().CreateGuestKey(short)
		require.ErrorIs(t, err, context.DeadlineExceeded, "the load waits for a node")
		require.NotErrorIs(t, err, space.ErrNoGuestRole)
		assert.Equal(t, sentBefore, sent.Load(), "nothing is sent")
	})

	t.Run("a space outside any organization", func(t *testing.T) {
		plain, err := spaces.Create(ctx, space.CreateRequest{Name: "Plain"})
		require.NoError(t, err)
		acl := aclOf(plain.Id())
		reader := randomIdentity(t)
		require.NoError(t, plain.ACL().AddAccounts(ctx, []space.MemberAdd{{Identity: reader, Permission: space.PermissionReader}}))
		request := postJoinRequest(t, ctx, network, plain, acl)

		sentBefore := sent.Load()
		invite, err := plain.ACL().CreateGuestKey(ctx)
		require.NoError(t, err)
		assert.Equal(t, space.InviteKindGuest, invite.Kind)
		assert.Equal(t, plain.Id(), invite.SpaceId)
		assert.Greater(t, sent.Load(), sentBefore, "the guest identity is published")
		acl.RLock()
		assert.True(t, acl.AclState().Permissions(invite.InviteKey.GetPublic()).IsGuest())
		acl.RUnlock()

		require.NoError(t, plain.ACL().AddAccounts(ctx, []space.MemberAdd{{Identity: randomIdentity(t), Permission: space.PermissionGuest}}))
		require.NoError(t, plain.ACL().ChangePermissions(ctx, []space.PermissionChange{{Identity: reader, Permission: space.PermissionGuest}}))
		require.NoError(t, plain.ACL().AcceptRequest(ctx, request, space.PermissionGuest))
		assert.Equal(t, 4, guestsIn(acl), "the guest key, an added guest, a Reader made one and an accepted joiner")

		require.NoError(t, plain.ACL().RevokeGuestKey(ctx))
		assert.Zero(t, guestsIn(acl))
	})
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

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
	require.NoError(t, err)
	// The row as it syncs in from the device that created the child
	// and was refused.
	childId := foreignChildId(t, parent.Id())
	_, err = sdk.tsp.Add(ctx, techspace.SpaceIndexRecord{
		Id:            childId,
		Type:          space.SpaceTypeOrganizationSpace,
		SpaceType:     space.SpaceTypeOrganizationSpace,
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

// Another device of the account deletes a refused child that this one
// holds: the row reads deleted and the copy stays until the next start.
// The device asks for the child no more, also once the coordinator
// would sign it.
func TestSDK_ChildSpace_DeletedOnAnotherDeviceIsNotAskedFor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	coord := &installFakeNetwork(t).coord
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	require.NoError(t, spaces.Track(ctx, foreignSpaceA))
	child, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: foreignSpaceA})
	require.NoError(t, err)
	childId := child.Id()
	coord.set(func() { coord.sign = func() error { return coordinatorproto.ErrParentPermissionDenied } })
	require.ErrorIs(t, spaces.RetryPush(ctx, childId), space.ErrParentPermissionDenied)
	require.Equal(t, space.RefusalParentPermissionDenied, child.Info().Refusal)

	// The other device's delete arrives as the tombstone on the synced
	// row.
	_, err = sdk.tsp.SetRemoteStatus(ctx, childId, techspace.StatusDeleted)
	require.NoError(t, err)
	require.True(t, sdk.app.SpaceExists(childId), "this device still holds the child")

	coord.set(func() {
		coord.signs = 0
		coord.sign = func() error { return nil }
	})
	require.ErrorIs(t, sdk.app.SignSpace(ctx, childId), space.ErrSpaceDeleted)
	require.ErrorIs(t, spaces.RetryPush(ctx, childId), space.ErrSpaceDeleted)
	coord.set(func() { assert.Zero(t, coord.signs, "the coordinator is not asked") })
	rec, ok := sdk.tsp.Get(ctx, childId)
	require.True(t, ok)
	assert.Equal(t, space.RefusalParentPermissionDenied, rec.Refusal, "and nothing is recorded as signed")
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

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
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

// The owner of an organization is usually no member of the children
// others created under it, so the index does not show them. With the
// coordinator in reach, Delete asks it before anything is written: an
// organization with children is not deleted and stays as it is.
func TestSDK_Delete_OwnedSpaceAsksTheCoordinatorFirst(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	coord := &installFakeNetwork(t).coord
	coord.del = func(string) error { return coordinatorproto.ErrSpaceHasChildren }
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	// Right after Create the ACL mirror may not have written the role.
	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
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

	// Only an organization has children: the delete of a child, and of
	// a space outside any organization, does not ask first.
	other, err := spaces.Create(ctx, space.CreateRequest{Name: "Other", SpaceType: space.SpaceTypeOrganization})
	require.NoError(t, err)
	child, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: other.Id()})
	require.NoError(t, err)
	plain, err := spaces.Create(ctx, space.CreateRequest{Name: "Plain"})
	require.NoError(t, err)
	asked := len(coord.deleted)
	coord.set(func() { coord.del = func(string) error { return anynet.ErrUnableToConnect } })
	require.NoError(t, spaces.Delete(ctx, child.Id()))
	require.NoError(t, spaces.Delete(ctx, plain.Id()))
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
		st := &coordinatorproto.SpaceStatusPayload{Status: coordinatorproto.SpaceStatus_SpaceStatusCreated}
		if !isParent(spaceId) {
			return st
		}
		st.Permissions = coordinatorproto.SpacePermissions_SpacePermissionsOwner
		if p.accepted.Load() {
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

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
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

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
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

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
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

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
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
// an ACL read or write on unchanged. The space is another account's.
// DeleteChild alone reads its status first, and sends no delete of a
// space the coordinator does not know.
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

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
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

	statusIs := func(status coordinatorproto.SpaceStatus) {
		net.coord.set(func() {
			net.coord.status = func(string) *coordinatorproto.SpaceStatusPayload {
				return &coordinatorproto.SpaceStatusPayload{Status: status}
			}
		})
	}
	// The coordinator's status says it does not know the space, or its
	// answer to the delete does.
	net.coord.set(func() { net.coord.del = func(string) error { t.Error("SpaceDelete sent"); return nil } })
	statusIs(coordinatorproto.SpaceStatus_SpaceStatusNotExists)
	require.ErrorIs(t, spaces.DeleteChild(ctx, child.id), space.ErrSpaceUnknown)
	statusIs(coordinatorproto.SpaceStatus_SpaceStatusCreated)
	net.coord.set(func() { net.coord.del = func(string) error { return coordinatorproto.ErrSpaceNotExists } })
	require.ErrorIs(t, spaces.DeleteChild(ctx, child.id), space.ErrSpaceUnknown)

	// A child already in deletion, or deleted, is what DeleteChild asks
	// for. The delete is sent in every status: the coordinator answers.
	for _, underWay := range []struct {
		status coordinatorproto.SpaceStatus
		answer error
	}{
		{coordinatorproto.SpaceStatus_SpaceStatusCreated, coordinatorproto.ErrSpaceDeletionPending},
		{coordinatorproto.SpaceStatus_SpaceStatusPendingDeletion, coordinatorproto.ErrSpaceDeletionPending},
		{coordinatorproto.SpaceStatus_SpaceStatusDeletionStarted, coordinatorproto.ErrSpaceIsDeleted},
		{coordinatorproto.SpaceStatus_SpaceStatusDeleted, coordinatorproto.ErrSpaceIsDeleted},
	} {
		statusIs(underWay.status)
		net.coord.set(func() {
			net.coord.deleted = nil
			net.coord.del = func(string) error { return underWay.answer }
		})
		require.NoError(t, spaces.DeleteChild(ctx, child.id))
		net.coord.set(func() { require.Equal(t, []string{child.id}, net.coord.deleted, "status %s", underWay.status) })
	}
	requireNothingHeld(t, ctx, sdk, child.id)
}

// DeleteChild of a top-level space of another account: the
// coordinator's answer to a delete from an account that owns neither
// the space nor a parent of it reads as no child space.
func TestSDK_DeleteChild_TopLevelSpaceOfAnotherAccount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	net := installFakeNetwork(t)
	net.coord.status = activeSpace
	net.coord.del = func(string) error { return coordinatorproto.ErrSpaceIsCreated }
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	err := spaces.DeleteChild(ctx, foreignSpaceB)
	require.ErrorIs(t, err, space.ErrNotChildSpace)
	require.ErrorIs(t, err, coordinatorproto.ErrSpaceIsCreated, "the coordinator's answer stays in the chain")
	requireNothingHeld(t, ctx, sdk, foreignSpaceB)

	// The same for a row whose header was never read: the index cannot
	// tell, the coordinator does, and the row stays as it was.
	require.NoError(t, spaces.Track(ctx, foreignSpaceC))
	require.ErrorIs(t, spaces.DeleteChild(ctx, foreignSpaceC), space.ErrNotChildSpace)
	rec, ok := sdk.tsp.Get(ctx, foreignSpaceC)
	require.True(t, ok)
	assert.False(t, rec.IsDeleted())
	net.coord.set(func() { assert.Equal(t, []string{foreignSpaceB, foreignSpaceC}, net.coord.deleted) })
}

// openOnCoordinatorModel opens an SDK on a fake network whose
// coordinator is a coordinatorModel.
func openOnCoordinatorModel(t *testing.T, ctx context.Context) (*fakeNetwork, *coordinatorModel, testNetworkKey, *SDK) {
	t.Helper()
	net := installFakeNetwork(t)
	coord := &coordinatorModel{spaces: map[string]coordSpace{}}
	net.coord.status, net.coord.del = coord.status, coord.delete
	dataDir := t.TempDir()
	cfg := childTestConfig(t, dataDir)
	network := newTestNetworkKey(t, &cfg)
	sdk, err := Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })
	return net, coord, network, sdk
}

// serveAcl makes the fake nodes serve the ACL of sp and take a record
// as the coordinator does: for an active space, on the authority of the
// parent when the account owns the parent, and on none otherwise. The
// coordinator's own record of the owner is left as it is. It returns
// the count of the records taken.
func serveAcl(t *testing.T, net *fakeNetwork, coord *coordinatorModel, sp *networkSpace, parentOwned bool) (taken *int) {
	t.Helper()
	taken = new(int)
	var authority list.ParentAuthorizer
	if parentOwned {
		authority = func(crypto.PubKey) (list.ParentAuthority, error) {
			return list.ParentAuthority{RemoveMembers: true, ChangeOwner: true}, nil
		}
	}
	net.nodes.set(func() {
		net.nodes.get = func(spaceId string) ([]*consensusproto.RawRecordWithId, error) {
			assert.Equal(t, sp.id, spaceId)
			if err := coord.acl(spaceId); err != nil {
				return nil, err
			}
			return sp.records, nil
		}
		net.nodes.add = func(spaceId string, rec *consensusproto.RawRecord) (*consensusproto.RawRecordWithId, error) {
			assert.Equal(t, sp.id, spaceId)
			if err := coord.acl(spaceId); err != nil {
				return nil, err
			}
			if err := sp.ownerAcl(t).ValidateRawRecordWithAuthorizer(rec, authority, nil); err != nil {
				return nil, err
			}
			*taken++
			return sp.accept(rec), nil
		}
	})
	return taken
}

// The coordinator takes the signed delete of a space from its owner,
// and an ACL its owner's change of the owner, in a top-level space too.
// For a space the index does not show as a child, DeleteChild and
// ChangeChildOwner act on a space the account itself owns only when its
// header, read from the nodes, names a parent. The account's own
// top-level space, on a device that holds no row of it or a row with no
// type, is not deleted and not handed over.
func TestSDK_DeleteChild_AndChangeChildOwner_OwnSpaceWithNoTypedRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	net, coord, network, sdk := openOnCoordinatorModel(t, ctx)
	spaces := sdk.Spaces()
	account := sdk.app.AccountKeys()

	const noRow, untypedRow = "no row", "a row with no type"
	rows := []string{noRow, untypedRow}
	track := func(t *testing.T, row string, sp *networkSpace) {
		t.Helper()
		if row == untypedRow {
			require.NoError(t, spaces.Track(ctx, sp.id))
		}
	}
	requireNothingSent := func(t *testing.T, sp *networkSpace, taken *int) {
		t.Helper()
		require.Zero(t, net.coord.deletesOf(sp.id), "no signed delete")
		require.Zero(t, *taken, "no ACL record")
	}
	ownChild := func(t *testing.T) *networkSpace {
		parent := newNetworkSpace(t, network, account, space.SpaceTypeOrganization, "")
		return newNetworkSpace(t, network, account, space.SpaceTypeOrganizationSpace, parent.id)
	}
	child := coordSpace{owned: true, parent: true, parentOwned: true}
	inDeletion := []coordinatorproto.SpaceStatus{
		coordinatorproto.SpaceStatus_SpaceStatusPendingDeletion,
		coordinatorproto.SpaceStatus_SpaceStatusDeletionStarted,
		coordinatorproto.SpaceStatus_SpaceStatusDeleted,
	}

	topLevel := []struct {
		name  string
		build func(t *testing.T, row string) *networkSpace
	}{
		{space.SpaceTypeAny, func(t *testing.T, _ string) *networkSpace {
			return newNetworkSpace(t, network, account, space.SpaceTypeAny, "")
		}},
		{space.SpaceTypeOrganization, func(t *testing.T, _ string) *networkSpace {
			return newNetworkSpace(t, network, account, space.SpaceTypeOrganization, "")
		}},
		{"derived", func(t *testing.T, row string) *networkSpace {
			payload, err := spacepayloads.StoragePayloadForSpaceDeriveV1(spacepayloads.SpaceDerivePayload{
				SigningKey:   account.SignKey,
				MasterKey:    account.SignKey,
				SpaceType:    space.SpaceTypeAny,
				SpacePayload: []byte(row),
			})
			require.NoError(t, err)
			return networkSpaceOf(network, account, payload)
		}},
	}
	for _, kind := range topLevel {
		for _, row := range rows {
			t.Run(kind.name+", "+row, func(t *testing.T) {
				own := kind.build(t, row)
				// A derived space takes no member.
				member := randomIdentity(t)
				if kind.name != "derived" {
					member = own.addMember(t, list.AclPermissionsWriter).Account()
				}
				track(t, row, own)
				coord.put(own.id, coordSpace{owned: true})
				taken := serveAcl(t, net, coord, own, false)
				net.nodes.serveHeaders(own.storage.SpaceHeaderWithId)

				require.ErrorIs(t, spaces.DeleteChild(ctx, own.id), space.ErrNotChildSpace)
				require.ErrorIs(t, spaces.ChangeChildOwner(ctx, own.id, member, space.PermissionAdmin), space.ErrNotChildSpace)
				// A removal on the authority of a parent is the network's
				// to refuse.
				require.Error(t, spaces.RemoveChildMembers(ctx, own.id, []string{member}))
				requireNothingSent(t, own, taken)

				rec, held := sdk.tsp.Get(ctx, own.id)
				assert.Equal(t, row == untypedRow, held)
				assert.False(t, rec.IsDeleted(), "the row stays as it was")
				assert.False(t, sdk.app.SpaceExists(own.id), "nothing is stored")
			})
		}
	}

	// A top-level space of another account that was handed to this one.
	// The ACL names the account the owner; the coordinator's record of
	// the owner, a copy it writes after the ACL took the record, names
	// the previous one. ChangeChildOwner reads the owner off the ACL it
	// builds its record on, and the coordinator refuses the delete by
	// its own record.
	for _, row := range rows {
		t.Run("handed to the account, the coordinator's record stale, "+row, func(t *testing.T) {
			previous, err := accountdata.NewRandom()
			require.NoError(t, err)
			handed := newNetworkSpace(t, network, previous, space.SpaceTypeOrganization, "")
			member := handed.addMember(t, list.AclPermissionsWriter).Account()
			handed.handTo(t, account.SignKey.GetPublic())
			track(t, row, handed)
			coord.put(handed.id, coordSpace{})
			taken := serveAcl(t, net, coord, handed, false)
			net.nodes.serveHeaders(handed.storage.SpaceHeaderWithId)

			require.ErrorIs(t, spaces.ChangeChildOwner(ctx, handed.id, member, space.PermissionAdmin), space.ErrNotChildSpace)
			require.Zero(t, *taken, "no ACL record")
			assert.True(t, handed.ownerAcl(t).AclState().Permissions(account.SignKey.GetPublic()).IsOwner())
			require.ErrorIs(t, spaces.DeleteChild(ctx, handed.id), space.ErrNotChildSpace)
			assert.Equal(t, coordSpace{}, coord.get(handed.id), "the space is as it was")
		})
	}

	// The hand-over lands between the read of the ACL and the post. The
	// ACL read names another account the owner, so no header is asked
	// for. The record names the head it is built on: the network
	// refuses it, and the space stays the account's.
	t.Run("handed to the account after the ACL is read", func(t *testing.T) {
		previous, err := accountdata.NewRandom()
		require.NoError(t, err)
		handed := newNetworkSpace(t, network, previous, space.SpaceTypeOrganization, "")
		member := handed.addMember(t, list.AclPermissionsWriter).Account()
		coord.put(handed.id, coordSpace{})
		taken := serveAcl(t, net, coord, handed, false)
		net.nodes.serveHeaders(handed.storage.SpaceHeaderWithId)
		net.nodes.set(func() {
			read := net.nodes.get
			net.nodes.get = func(spaceId string) ([]*consensusproto.RawRecordWithId, error) {
				recs, err := read(spaceId)
				handed.handTo(t, account.SignKey.GetPublic())
				return recs, err
			}
		})

		err = spaces.ChangeChildOwner(ctx, handed.id, member, space.PermissionAdmin)
		require.ErrorIs(t, err, list.ErrIncorrectRecordSequence)
		require.Zero(t, *taken, "no ACL record")
		assert.True(t, handed.ownerAcl(t).AclState().Permissions(account.SignKey.GetPublic()).IsOwner())
	})

	// The account's own child: its header names the parent.
	for _, row := range rows {
		t.Run("own child, "+row, func(t *testing.T) {
			handed, deleted := ownChild(t), ownChild(t)
			member := handed.addMember(t, list.AclPermissionsWriter).Account()
			track(t, row, handed)
			track(t, row, deleted)
			coord.put(handed.id, child)
			coord.put(deleted.id, child)
			net.nodes.serveHeaders(handed.storage.SpaceHeaderWithId, deleted.storage.SpaceHeaderWithId)

			taken := serveAcl(t, net, coord, handed, true)
			require.NoError(t, spaces.ChangeChildOwner(ctx, handed.id, member, space.PermissionAdmin))
			assert.Equal(t, 1, *taken)
			require.NoError(t, spaces.DeleteChild(ctx, deleted.id))
			assert.Equal(t, 1, net.coord.deletesOf(deleted.id))
			if row == untypedRow {
				assert.Equal(t, space.StatusDeleted, spaceInfoOf(t, ctx, sdk, deleted.id).Status)
			}
		})
	}

	// No header tells that the account's own child is one: the nodes are
	// out of reach, none of them holds the space, or one serves the
	// header of another space.
	t.Run("own child, no header", func(t *testing.T) {
		own := ownChild(t)
		member := own.addMember(t, list.AclPermissionsWriter).Account()
		coord.put(own.id, child)
		taken := serveAcl(t, net, coord, own, true)
		both := func(want error) {
			t.Helper()
			require.ErrorIs(t, spaces.DeleteChild(ctx, own.id), want)
			require.ErrorIs(t, spaces.ChangeChildOwner(ctx, own.id, member, space.PermissionAdmin), want)
		}
		net.nodes.answerHeaders(nil)
		both(space.ErrOffline)
		// On no node the space has no ACL either: ChangeChildOwner has
		// no owner to read, and DeleteChild no header.
		net.nodes.serveHeaders()
		both(space.ErrSpaceUnknown)

		other := ownChild(t).storage.SpaceHeaderWithId
		net.nodes.answerHeaders(func(string) (*spacesyncproto.RawSpaceHeaderWithId, error) { return other, nil })
		require.Error(t, spaces.DeleteChild(ctx, own.id))
		require.Error(t, spaces.ChangeChildOwner(ctx, own.id, member, space.PermissionAdmin))
		requireNothingSent(t, own, taken)
	})

	// The account's own space in deletion or deleted, a child or a
	// top-level one: no header is read and nothing is sent. The row the
	// account holds of it becomes a deleted one.
	noHeader := func(string) (*spacesyncproto.RawSpaceHeaderWithId, error) {
		t.Error("a header was read")
		return nil, spacesyncproto.ErrSpaceMissing
	}
	for _, status := range inDeletion {
		for _, own := range []struct {
			name  string
			build func(t *testing.T) *networkSpace
			coord coordSpace
		}{
			{"own child", ownChild, child},
			{"own top-level space", func(t *testing.T) *networkSpace {
				return newNetworkSpace(t, network, account, space.SpaceTypeAny, "")
			}, coordSpace{owned: true}},
		} {
			for _, row := range rows {
				t.Run(own.name+", "+status.String()+", "+row, func(t *testing.T) {
					sp := own.build(t)
					member := sp.addMember(t, list.AclPermissionsWriter).Account()
					track(t, row, sp)
					held := own.coord
					held.status = status
					coord.put(sp.id, held)
					taken := serveAcl(t, net, coord, sp, own.coord.parentOwned)
					net.nodes.answerHeaders(noHeader)

					require.ErrorIs(t, spaces.ChangeChildOwner(ctx, sp.id, member, space.PermissionAdmin), space.ErrSpaceDeleted)
					require.NoError(t, spaces.DeleteChild(ctx, sp.id))
					requireNothingSent(t, sp, taken)
					if row == untypedRow {
						assert.Equal(t, space.StatusDeleted, spaceInfoOf(t, ctx, sdk, sp.id).Status)
					} else {
						requireNothingHeld(t, ctx, sdk, sp.id)
					}
				})
			}
		}
	}

	// A status this SDK does not know is no deletion: DeleteChild sends
	// nothing, and the row stays.
	t.Run("own child, unknown status", func(t *testing.T) {
		own := ownChild(t)
		require.NoError(t, spaces.Track(ctx, own.id))
		unknown := child
		unknown.status = 99
		coord.put(own.id, unknown)
		net.nodes.serveHeaders(own.storage.SpaceHeaderWithId)

		require.Error(t, spaces.DeleteChild(ctx, own.id))
		require.Zero(t, net.coord.deletesOf(own.id), "no signed delete")
		rec, held := sdk.tsp.Get(ctx, own.id)
		require.True(t, held)
		assert.False(t, rec.IsDeleted())
	})
}

// A space the coordinator holds as another account's is the
// coordinator's to guard against a delete, and an ACL that names
// another account the owner guards itself, with a typed row or without.
// DeleteChild sends the delete in every status of the space, so the
// Owner of the parent takes over a deletion the child's owner made, and
// ChangeChildOwner posts its record. No header is read.
func TestSDK_DeleteChild_AndChangeChildOwner_SpaceOfAnotherAccountWithNoTypedRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	net, coord, network, sdk := openOnCoordinatorModel(t, ctx)
	net.nodes.answerHeaders(func(string) (*spacesyncproto.RawSpaceHeaderWithId, error) {
		t.Error("a header was read")
		return nil, spacesyncproto.ErrSpaceMissing
	})
	spaces := sdk.Spaces()
	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
	require.NoError(t, err)

	const (
		pending = coordinatorproto.SpaceStatus_SpaceStatusPendingDeletion
		deleted = coordinatorproto.SpaceStatus_SpaceStatusDeleted
	)
	childOfOwn := coordSpace{parent: true, parentOwned: true}

	t.Run("child of the account's organization", func(t *testing.T) {
		child := newForeignChild(t, network, parent.Id())
		member := child.addMember(t, list.AclPermissionsWriter).Account()
		coord.put(child.id, childOfOwn)
		taken := serveAcl(t, net, coord, child, true)

		require.NoError(t, spaces.ChangeChildOwner(ctx, child.id, member, space.PermissionAdmin))
		assert.Equal(t, 1, *taken)
		require.NoError(t, spaces.DeleteChild(ctx, child.id))
		assert.Equal(t, coordSpace{status: pending, parent: true, parentOwned: true, byParentOwner: true}, coord.get(child.id))
		requireNothingHeld(t, ctx, sdk, child.id)
	})

	// The Owner of the parent takes over a deletion the child's owner
	// made: the delete reaches the coordinator, and the deletion becomes
	// the Owner's.
	for _, row := range []string{"no row", "a row with no type"} {
		t.Run("child in deletion by its owner, "+row, func(t *testing.T) {
			child := newForeignChild(t, network, parent.Id())
			member := child.addMember(t, list.AclPermissionsWriter).Account()
			if row != "no row" {
				require.NoError(t, spaces.Track(ctx, child.id))
			}
			inDeletion := childOfOwn
			inDeletion.status = pending
			coord.put(child.id, inDeletion)
			taken := serveAcl(t, net, coord, child, true)

			require.NoError(t, spaces.DeleteChild(ctx, child.id))
			assert.True(t, coord.get(child.id).byParentOwner, "the deletion is the one of the Owner of the parent")
			require.NoError(t, spaces.DeleteChild(ctx, child.id), "the deletion is its own already")
			inDeletion.status = deleted
			coord.put(child.id, inDeletion)
			require.NoError(t, spaces.DeleteChild(ctx, child.id), "deleted")
			assert.Equal(t, 3, net.coord.deletesOf(child.id), "each was sent")

			// ChangeChildOwner reads the coordinator's answer off the
			// ACL.
			require.ErrorIs(t, spaces.ChangeChildOwner(ctx, child.id, member, space.PermissionAdmin), space.ErrSpaceDeleted)
			assert.Zero(t, *taken)
		})
	}

	t.Run("child of another account's organization", func(t *testing.T) {
		child := newForeignChild(t, network, foreignSpaceA)
		coord.put(child.id, coordSpace{parent: true})
		require.ErrorIs(t, spaces.DeleteChild(ctx, child.id), space.ErrParentPermissionDenied)
		assert.Equal(t, coordSpace{parent: true}, coord.get(child.id))
	})

	// A top-level space: the coordinator refuses the delete in the
	// status the space has, and the ACL the change of the owner.
	t.Run("top-level space", func(t *testing.T) {
		owner, err := accountdata.NewRandom()
		require.NoError(t, err)
		foreign := newNetworkSpace(t, network, owner, space.SpaceTypeOrganization, "")
		member := foreign.addMember(t, list.AclPermissionsWriter).Account()
		coord.put(foreign.id, coordSpace{})
		taken := serveAcl(t, net, coord, foreign, false)

		err = spaces.DeleteChild(ctx, foreign.id)
		require.ErrorIs(t, err, space.ErrNotChildSpace)
		require.ErrorIs(t, err, coordinatorproto.ErrSpaceIsCreated, "the coordinator's answer stays in the chain")
		require.Error(t, spaces.ChangeChildOwner(ctx, foreign.id, member, space.PermissionAdmin))
		assert.Zero(t, *taken)
		for _, status := range []coordinatorproto.SpaceStatus{pending, deleted} {
			coord.put(foreign.id, coordSpace{status: status})
			require.NoError(t, spaces.DeleteChild(ctx, foreign.id), status.String())
			assert.Equal(t, coordSpace{status: status}, coord.get(foreign.id), "the space is as it was")
		}
		requireNothingHeld(t, ctx, sdk, foreign.id)
	})

	t.Run("space the coordinator does not know", func(t *testing.T) {
		unknown := foreignChildId(t, parent.Id())
		net.nodes.set(func() {
			net.nodes.get = func(spaceId string) ([]*consensusproto.RawRecordWithId, error) {
				return nil, coord.acl(spaceId)
			}
		})
		require.ErrorIs(t, spaces.DeleteChild(ctx, unknown), space.ErrSpaceUnknown)
		require.ErrorIs(t, spaces.ChangeChildOwner(ctx, unknown, randomIdentity(t), space.PermissionAdmin), space.ErrSpaceUnknown)
		assert.Zero(t, net.coord.deletesOf(unknown))
	})

	// A status this SDK does not know is no deletion, and DeleteChild
	// sends nothing.
	t.Run("unknown status", func(t *testing.T) {
		child := newForeignChild(t, network, parent.Id())
		unknown := childOfOwn
		unknown.status = 99
		coord.put(child.id, unknown)

		require.Error(t, spaces.DeleteChild(ctx, child.id))
		assert.Zero(t, net.coord.deletesOf(child.id))
	})

	// The account's own 1-1 space: the coordinator reports no account as
	// the owner of one and refuses its delete, and the ACL names neither
	// account its owner.
	t.Run("one-to-one space of the account", func(t *testing.T) {
		account := sdk.app.AccountKeys()
		peer, err := accountdata.NewRandom()
		require.NoError(t, err)
		payload, err := spacepayloads.StoragePayloadForOneToOneSpaceWithType(account.SignKey, peer.SignKey.GetPublic(), space.SpaceTypeOneToOne)
		require.NoError(t, err)
		oneToOne := networkSpaceOf(network, account, payload)
		coord.put(oneToOne.id, coordSpace{owned: true, oneToOne: true})
		taken := serveAcl(t, net, coord, oneToOne, false)

		require.ErrorIs(t, spaces.DeleteChild(ctx, oneToOne.id), coordinatorproto.ErrUnexpected)
		require.Error(t, spaces.ChangeChildOwner(ctx, oneToOne.id, peer.SignKey.GetPublic().Account(), space.PermissionAdmin))
		assert.Zero(t, *taken)
		assert.Equal(t, coordSpace{owned: true, oneToOne: true}, coord.get(oneToOne.id), "the space is as it was")
		requireNothingHeld(t, ctx, sdk, oneToOne.id)
	})
}

// A child the coordinator has signed and no node holds. The node that
// stores a space creates the log of its ACL, so the coordinator finds
// none: each call that reads the ACL answers ErrSpaceUnknown, for a
// child of another account and for the account's own with no row here.
// The calls go on once a node holds the child.
func TestSDK_ChildCalls_ChildOnNoNode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	net, coord, network, sdk := openOnCoordinatorModel(t, ctx)
	spaces := sdk.Spaces()
	account := sdk.app.AccountKeys()
	another, err := accountdata.NewRandom()
	require.NoError(t, err)
	parent := newNetworkSpace(t, network, account, space.SpaceTypeOrganization, "")

	for _, tc := range []struct {
		name  string
		owner *accountdata.AccountKeys
	}{
		{"child of another account", another},
		{"the account's own child", account},
	} {
		t.Run(tc.name, func(t *testing.T) {
			child := newNetworkSpace(t, network, tc.owner, space.SpaceTypeOrganizationSpace, parent.id)
			member := child.addMember(t, list.AclPermissionsWriter).Account()
			coord.put(child.id, coordSpace{owned: tc.owner == account, parent: true, parentOwned: true})
			taken := serveAcl(t, net, coord, child, true)

			net.nodes.serveHeaders()
			_, err := spaces.ChildMembers(ctx, child.id)
			require.ErrorIs(t, err, space.ErrSpaceUnknown)
			require.ErrorIs(t, err, consensuserr.ErrLogNotFound, "the coordinator's answer stays in the chain")
			_, err = spaces.ChildRotationPending(ctx, child.id)
			require.ErrorIs(t, err, space.ErrSpaceUnknown)
			require.ErrorIs(t, spaces.RemoveChildMembers(ctx, child.id, []string{member}), space.ErrSpaceUnknown)
			require.ErrorIs(t, spaces.ChangeChildOwner(ctx, child.id, member, space.PermissionAdmin), space.ErrSpaceUnknown)
			require.Zero(t, *taken, "no ACL record")

			net.nodes.serveHeaders(child.storage.SpaceHeaderWithId)
			members, err := spaces.ChildMembers(ctx, child.id)
			require.NoError(t, err)
			assert.Len(t, members, 2)
			require.NoError(t, spaces.ChangeChildOwner(ctx, child.id, member, space.PermissionAdmin))
			assert.Equal(t, 1, *taken)
			requireNothingHeld(t, ctx, sdk, child.id)
		})
	}
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

	parent, err := spaces.Create(ctx, space.CreateRequest{Name: "Parent", SpaceType: space.SpaceTypeOrganization})
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

// A refused child is on no node, so it is shared with nobody. The
// sharing calls ask the coordinator once and, when it does not know the
// child, answer with the error of the recorded refusal: they do not
// wait for a push. The row alone is not trusted: for a child the
// coordinator knows the call goes on, whatever the row still holds.
func TestSDK_ChildSpace_SharingARefusedChild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	net := installFakeNetwork(t)
	var (
		makeShareable atomic.Int32
		known         atomic.Bool
	)
	answer := func(string) error {
		makeShareable.Add(1)
		if known.Load() {
			return nil
		}
		return coordinatorproto.ErrSpaceNotExists
	}
	net.coord.shareable = answer
	dataDir := t.TempDir()
	cfg := childTestConfig(t, dataDir)
	network := newTestNetworkKey(t, &cfg)
	sdk, err := Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	require.NoError(t, spaces.Track(ctx, foreignSpaceA))
	child, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: foreignSpaceA})
	require.NoError(t, err)
	share := map[string]func(context.Context) error{
		"CreateInvite": func(ctx context.Context) error {
			_, err := child.ACL().CreateInvite(ctx)
			return err
		},
		"AddAccounts": func(ctx context.Context) error {
			return child.ACL().AddAccounts(ctx, []space.MemberAdd{{Identity: randomIdentity(t), Permission: space.PermissionWriter}})
		},
	}

	for _, tc := range []struct {
		answer error
		want   error
	}{
		{coordinatorproto.ErrParentSpaceNotAvailable, space.ErrParentSpaceNotAvailable},
		{coordinatorproto.ErrParentSpaceInvalid, space.ErrParentSpaceInvalid},
		{coordinatorproto.ErrParentPermissionDenied, space.ErrParentPermissionDenied},
		{coordinatorproto.ErrChildSpacesLimitReached, space.ErrChildSpacesLimitReached},
	} {
		net.coord.set(func() { net.coord.sign = func() error { return tc.answer } })
		require.ErrorIs(t, spaces.RetryPush(ctx, child.Id()), tc.want)
		for name, call := range share {
			asked := makeShareable.Load()
			// Well inside the wait for a first push.
			noWait, stop := context.WithTimeout(ctx, 5*time.Second)
			require.ErrorIs(t, call(noWait), tc.want, "%s on a child refused for %v", name, tc.want)
			stop()
			assert.Equal(t, asked+1, makeShareable.Load(), "%s asks the coordinator once", name)
		}
	}

	// Offline the row is not read: the call fails as on any space.
	net.coord.set(func() { net.coord.shareable = nil })
	err = share["CreateInvite"](ctx)
	require.Error(t, err)
	require.NotErrorIs(t, err, space.ErrChildSpacesLimitReached)
	net.coord.set(func() { net.coord.shareable = answer })

	// Another device got the child signed, and the refusal of this one
	// reached the row after that: the coordinator knows the child, and
	// the call goes on.
	known.Store(true)
	net.nodes.set(func() {
		net.nodes.add = func(_ string, rec *consensusproto.RawRecord) (*consensusproto.RawRecordWithId, error) {
			return network.accept(rec), nil
		}
	})
	invite, err := child.ACL().CreateInvite(ctx)
	require.NoError(t, err)
	assert.Equal(t, child.Id(), invite.SpaceId)
	assert.Equal(t, space.RefusalChildLimitReached, child.Info().Refusal, "the row still holds the refusal")
}

// A space the coordinator does not know yet and that holds no refusal
// is waited for: its first push is on the way. A wait that runs out
// reads ErrSpaceUnknown, for a top-level space and for a child, and a
// refusal that lands during the wait ends it with its error.
func TestSDK_Sharing_WaitsForTheFirstPush(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	coord := &installFakeNetwork(t).coord
	var makeShareable atomic.Int32
	coord.shareable = func(string) error {
		makeShareable.Add(1)
		return coordinatorproto.ErrSpaceNotExists
	}
	sdk := openChildTestSDK(t, ctx, t.TempDir())
	defer func() { _ = sdk.Close() }()
	spaces := sdk.Spaces()

	top, err := spaces.Create(ctx, space.CreateRequest{Name: "Top-level"})
	require.NoError(t, err)
	require.NoError(t, spaces.Track(ctx, foreignSpaceA))
	child, err := spaces.CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: foreignSpaceA})
	require.NoError(t, err)

	restore := spaceimpl.SetMakeShareableBackoffForTest(time.Millisecond)
	t.Cleanup(restore)
	for _, sp := range []space.Space{top, child} {
		asked := makeShareable.Load()
		_, err := sp.ACL().CreateInvite(ctx)
		require.ErrorIs(t, err, space.ErrSpaceUnknown, sp.Info().Name)
		assert.Greater(t, makeShareable.Load(), asked+1, "the request is repeated while the call waits")
	}
	restore()

	done := make(chan error, 1)
	go func() {
		_, err := child.ACL().CreateInvite(ctx)
		done <- err
	}()
	coord.set(func() { coord.sign = func() error { return coordinatorproto.ErrChildSpacesLimitReached } })
	require.ErrorIs(t, spaces.RetryPush(ctx, child.Id()), space.ErrChildSpacesLimitReached)
	select {
	case err := <-done:
		require.ErrorIs(t, err, space.ErrChildSpacesLimitReached)
	case <-time.After(10 * time.Second):
		t.Fatal("the call still waits for a push of a refused child")
	}
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
// watcher that rotates, with no Get of the space. The row tells the
// child by its parent id, and by its type when it names no parent.
func TestSDK_ChildSpace_RotationAfterRestartWithoutGet(t *testing.T) {
	t.Run("the row names the parent", func(t *testing.T) { rotationAfterRestartWithoutGet(t, foreignSpaceA) })
	t.Run("the row has the type alone", func(t *testing.T) { rotationAfterRestartWithoutGet(t, "") })
}

func rotationAfterRestartWithoutGet(t *testing.T, rowParent string) {
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
	acl := child.heldBy(t, ctx, sdk, rowParent, list.AclPermissionsAdmin)
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
