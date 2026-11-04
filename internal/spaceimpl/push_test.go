package spaceimpl

import (
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/recordverifier"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/pushclient"
)

// testAcl builds an in-memory ACL from a single root record. A root
// without a read key change is the derived shape.
func testAcl(t *testing.T, keys *accountdata.AccountKeys, spaceId string, change list.ReadKeyChangePayload) list.AclList {
	t.Helper()
	master, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	builder := list.NewAclRecordBuilder("", crypto.NewKeyStorage(), keys, recordverifier.NewValidateFull())
	root, err := builder.BuildRoot(list.RootContent{
		PrivKey:   keys.SignKey,
		MasterKey: master,
		SpaceId:   spaceId,
		Change:    change,
	})
	require.NoError(t, err)
	st, err := list.NewInMemoryStorage(root.Id, []*consensusproto.RawRecordWithId{root})
	require.NoError(t, err)
	acl, err := list.BuildAclListWithIdentity(keys, st, recordverifier.NewValidateFull())
	require.NoError(t, err)
	return acl
}

func derivePushSpaceKey(t *testing.T, acl list.AclList, spaceId string) crypto.PrivKey {
	t.Helper()
	acl.RLock()
	derive, err := pushSpaceKeySource(acl, spaceId)
	acl.RUnlock()
	require.NoError(t, err)
	key, err := derive()
	require.NoError(t, err)
	return key
}

func TestPushSpaceKeySource_DerivedAcl(t *testing.T) {
	keys, err := accountdata.NewRandom()
	require.NoError(t, err)
	acl := testAcl(t, keys, "derived-a", list.ReadKeyChangePayload{})

	_, fmErr := acl.AclState().FirstMetadataKey()
	require.ErrorIs(t, fmErr, list.ErrNoMetadataKey, "a derived root carries no metadata key")

	got := derivePushSpaceKey(t, acl, "derived-a")

	rootReadKey := acl.AclState().Keys()[acl.Id()].ReadKey
	require.NotNil(t, rootReadKey)
	want, err := pushclient.DeriveDerivedSpaceKey(rootReadKey, "derived-a")
	require.NoError(t, err)
	assert.True(t, want.Equals(got))

	// Same account, another derived space: same root read key, a
	// different push identity.
	other := derivePushSpaceKey(t, testAcl(t, keys, "derived-b", list.ReadKeyChangePayload{}), "derived-b")
	assert.False(t, got.Equals(other))
}

func TestPushSpaceKeySource_RegularAcl(t *testing.T) {
	keys, err := accountdata.NewRandom()
	require.NoError(t, err)
	metaKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	acl := testAcl(t, keys, "regular", list.ReadKeyChangePayload{
		MetadataKey: metaKey,
		ReadKey:     crypto.NewAES(),
	})

	got := derivePushSpaceKey(t, acl, "regular")

	want, err := pushclient.DeriveSpaceKey(metaKey)
	require.NoError(t, err)
	assert.True(t, want.Equals(got), "a regular space keeps the first-metadata-key derivation")
}
