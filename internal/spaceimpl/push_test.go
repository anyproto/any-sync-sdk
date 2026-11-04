package spaceimpl

import (
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/list/listtest"
	"github.com/anyproto/any-sync/commonspace/object/acl/recordverifier"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/pushclient"
)

// testAcl builds an in-memory ACL from a single root record, read as
// the identity `as`. A root without a read key change and without a
// SpaceId is the derive-V1 shape: every derived space of an account has
// this same root.
func testAcl(t *testing.T, owner, as *accountdata.AccountKeys, master crypto.PrivKey, change list.ReadKeyChangePayload) list.AclList {
	t.Helper()
	builder := list.NewAclRecordBuilder("", crypto.NewKeyStorage(), owner, recordverifier.NewValidateFull())
	root, err := builder.BuildRoot(list.RootContent{
		PrivKey:   owner.SignKey,
		MasterKey: master,
		Change:    change,
	})
	require.NoError(t, err)
	st, err := list.NewInMemoryStorage(root.Id, []*consensusproto.RawRecordWithId{root})
	require.NoError(t, err)
	acl, err := list.BuildAclListWithIdentity(as, st, recordverifier.NewValidateFull())
	require.NoError(t, err)
	return acl
}

func lockedPushSpaceKey(acl list.AclList, spaceId string) (crypto.PrivKey, error) {
	acl.RLock()
	defer acl.RUnlock()
	return pushSpaceKey(acl, spaceId)
}

func TestPushSpaceKey_DerivedAcl(t *testing.T) {
	keys, err := accountdata.NewRandom()
	require.NoError(t, err)
	// derive-V1 signs the root with the account key as the master key
	acl := testAcl(t, keys, keys, keys.SignKey, list.ReadKeyChangePayload{})

	_, fmErr := acl.AclState().FirstMetadataKey()
	require.ErrorIs(t, fmErr, list.ErrNoMetadataKey, "a derived root carries no metadata key")

	got, err := lockedPushSpaceKey(acl, "derived-a")
	require.NoError(t, err)

	// A derived root's read key is any-sync's account-derived key.
	signRaw, err := keys.SignKey.Raw()
	require.NoError(t, err)
	rootReadKey, err := crypto.DeriveSymmetricKey(signRaw, crypto.AnysyncSpacePath)
	require.NoError(t, err)
	want, err := pushclient.DeriveDerivedSpaceKey(rootReadKey, "derived-a")
	require.NoError(t, err)
	assert.True(t, want.Equals(got))

	// Another derived space of the account has the same root: only
	// the space id separates the push identities.
	other, err := lockedPushSpaceKey(acl, "derived-b")
	require.NoError(t, err)
	assert.False(t, got.Equals(other))

	// A read-key rotation leaves the push identity unchanged.
	metaKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	acl.Lock()
	rec, err := acl.RecordBuilder().BuildReadKeyChange(list.ReadKeyChangePayload{
		MetadataKey: metaKey,
		ReadKey:     crypto.NewAES(),
	})
	require.NoError(t, err)
	require.NoError(t, acl.AddRawRecord(listtest.WrapAclRecord(rec)))
	acl.Unlock()
	require.NotEqual(t, acl.Id(), acl.AclState().CurrentReadKeyId(), "the rotation must have applied")
	rotated, err := lockedPushSpaceKey(acl, "derived-a")
	require.NoError(t, err)
	assert.True(t, got.Equals(rotated), "the push space key is fixed for the space's life")
}

func TestPushSpaceKey_DerivedAclWithoutRootReadKey(t *testing.T) {
	owner, err := accountdata.NewRandom()
	require.NoError(t, err)
	tracker, err := accountdata.NewRandom()
	require.NoError(t, err)
	master, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	acl := testAcl(t, owner, tracker, master, list.ReadKeyChangePayload{})

	_, err = lockedPushSpaceKey(acl, "derived-a")
	assert.ErrorIs(t, err, list.ErrNoReadKey)
}

func TestPushSpaceKey_RegularAcl(t *testing.T) {
	keys, err := accountdata.NewRandom()
	require.NoError(t, err)
	master, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	metaKey, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	acl := testAcl(t, keys, keys, master, list.ReadKeyChangePayload{
		MetadataKey: metaKey,
		ReadKey:     crypto.NewAES(),
	})

	got, err := lockedPushSpaceKey(acl, "regular")
	require.NoError(t, err)

	want, err := pushclient.DeriveSpaceKey(metaKey)
	require.NoError(t, err)
	assert.True(t, want.Equals(got), "a regular space keeps the first-metadata-key derivation")
}
