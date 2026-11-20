package spaceimpl

import (
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/aclrecordproto"
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

	meta, err := pushclient.DeriveDerivedSpaceMetadataKey(keys.SignKey, "derived-a")
	require.NoError(t, err)
	want, err := pushclient.DeriveSpaceKey(meta)
	require.NoError(t, err)
	assert.True(t, want.Equals(got), "a derived space's key comes from a metadata key derived off the account key")

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

// Another account that tracks the derived space gets no push key: only
// the deriving account's devices can derive it.
func TestPushSpaceKey_DerivedAclOfAnotherAccount(t *testing.T) {
	owner, err := accountdata.NewRandom()
	require.NoError(t, err)
	tracker, err := accountdata.NewRandom()
	require.NoError(t, err)
	master, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	acl := testAcl(t, owner, tracker, master, list.ReadKeyChangePayload{})

	_, err = lockedPushSpaceKey(acl, "derived-a")
	assert.ErrorContains(t, err, "belongs to another account")
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

// A 1-1 space keeps the standard path: any-sync derives its metadata key
// from the two accounts' keys, so both participants get the same push
// space key, the one heart derives too.
func TestPushSpaceKey_OneToOneAcl(t *testing.T) {
	alice, err := accountdata.NewRandom()
	require.NoError(t, err)
	bob, err := accountdata.NewRandom()
	require.NoError(t, err)
	shared, err := crypto.GenerateSharedKey(alice.SignKey, bob.SignKey.GetPublic(), crypto.AnysyncOneToOneSpacePath)
	require.NoError(t, err)
	sharedPub, err := shared.GetPublic().Marshall()
	require.NoError(t, err)
	var writers [][]byte
	for _, k := range []*accountdata.AccountKeys{alice, bob} {
		w, err := k.SignKey.GetPublic().Marshall()
		require.NoError(t, err)
		writers = append(writers, w)
	}
	builder := list.NewAclRecordBuilder("", crypto.NewKeyStorage(), nil, recordverifier.NewValidateFull())
	root, err := builder.BuildOneToOneRoot(list.RootContent{PrivKey: shared, MasterKey: shared},
		&aclrecordproto.AclOneToOneInfo{Owner: sharedPub, Writers: writers})
	require.NoError(t, err)

	var keys []crypto.PrivKey
	for _, as := range []*accountdata.AccountKeys{alice, bob} {
		st, err := list.NewInMemoryStorage(root.Id, []*consensusproto.RawRecordWithId{root})
		require.NoError(t, err)
		acl, err := list.BuildAclListWithIdentity(as, st, recordverifier.NewValidateFull())
		require.NoError(t, err)
		got, err := lockedPushSpaceKey(acl, "one-to-one")
		require.NoError(t, err)
		meta, err := acl.AclState().FirstMetadataKey()
		require.NoError(t, err)
		want, err := pushclient.DeriveSpaceKey(meta)
		require.NoError(t, err)
		assert.True(t, want.Equals(got), "a 1-1 space derives from the ACL's first metadata key")
		keys = append(keys, got)
	}
	assert.True(t, keys[0].Equals(keys[1]), "both 1-1 participants must get the same push space key")
}
