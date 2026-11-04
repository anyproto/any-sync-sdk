package pushclient

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"testing"

	"github.com/anyproto/any-sync/util/crypto"
	slip10 "github.com/anyproto/go-slip10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testPrivKey mints a deterministic ed25519 key from a fixed byte fill
// — same fill, same key on every run.
func testPrivKey(t *testing.T, fill byte) crypto.PrivKey {
	t.Helper()
	seed := bytes.Repeat([]byte{fill}, 32)
	key, _, err := crypto.GenerateEd25519Key(bytes.NewReader(seed))
	require.NoError(t, err)
	return key
}

// testSymKey mints a deterministic AES key from a fixed byte fill.
func testSymKey(t *testing.T, fill byte) crypto.SymKey {
	t.Helper()
	key, err := crypto.UnmarshallAESKey(bytes.Repeat([]byte{fill}, 32))
	require.NoError(t, err)
	return key
}

func rawKey(t *testing.T, k crypto.Key) []byte {
	t.Helper()
	raw, err := k.Raw()
	require.NoError(t, err)
	return raw
}

func TestDeriveSpaceKey_Deterministic(t *testing.T) {
	meta := testPrivKey(t, 0x01)

	k1, err := DeriveSpaceKey(meta)
	require.NoError(t, err)
	k2, err := DeriveSpaceKey(meta)
	require.NoError(t, err)
	assert.Equal(t, rawKey(t, k1), rawKey(t, k2), "same metadata key must derive the same push key")

	other, err := DeriveSpaceKey(testPrivKey(t, 0x02))
	require.NoError(t, err)
	assert.NotEqual(t, rawKey(t, k1), rawKey(t, other), "different metadata keys must derive different push keys")

	assert.NotEqual(t, rawKey(t, meta), rawKey(t, k1), "derived key must differ from its input")
}

// Pins the SLIP-10 path: an accidental edit to spaceKeyDerivationPath
// would silently re-key every space on the push server (heart and this
// SDK must derive identically). The inline re-derivation hardcodes the
// path literal on purpose.
func TestDeriveSpaceKey_MatchesInlineDerivation(t *testing.T) {
	meta := testPrivKey(t, 0x2a)

	got, err := DeriveSpaceKey(meta)
	require.NoError(t, err)

	node, err := slip10.DeriveForPath("m/99999'/1'", rawKey(t, meta))
	require.NoError(t, err)
	want, _, err := crypto.GenerateEd25519Key(bytes.NewReader(node.RawSeed()))
	require.NoError(t, err)

	assert.Equal(t, rawKey(t, want), rawKey(t, got))
}

func TestDeriveSpaceKey_NilInput(t *testing.T) {
	_, err := DeriveSpaceKey(nil)
	assert.Error(t, err)
}

// The account key is the same for every derived space of the account,
// so the space id alone must separate their push identities.
func TestDeriveDerivedSpaceKey_PerSpace(t *testing.T) {
	account := testPrivKey(t, 0x05)

	k1, err := DeriveDerivedSpaceKey(account, "space-a")
	require.NoError(t, err)
	k2, err := DeriveDerivedSpaceKey(account, "space-a")
	require.NoError(t, err)
	assert.Equal(t, rawKey(t, k1), rawKey(t, k2), "same account key and space id must derive the same push key")

	other, err := DeriveDerivedSpaceKey(account, "space-b")
	require.NoError(t, err)
	assert.NotEqual(t, rawKey(t, k1), rawKey(t, other), "derived spaces of one account must get different push keys")

	otherAccount, err := DeriveDerivedSpaceKey(testPrivKey(t, 0x06), "space-a")
	require.NoError(t, err)
	assert.NotEqual(t, rawKey(t, k1), rawKey(t, otherAccount), "different accounts must derive different push keys")
}

// Pins the SLIP-21 path: an edit would re-key every derived space on
// the push server. The inline re-derivation hardcodes the literal.
func TestDeriveDerivedSpaceKey_MatchesInlineDerivation(t *testing.T) {
	account := testPrivKey(t, 0x2c)
	const spaceId = "bafyreiexample.derived"

	got, err := DeriveDerivedSpaceKey(account, spaceId)
	require.NoError(t, err)

	seed, err := crypto.DeriveSymmetricKey(rawKey(t, account), "m/SLIP-0021/anysync-sdk/push/derived-space/"+spaceId)
	require.NoError(t, err)
	want, _, err := crypto.GenerateEd25519Key(bytes.NewReader(rawKey(t, seed)))
	require.NoError(t, err)

	assert.Equal(t, rawKey(t, want), rawKey(t, got))
}

// Known-answer vector (checked against an independent SLIP-21 +
// ed25519 computation): catches a change inside the any-sync derivation
// helpers, which the inline re-derivation above shares.
func TestDeriveDerivedSpaceKey_KnownAnswer(t *testing.T) {
	got, err := DeriveDerivedSpaceKey(testPrivKey(t, 0x2c), "bafyreiexample.derived")
	require.NoError(t, err)
	assert.Equal(t, "b88cb16ec07bad5dc746e04a49a2ce2a950089cc3bdb251fb8f96fa08ffd4180", hex.EncodeToString(rawKey(t, got.GetPublic())))
}

func TestDeriveDerivedSpaceKey_InvalidInput(t *testing.T) {
	_, err := DeriveDerivedSpaceKey(nil, "space-a")
	assert.Error(t, err)
	_, err = DeriveDerivedSpaceKey(testPrivKey(t, 0x07), "")
	assert.Error(t, err)
}

func TestDeriveEncKey_Deterministic(t *testing.T) {
	readKey := testSymKey(t, 0x03)

	k1, err := DeriveEncKey(readKey)
	require.NoError(t, err)
	k2, err := DeriveEncKey(readKey)
	require.NoError(t, err)
	assert.Equal(t, rawKey(t, k1), rawKey(t, k2), "same read key must derive the same enc key")

	other, err := DeriveEncKey(testSymKey(t, 0x04))
	require.NoError(t, err)
	assert.NotEqual(t, rawKey(t, k1), rawKey(t, other), "different read keys must derive different enc keys")

	assert.NotEqual(t, rawKey(t, readKey), rawKey(t, k1), "derived key must differ from its input")
}

// Pins the SLIP-21 path — same rationale as the space-key vector: the
// inline re-derivation hardcodes the literal.
func TestDeriveEncKey_MatchesInlineDerivation(t *testing.T) {
	readKey := testSymKey(t, 0x2b)

	got, err := DeriveEncKey(readKey)
	require.NoError(t, err)

	want, err := crypto.DeriveSymmetricKey(rawKey(t, readKey), "m/SLIP-0021/anytype/space/key")
	require.NoError(t, err)

	assert.Equal(t, rawKey(t, want), rawKey(t, got))
}

func TestDeriveEncKey_NilInput(t *testing.T) {
	_, err := DeriveEncKey(nil)
	assert.Error(t, err)
}

// EncKeyId is the receiver's key selector — pinned to
// hex(sha256(rawKey)) because heart computes the same id on its side.
func TestEncKeyId(t *testing.T) {
	key := testSymKey(t, 0x05)

	id, err := EncKeyId(key)
	require.NoError(t, err)

	sum := sha256.Sum256(rawKey(t, key))
	assert.Equal(t, hex.EncodeToString(sum[:]), id)

	again, err := EncKeyId(key)
	require.NoError(t, err)
	assert.Equal(t, id, again, "id must be stable")

	otherId, err := EncKeyId(testSymKey(t, 0x06))
	require.NoError(t, err)
	assert.NotEqual(t, id, otherId, "distinct keys must have distinct ids")

	_, err = EncKeyId(nil)
	assert.Error(t, err)
}

// EncodeSpaceKey / EncodeEncKey are the receiver-cache wire encodings
// (heart's spacePushNotificationKey / ...EncryptionKey details): the
// space key must round-trip through UnmarshalEd25519PrivateKeyProto,
// the enc key through UnmarshallAESKey, and EncKeyId must be the
// sha256 of the bytes EncodeEncKey carries — a client may recompute
// the cache id from the encoded key alone.
func TestEncodeKeys_RoundTrip(t *testing.T) {
	spaceKey := testPrivKey(t, 0x21)
	encKey := testSymKey(t, 0x42)

	skB64, err := EncodeSpaceKey(spaceKey)
	require.NoError(t, err)
	skRaw, err := base64.StdEncoding.DecodeString(skB64)
	require.NoError(t, err)
	skBack, err := crypto.UnmarshalEd25519PrivateKeyProto(skRaw)
	require.NoError(t, err)
	assert.True(t, spaceKey.Equals(skBack), "space key must round-trip through proto-marshal + base64")

	ekB64, err := EncodeEncKey(encKey)
	require.NoError(t, err)
	ekRaw, err := base64.StdEncoding.DecodeString(ekB64)
	require.NoError(t, err)
	assert.Equal(t, rawKey(t, encKey), ekRaw, "enc key encoding must be the raw AES bytes")

	keyId, err := EncKeyId(encKey)
	require.NoError(t, err)
	sum := sha256.Sum256(ekRaw)
	assert.Equal(t, hex.EncodeToString(sum[:]), keyId, "EncKeyId must be recomputable from the encoded key")

	_, err = EncodeSpaceKey(nil)
	assert.Error(t, err)
	_, err = EncodeEncKey(nil)
	assert.Error(t, err)
}
