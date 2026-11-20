package space

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"

	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeriveAccountMetadataSymKey_Deterministic(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	k1, err := DeriveAccountMetadataSymKey(priv)
	require.NoError(t, err)
	k2, err := DeriveAccountMetadataSymKey(priv)
	require.NoError(t, err)
	assert.True(t, crypto.KeyEquals(k1, k2), "same account key must derive the same symkey")

	// A different account derives a different key.
	other, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	k3, err := DeriveAccountMetadataSymKey(other)
	require.NoError(t, err)
	assert.False(t, crypto.KeyEquals(k1, k3), "distinct accounts must derive distinct symkeys")
}

func TestSymKeyMarshalRoundTrip(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	k, err := DeriveAccountMetadataSymKey(priv)
	require.NoError(t, err)

	s, err := MarshalSymKey(k)
	require.NoError(t, err)
	assert.NotEmpty(t, s)

	back, err := UnmarshalSymKey(s)
	require.NoError(t, err)
	assert.True(t, crypto.KeyEquals(k, back), "symkey must survive marshal/unmarshal")

	// The derived key actually encrypts/decrypts.
	ct, err := back.Encrypt([]byte("alice"))
	require.NoError(t, err)
	pt, err := k.Decrypt(ct)
	require.NoError(t, err)
	assert.Equal(t, "alice", string(pt))
}

func TestEncryptDecryptProfile_RoundTrip(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	key, err := DeriveAccountMetadataSymKey(priv)
	require.NoError(t, err)

	fileKey := make([]byte, iconKeySize)
	_, err = rand.Read(fileKey)
	require.NoError(t, err)
	p := Profile{
		AccountMetadata: AccountMetadata{Name: "Alice", Description: "hi", IconCID: "cid123"},
		IconSpaceId:     "techspace",
		IconKey:         EncodeIconKey(fileKey),
	}
	blob, err := EncryptProfile(p, key)
	require.NoError(t, err)
	require.NotEmpty(t, blob)

	got, ok := DecryptProfile(blob, key)
	require.True(t, ok)
	assert.Equal(t, p, got)
	assert.True(t, got.HasIcon())
	gotKey, ok := got.IconKeyBytes()
	require.True(t, ok)
	assert.Equal(t, fileKey, gotKey)
}

// The wire layout: three NUL-separated fields without an avatar (the
// pre-avatar record, byte for byte), five with one. A three-field
// record decodes with no avatar; fewer fields degrade as before.
func TestEncodeProfile_Layout(t *testing.T) {
	plain := Profile{AccountMetadata: AccountMetadata{Name: "Alice", Description: "hi", IconCID: "cid"}}
	assert.Equal(t, []byte("Alice\x00hi\x00cid"), EncodeProfile(plain))
	assert.Equal(t, plain, DecodeProfile(EncodeProfile(plain)))

	withIcon := plain
	withIcon.IconSpaceId, withIcon.IconKey = "sp", "a2V5"
	assert.Equal(t, []byte("Alice\x00hi\x00cid\x00sp\x00a2V5"), EncodeProfile(withIcon))
	assert.Equal(t, withIcon, DecodeProfile(EncodeProfile(withIcon)))

	// Clearing every field still produces a record: the push that
	// removes a name or an avatar must overwrite the stored one.
	assert.Equal(t, []byte("\x00\x00"), EncodeProfile(Profile{}))
	assert.Equal(t, Profile{}, DecodeProfile([]byte("\x00\x00")))

	assert.Equal(t, Profile{}, DecodeProfile(nil))
	assert.Equal(t, Profile{}, DecodeProfile([]byte("Alice")))
	assert.Equal(t, Profile{AccountMetadata: AccountMetadata{Name: "Alice"}}, DecodeProfile([]byte("Alice\x00hi")))

	// A malformed key is no key.
	_, ok := Profile{IconKey: "not-base64!"}.IconKeyBytes()
	assert.False(t, ok)
	_, ok = Profile{IconKey: EncodeIconKey([]byte("short"))}.IconKeyBytes()
	assert.False(t, ok)
	_, ok = Profile{}.IconKeyBytes()
	assert.False(t, ok)
}

func TestLimitIcon(t *testing.T) {
	under := make([]byte, MaxIconSize)
	got, err := io.ReadAll(LimitIcon(bytes.NewReader(under)))
	require.NoError(t, err)
	assert.Len(t, got, MaxIconSize)

	over := make([]byte, MaxIconSize+1)
	_, err = io.ReadAll(LimitIcon(bytes.NewReader(over)))
	assert.ErrorIs(t, err, ErrIconTooLarge)
}

func TestDecryptProfile_NoKeyOrWrongKey(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	key, err := DeriveAccountMetadataSymKey(priv)
	require.NoError(t, err)
	blob, err := EncryptProfile(Profile{AccountMetadata: AccountMetadata{Name: "Alice"}}, key)
	require.NoError(t, err)

	// No key → unresolved, never garbage.
	_, ok := DecryptProfile(blob, nil)
	assert.False(t, ok)

	// Wrong key → fails the GCM tag, not a garbage name.
	other, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	wrong, err := DeriveAccountMetadataSymKey(other)
	require.NoError(t, err)
	_, ok = DecryptProfile(blob, wrong)
	assert.False(t, ok)
}
