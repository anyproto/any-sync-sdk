package space

import (
	"crypto/rand"
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

	meta := AccountMetadata{Name: "Alice", Description: "hi", IconCID: "cid123"}
	blob, err := EncryptProfile(meta, key)
	require.NoError(t, err)
	require.NotEmpty(t, blob)

	got, ok := DecryptProfile(blob, key)
	require.True(t, ok)
	assert.Equal(t, meta, got)
}

func TestEncryptProfile_EmptyIsNil(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	key, err := DeriveAccountMetadataSymKey(priv)
	require.NoError(t, err)

	blob, err := EncryptProfile(AccountMetadata{}, key)
	require.NoError(t, err)
	assert.Nil(t, blob, "empty profile must not push (would clobber)")
}

func TestDecryptProfile_NoKeyOrWrongKey(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	key, err := DeriveAccountMetadataSymKey(priv)
	require.NoError(t, err)
	blob, err := EncryptProfile(AccountMetadata{Name: "Alice"}, key)
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
