package space_test

import (
	"crypto/rand"
	"errors"
	"testing"

	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// TestEncodeDecodeInvite_Roundtrip verifies the share-friendly invite
// token survives a round-trip through EncodeInvite/DecodeInvite.
func TestEncodeDecodeInvite_Roundtrip(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	original := space.Invite{
		SpaceId:   "abc.xyz",
		InviteKey: priv,
	}

	token, err := space.EncodeInvite(original)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	got, err := space.DecodeInvite(token)
	require.NoError(t, err)
	assert.Equal(t, original.SpaceId, got.SpaceId)

	// Compare the two private keys by their marshalled bytes — PrivKey
	// is an interface; same semantic key serializes identically.
	wantBytes, err := original.InviteKey.Marshall()
	require.NoError(t, err)
	gotBytes, err := got.InviteKey.Marshall()
	require.NoError(t, err)
	assert.Equal(t, wantBytes, gotBytes)
}

// TestDecodeInvite_RejectsMalformed exercises every branch of the
// validation logic so a future format bump can't silently accept old
// tokens.
func TestDecodeInvite_RejectsMalformed(t *testing.T) {
	_, err := space.DecodeInvite("")
	assert.ErrorIs(t, err, space.ErrInvalidInvite)

	_, err = space.DecodeInvite("not!base58!")
	assert.ErrorIs(t, err, space.ErrInvalidInvite)
}

// TestEncodeInvite_Validation rejects missing inputs at the boundary
// rather than producing an unusable token.
func TestEncodeInvite_Validation(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	_, err = space.EncodeInvite(space.Invite{InviteKey: priv})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SpaceId required")

	_, err = space.EncodeInvite(space.Invite{SpaceId: "abc"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InviteKey required")
}

// TestDecodeInvite_VersionMismatch confirms the version byte gate
// works — feed a clearly future-format payload, expect rejection.
func TestDecodeInvite_VersionMismatch(t *testing.T) {
	_, err := space.DecodeInvite("ZZZZ") // base58 of [0x4b, 0x4b, 0xed]; version 0x4b
	require.Error(t, err)
	assert.True(t, errors.Is(err, space.ErrInvalidInvite))
}
