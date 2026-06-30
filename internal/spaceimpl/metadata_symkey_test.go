package spaceimpl

import (
	"crypto/rand"
	"testing"

	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// encodeSelfSymKeyMetadata must produce bytes that round-trip back to the
// account's derived metadata symkey — this is the value that lands
// (encrypted at rest by any-sync) in the ACL RequestMetadata and the 1-1
// invite body, and that the receiver caches and uses to decrypt the
// sender's identityRepo profile.
func TestEncodeSelfSymKeyMetadata_RoundTrip(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	blob, err := encodeSelfSymKeyMetadata(priv)
	require.NoError(t, err)
	require.NotEmpty(t, blob)

	// The receiver recovers the symkey from the (decrypted) blob bytes...
	got, err := space.UnmarshalSymKey(string(blob))
	require.NoError(t, err)

	// ...and it equals the sender's deterministically derived key.
	want, err := space.DeriveAccountMetadataSymKey(priv)
	require.NoError(t, err)
	assert.True(t, crypto.KeyEquals(want, got), "ACL/invite symkey must round-trip to the account's derived key")
}
