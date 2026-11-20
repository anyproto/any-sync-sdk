// Contract tests for the request builders: each test REPLICATES the
// push server's verification code (anytype-push-server) against the
// bytes our builders produce — that replication IS the contract, since
// an in-process end-to-end run would need the full secure-channel
// handshake. The DRPC transport (client.go) stays a thin untested
// shim, like the files broker.

package pushclient

import (
	"testing"

	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pushTestKeys is the full key set one space's push traffic needs:
// account signing key, the ACL-derived push space key, the read-key-
// derived payload enc key.
type pushTestKeys struct {
	account  crypto.PrivKey
	spaceKey crypto.PrivKey
	encKey   crypto.SymKey
}

func newPushTestKeys(t *testing.T) pushTestKeys {
	t.Helper()
	spaceKey, err := DeriveSpaceKey(testPrivKey(t, 0x11))
	require.NoError(t, err)
	encKey, err := DeriveEncKey(testSymKey(t, 0x12))
	require.NoError(t, err)
	return pushTestKeys{
		account:  testPrivKey(t, 0x10),
		spaceKey: spaceKey,
		encKey:   encKey,
	}
}

// Server check (subscribe/notify path): for every topic,
// UnmarshalEd25519PublicKey(SpaceKey).Verify([]byte(Topic), Signature).
func TestBuildTopics_ServerVerification(t *testing.T) {
	keys := newPushTestKeys(t)
	names := []string{"chats", "chats/deadbeef", keys.account.GetPublic().Account()}

	topics, err := buildTopics(keys.spaceKey, names)
	require.NoError(t, err)
	require.Len(t, topics.Topics, len(names))

	for i, tp := range topics.Topics {
		assert.Equal(t, names[i], tp.Topic)

		pub, err := crypto.UnmarshalEd25519PublicKey(tp.SpaceKey)
		require.NoError(t, err, "SpaceKey must be a RAW ed25519 public key")
		ok, err := pub.Verify([]byte(tp.Topic), tp.Signature)
		require.NoError(t, err)
		assert.True(t, ok, "server verification must pass for topic %q", tp.Topic)

		// The signature must be over the topic STRING — a swapped topic
		// must not verify.
		ok, err = pub.Verify([]byte(tp.Topic+"-tampered"), tp.Signature)
		require.NoError(t, err)
		assert.False(t, ok, "tampered topic must fail verification")
	}
}

// Server check (CreateSpace/RemoveSpace):
// UnmarshalEd25519PublicKey(SpaceKey).Verify([]byte(identity),
// AccountSignature), where identity is the account the server read off
// the secure channel.
func TestBuildSpaceRequest_ServerVerification(t *testing.T) {
	keys := newPushTestKeys(t)
	identity := keys.account.GetPublic().Account()

	keyRaw, accountSig, err := buildSpaceRequest(keys.spaceKey, identity)
	require.NoError(t, err)

	pub, err := crypto.UnmarshalEd25519PublicKey(keyRaw)
	require.NoError(t, err, "SpaceKey must be a RAW ed25519 public key")
	ok, err := pub.Verify([]byte(identity), accountSig)
	require.NoError(t, err)
	assert.True(t, ok, "server verification must pass")

	// The signature binds THIS identity — another account presenting
	// the same request must fail.
	ok, err = pub.Verify([]byte("someOtherIdentity"), accountSig)
	require.NoError(t, err)
	assert.False(t, ok, "signature must not verify for a different identity")
}

// Server check (Notify): accountPubKey.Verify(Payload, Signature) —
// over the CIPHERTEXT. Receiver check: KeyId selects the enc key,
// Decrypt(Payload) restores the cleartext.
func TestBuildMessage_ServerVerification(t *testing.T) {
	keys := newPushTestKeys(t)
	cleartext := []byte(`{"type":"chat_message","text":"hi"}`)

	msg, err := buildMessage(keys.account, keys.encKey, cleartext)
	require.NoError(t, err)

	// Server side: signature over the ciphertext, against the dialer.
	ok, err := keys.account.GetPublic().Verify(msg.Payload, msg.Signature)
	require.NoError(t, err)
	assert.True(t, ok, "server verification must pass")

	// The signature is over the CIPHERTEXT, not the cleartext.
	ok, err = keys.account.GetPublic().Verify(cleartext, msg.Signature)
	require.NoError(t, err)
	assert.False(t, ok, "signature must not be over the cleartext")

	// Receiver side: KeyId resolves the key, Decrypt restores the body.
	wantKeyId, err := EncKeyId(keys.encKey)
	require.NoError(t, err)
	assert.Equal(t, wantKeyId, msg.KeyId)

	got, err := keys.encKey.Decrypt(msg.Payload)
	require.NoError(t, err)
	assert.Equal(t, cleartext, got)

	// And the payload actually is ciphertext.
	assert.NotEqual(t, cleartext, msg.Payload)
}
