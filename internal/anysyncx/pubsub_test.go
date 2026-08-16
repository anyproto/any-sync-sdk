package anysyncx

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/payloads"
)

func fixedReadKey(t *testing.T) crypto.SymKey {
	t.Helper()
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = byte(i)
	}
	key, err := crypto.UnmarshallAESKey(raw)
	require.NoError(t, err)
	return key
}

// TestPubSubCrypto_DeriveVector pins the pubsub key derivation: same
// read key → same derived key across instances, domain-separated from
// both the raw read key and the payloads derivation. The golden hex
// pins pubsubEncKeyPath as a wire-compat constant — if this test
// breaks, deployed peers can no longer decrypt each other.
func TestPubSubCrypto_DeriveVector(t *testing.T) {
	readKey := fixedReadKey(t)

	a := &pubsubCrypto{}
	b := &pubsubCrypto{}
	ka, err := a.derived("kid1", readKey)
	require.NoError(t, err)
	kb, err := b.derived("kid1", readKey)
	require.NoError(t, err)

	rawA, err := ka.Raw()
	require.NoError(t, err)
	rawB, err := kb.Raw()
	require.NoError(t, err)
	assert.Equal(t, rawA, rawB, "derivation must be deterministic")

	rawRead, err := readKey.Raw()
	require.NoError(t, err)
	assert.NotEqual(t, rawRead, rawA, "derived key must differ from the raw read key")

	pk, err := payloads.DeriveEncKey(readKey)
	require.NoError(t, err)
	rawP, err := pk.Raw()
	require.NoError(t, err)
	assert.NotEqual(t, rawP, rawA, "pubsub and payloads derivations must be domain-separated")

	const golden = "a1e6b27f690a287bad6b0a6b61afaf42d1bbe796919b8e199a9a65537c514555"
	assert.Equal(t, golden, hex.EncodeToString(rawA),
		"pubsubEncKeyPath changed — this breaks decryption between deployed peers")
}

// TestPubSubCrypto_DecryptCacheHit covers the receive-path fast path:
// a kid already derived decrypts without touching the App/ACL at all.
func TestPubSubCrypto_DecryptCacheHit(t *testing.T) {
	readKey := fixedReadKey(t)
	c := &pubsubCrypto{} // app nil on purpose: cache hit must not need it
	key, err := c.derived("kid1", readKey)
	require.NoError(t, err)

	enc, err := key.Encrypt([]byte("payload"))
	require.NoError(t, err)
	dec, err := c.Decrypt("space1", "kid1", enc)
	require.NoError(t, err)
	assert.Equal(t, []byte("payload"), dec)

	// Unknown kid with no app wired errors instead of panicking.
	_, err = c.Decrypt("space1", "kid-unknown", enc)
	require.Error(t, err)
}

// TestSpacePeerManager_OnSubscribedHook: broadcastSubscribe fires the
// pubsub interest hook after each node-subscribe broadcast.
func TestSpacePeerManager_OnSubscribedHook(t *testing.T) {
	var got []string
	m := &spacePeerManager{
		spaceId:      "space1",
		onSubscribed: func(id string) { got = append(got, id) },
	}
	m.runCtx, m.runCancel = context.WithCancel(context.Background())
	defer m.runCancel()

	// Empty subscribeMsgRaw short-circuits KeepAlive, isolating the hook.
	m.broadcastSubscribe()
	m.broadcastSubscribe()
	assert.Equal(t, []string{"space1", "space1"}, got)
}
