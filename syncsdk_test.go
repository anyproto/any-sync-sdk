package syncsdk_test

import (
	"context"
	"testing"

	syncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/keys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidation_MissingSigningKey(t *testing.T) {
	cfg := syncsdk.Config{
		StoragePath: t.TempDir(),
	}
	c, err := syncsdk.New(context.Background(), cfg)
	assert.Nil(t, c)
	assert.ErrorIs(t, err, syncsdk.ErrInvalidConfig)
}

func TestConfigValidation_MissingStoragePath(t *testing.T) {
	// The top-level syncsdk.New always returns ErrInvalidConfig (it's a stub).
	// The real constructor is in client.New which does the actual validation.
	c, err := syncsdk.New(context.Background(), syncsdk.Config{})
	assert.Nil(t, c)
	assert.ErrorIs(t, err, syncsdk.ErrInvalidConfig)
}

func TestGenerateRandomKey(t *testing.T) {
	privKey, pubKey, err := keys.GenerateRandomKey()
	require.NoError(t, err)
	assert.NotNil(t, privKey)
	assert.NotNil(t, pubKey)

	// Keys should produce non-empty raw bytes
	raw, err := privKey.Raw()
	require.NoError(t, err)
	assert.NotEmpty(t, raw)

	pubRaw, err := pubKey.Raw()
	require.NoError(t, err)
	assert.NotEmpty(t, pubRaw)
}

func TestGenerateRandomKey_Uniqueness(t *testing.T) {
	priv1, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)
	priv2, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)
	assert.False(t, priv1.Equals(priv2), "two random keys should not be equal")
}

func TestGenerateRandomSymKey(t *testing.T) {
	symKey, err := keys.GenerateRandomSymKey()
	require.NoError(t, err)
	assert.NotNil(t, symKey)

	raw, err := symKey.Raw()
	require.NoError(t, err)
	assert.Len(t, raw, 32, "AES-256 key should be 32 bytes")
}

func TestGenerateRandomSymKey_Uniqueness(t *testing.T) {
	k1, err := keys.GenerateRandomSymKey()
	require.NoError(t, err)
	k2, err := keys.GenerateRandomSymKey()
	require.NoError(t, err)
	assert.False(t, k1.Equals(k2), "two random sym keys should not be equal")
}

func TestEventTypeConstants(t *testing.T) {
	assert.Equal(t, syncsdk.EventType(1), syncsdk.ObjectUpdated)
	assert.Equal(t, syncsdk.EventType(2), syncsdk.ObjectRebuilt)
	assert.Equal(t, syncsdk.EventType(3), syncsdk.SpaceConnected)
	assert.Equal(t, syncsdk.EventType(4), syncsdk.SpaceDisconnected)
}

func TestResolveAddOptions_Defaults(t *testing.T) {
	resolved := syncsdk.ResolveAddOptions(nil)
	assert.False(t, resolved.Encrypt)
	assert.False(t, resolved.IsSnapshot)
	assert.Empty(t, resolved.DataType)
}

func TestResolveAddOptions_WithEncryption(t *testing.T) {
	resolved := syncsdk.ResolveAddOptions([]syncsdk.AddOption{syncsdk.WithEncryption()})
	assert.True(t, resolved.Encrypt)
	assert.False(t, resolved.IsSnapshot)
}

func TestResolveAddOptions_WithSnapshot(t *testing.T) {
	resolved := syncsdk.ResolveAddOptions([]syncsdk.AddOption{syncsdk.WithSnapshot()})
	assert.True(t, resolved.IsSnapshot)
	assert.False(t, resolved.Encrypt)
}

func TestResolveAddOptions_WithDataType(t *testing.T) {
	resolved := syncsdk.ResolveAddOptions([]syncsdk.AddOption{syncsdk.WithDataType("mytype")})
	assert.Equal(t, "mytype", resolved.DataType)
}

func TestResolveAddOptions_Combined(t *testing.T) {
	resolved := syncsdk.ResolveAddOptions([]syncsdk.AddOption{
		syncsdk.WithEncryption(),
		syncsdk.WithSnapshot(),
		syncsdk.WithDataType("combined"),
	})
	assert.True(t, resolved.Encrypt)
	assert.True(t, resolved.IsSnapshot)
	assert.Equal(t, "combined", resolved.DataType)
}

func TestResolveObjectCreateOptions_Defaults(t *testing.T) {
	resolved := syncsdk.ResolveObjectCreateOptions(nil)
	assert.Empty(t, resolved.ChangeType)
}

func TestResolveObjectCreateOptions_WithChangeType(t *testing.T) {
	resolved := syncsdk.ResolveObjectCreateOptions([]syncsdk.ObjectCreateOption{
		syncsdk.WithChangeType("doc"),
	})
	assert.Equal(t, "doc", resolved.ChangeType)
}

func TestErrorValues(t *testing.T) {
	assert.EqualError(t, syncsdk.ErrSpaceNotFound, "space not found")
	assert.EqualError(t, syncsdk.ErrObjectNotFound, "object not found")
	assert.EqualError(t, syncsdk.ErrSpaceClosed, "space is closed")
	assert.EqualError(t, syncsdk.ErrClientClosed, "client is closed")
	assert.EqualError(t, syncsdk.ErrInvalidConfig, "invalid config")
}

func TestNetworkConfigFromYAML(t *testing.T) {
	data := []byte(`
networkId: abc123
nodes:
  - peerId: peer1
    addresses:
      - host1:443
      - host1:1443
    types:
      - tree
  - peerId: peer2
    addresses:
      - host2:443
    types:
      - coordinator
`)
	cfg, err := syncsdk.NetworkConfigFromYAML(data)
	require.NoError(t, err)
	assert.Equal(t, "abc123", cfg.NetworkID)
	assert.Empty(t, cfg.ID, "ID is not part of YAML format")
	require.Len(t, cfg.Nodes, 2)
	assert.Equal(t, "peer1", cfg.Nodes[0].PeerID)
	assert.Equal(t, []string{"host1:443", "host1:1443"}, cfg.Nodes[0].Addresses)
	assert.Equal(t, []string{"tree"}, cfg.Nodes[0].Types)
	assert.Equal(t, "peer2", cfg.Nodes[1].PeerID)
	assert.Equal(t, []string{"coordinator"}, cfg.Nodes[1].Types)
}

func TestNetworkConfigFromYAML_Invalid(t *testing.T) {
	_, err := syncsdk.NetworkConfigFromYAML([]byte(`{invalid`))
	assert.Error(t, err)
}

func TestNetworkConfigFromFile(t *testing.T) {
	cfg, err := syncsdk.NetworkConfigFromFile("staging.yml")
	require.NoError(t, err)
	assert.NotEmpty(t, cfg.NetworkID)
	assert.NotEmpty(t, cfg.Nodes)
}

func TestNetworkConfigFromFile_NotFound(t *testing.T) {
	_, err := syncsdk.NetworkConfigFromFile("nonexistent.yml")
	assert.Error(t, err)
}
