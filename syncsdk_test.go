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

func TestPermissionConstants(t *testing.T) {
	assert.Equal(t, syncsdk.Permission(1), syncsdk.PermissionOwner)
	assert.Equal(t, syncsdk.Permission(2), syncsdk.PermissionAdmin)
	assert.Equal(t, syncsdk.Permission(3), syncsdk.PermissionWriter)
	assert.Equal(t, syncsdk.Permission(4), syncsdk.PermissionReader)
}

func TestMemberStatusConstants(t *testing.T) {
	assert.Equal(t, syncsdk.MemberStatus(1), syncsdk.MemberStatusActive)
	assert.Equal(t, syncsdk.MemberStatus(2), syncsdk.MemberStatusJoining)
	assert.Equal(t, syncsdk.MemberStatus(3), syncsdk.MemberStatusRemoving)
}

func TestResolveInviteOptions_Defaults(t *testing.T) {
	resolved := syncsdk.ResolveInviteOptions(nil)
	assert.Equal(t, syncsdk.PermissionWriter, resolved.Permission)
	assert.False(t, resolved.ApprovalRequired)
}

func TestResolveInviteOptions_CustomPermission(t *testing.T) {
	resolved := syncsdk.ResolveInviteOptions([]syncsdk.InviteOption{
		syncsdk.WithInvitePermission(syncsdk.PermissionReader),
	})
	assert.Equal(t, syncsdk.PermissionReader, resolved.Permission)
	assert.False(t, resolved.ApprovalRequired)
}

func TestResolveInviteOptions_ApprovalRequired(t *testing.T) {
	resolved := syncsdk.ResolveInviteOptions([]syncsdk.InviteOption{
		syncsdk.WithApprovalRequired(),
	})
	assert.True(t, resolved.ApprovalRequired)
}

func TestInviteEncodeDecode_AnyoneCanJoin(t *testing.T) {
	privKey, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)

	encoded, err := syncsdk.EncodeInvite("space123", privKey, false)
	require.NoError(t, err)
	assert.NotEmpty(t, encoded)

	spaceID, decodedKey, approvalRequired, err := syncsdk.DecodeInvite(encoded)
	require.NoError(t, err)
	assert.Equal(t, "space123", spaceID)
	assert.False(t, approvalRequired)
	assert.True(t, decodedKey.GetPublic().Equals(privKey.GetPublic()))
}

func TestInviteEncodeDecode_RequestToJoin(t *testing.T) {
	privKey, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)

	encoded, err := syncsdk.EncodeInvite("spaceXYZ", privKey, true)
	require.NoError(t, err)

	spaceID, decodedKey, approvalRequired, err := syncsdk.DecodeInvite(encoded)
	require.NoError(t, err)
	assert.Equal(t, "spaceXYZ", spaceID)
	assert.True(t, approvalRequired)
	assert.True(t, decodedKey.GetPublic().Equals(privKey.GetPublic()))
}

func TestParseInvite_Valid(t *testing.T) {
	privKey, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)

	encoded, err := syncsdk.EncodeInvite("space456", privKey, false)
	require.NoError(t, err)

	spaceID, err := syncsdk.ParseInvite(encoded)
	require.NoError(t, err)
	assert.Equal(t, "space456", spaceID)
}

func TestParseInvite_InvalidBase64(t *testing.T) {
	_, err := syncsdk.ParseInvite("not-valid-base64!!!")
	assert.ErrorIs(t, err, syncsdk.ErrInvalidInvite)
}

func TestParseInvite_InvalidJSON(t *testing.T) {
	// Valid base64 but not JSON
	_, err := syncsdk.ParseInvite("aGVsbG8=")
	assert.ErrorIs(t, err, syncsdk.ErrInvalidInvite)
}

func TestDecodeInvite_BadBase64(t *testing.T) {
	_, _, _, err := syncsdk.DecodeInvite("not-base64!!!")
	assert.ErrorIs(t, err, syncsdk.ErrInvalidInvite)
}

func TestDecodeInvite_MissingSpaceID(t *testing.T) {
	// Encode valid base64 JSON with empty space ID
	_, _, _, err := syncsdk.DecodeInvite("eyJzIjoiIiwiayI6ImFiYyIsInQiOjF9")
	assert.ErrorIs(t, err, syncsdk.ErrInvalidInvite)
}

func TestDecodeInvite_MissingKey(t *testing.T) {
	// {"s":"space","k":"","t":1}
	_, _, _, err := syncsdk.DecodeInvite("eyJzIjoic3BhY2UiLCJrIjoiIiwidCI6MX0=")
	assert.ErrorIs(t, err, syncsdk.ErrInvalidInvite)
}

func TestDecodeInvite_InvalidKeyBytes(t *testing.T) {
	// {"s":"space","k":"aGVsbG8=","t":1} - valid base64 key but not a valid ed25519 key
	_, _, _, err := syncsdk.DecodeInvite("eyJzIjoic3BhY2UiLCJrIjoiYUdWc2JHOD0iLCJ0IjoxfQ==")
	assert.ErrorIs(t, err, syncsdk.ErrInvalidInvite)
}

func TestErrorValues_NewErrors(t *testing.T) {
	assert.EqualError(t, syncsdk.ErrJoinRequestPending, "join request pending approval")
	assert.EqualError(t, syncsdk.ErrInvalidInvite, "invalid invite")
}
