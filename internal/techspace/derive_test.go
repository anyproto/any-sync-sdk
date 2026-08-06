package techspace

import (
	"testing"

	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Fixed valid BIP-39 phrase — derivation is deterministic, so the
// resulting tech-space ids are stable across runs and versions.
const testPhrase = "tag volcano eight thank tide danger coast health above argue embrace heavy"

func deriveKey(t *testing.T, index uint32) crypto.PrivKey {
	t.Helper()
	res, err := crypto.Mnemonic(testPhrase).DeriveKeys(index)
	require.NoError(t, err)
	return res.Identity
}

// The legacy (index-0) tech-space derive payload must never change:
// the header is content-addressed into the derived space id, and a
// shifted id orphans every existing account's tech space. The golden
// id pins the payload byte-for-byte.
func TestDeriveCfg_LegacyPayloadStable(t *testing.T) {
	key := deriveKey(t, 0)
	cfg := deriveCfg(key, TechSpaceType)
	assert.Equal(t, spacesyncproto.SpaceFileProtoVersion_SpaceFileProtoVersionUnspecified, cfg.FileProtoVersion,
		"legacy payload must not carry a fileproto version")

	payload, err := spacepayloads.StoragePayloadForSpaceDeriveV1(cfg)
	require.NoError(t, err)
	assert.Equal(t,
		"bafyreihg3t6kf3e3e3gn3sx3yyi3zvshwmuwqkn2wnzqjqnewlb24tlofa.45cqb4v9opu0",
		payload.SpaceHeaderWithId.Id,
		"legacy tech-space id shifted — the index-0 derive payload changed")
}

// The any.techspace payload differs (new type + fileproto v2) and is
// itself deterministic.
func TestDeriveCfg_AnyTechSpace(t *testing.T) {
	key := deriveKey(t, 1)
	cfg := deriveCfg(key, AnyTechSpaceType)
	assert.Equal(t, spacesyncproto.SpaceFileProtoVersion_SpaceFileProtoVersionV2, cfg.FileProtoVersion)
	assert.Equal(t, AnyTechSpaceType, cfg.SpaceType)

	a, err := spacepayloads.StoragePayloadForSpaceDeriveV1(cfg)
	require.NoError(t, err)
	b, err := spacepayloads.StoragePayloadForSpaceDeriveV1(deriveCfg(key, AnyTechSpaceType))
	require.NoError(t, err)
	assert.Equal(t, a.SpaceHeaderWithId.Id, b.SpaceHeaderWithId.Id, "derive must be deterministic")

	legacy, err := spacepayloads.StoragePayloadForSpaceDeriveV1(deriveCfg(key, TechSpaceType))
	require.NoError(t, err)
	assert.NotEqual(t, legacy.SpaceHeaderWithId.Id, a.SpaceHeaderWithId.Id,
		"any.techspace must derive a different id than the legacy type")
}
