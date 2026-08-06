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
// resulting tech-space id is stable across runs and versions.
const testPhrase = "tag volcano eight thank tide danger coast health above argue embrace heavy"

// The tech-space derive payload must never change: the header is
// content-addressed into the derived space id, and a shifted id
// orphans every account's tech space. The golden id pins the payload
// byte-for-byte.
func TestDeriveCfg_Stable(t *testing.T) {
	res, err := crypto.Mnemonic(testPhrase).DeriveKeys(1)
	require.NoError(t, err)
	cfg := deriveCfg(res.Identity)
	assert.Equal(t, TechSpaceType, cfg.SpaceType)
	assert.Equal(t, spacesyncproto.SpaceFileProtoVersion_SpaceFileProtoVersionV2, cfg.FileProtoVersion)

	payload, err := spacepayloads.StoragePayloadForSpaceDeriveV1(cfg)
	require.NoError(t, err)
	assert.Equal(t,
		"bafyreifsm4ju5lrujkdwjfwp64n5vmx3gknthl7p3dmjkfcrn4pgzj35ry.17zrtgn7oozoh",
		payload.SpaceHeaderWithId.Id,
		"tech-space id shifted — the derive payload changed")
}
