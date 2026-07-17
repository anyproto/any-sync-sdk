package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"testing"
	"time"

	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// pushKeysProvider is the (exported but interface-hidden) derivation
// entry on spaceimpl.Service — the sender-side source of truth the
// mirrored row values must match.
type pushKeysProvider interface {
	PushKeys(ctx context.Context, spaceId string) (crypto.PrivKey, crypto.SymKey, error)
}

// TestPushKeys_MirroredToSpaceRow: creating a space must (async, via
// the push-key watcher) land the derived push key material on the
// tech-space row, surfaced as SpaceInfo.PushKeys — the receiver-side
// cache contract for clients that decrypt pushes while the SDK is
// down. The mirrored values must round-trip and match the sender-side
// PushKeys derivation exactly, and EncKeyId must be recomputable from
// EncKey alone (hex sha256 of the raw bytes — pushapi.Message.KeyId).
func TestPushKeys_MirroredToSpaceRow(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	cfg := config.Config{
		Storage: config.Storage{
			DataDir:  t.TempDir(),
			Topology: config.StorageShared,
		},
		Network: config.Network{NodeConfYAML: yaml},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{
		Name:      "push-keys",
		SpaceType: space.SpaceTypeRegular,
	})
	require.NoError(t, err)

	// The mirror is primed at wiring time but runs on its own
	// goroutine — poll the space list until the row carries it.
	var got *space.PushKeys
	require.Eventually(t, func() bool {
		list, lerr := sdk.Spaces().List(ctx)
		if lerr != nil {
			return false
		}
		for _, info := range list {
			if info.Id == sp.Id() && info.PushKeys != nil {
				got = info.PushKeys
				return true
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond, "push keys should be mirrored onto the space row")

	// EncKeyId is recomputable from EncKey alone — the client-side
	// cache invariant.
	encRaw, err := base64.StdEncoding.DecodeString(got.EncKey)
	require.NoError(t, err)
	sum := sha256.Sum256(encRaw)
	assert.Equal(t, hex.EncodeToString(sum[:]), got.EncKeyId)

	// Both keys must equal the sender-side derivation.
	prov, ok := sdk.Spaces().(pushKeysProvider)
	require.True(t, ok, "Spaces() impl should expose PushKeys")
	spaceKey, encKey, err := prov.PushKeys(ctx, sp.Id())
	require.NoError(t, err)

	wantEncRaw, err := encKey.Raw()
	require.NoError(t, err)
	assert.Equal(t, wantEncRaw, encRaw, "mirrored enc key must match the sender-side derivation")

	skRaw, err := base64.StdEncoding.DecodeString(got.SpaceKey)
	require.NoError(t, err)
	skBack, err := crypto.UnmarshalEd25519PrivateKeyProto(skRaw)
	require.NoError(t, err)
	assert.True(t, spaceKey.Equals(skBack), "mirrored space key must match the sender-side derivation")
}
