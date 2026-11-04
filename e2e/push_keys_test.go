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
	})
	require.NoError(t, err)

	// The mirror is primed at wiring time but runs on its own
	// goroutine — poll the space list until the row carries it.
	got := waitMirroredPushKeys(t, ctx, sdk, sp.Id())

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

	assert.Equal(t, rawSymKey(t, encKey), encRaw, "mirrored enc key must match the sender-side derivation")

	skRaw, err := base64.StdEncoding.DecodeString(got.SpaceKey)
	require.NoError(t, err)
	skBack, err := crypto.UnmarshalEd25519PrivateKeyProto(skRaw)
	require.NoError(t, err)
	assert.True(t, spaceKey.Equals(skBack), "mirrored space key must match the sender-side derivation")
}

// TestPushKeys_DerivedSpace: a derived space's ACL has no metadata key,
// so its push space key comes from the root read key and the space id.
// Both devices of the account must derive the same keys, two derived
// spaces (which share the read key) must get distinct push identities,
// and the row must mirror the sender-side keys.
func TestPushKeys_DerivedSpace(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	open := func(keys *fixedSeedProvider) (*anysyncsdk.SDK, pushKeysProvider) {
		cfg := config.Config{
			Storage: config.Storage{
				DataDir:  t.TempDir(),
				Topology: config.StorageShared,
			},
			Network: config.Network{NodeConfYAML: yaml},
		}
		sdk, err := anysyncsdk.Open(ctx, cfg, keys)
		require.NoError(t, err)
		t.Cleanup(func() { _ = sdk.Close() })
		prov, ok := sdk.Spaces().(pushKeysProvider)
		require.True(t, ok, "Spaces() impl should expose PushKeys")
		return sdk, prov
	}
	device1Keys := newFixedSeedProvider(t)
	sdk1, prov1 := open(device1Keys)
	sdk2, prov2 := open(sameAccountFreshDevice(t, device1Keys))

	var spaceKeys []crypto.PrivKey
	for _, seed := range []string{"push-keys-derived-a", "push-keys-derived-b"} {
		sp1, err := sdk1.Spaces().Derive(ctx, space.DeriveRequest{Seed: []byte(seed), Name: seed})
		require.NoError(t, err)
		sp2, err := sdk2.Spaces().Derive(ctx, space.DeriveRequest{Seed: []byte(seed)})
		require.NoError(t, err)
		require.Equal(t, sp1.Id(), sp2.Id())

		spaceKey, encKey, err := prov1.PushKeys(ctx, sp1.Id())
		require.NoError(t, err, "a derived space must have push keys")
		spaceKey2, encKey2, err := prov2.PushKeys(ctx, sp2.Id())
		require.NoError(t, err)
		assert.True(t, spaceKey.Equals(spaceKey2), "every device of the account must derive the same push space key")
		assert.Equal(t, rawSymKey(t, encKey), rawSymKey(t, encKey2), "every device of the account must derive the same enc key")
		spaceKeys = append(spaceKeys, spaceKey)

		got := waitMirroredPushKeys(t, ctx, sdk1, sp1.Id())
		skRaw, err := base64.StdEncoding.DecodeString(got.SpaceKey)
		require.NoError(t, err)
		skBack, err := crypto.UnmarshalEd25519PrivateKeyProto(skRaw)
		require.NoError(t, err)
		assert.True(t, spaceKey.Equals(skBack), "mirrored space key must match the sender-side derivation")
		encRaw, err := base64.StdEncoding.DecodeString(got.EncKey)
		require.NoError(t, err)
		assert.Equal(t, rawSymKey(t, encKey), encRaw, "mirrored enc key must match the sender-side derivation")
	}

	require.Len(t, spaceKeys, 2)
	assert.False(t, spaceKeys[0].Equals(spaceKeys[1]), "derived spaces must get distinct push space keys")
}

func rawSymKey(t *testing.T, k crypto.SymKey) []byte {
	t.Helper()
	raw, err := k.Raw()
	require.NoError(t, err)
	return raw
}

// waitMirroredPushKeys polls the space list until the row of spaceId
// carries push keys; the mirror runs on its own goroutine.
func waitMirroredPushKeys(t *testing.T, ctx context.Context, sdk *anysyncsdk.SDK, spaceId string) *space.PushKeys {
	t.Helper()
	var got *space.PushKeys
	require.Eventually(t, func() bool {
		list, err := sdk.Spaces().List(ctx)
		if err != nil {
			return false
		}
		for _, info := range list {
			if info.Id == spaceId && info.PushKeys != nil {
				got = info.PushKeys
				return true
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond, "push keys should be mirrored onto the space row")
	return got
}
