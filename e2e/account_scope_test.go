package e2e

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// sameAccountFreshDevice derives a provider for a SECOND device of the
// same account: identical account key, fresh device key. The fresh
// device key matters — two devices sharing one device key collide on
// peerId, which silently kills realtime push and leaves only the slow
// periodic diff-sync.
func sameAccountFreshDevice(t *testing.T, p *fixedSeedProvider) *fixedSeedProvider {
	t.Helper()
	_, devPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return &fixedSeedProvider{account: p.account, device: devPriv}
}

// TestE2E_AccountScopeSync: one account, two devices, one shared-space
// object with a synced title and an account-scoped `read` flag.
//
//   - Device A sets read=true → must be visible on A immediately
//     (read-your-writes inline mirror) and converge to device B via
//     the tech-space carrier + B's mirror.
//   - Ordering tolerance: B may receive the carrier record before or
//     after the target object's tree — the row-created replay hook
//     and the state re-mirror cover both orders.
//   - A then unsets it (nil patch value) → the carrier's retained
//     `_ver` entry must propagate the unset to B.
//   - A second object is created and account-flagged while B is
//     already online — the live mirror path (carrier events) must
//     deliver it without a space reload.
func TestE2E_AccountScopeSync(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("account-scope e2e is slow (~2-3min); rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	providerA := newFixedSeedProvider(t)
	providerB := sameAccountFreshDevice(t, providerA)

	// ---- Device A: create everything, write account value ----
	sdkA, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}, providerA)
	require.NoError(t, err, "device A: Open")
	t.Cleanup(func() { _ = sdkA.Close() })

	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "AccountScope"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("device A: Spaces().Create: %v", err)
	}
	spaceId := spA.Id()

	typeId, err := spA.Types().Create(ctx, space.TypeCreateParams{Name: "Doc"})
	require.NoError(t, err)
	titleProp, err := spA.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Title", Kind: space.PropertyKindString,
	})
	require.NoError(t, err)
	readProp, err := spA.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Read", Kind: space.PropertyKindBoolean, Scope: space.ScopeAccount,
	})
	require.NoError(t, err)

	objA, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	_, err = spA.Properties().Set(ctx, objA, typeId, map[string]any{titleProp: "first"})
	require.NoError(t, err)

	res, err := spA.Properties().Set(ctx, objA, typeId, map[string]any{readProp: true})
	require.NoError(t, err, "device A: account-scope Set")
	require.NotEmpty(t, res.VersionId, "account Set reports the carrier versionId")

	// Read-your-writes on A: the inline mirror applied it.
	rowA, err := spA.Properties().Get(ctx, objA)
	require.NoError(t, err)
	require.NotNil(t, rowA)
	require.True(t, rowA.GetBool(typeId, readProp), "device A sees its own account value immediately")
	require.Equal(t, "first", rowA.GetString(typeId, titleProp))

	// ---- Device B: fresh data dir, same account ----
	sdkB, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}, providerB)
	require.NoError(t, err, "device B: Open")
	t.Cleanup(func() { _ = sdkB.Close() })

	// The space row arrives via the tech space; Get loads the space and
	// starts B's account mirror.
	var spB space.Space
	require.Eventually(t, func() bool {
		list, lerr := sdkB.Spaces().List(ctx)
		if lerr != nil {
			return false
		}
		for _, info := range list {
			if info.Id == spaceId {
				spB, lerr = sdkB.Spaces().Get(ctx, spaceId)
				return lerr == nil
			}
		}
		return false
	}, 3*time.Minute, 3*time.Second, "device B: space never appeared")

	readOnB := func(objectId string) func() bool {
		return func() bool {
			row, gerr := spB.Properties().Get(ctx, objectId)
			if gerr != nil {
				t.Logf("device B poll: Get(%s) err=%v", objectId, gerr)
				return false
			}
			if row == nil {
				t.Logf("device B poll: row %s absent (object tree not synced yet)", objectId)
				return false
			}
			t.Logf("device B poll: row %s title=%q read=%v defsKnown=%v",
				objectId, row.GetString(typeId, titleProp), row.Get(typeId, readProp) != nil, defsKnownOnB(ctx, spB, typeId, readProp))
			return row.GetBool(typeId, readProp)
		}
	}

	// Account value converges to B — whichever of (carrier record,
	// object tree) arrived first.
	require.Eventually(t, readOnB(objA), 2*time.Minute, 5*time.Second,
		"device B: account value never mirrored onto the row")

	// The synced title must be there too (sanity: object content synced).
	rowB, err := spB.Properties().Get(ctx, objA)
	require.NoError(t, err)
	require.Equal(t, "first", rowB.GetString(typeId, titleProp))

	// ---- Unset propagation ----
	_, err = spA.Properties().Set(ctx, objA, typeId, map[string]any{readProp: nil})
	require.NoError(t, err, "device A: account unset")
	rowA, err = spA.Properties().Get(ctx, objA)
	require.NoError(t, err)
	require.Nil(t, rowA.Get(typeId, readProp), "device A: unset visible immediately")

	require.Eventually(t, func() bool {
		row, gerr := spB.Properties().Get(ctx, objA)
		return gerr == nil && row != nil && row.Get(typeId, readProp) == nil
	}, 2*time.Minute, 3*time.Second, "device B: unset never propagated")

	// ---- Live path: new object + account value while B is online ----
	objA2, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	_, err = spA.Properties().Set(ctx, objA2, typeId, map[string]any{readProp: true})
	require.NoError(t, err)

	require.Eventually(t, readOnB(objA2), 2*time.Minute, 3*time.Second,
		"device B: live account value for a new object never arrived")
}

// defsKnownOnB reports whether the type's property definitions have
// synced to this device (the mirror's scope resolver depends on them).
func defsKnownOnB(ctx context.Context, sp space.Space, typeId, propId string) bool {
	defs, err := sp.Types().Properties(ctx, typeId)
	if err != nil {
		return false
	}
	for _, d := range defs {
		if d.Id == propId {
			return d.Scope == space.ScopeAccount
		}
	}
	return false
}
