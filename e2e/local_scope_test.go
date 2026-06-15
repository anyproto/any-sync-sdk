package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_LocalScopeIsolation: one account, two devices, one object
// carrying a SYNCED title and a LOCAL `pin` flag.
//
// The contract under test is a negative one — a local-scoped value
// must NEVER leave the device — which only a second real device can
// prove. The test makes the negative airtight with a causal clock: A
// writes the local value FIRST, then a synced value; once B has
// converged the synced value it has provably caught up past the point
// in time the local write was made, so if the local value were ever
// going to sync it would be present. It must be absent.
//
// It also proves device-LOCAL independence both ways: A's `pin` and
// B's `pin` are distinct device-local values at the same path. A round
// of synced writes in each direction (the clock) sandwiches the
// assertions that neither device's local value ever bleeds into the
// other.
func TestE2E_LocalScopeIsolation(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("local-scope e2e is slow (~1-2min); rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	providerA := newFixedSeedProvider(t)
	providerB := sameAccountFreshDevice(t, providerA)

	// ---- Device A: create everything, write synced + local ----
	sdkA, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}, providerA)
	require.NoError(t, err, "device A: Open")
	t.Cleanup(func() { _ = sdkA.Close() })

	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "LocalScope"})
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
	pinProp, err := spA.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Pin", Kind: space.PropertyKindBoolean, Scope: space.ScopeLocal,
	})
	require.NoError(t, err)

	objId, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)

	// Local write FIRST (this is the value B must never see), then the
	// synced clock value.
	_, err = spA.Properties().Set(ctx, objId, typeId, map[string]any{pinProp: true})
	require.NoError(t, err, "device A: local-scope Set")
	_, err = spA.Properties().Set(ctx, objId, typeId, map[string]any{titleProp: "first"})
	require.NoError(t, err, "device A: synced Set")

	// Read-your-writes on A: both visible.
	rowA, err := spA.Properties().Get(ctx, objId)
	require.NoError(t, err)
	require.NotNil(t, rowA)
	require.True(t, rowA.GetBool(typeId, pinProp), "device A sees its own local value")
	require.Equal(t, "first", rowA.GetString(typeId, titleProp))

	// ---- Device B: fresh data dir, same account ----
	sdkB, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}, providerB)
	require.NoError(t, err, "device B: Open")
	t.Cleanup(func() { _ = sdkB.Close() })

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

	// Clock: wait until the synced title converges on B. After this, B
	// has provably applied changes made AFTER A's local write.
	require.Eventually(t, func() bool {
		row, gerr := spB.Properties().Get(ctx, objId)
		if gerr != nil || row == nil {
			return false
		}
		return row.GetString(typeId, titleProp) == "first"
	}, 2*time.Minute, 3*time.Second, "device B: synced title never converged")

	// THE NEGATIVE: B must not have A's local value. The causal clock
	// above makes this airtight; the GetBool default-false would also
	// be false if the path were simply absent, so assert absence
	// explicitly via Get(...) == nil.
	rowB, err := spB.Properties().Get(ctx, objId)
	require.NoError(t, err)
	require.NotNil(t, rowB)
	require.Equal(t, "first", rowB.GetString(typeId, titleProp), "sanity: synced value present")
	require.Nil(t, rowB.Get(typeId, pinProp), "device B must NEVER see device A's local-scoped value")

	// ---- Device-local independence, both directions ----
	// B writes its OWN local value (the opposite bool) and a synced
	// clock value back toward A. The type DEFS sync on a separate tree
	// from the object, so wait for the local-scoped def to resolve on
	// B first — otherwise resolveRoute would fall back to synced and B's
	// write would corrupt the shared value (and silently defeat the
	// test's premise).
	require.Eventually(t, func() bool {
		return defsKnownWithScope(ctx, spB, typeId, pinProp, space.ScopeLocal)
	}, 2*time.Minute, 3*time.Second, "device B: local-scoped def never resolved")
	_, err = spB.Properties().Set(ctx, objId, typeId, map[string]any{pinProp: false})
	require.NoError(t, err, "device B: own local-scope Set")
	_, err = spB.Properties().Set(ctx, objId, typeId, map[string]any{titleProp: "second"})
	require.NoError(t, err, "device B: synced Set back toward A")

	// B sees its own local value immediately.
	rowB, err = spB.Properties().Get(ctx, objId)
	require.NoError(t, err)
	require.False(t, rowB.GetBool(typeId, pinProp), "device B sees its own local value (false)")

	// Clock the other way: A converges B's synced title="second".
	require.Eventually(t, func() bool {
		row, gerr := spA.Properties().Get(ctx, objId)
		if gerr != nil || row == nil {
			return false
		}
		return row.GetString(typeId, titleProp) == "second"
	}, 2*time.Minute, 3*time.Second, "device A: did not converge B's synced write")

	// A's local value is still its OWN (true) — B's local false never
	// crossed, even though B wrote it to the same path before the
	// synced clock value A just observed.
	rowA, err = spA.Properties().Get(ctx, objId)
	require.NoError(t, err)
	require.True(t, rowA.GetBool(typeId, pinProp),
		"device A's local value must be untouched by device B's local write at the same path")
}
