package e2e

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_AnySpaceTypeReachesOtherDevice pins what a second device of
// the account sees of a space the first one created for a use: the use
// is in its space list once the list has synced, before the space
// loads, and a second value is refused there. The space's index is on
// the second device after it has synced the space.
func TestE2E_AnySpaceTypeReachesOtherDevice(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("two-device e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	// One account, a device key each: two live SDKs on one device key
	// are one peer to the nodes.
	_, accountKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	newDevice := func(label string) *anysyncsdk.SDK {
		_, deviceKey, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		cfg := config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
		}
		sdk, err := anysyncsdk.Open(ctx, cfg, &fixedSeedProvider{account: accountKey, device: deviceKey})
		require.NoError(t, err, "%s: Open", label)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk
	}

	// --- Device A creates a space for a use, and gives a second one its
	// use afterwards. ---
	sdkA := newDevice("device A")
	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "Bao", AnySpaceType: "bao"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on create: %v", err)
		}
		t.Fatalf("device A: Create: %v", err)
	}
	require.True(t, spA.IndexSeeded(ctx), "device A: the creating device has the index at once")
	later, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "Notes"})
	require.NoError(t, err)
	require.NoError(t, sdkA.Spaces().SetAnySpaceType(ctx, later.Id(), "notes"))

	// A's rows and the space reach a node before B opens.
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = sdkA.Spaces().SyncSpaceList(ctx)
		_ = spA.SyncHeads(ctx)
		return sdkA.Spaces().Status(sdkA.TechSpaceId()).State == space.SyncStateSynced &&
			sdkA.Spaces().Status(spA.Id()).State == space.SyncStateSynced
	}), "device A: the tech space and the created space never reached synced")

	// --- Device B reads the use from its space list. ---
	sdkB := newDevice("device B")
	require.Equal(t, sdkA.Account().Id(), sdkB.Account().Id())

	waitCtx, waitCancel := context.WithTimeout(ctx, 90*time.Second)
	require.NoError(t, sdkB.Spaces().WaitListSynced(waitCtx), "device B: WaitListSynced")
	waitCancel()
	var bao, notes *space.SpaceInfo
	require.True(t, waitFor(ctx, 30*time.Second, 250*time.Millisecond, func() bool {
		list, lerr := sdkB.Spaces().List(ctx)
		if lerr != nil {
			return false
		}
		bao, notes = findSpace(list, spA.Id()), findSpace(list, later.Id())
		return bao != nil && notes != nil && notes.AnySpaceType != ""
	}), "device B: the rows never arrived")
	assert.Equal(t, "bao", bao.AnySpaceType)
	assert.Equal(t, "notes", notes.AnySpaceType)
	assert.Equal(t, space.SpaceTypeAny, bao.SpaceType, "the space itself is untagged")

	assert.ErrorIs(t, sdkB.Spaces().SetAnySpaceType(ctx, spA.Id(), "other"), space.ErrAnySpaceTypeSet)
	require.NoError(t, sdkB.Spaces().SetAnySpaceType(ctx, spA.Id(), "bao"), "the same value again")

	// --- The downloaded space: its index arrives by sync. ---
	spB, err := sdkB.Spaces().Get(ctx, spA.Id())
	require.NoError(t, err, "device B: Get")
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		return spB.IndexSeeded(ctx)
	}), "device B: the index never arrived")
	assert.Equal(t, "bao", spB.Info().AnySpaceType)
	assert.Equal(t, "Bao", spB.Info().Name)
}
