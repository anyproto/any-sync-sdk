package e2e

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// deviceRow returns peer's row in svc's registry; false when it is
// missing or the registry can't be read.
func deviceRow(ctx context.Context, svc space.Service, peer string) (space.Device, bool) {
	devices, err := svc.ListDevices(ctx)
	if err != nil {
		return space.Device{}, false
	}
	for _, d := range devices {
		if d.PeerId == peer {
			return d, true
		}
	}
	return space.Device{}, false
}

// TestE2E_Devices_ConcurrentClaimsMintDistinctSeqs fires overlapping
// ClaimActive calls on one device. Each claim reads the registry and
// mints max+1 on the device's one claim slot, so they must be
// serialized: n claims leave seq == n. Unserialized, they mint the same
// seq, or a later write lands a lower one and the slot's seq goes
// backwards. Runs offline on the tracked local nodeconf.
func TestE2E_Devices_ConcurrentClaimsMintDistinctSeqs(t *testing.T) {
	t.Parallel()
	yaml, err := loadLocalNetwork()
	if err != nil {
		t.Fatalf("local network config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	sdk := openTechDevice(t, ctx, yaml, newFixedSeedProvider(t), "device")
	svc := sdk.Spaces()
	self := sdk.PeerId()

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = svc.ClaimActive(ctx, "bao", "")
		}()
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "claim %d", i)
	}

	mine, ok := deviceRow(ctx, svc, self)
	require.True(t, ok, "own row")
	assert.Equal(t, int64(n), mine.ActiveClaims["bao"].Seq)
	devices, err := svc.ListDevices(ctx)
	require.NoError(t, err)
	winner, ok := space.ActiveDevice(devices, "bao")
	require.True(t, ok)
	assert.Equal(t, self, winner)
}

// TestE2E_Devices_SwitchToAnotherDevice moves an app between two
// devices of one account over the network. A runs bao and B has it
// installed; A hands bao to B, then B hands it back. Each switch is a
// claim on the claimer's own row naming the target, and both replicas
// must elect the target once they converge.
func TestE2E_Devices_SwitchToAnotherDevice(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("devices switch e2e syncs two devices; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	provider := newFixedSeedProvider(t)
	sdkA := openTechDevice(t, ctx, yaml, provider, "device A")
	sdkB := openTechDevice(t, ctx, yaml, sameAccountFreshDevice(t, provider), "device B")
	a, b := sdkA.PeerId(), sdkB.PeerId()
	svcA, svcB := sdkA.Spaces(), sdkB.Spaces()

	require.NoError(t, svcA.ClaimActive(ctx, "bao", ""), "device A: self claim")
	require.NoError(t, svcB.SetDevice(ctx, space.DeviceUpsert{
		Apps: map[string]map[string]any{"bao": {}},
	}), "device B: install bao")

	hasBao := func(svc space.Service, peer string) bool {
		d, ok := deviceRow(ctx, svc, peer)
		_, installed := d.Apps["bao"]
		return ok && installed
	}
	waitWinner := func(label string, want string) {
		t.Helper()
		for _, r := range []struct {
			name string
			svc  space.Service
		}{{"A", svcA}, {"B", svcB}} {
			var got string
			require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
				devices, err := r.svc.ListDevices(ctx)
				if err != nil {
					return false
				}
				got, _ = space.ActiveDevice(devices, "bao")
				return got == want
			}), "%s: device %s elects %q, want %q", label, r.name, got, want)
		}
	}

	require.ErrorIs(t, svcA.ClaimActive(ctx, "bao", "12D3KooWNobody"), space.ErrDeviceUnknown)
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		return hasBao(svcA, b)
	}), "device A: B's bao row never arrived")
	require.ErrorIs(t, svcA.ClaimActive(ctx, "chess", b), space.ErrDeviceAppNotInstalled)

	require.NoError(t, svcA.ClaimActive(ctx, "bao", b), "device A: switch to B")
	claimA, ok := deviceRow(ctx, svcA, a)
	require.True(t, ok, "device A: own row")
	assert.Equal(t, b, claimA.ActiveClaims["bao"].Target, "the switch lives on A's row")
	waitWinner("after A switched to B", b)

	require.True(t, hasBao(svcB, a), "device B: A's bao row")
	require.NoError(t, svcB.ClaimActive(ctx, "bao", a), "device B: switch back to A")
	waitWinner("after B switched back to A", a)
}
