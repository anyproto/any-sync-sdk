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

	devices, err := svc.ListDevices(ctx)
	require.NoError(t, err)
	var mine space.Device
	for _, d := range devices {
		if d.PeerId == self {
			mine = d
		}
	}
	assert.Equal(t, int64(n), mine.ActiveClaims["bao"].Seq)
	winner, ok := space.ActiveDevice(devices, "bao")
	require.True(t, ok)
	assert.Equal(t, self, winner)
}
