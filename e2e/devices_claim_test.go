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
// ClaimActive calls on one device (the runtime's automatic self claim
// racing a UI click). Each claim reads the registry and mints max+1, so
// they must be serialized: n claims leave seq == n, and the last one
// written is the one the election sees.
func TestE2E_Devices_ConcurrentClaimsMintDistinctSeqs(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)

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
