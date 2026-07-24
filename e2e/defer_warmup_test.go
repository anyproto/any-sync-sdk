package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestSDK_DeferWarmup_OpenFastThenCompletes proves the DeferWarmup
// contract end-to-end: a reopen with the flag returns before the eager
// space-loading loop, local reads (Spaces().List) serve the tech-space
// index immediately, and the warmup completes on its own within a
// bounded window.
//
// Warming()==true is deliberately NOT asserted right after Open — a
// small account can legitimately finish warmup before the assertion
// runs; the held-open case is pinned by the root-package
// TestOpen_DeferWarmup_CloseMidWarmup instead.
func TestSDK_DeferWarmup_OpenFastThenCompletes(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	dataDir := t.TempDir()
	provider := newFixedSeedProvider(t)
	cfg := config.Config{
		Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}

	// Seed session: synchronous Open, two spaces, close.
	{
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		sdk, err := anysyncsdk.Open(ctx, cfg, provider)
		require.NoError(t, err)
		_, err = sdk.Spaces().Create(ctx, space.CreateRequest{Name: "WarmA"})
		require.NoError(t, err)
		_, err = sdk.Spaces().Create(ctx, space.CreateRequest{Name: "WarmB"})
		require.NoError(t, err)
		require.NoError(t, sdk.Close())
	}

	// Deferred reopen: local index serves before warmup finishes.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cfg.DeferWarmup = true
	sdk, err := anysyncsdk.Open(ctx, cfg, provider)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	list, err := sdk.Spaces().List(ctx)
	require.NoError(t, err)
	require.Len(t, list, 2, "local space index must serve right after Open")
	require.NotEmpty(t, sdk.Account().Id())

	select {
	case <-sdk.WarmupDone():
	case <-ctx.Done():
		t.Fatal("warmup did not complete within the deadline")
	}
	assert.False(t, sdk.Warming())
}

// TestSDK_DeferWarmup_HeadlessWins pins the precedence: Headless skips
// warmup entirely, so DeferWarmup is a no-op — no warmup goroutine, no
// warming state.
func TestSDK_DeferWarmup_HeadlessWins(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sdk, err := anysyncsdk.Open(ctx, config.Config{
		Storage:     config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network:     config.Network{NodeConfYAML: yaml},
		Headless:    true,
		DeferWarmup: true,
	}, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	assert.False(t, sdk.Warming())
	select {
	case <-sdk.WarmupDone():
	default:
		t.Fatal("WarmupDone must be pre-closed under Headless")
	}
}
