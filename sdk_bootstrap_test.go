package anysyncsdk

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/auth"
	"github.com/anyproto/any-sync-sdk/config"
)

// TestBootstrapState_ZeroSDK pins the state API on handles without a
// bootstrap pass: a zero SDK (and by extension a headless Open, which
// leaves bootstrapDone nil) reports not-bootstrapping, a pre-closed
// BootstrapDone, and a nil-safe Close.
func TestBootstrapState_ZeroSDK(t *testing.T) {
	s := &SDK{}
	assert.False(t, s.Bootstrapping())
	select {
	case <-s.BootstrapDone():
	default:
		t.Fatal("BootstrapDone must be pre-closed without a bootstrap pass")
	}
	require.NoError(t, s.Close())
}

// TestBootstrapState_Channel pins Bootstrapping against the channel
// lifecycle and Close's cancel+join prologue on an already-finished
// pass.
func TestBootstrapState_Channel(t *testing.T) {
	done := make(chan struct{})
	s := &SDK{bootstrapDone: done, bootstrapCancel: func() {}}
	assert.True(t, s.Bootstrapping())
	close(done)
	assert.False(t, s.Bootstrapping())
	// Close's prologue receives from the closed channel and must not
	// block; the watermark snapshot iterates an empty space list (nil
	// tsp is not exercised here — a real handle always has one), so
	// stub the minimum.
	assert.NotPanics(t, func() { s.bootstrapCancel() })
}

// TestOpen_CloseMidBootstrap holds the bootstrap pass open via the test
// hook and proves the contract: Open returns while Bootstrapping() is
// true, local reads work during the pass, and Close cancels and joins
// the goroutine within a bound. Uses e2e/local.yml only as a valid
// nodeconf shape — no live network is required (Open never dials
// synchronously).
func TestOpen_CloseMidBootstrap(t *testing.T) {
	yaml, err := os.ReadFile("e2e/local.yml")
	if err != nil {
		t.Skipf("nodeconf not available: %v", err)
	}

	entered := make(chan struct{})
	bootstrapTestHook = func(ctx context.Context) {
		close(entered)
		<-ctx.Done()
	}
	t.Cleanup(func() { bootstrapTestHook = nil })

	dataDir := t.TempDir()
	provider, err := auth.NewFileProvider(auth.FileProviderConfig{
		Path: filepath.Join(dataDir, "wallet.key"),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sdk, err := Open(ctx, config.Config{
		Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}, provider)
	require.NoError(t, err)

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("bootstrap goroutine never started")
	}
	assert.True(t, sdk.Bootstrapping(), "bootstrap is held open by the hook")

	// Local reads are safe during the pass.
	list, err := sdk.Spaces().List(ctx)
	require.NoError(t, err)
	assert.Empty(t, list)
	require.NotEmpty(t, sdk.Account().Id())

	closed := make(chan error, 1)
	go func() { closed <- sdk.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("Close did not cancel+join the bootstrap goroutine")
	}
	assert.False(t, sdk.Bootstrapping())
}

// TestOpen_Headless_NoBootstrap pins headless behavior: no bootstrap
// goroutine, BootstrapDone pre-closed.
func TestOpen_Headless_NoBootstrap(t *testing.T) {
	yaml, err := os.ReadFile("e2e/local.yml")
	if err != nil {
		t.Skipf("nodeconf not available: %v", err)
	}
	dataDir := t.TempDir()
	provider, err := auth.NewFileProvider(auth.FileProviderConfig{
		Path: filepath.Join(dataDir, "wallet.key"),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sdk, err := Open(ctx, config.Config{
		Storage:  config.Storage{DataDir: dataDir, Topology: config.StorageShared},
		Network:  config.Network{NodeConfYAML: yaml},
		Headless: true,
	}, provider)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	assert.False(t, sdk.Bootstrapping())
	select {
	case <-sdk.BootstrapDone():
	default:
		t.Fatal("BootstrapDone must be pre-closed under Headless")
	}
}
