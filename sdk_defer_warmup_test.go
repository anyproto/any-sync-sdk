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

// TestWarmupState_ZeroSDK pins the state API on handles without a
// deferred warmup: a zero SDK (and by extension a synchronous or
// headless Open, which leave warmupDone nil) reports not-warming, a
// pre-closed WarmupDone, and a nil-safe Close.
func TestWarmupState_ZeroSDK(t *testing.T) {
	s := &SDK{}
	assert.False(t, s.Warming())
	select {
	case <-s.WarmupDone():
	default:
		t.Fatal("WarmupDone must be pre-closed when warmup ran synchronously")
	}
	require.NoError(t, s.Close())
}

// TestWarmupState_Channel pins Warming against the channel lifecycle
// and Close's cancel+join prologue on an already-finished warmup.
func TestWarmupState_Channel(t *testing.T) {
	done := make(chan struct{})
	s := &SDK{warmupDone: done, warmupCancel: func() {}}
	assert.True(t, s.Warming())
	close(done)
	assert.False(t, s.Warming())
	// Close's prologue receives from the closed channel and must not block.
	require.NoError(t, s.Close())
}

// TestOpen_DeferWarmup_CloseMidWarmup holds a deferred warmup open via
// the test hook and proves the contract: Open returns while
// Warming()==true, local reads work during warmup, and Close cancels
// and joins the warmup goroutine within a bound. Uses e2e/local.yml
// only as a valid nodeconf shape — no live network is required (Open
// never dials synchronously).
func TestOpen_DeferWarmup_CloseMidWarmup(t *testing.T) {
	yaml, err := os.ReadFile("e2e/local.yml")
	if err != nil {
		t.Skipf("nodeconf not available: %v", err)
	}

	entered := make(chan struct{})
	warmupTestHook = func(ctx context.Context) {
		close(entered)
		<-ctx.Done()
	}
	t.Cleanup(func() { warmupTestHook = nil })

	dataDir := t.TempDir()
	provider, err := auth.NewFileProvider(auth.FileProviderConfig{
		Path: filepath.Join(dataDir, "wallet.key"),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sdk, err := Open(ctx, config.Config{
		Storage:     config.Storage{DataDir: dataDir, Topology: config.StorageShared},
		Network:     config.Network{NodeConfYAML: yaml},
		DeferWarmup: true,
	}, provider)
	require.NoError(t, err)

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("warmup goroutine never started")
	}
	assert.True(t, sdk.Warming(), "warmup is held open by the hook")

	// Local reads are safe during warmup.
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
		t.Fatal("Close did not cancel+join the warmup goroutine")
	}
	assert.False(t, sdk.Warming())
}
