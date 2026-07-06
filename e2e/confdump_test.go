package e2e

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
)

// TestDumpCoordinatorNetworkConfig is a manual diagnostic: connect to
// staging, wait for the coordinator's network-config update, and dump
// the stored config so its id and hostnames can be inspected.
// Gated behind ANY_SDK_E2E_CONFDUMP=1; never runs in the suite.
func TestDumpCoordinatorNetworkConfig(t *testing.T) {
	if os.Getenv("ANY_SDK_E2E_CONFDUMP") == "" {
		t.Skip("set ANY_SDK_E2E_CONFDUMP=1 to dump the coordinator-served network config")
	}
	yaml, _, err := loadAnySyncNetwork()
	require.NoError(t, err)

	dir := os.Getenv("ANY_SDK_E2E_CONFDUMP_DIR")
	require.NotEmpty(t, dir)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	sdk, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: dir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	// The nodeconf store writes <networkId>.yml when the coordinator
	// pushes an update; poll for any yml under the data dir.
	deadline := time.Now().Add(75 * time.Second)
	for time.Now().Before(deadline) {
		matches, _ := filepath.Glob(filepath.Join(dir, "anysync", "nodeconf", "*.yml"))
		if len(matches) > 0 {
			for _, m := range matches {
				t.Logf("stored network config: %s", m)
			}
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatal("no network-config update received from the coordinator within 75s")
}
