package anysyncx_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
)

// fixedSeedProvider is a minimal auth.Provider for tests — random
// keypairs generated once per test, kept in memory.
type fixedSeedProvider struct {
	account, device []byte
}

func newFixedSeedProvider(t *testing.T) *fixedSeedProvider {
	t.Helper()
	_, accPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, devPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return &fixedSeedProvider{account: accPriv, device: devPriv}
}

func (p *fixedSeedProvider) AccountKey(_ context.Context) ([]byte, error) { return p.account, nil }
func (p *fixedSeedProvider) DeviceKey(_ context.Context) ([]byte, error)  { return p.device, nil }

// TestAppStart_Staging brings up anysyncx against the staging nodeconf.
// It does NOT touch the network — Start only opens local components,
// the actual dial happens lazily on first peer use.
func TestAppStart_Staging(t *testing.T) {
	confPath := filepath.Join("..", "..", "..", "test-etc", "staging.yml")
	yaml, err := os.ReadFile(confPath)
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	dataDir := t.TempDir()

	cfg := config.Config{
		Storage: config.Storage{
			DataDir:  dataDir,
			Topology: config.StorageShared,
		},
		Network: config.Network{NodeConfYAML: yaml},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	a, err := anysyncx.New(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = a.Close(context.Background())
	})

	require.NotNil(t, a.SpaceService())
	require.NotNil(t, a.Coordinator())
	require.NotNil(t, a.StreamPool())
	require.NotNil(t, a.AccountKeys())
}
