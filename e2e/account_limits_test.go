package e2e

import (
	"context"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// nodesAt rewrites every node address to addr.
func nodesAt(t *testing.T, netYaml []byte, addr string) []byte {
	t.Helper()
	var conf map[string]any
	require.NoError(t, yaml.Unmarshal(netYaml, &conf))
	nodes, _ := conf["nodes"].([]any)
	for _, n := range nodes {
		node, _ := n.(map[string]any)
		node["addresses"] = []any{addr}
	}
	out, err := yaml.Marshal(conf)
	require.NoError(t, err)
	return out
}

// forgetStoredNodeconf drops the network configuration the coordinator
// last gave this device. A device prefers that copy over the one passed
// at open, so without this the next open could still reach the nodes.
func forgetStoredNodeconf(t *testing.T, dataDir string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(dataDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.IsDir() || d.Name() != "nodeconf" {
			return err
		}
		require.NoError(t, os.RemoveAll(path))
		return fs.SkipDir
	}))
}

// silentListener accepts connections and never says a word: a network
// that neither answers nor refuses.
func silentListener(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var conns []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			conns = append(conns, c)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
		for _, c := range conns {
			_ = c.Close()
		}
	})
	return ln.Addr().String()
}

// TestE2E_AccountLimits reads the account's network limits against a
// live local network, then offline: the device serves the answer the
// network gave last, at once when the nodes refuse the connection and
// after one bounded wait when they stay silent. A device that never
// reached the network knows no limits, and a network without file
// nodes has no storage pool.
func TestE2E_AccountLimits(t *testing.T) {
	t.Parallel()
	netYaml, _, _ := loadLocalFilesV2Network(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	dataDir := t.TempDir()
	provider := newFixedSeedProvider(t)
	p2pOff := false
	open := func(dir string, nodeconf []byte) *anysyncsdk.SDK {
		t.Helper()
		sdk, err := anysyncsdk.Open(ctx, config.Config{
			Storage: config.Storage{DataDir: dir, Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: nodeconf},
			P2P:     config.P2P{Enabled: &p2pOff},
		}, provider)
		require.NoError(t, err)
		return sdk
	}

	sdk := open(dataDir, netYaml)
	// A file node whose own coordinator lookup failed has no pool to
	// report until it retries.
	var live space.AccountLimits
	require.True(t, waitFor(ctx, 45*time.Second, time.Second, func() bool {
		var err error
		live, err = sdk.Account().Limits(ctx)
		require.NoError(t, err)
		return live.Storage != nil && live.SharedSpaces != nil
	}), "the network never reported both parts: %+v", live)
	assert.NotZero(t, live.Storage.LimitBytes, "the local network grants a storage pool")
	assert.Zero(t, live.Storage.UsageBytes)
	assert.NotZero(t, live.SharedSpaces.Limit)
	assert.WithinDuration(t, time.Now(), live.Storage.FetchedAt, time.Minute)
	assert.WithinDuration(t, time.Now(), live.SharedSpaces.FetchedAt, time.Minute)
	require.NoError(t, sdk.Close())
	forgetStoredNodeconf(t, dataDir)

	stored := func(t *testing.T, got space.AccountLimits) {
		t.Helper()
		require.NotNil(t, got.Storage)
		require.NotNil(t, got.SharedSpaces)
		assert.Equal(t, live.Storage.LimitBytes, got.Storage.LimitBytes)
		assert.Equal(t, live.SharedSpaces.Limit, got.SharedSpaces.Limit)
		assert.True(t, live.Storage.FetchedAt.Equal(got.Storage.FetchedAt), "the stored answer, not a new one")
		assert.True(t, live.SharedSpaces.FetchedAt.Equal(got.SharedSpaces.FetchedAt), "the stored answer, not a new one")
	}

	// Nodes that refuse the connection: the stored answer, at once.
	sdk = open(dataDir, nodesAt(t, netYaml, "127.0.0.1:1"))
	start := time.Now()
	refused, err := sdk.Account().Limits(ctx)
	require.NoError(t, err)
	assert.Less(t, time.Since(start), 3*time.Second)
	stored(t, refused)
	require.NoError(t, sdk.Close())

	// Nodes that stay silent: one bounded wait, then no wait at all.
	sdk = open(dataDir, nodesAt(t, netYaml, silentListener(t)))
	start = time.Now()
	silent, err := sdk.Account().Limits(ctx)
	require.NoError(t, err)
	assert.Greater(t, time.Since(start), 3*time.Second, "the first call waits for the network")
	assert.Less(t, time.Since(start), 10*time.Second)
	stored(t, silent)
	start = time.Now()
	silent, err = sdk.Account().Limits(ctx)
	require.NoError(t, err)
	assert.Less(t, time.Since(start), time.Second, "a failing network is not waited for again")
	stored(t, silent)
	start = time.Now()
	require.NoError(t, sdk.Close())
	assert.Less(t, time.Since(start), 10*time.Second, "close does not wait out the fetch")

	// A device that never reached the network knows no limits.
	sdk = open(t.TempDir(), nodesAt(t, netYaml, "127.0.0.1:1"))
	never, err := sdk.Account().Limits(ctx)
	require.NoError(t, err)
	assert.Nil(t, never.Storage)
	assert.Nil(t, never.SharedSpaces)
	require.NoError(t, sdk.Close())

	sdk = open(t.TempDir(), withoutFileNodes(t, netYaml))
	t.Cleanup(func() { _ = sdk.Close() })
	noFiles, err := sdk.Account().Limits(ctx)
	require.NoError(t, err)
	assert.Nil(t, noFiles.Storage)
	assert.NotNil(t, noFiles.SharedSpaces)
}
