package e2e

import (
	"bytes"
	"context"
	"io"
	mrand "math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// fileNodesUnreachable rewrites every fileV2 node address to a dead
// port: the rest of the network stays up, backups cannot land.
func fileNodesUnreachable(t *testing.T, netYaml []byte) []byte {
	t.Helper()
	var conf map[string]any
	require.NoError(t, yaml.Unmarshal(netYaml, &conf))
	nodes, _ := conf["nodes"].([]any)
	for _, n := range nodes {
		node, _ := n.(map[string]any)
		types, _ := node["types"].([]any)
		for _, typ := range types {
			if typ == "fileV2" {
				node["addresses"] = []any{"127.0.0.1:1"}
			}
		}
	}
	out, err := yaml.Marshal(conf)
	require.NoError(t, err)
	return out
}

// withoutFileNodes drops every fileV2 node: a local-only network as far
// as files go.
func withoutFileNodes(t *testing.T, netYaml []byte) []byte {
	t.Helper()
	var conf map[string]any
	require.NoError(t, yaml.Unmarshal(netYaml, &conf))
	nodes, _ := conf["nodes"].([]any)
	kept := nodes[:0]
	for _, n := range nodes {
		node, _ := n.(map[string]any)
		types, _ := node["types"].([]any)
		isFile := false
		for _, typ := range types {
			isFile = isFile || typ == "fileV2"
		}
		if !isFile {
			kept = append(kept, n)
		}
	}
	conf["nodes"] = kept
	out, err := yaml.Marshal(conf)
	require.NoError(t, err)
	return out
}

// TestE2E_FilesV2_NoFileNodes: with no file nodes in the network config
// a file registers, stays readable and sits in-flight without counting
// failures; once a config with file nodes arrives it backs up.
func TestE2E_FilesV2_NoFileNodes(t *testing.T) {
	t.Parallel()
	netYaml, _, _ := loadLocalFilesV2Network(t)
	if testing.Short() {
		t.Skip("files-v2 e2e is slow; rerun without -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dataDir := t.TempDir()
	provider := newFixedSeedProvider(t)
	cfg := func(nodeconf []byte) config.Config {
		return config.Config{
			Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: nodeconf},
		}
	}
	sdk, err := anysyncsdk.Open(ctx, cfg(withoutFileNodes(t, netYaml)), provider)
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = sdk.Close()
		}
	})
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "NoFileNodes"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}
	typeId, _ := setupMovieType(t, ctx, sp)
	ownerId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)
	_ = sdk.Spaces().SyncSpaceList(ctx)
	_ = sp.SyncHeads(ctx)

	content := make([]byte, 400_000)
	mrand.New(mrand.NewSource(262)).Read(content)
	info, err := sp.Files().Attach(ctx, ownerId, bytes.NewReader(content), space.AttachOpts{Name: "local.bin"})
	require.NoError(t, err)
	require.False(t, info.Durable)

	require.True(t, waitFor(ctx, 30*time.Second, 100*time.Millisecond, func() bool {
		st, err := sp.Files().Status(ctx, info.FileId)
		return err == nil && st.LastErr != ""
	}), "the job never reported why it waits")
	st, err := sp.Files().Status(ctx, info.FileId)
	require.NoError(t, err)
	require.Equal(t, space.FileStateInFlight, st.State)
	require.Zero(t, st.Attempts, "no file nodes is not a failed attempt")
	require.Contains(t, st.LastErr, "no file nodes")

	// The file is fully usable meanwhile.
	r, err := sp.Files().Open(ctx, info.FileId, space.VariantOriginal)
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	_ = r.Close()
	require.NoError(t, err)
	require.Equal(t, content, got)
	spaceId := sp.Id()

	require.NoError(t, sdk.Close())
	closed = true

	sdk2, err := anysyncsdk.Open(ctx, cfg(netYaml), provider)
	require.NoError(t, err, "reopen with file nodes")
	t.Cleanup(func() { _ = sdk2.Close() })
	sp2, err := sdk2.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)
	waitFileDurable(t, ctx, sp2, info.FileId)
}

// TestE2E_FilesV2_OfflineAttachRestartOnline: a file attached while the
// file nodes are unreachable stays registered and queued across a
// process stop, and the next start with the nodes reachable backs it up
// at once — the backoff earned offline does not carry over.
func TestE2E_FilesV2_OfflineAttachRestartOnline(t *testing.T) {
	t.Parallel()
	netYaml, _, _ := loadLocalFilesV2Network(t)
	if testing.Short() {
		t.Skip("files-v2 e2e is slow; rerun without -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dataDir := t.TempDir()
	provider := newFixedSeedProvider(t)
	cfg := func(nodeconf []byte) config.Config {
		return config.Config{
			Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: nodeconf},
		}
	}

	sdk, err := anysyncsdk.Open(ctx, cfg(fileNodesUnreachable(t, netYaml)), provider)
	require.NoError(t, err, "Open (file nodes unreachable)")
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = sdk.Close()
		}
	})
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "OfflineAttach"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}
	typeId, _ := setupMovieType(t, ctx, sp)
	ownerId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)
	_ = sdk.Spaces().SyncSpaceList(ctx)
	_ = sp.SyncHeads(ctx)

	content := make([]byte, 1_500_000)
	mrand.New(mrand.NewSource(261)).Read(content)
	start := time.Now()
	info, err := sp.Files().Attach(ctx, ownerId, bytes.NewReader(content),
		space.AttachOpts{Name: "offline.bin", Mime: "application/octet-stream"})
	require.NoError(t, err, "Attach with unreachable file nodes")
	require.Less(t, time.Since(start), 5*time.Second, "Attach must not wait on the network")
	require.False(t, info.Durable)

	// The queue tried, failed and backed off.
	require.True(t, waitFor(ctx, 60*time.Second, 200*time.Millisecond, func() bool {
		st, err := sp.Files().Status(ctx, info.FileId)
		return err == nil && st.State == space.FileStateInFlight && st.Attempts >= 1
	}), "the backup attempt never failed")
	st, err := sp.Files().Status(ctx, info.FileId)
	require.NoError(t, err)
	require.NotEmpty(t, st.LastErr)
	require.False(t, strings.Contains(st.LastErr, "limit"))
	spaceId := sp.Id()

	require.NoError(t, sdk.Close())
	closed = true

	// Same data dir, file nodes reachable.
	sdk2, err := anysyncsdk.Open(ctx, cfg(netYaml), provider)
	require.NoError(t, err, "reopen")
	t.Cleanup(func() { _ = sdk2.Close() })
	reopened := time.Now()
	sp2, err := sdk2.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)
	waitFileDurable(t, ctx, sp2, info.FileId)
	// The first backoff step is 30s; landing sooner proves the restart
	// made the job due.
	require.Less(t, time.Since(reopened), 25*time.Second, "restart must not wait out the offline backoff")

	got, err := sp2.Files().Get(ctx, info.FileId)
	require.NoError(t, err)
	require.True(t, got.Durable)
}
