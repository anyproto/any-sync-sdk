package e2e

import (
	"bytes"
	"context"
	mrand "math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_FilesV2_StatusAndPin proves the SYN-29 surface against a
// live local network: Status/Stats derive durability truthfully on
// both devices, SubscribeStatus delivers local transitions, and Pin
// drives a persistent queue-executed background fetch to a complete
// local copy on the second device.
func TestE2E_FilesV2_StatusAndPin(t *testing.T) {
	t.Parallel()
	netYaml, fileV2Peers, _ := loadLocalFilesV2Network(t)
	if testing.Short() {
		t.Skip("files-v2 e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	provider := newFixedSeedProvider(t)

	// --- Device A: attach one S3-tier + one inline file.
	cfgA := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: netYaml},
	}
	sdkA, err := anysyncsdk.Open(ctx, cfgA, provider)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdkA.Close() })

	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "FilesV2Status"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}
	typeId, _ := setupMovieType(t, ctx, spA)
	ownerId, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)
	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = spA.SyncHeads(ctx)

	content := make([]byte, 1_800_000)
	mrand.New(mrand.NewSource(29)).Read(content)
	big, err := spA.Files().Attach(ctx, ownerId, bytes.NewReader(content),
		space.AttachOpts{Name: "pinned.bin", Mime: "application/octet-stream"})
	require.NoError(t, err)
	require.True(t, big.Durable)
	small, err := spA.Files().Attach(ctx, ownerId, bytes.NewReader([]byte("inline status")),
		space.AttachOpts{Name: "s.txt"})
	require.NoError(t, err)
	_ = spA.SyncHeads(ctx)

	// Status/Stats on the writer.
	stA, err := spA.Files().Status(ctx, big.FileId)
	require.NoError(t, err)
	assert.Equal(t, space.FileStateDurable, stA.State)
	assert.True(t, stA.Cached)
	statsA, err := spA.Files().Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, space.FileStats{Total: 2, Durable: 2}, statsA)

	// --- Device B behind the CDN stand-in.
	cdn := newBlobCDN(t, ctx, sdkA, fileV2Peers, spA.Id())
	t.Cleanup(cdn.Close)
	cfgB := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: netYaml},
		Files:   config.Files{PublicReadBaseUrl: cdn.URL},
	}
	sdkB, err := anysyncsdk.Open(ctx, cfgB, provider)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdkB.Close() })

	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = sdkB.Spaces().SyncSpaceList(ctx)
		list, err := sdkB.Spaces().List(ctx)
		if err != nil {
			return false
		}
		for _, info := range list {
			if info.Id == spA.Id() {
				return true
			}
		}
		return false
	}), "device B never saw the space")
	spB, err := sdkB.Spaces().Get(ctx, spA.Id())
	require.NoError(t, err)
	require.True(t, waitFor(ctx, 120*time.Second, time.Second, func() bool {
		_ = spB.SyncHeads(ctx)
		st, err := spB.Files().Status(ctx, big.FileId)
		return err == nil && st.State == space.FileStateDurable
	}), "device B never derived durable")

	stB, err := spB.Files().Status(ctx, big.FileId)
	require.NoError(t, err)
	assert.False(t, stB.Cached, "durable but nothing local yet")
	assert.Equal(t, ownerId, stB.ObjectId)

	// --- Pin: queue-executed background fetch to a complete local copy.
	var mu sync.Mutex
	var events []space.FileStatus
	unsub := spB.Files().SubscribeStatus(func(st space.FileStatus) {
		mu.Lock()
		events = append(events, st)
		mu.Unlock()
	})
	defer unsub()

	require.NoError(t, spB.Files().Pin(ctx, big.FileId))
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		st, err := spB.Files().Status(ctx, big.FileId)
		return err == nil && st.Cached
	}), "Pin never completed the local copy")

	// The pinned copy serves the content offline-style (all local now).
	fr, err := spB.Files().Open(ctx, big.FileId, space.VariantOriginal)
	require.NoError(t, err)
	buf := make([]byte, 4096)
	_, err = fr.Read(buf)
	require.NoError(t, err)
	require.NoError(t, fr.Close())
	assert.Equal(t, content[:4096], buf)

	mu.Lock()
	gotEvent := false
	for _, ev := range events {
		if ev.FileId == big.FileId {
			gotEvent = true
			break
		}
	}
	mu.Unlock()
	assert.True(t, gotEvent, "SubscribeStatus must deliver the pin transition")

	// Inline file on B: durable + cached by definition once the row syncs.
	require.True(t, waitFor(ctx, 60*time.Second, time.Second, func() bool {
		st, err := spB.Files().Status(ctx, small.FileId)
		return err == nil && st.State == space.FileStateDurable && st.Cached
	}), "inline status")

	// Retry on a durable file is a clean no-op.
	require.NoError(t, spB.Files().Retry(ctx, big.FileId))

	statsB, err := spB.Files().Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, space.FileStats{Total: 2, Durable: 2}, statsB)

	t.Logf("files-v2 status e2e OK: space=%s pinned=%s cdnRequests=%d", spA.Id(), big.FileId, cdn.Requests())
}
