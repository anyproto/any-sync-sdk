package e2e

import (
	"bytes"
	"context"
	"io"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonfile/fileproto/fileprotov2"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_FilesV2_RealPublicRead proves the PRODUCTION download path
// with no test scaffolding: device B carries no PublicReadBaseUrl
// override, resolves the base from the network's fileV2 node via the
// Info RPC, persists it, and Range-reads the real object store
// directly. Skips when the network advertises no public base (private
// deployment) — the CDN-stand-in e2e covers that mode.
func TestE2E_FilesV2_RealPublicRead(t *testing.T) {
	netYaml, fileV2Peers, _ := loadLocalFilesV2Network(t)
	if testing.Short() {
		t.Skip("files-v2 e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	provider := newFixedSeedProvider(t)

	cfgA := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: netYaml},
	}
	sdkA, err := anysyncsdk.Open(ctx, cfgA, provider)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdkA.Close() })

	// Gate on the network actually advertising a public base.
	var info *fileprotov2.InfoResponse
	require.True(t, waitFor(ctx, 60*time.Second, time.Second, func() bool {
		return doFileV2(ctx, sdkA, fileV2Peers, func(cl fileprotov2.DRPCFileV2Client) error {
			var err error
			info, err = cl.Info(ctx, &fileprotov2.InfoRequest{})
			return err
		}) == nil
	}), "filenode2 Info never answered")
	if info.PublicReadBaseUrl == "" {
		t.Skip("network advertises no publicReadBaseUrl; covered by the CDN-stand-in e2e")
	}
	t.Logf("real public base: %s", info.PublicReadBaseUrl)

	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "FilesV2PublicRead"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}
	typeId, _ := setupMovieType(t, ctx, spA)
	ownerId, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = spA.SyncHeads(ctx)

	content := make([]byte, 2_200_000)
	mrand.New(mrand.NewSource(32)).Read(content)
	fi, err := spA.Files().Attach(ctx, ownerId, bytes.NewReader(content),
		space.AttachOpts{Name: "real.bin", Mime: "application/octet-stream"})
	require.NoError(t, err)
	require.True(t, fi.Durable)
	_ = spA.SyncHeads(ctx)

	// Device B: NO Files config at all — the production configuration.
	cfgB := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: netYaml},
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
		for _, si := range list {
			if si.Id == spA.Id() {
				return true
			}
		}
		return false
	}), "device B never saw the space")
	spB, err := sdkB.Spaces().Get(ctx, spA.Id())
	require.NoError(t, err)
	require.True(t, waitFor(ctx, 120*time.Second, time.Second, func() bool {
		_ = spB.SyncHeads(ctx)
		st, err := spB.Files().Status(ctx, fi.FileId)
		return err == nil && st.State == space.FileStateDurable
	}), "device B never derived durable")

	// Full read straight off the real object store.
	fr, err := spB.Files().Open(ctx, fi.FileId, space.VariantOriginal)
	require.NoError(t, err, "Open must resolve the base via Info and read the real bucket")
	got, err := io.ReadAll(fr)
	require.NoError(t, err)
	require.NoError(t, fr.Close())
	require.True(t, bytes.Equal(content, got), "byte equality through the production path")

	// Targeted seek against the real bucket.
	fr2, err := spB.Files().Open(ctx, fi.FileId, space.VariantOriginal)
	require.NoError(t, err)
	off := int64(1_600_000)
	_, err = fr2.Seek(off, io.SeekStart)
	require.NoError(t, err)
	buf := make([]byte, 32<<10)
	_, err = io.ReadFull(fr2, buf)
	require.NoError(t, err)
	require.NoError(t, fr2.Close())
	require.True(t, bytes.Equal(content[off:off+int64(len(buf))], buf))

	t.Logf("files-v2 REAL public-read e2e OK: space=%s file=%s base=%s",
		spA.Id(), fi.FileId, info.PublicReadBaseUrl)
}
