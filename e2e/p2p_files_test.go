package e2e

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	sdkp2p "github.com/anyproto/any-sync-sdk/p2p"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_P2PFileFetch proves file fetch over the LAN with NO reachable
// file node and NO public CDN: device A attaches a file (its blocks land
// in A's local CAR store; durability is deferred since the nodes are
// dead), and device B — same account, empty storage, connected only over
// the in-process fake LAN driver — resolves the synced payloads row and
// reads the decrypted content back through Files().Open, sourced entirely
// from A over the FileP2P ObjectRead path (FileCheck → seed → verify →
// CFB decrypt).
//
// Needs no external services, so it never skips.
func TestE2E_P2PFileFetch(t *testing.T) {
	if testing.Short() {
		t.Skip("p2p file e2e takes tens of seconds; rerun without -short")
	}
	yaml, err := loadLocalNetwork() // dead nodeconf: no coordinator, no file nodes
	require.NoError(t, err)

	lan := newVirtualLAN()
	sdkp2p.SetDriverFactory(func() sdkp2p.Driver { return lanDriver{lan} })
	t.Cleanup(func() { sdkp2p.SetDriverFactory(nil) })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	providerA := newFixedSeedProvider(t)
	_, devB, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	providerB := &fixedSeedProvider{account: providerA.account, device: devB}

	newCfg := func() config.Config {
		return config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
		}
	}

	// Device A: create a space + object, attach a full-tier file.
	sdkA, err := anysyncsdk.Open(ctx, newCfg(), providerA)
	require.NoError(t, err, "device A: Open")
	t.Cleanup(func() { _ = sdkA.Close() })

	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "P2PFiles"})
	require.NoError(t, err)
	typeId, err := spA.Types().Create(ctx, space.TypeCreateParams{Name: "Doc"})
	require.NoError(t, err)
	objId, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)

	content := make([]byte, 300_000) // well above the inline tier → real CAR
	mrand.New(mrand.NewSource(48)).Read(content)
	fileInfo, err := spA.Files().Attach(ctx, objId, bytes.NewReader(content),
		space.AttachOpts{Name: "doc.bin", Mime: "application/octet-stream"})
	require.NoError(t, err, "device A: Attach (must work offline)")
	require.False(t, fileInfo.Inline, "test needs a full-tier (non-inline) file")
	require.False(t, fileInfo.Durable, "no file node reachable, so the file is non-durable")
	spaceId := spA.Id()

	// Device B: empty storage, same account.
	sdkB, err := anysyncsdk.Open(ctx, newCfg(), providerB)
	require.NoError(t, err, "device B: Open")
	t.Cleanup(func() { _ = sdkB.Close() })

	// p2p handshake.
	require.True(t, waitFor(ctx, 30*time.Second, 100*time.Millisecond, func() bool {
		return len(sdkA.P2PStatus().Peers) >= 1 && len(sdkB.P2PStatus().Peers) >= 1
	}), "devices never handshaked")

	// The space + payloads row travel over the LAN.
	require.True(t, waitFor(ctx, 60*time.Second, 250*time.Millisecond, func() bool {
		_ = sdkB.Spaces().SyncSpaceList(ctx)
		list, lErr := sdkB.Spaces().List(ctx)
		return lErr == nil && containsString(spaceIdsOnly(list), spaceId)
	}), "device B never learned the space over p2p")

	spB, err := sdkB.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)

	var gotInfo space.FileInfo
	require.True(t, waitFor(ctx, 60*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		gotInfo, err = spB.Files().Get(ctx, fileInfo.FileId)
		return err == nil
	}), "device B never saw the payloads row")
	require.Equal(t, fileInfo.RootCid, gotInfo.RootCid)
	require.False(t, gotInfo.Cached, "nothing local before the first read")

	// Read the whole file — sourced from A over p2p (no node, no CDN).
	fr, err := spB.Files().Open(ctx, fileInfo.FileId, space.VariantOriginal)
	require.NoError(t, err, "device B: Open over p2p")
	require.EqualValues(t, len(content), fr.Size())
	got, err := io.ReadAll(fr)
	require.NoError(t, err)
	require.NoError(t, fr.Close())
	require.True(t, bytes.Equal(content, got), "cross-device byte equality over p2p")

	// A targeted seek on a fresh handle still resolves holes over p2p.
	fr2, err := spB.Files().Open(ctx, fileInfo.FileId, space.VariantOriginal)
	require.NoError(t, err)
	off := int64(200_000)
	_, err = fr2.Seek(off, io.SeekStart)
	require.NoError(t, err)
	buf := make([]byte, 32<<10)
	_, err = io.ReadFull(fr2, buf)
	require.NoError(t, err)
	require.NoError(t, fr2.Close())
	require.True(t, bytes.Equal(content[off:off+int64(len(buf))], buf), "seek read over p2p")
}
