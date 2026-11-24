package e2e

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_GlobalP2PFileDialOnDemand proves a fetch reaches a holder the
// connector's cover left unconnected: B (MaxConnections 1) is connected
// only to C, which shares the space but lacks the file, and still reads a
// non-durable file only A holds.
//
// The cover keeps one connection per space. B picks C while A is away; A
// comes back with the nodes unreachable, so C is the only peer it knows
// until C hands it B's record.
func TestE2E_GlobalP2PFileDialOnDemand(t *testing.T) {
	if testing.Short() {
		t.Skip("global p2p e2e takes minutes; rerun without -short")
	}
	// No file nodes: the file stays non-durable, so only a peer serves it.
	live := withoutFileNodes(t, loadLiveLocalNetwork(t))
	dead, err := loadLocalNetwork()
	require.NoError(t, err)
	relayURL := startEmbeddedRelay(t)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	providerA := newFixedSeedProvider(t)
	device := func() *fixedSeedProvider {
		_, dev, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		return &fixedSeedProvider{account: providerA.account, device: dev}
	}
	providerB, providerC := device(), device()
	dirA := t.TempDir()

	open := func(name, dir string, nodeConf []byte, p *fixedSeedProvider, maxConns int) *anysyncsdk.SDK {
		cfg := globalOnlyConfig(dir, nodeConf, relayURL)
		cfg.P2P.Global.MaxConnections = maxConns
		sdk, err := anysyncsdk.Open(ctx, cfg, p)
		require.NoError(t, err, "device %s: Open", name)
		require.True(t, waitFor(ctx, 60*time.Second, 200*time.Millisecond, func() bool {
			return sdk.P2PStatus().Global.Ticket != ""
		}), "device %s never published a ticket", name)
		return sdk
	}
	connected := func(sdk *anysyncsdk.SDK, peerId string) bool {
		p := globalPeer(sdk, peerId)
		return p != nil && p.Connected
	}

	// A: the holder.
	sdkA := open("A", dirA, live, providerA, 0)
	closedA := false
	defer func() {
		if !closedA {
			_ = sdkA.Close()
		}
	}()
	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "DialOnDemand"})
	require.NoError(t, err)
	spaceId := spA.Id()
	typeId, err := spA.Types().Create(ctx, space.TypeCreateParams{Name: "Doc"})
	require.NoError(t, err)
	objId, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)
	content := make([]byte, 600_000)
	mrand.New(mrand.NewSource(7)).Read(content)
	file, err := spA.Files().Attach(ctx, objId, bytes.NewReader(content),
		space.AttachOpts{Name: "held.bin", Mime: "application/octet-stream"})
	require.NoError(t, err)
	require.False(t, file.Inline, "the case needs a full-tier file")
	require.False(t, file.Durable)
	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = spA.SyncHeads(ctx)
	peerA := sdkA.P2PStatus().PeerId

	// C: shares the space, never reads the file.
	sdkC := open("C", t.TempDir(), live, providerC, 0)
	defer func() { _ = sdkC.Close() }()
	peerC := sdkC.P2PStatus().PeerId
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = sdkC.Spaces().SyncSpaceList(ctx)
		list, lErr := sdkC.Spaces().List(ctx)
		return lErr == nil && containsString(spaceIdsOnly(list), spaceId)
	}), "device C never learned the space")
	_, err = sdkC.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		return globalPeer(sdkA, peerC) != nil && globalPeer(sdkC, peerA) != nil
	}), "A and C never learned each other's records")

	// A leaves, so B's cover can only settle on C.
	require.NoError(t, sdkA.Close())
	closedA = true

	// B: the fetcher.
	sdkB := open("B", t.TempDir(), live, providerB, 1)
	defer func() { _ = sdkB.Close() }()
	peerB := sdkB.P2PStatus().PeerId
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = sdkB.Spaces().SyncSpaceList(ctx)
		list, lErr := sdkB.Spaces().List(ctx)
		return lErr == nil && containsString(spaceIdsOnly(list), spaceId)
	}), "device B never learned the space")
	spB, err := sdkB.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		_, gErr := spB.Files().Get(ctx, file.FileId)
		return gErr == nil
	}), "device B never saw the payloads row")
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		return connected(sdkB, peerC) && globalPeer(sdkB, peerA) != nil
	}), "B never connected to C or never learned A: %+v", sdkB.P2PStatus().Global)
	coveredAt := time.Now()
	t.Logf("peers: A=%s B=%s C=%s", peerA, peerB, peerC)

	// A comes back cut off from the nodes: it dials C, the one peer it
	// knows, and learns B's record from C afterwards.
	sdkA = open("A", dirA, dead, providerA, 0)
	closedA = false
	_, err = sdkA.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)
	require.True(t, waitFor(ctx, 120*time.Second, 200*time.Millisecond, func() bool {
		return connected(sdkA, peerC)
	}), "A never reconnected to C: %+v", sdkA.P2PStatus().Global)
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		return globalPeer(sdkA, peerB) != nil
	}), "A never learned B's record: %+v", sdkA.P2PStatus().Global)

	// A dial B's cover tried while A was away keeps demands off A this long.
	if pA := globalPeer(sdkB, peerA); pA != nil && pA.Failures > 0 {
		if wait := 31*time.Second - time.Since(coveredAt); wait > 0 {
			t.Logf("B failed %d dial(s) to A while it was away; waiting %s", pA.Failures, wait.Round(time.Second))
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
	}

	info, err := spB.Files().Get(ctx, file.FileId)
	require.NoError(t, err)
	require.False(t, info.Durable, "only a peer can serve the file")
	require.False(t, info.Cached)
	require.True(t, connected(sdkB, peerC))
	require.False(t, connected(sdkB, peerA), "B must not be connected to A before the read: B=%+v A=%+v",
		sdkB.P2PStatus().Global.Peers, sdkA.P2PStatus().Global.Peers)
	require.False(t, connected(sdkA, peerB), "A must not be connected to B before the read")

	start := time.Now()
	got, err := readFile(ctx, spB, file.FileId)
	elapsed := time.Since(start)
	require.NoError(t, err, "B: read of a file only an unconnected peer holds")
	require.True(t, bytes.Equal(content, got), "byte equality over the dialed peer")
	t.Logf("demand-dial read took %s", elapsed.Round(time.Millisecond))
	require.True(t, connected(sdkB, peerA), "the read dialed A")
}
