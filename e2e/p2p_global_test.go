package e2e

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	mrand "math/rand"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	anyiroh "github.com/anyproto/any-sync/net/transport/iroh"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/go-iroh/endpointticket"
	goiroh "github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
	"github.com/tmc/go-iroh/relayserver"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	sdkp2p "github.com/anyproto/any-sync-sdk/p2p"
	"github.com/anyproto/any-sync-sdk/space"
)

// startEmbeddedRelay runs an iroh relay in-process (plain HTTP: the
// relay client maps http:// to a ws:// session, which is what
// InsecureRelay admits) and returns its URL. One relay per test: the
// endpoint ticket carries the relay URL, so a restarted relay on a new
// port would invalidate every published record.
func startEmbeddedRelay(t *testing.T) string {
	t.Helper()
	ts := httptest.NewServer(relayserver.New())
	t.Cleanup(ts.Close)
	return ts.URL
}

// globalOnlyConfig is a device with the LAN layer off and the global
// layer on: every direct peer it reaches goes through iroh.
func globalOnlyConfig(dataDir string, nodeConf []byte, relayURL string) config.Config {
	lanOff, globalOn := false, true
	return config.Config{
		Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: nodeConf},
		P2P: config.P2P{
			Enabled: &lanOff,
			Global: config.GlobalP2P{
				Enabled:       &globalOn,
				RelayURLs:     []string{relayURL},
				InsecureRelay: true,
			},
		},
	}
}

// loadLiveLocalNetwork returns the local-infra nodeconf as is — a LIVE
// network. Gated: the global e2e needs real sync nodes for its first
// phase (a fresh device restores through them, which is how it learns
// the other device's key-value record in the first place).
func loadLiveLocalNetwork(t *testing.T) []byte {
	t.Helper()
	if os.Getenv("ANYSYNC_E2E_LOCAL") != "1" {
		t.Skip("global p2p e2e: set ANYSYNC_E2E_LOCAL=1 (and provide e2e/local.yml) to run against a local network")
	}
	path := os.Getenv("ANYSYNC_E2E_LOCAL_NETWORK")
	if path == "" {
		path = "local.yml"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("global p2p e2e: local network config not readable at %s: %v", path, err)
	}
	return data
}

// globalPeer returns the global-layer status of peerId on sdk, or nil.
func globalPeer(sdk *anysyncsdk.SDK, peerId string) *sdkp2p.PeerStatus {
	for _, p := range sdk.P2PStatus().Global.Peers {
		if p.PeerId == peerId {
			return &p
		}
	}
	return nil
}

// TestE2E_GlobalP2PSync proves the internet-wide layer end to end with
// the LAN layer switched off, so every direct byte between the two
// devices travels over iroh through the embedded relay.
//
// Phase 1 (nodes up): device A creates a space and an object; device B
// — same account, empty data dir — restores through the nodes, learns
// A's key-value record and connects to it globally. Both sides must
// see each other as an active, connected global peer.
//
// Phase 2 (nodes dead): both SDKs restart against a nodeconf rewritten
// to dead ports and keep their data dirs. A writes; B converges. With
// the LAN layer off and no node reachable, the iroh connection is the
// only path left, so convergence itself is the proof — corroborated by
// the sync status (zero network peers, zero local peers, at least one
// global peer). A file attached offline travels the same way.
func TestE2E_GlobalP2PSync(t *testing.T) {
	if testing.Short() {
		t.Skip("global p2p e2e takes minutes; rerun without -short")
	}
	live := loadLiveLocalNetwork(t)
	relayURL := startEmbeddedRelay(t)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	// One account, two devices: distinct device keys, so distinct peer
	// ids and distinct iroh endpoints.
	providerA := newFixedSeedProvider(t)
	_, devB, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	providerB := &fixedSeedProvider{account: providerA.account, device: devB}
	dirA, dirB := t.TempDir(), t.TempDir()

	// ---- phase 1: nodes reachable ------------------------------------
	sdkA, err := anysyncsdk.Open(ctx, globalOnlyConfig(dirA, live, relayURL), providerA)
	require.NoError(t, err, "device A: Open")
	closedA := false
	defer func() {
		if !closedA {
			_ = sdkA.Close()
		}
	}()

	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "Global"})
	require.NoError(t, err)
	spaceId := spA.Id()
	typeId, err := spA.Types().Create(ctx, space.TypeCreateParams{Name: "GlobalType"})
	require.NoError(t, err)
	propId, err := spA.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Title", XKey: "title", Kind: space.PropertyKindString,
	})
	require.NoError(t, err)
	objId, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	_, err = spA.Properties().Set(ctx, objId, typeId, map[string]any{propId: "through-the-nodes"})
	require.NoError(t, err)

	sdkB, err := anysyncsdk.Open(ctx, globalOnlyConfig(dirB, live, relayURL), providerB)
	require.NoError(t, err, "device B: Open")
	closedB := false
	defer func() {
		if !closedB {
			_ = sdkB.Close()
		}
	}()

	peerA, peerB := sdkA.P2PStatus().PeerId, sdkB.P2PStatus().PeerId
	require.NotEqual(t, peerA, peerB, "devices of one account must have distinct peer ids")

	// Both endpoints must reach the relay before anything can be
	// published or dialed.
	require.True(t, waitFor(ctx, 60*time.Second, 200*time.Millisecond, func() bool {
		return sdkA.P2PStatus().Global.Ticket != "" && sdkB.P2PStatus().Global.Ticket != ""
	}), "endpoints never published a ticket: A=%+v B=%+v", sdkA.P2PStatus().Global, sdkB.P2PStatus().Global)
	gA := sdkA.P2PStatus().Global
	assert.True(t, gA.Enabled)
	assert.True(t, gA.RelayConnected, "home relay session must be up")
	assert.Equal(t, relayURL+"/", gA.HomeRelay)
	assert.NotEmpty(t, gA.EndpointId)

	// B restores the space list through the nodes.
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = sdkB.Spaces().SyncSpaceList(ctx)
		list, lErr := sdkB.Spaces().List(ctx)
		return lErr == nil && containsString(spaceIdsOnly(list), spaceId)
	}), "device B never learned about the space")

	spB, err := sdkB.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		rec, rErr := spB.Properties().Get(ctx, objId)
		return rErr == nil && rec != nil && rec.GetString(typeId, propId) == "through-the-nodes"
	}), "device B never converged on the object")

	// B learns A's record (through the key-value store, carried by the
	// nodes) and the connector dials it.
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		p := globalPeer(sdkB, peerA)
		return p != nil && p.Connected
	}), "device B never connected to A globally: %+v", sdkB.P2PStatus().Global)
	pA := globalPeer(sdkB, peerA)
	require.NotNil(t, pA)
	assert.Equal(t, "active", pA.Tier, "a peer with a fresh record is active")
	assert.False(t, pA.LastSeen.IsZero(), "LastSeen must carry the record timestamp")
	assert.Contains(t, pA.SpaceIds, spaceId)
	assert.Contains(t, pA.Sources, "global")

	// B publishes its own record once it joins, so A sees B too. A may
	// learn it as an accepted inbound connection, which the status
	// surface picks up on its one-minute liveness sweep.
	require.True(t, waitFor(ctx, 180*time.Second, 500*time.Millisecond, func() bool {
		p := globalPeer(sdkA, peerB)
		return p != nil && p.Connected
	}), "device A never saw B as a connected global peer: %+v", sdkA.P2PStatus().Global)

	// Per-space status counts the global peer on both sides.
	for name, sdk := range map[string]*anysyncsdk.SDK{"A": sdkA, "B": sdkB} {
		var st space.SpaceSyncStatus
		require.True(t, waitFor(ctx, 60*time.Second, 250*time.Millisecond, func() bool {
			st = sdk.Spaces().Status(spaceId)
			return st.GlobalPeers >= 1 && st.P2P == space.P2PStateConnected
		}), "device %s: global peer never reflected in sync status; last=%+v", name, st)
		assert.Zero(t, st.LocalPeers, "device %s: the LAN layer is off", name)
	}

	require.NoError(t, sdkA.Close())
	closedA = true
	require.NoError(t, sdkB.Close())
	closedB = true

	// ---- phase 2: nodes unreachable ----------------------------------
	dead, err := loadLocalNetwork()
	require.NoError(t, err)

	sdkA2, err := anysyncsdk.Open(ctx, globalOnlyConfig(dirA, dead, relayURL), providerA)
	require.NoError(t, err, "device A: reopen offline")
	defer func() { _ = sdkA2.Close() }()
	sdkB2, err := anysyncsdk.Open(ctx, globalOnlyConfig(dirB, dead, relayURL), providerB)
	require.NoError(t, err, "device B: reopen offline")
	defer func() { _ = sdkB2.Close() }()

	// The spaces must be loaded for the records to be re-read and the
	// connector to have a target.
	spA2, err := sdkA2.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)
	spB2, err := sdkB2.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)

	require.True(t, waitFor(ctx, 180*time.Second, 500*time.Millisecond, func() bool {
		a, b := globalPeer(sdkA2, peerB), globalPeer(sdkB2, peerA)
		return a != nil && a.Connected && b != nil && b.Connected
	}), "devices never reconnected globally after restart: A=%+v B=%+v",
		sdkA2.P2PStatus().Global, sdkB2.P2PStatus().Global)

	// A writes with nothing but the iroh connection available.
	objId2, err := spA2.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	_, err = spA2.Properties().Set(ctx, objId2, typeId, map[string]any{propId: "over-iroh"})
	require.NoError(t, err)

	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spB2.SyncHeads(ctx)
		rec, rErr := spB2.Properties().Get(ctx, objId2)
		return rErr == nil && rec != nil && rec.GetString(typeId, propId) == "over-iroh"
	}), "device B never converged over iroh; status=%+v", sdkB2.Spaces().Status(spaceId))

	// Nothing else could have carried it: no node peer, no LAN peer.
	stB := sdkB2.Spaces().Status(spaceId)
	assert.Zero(t, stB.NetworkPeers, "no node is reachable")
	assert.Zero(t, stB.LocalPeers, "the LAN layer is off")
	assert.GreaterOrEqual(t, stB.GlobalPeers, 1)
	assert.Equal(t, space.P2PStateConnected, stB.P2P)

	// Files travel the same path: the p2p file source picks the same
	// connection.
	content := make([]byte, 300_000) // above the inline tier → a real CAR fetch
	mrand.New(mrand.NewSource(190)).Read(content)
	fileInfo, err := spA2.Files().Attach(ctx, objId2, bytes.NewReader(content),
		space.AttachOpts{Name: "global.bin", Mime: "application/octet-stream"})
	require.NoError(t, err, "device A: Attach offline")
	require.False(t, fileInfo.Inline, "the file case needs a full-tier file")

	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spB2.SyncHeads(ctx)
		_, gErr := spB2.Files().Get(ctx, fileInfo.FileId)
		return gErr == nil
	}), "device B never saw the payloads row over iroh")

	fr, err := spB2.Files().Open(ctx, fileInfo.FileId, space.VariantOriginal)
	require.NoError(t, err, "device B: Open over iroh")
	got, err := io.ReadAll(fr)
	require.NoError(t, err)
	require.NoError(t, fr.Close())
	assert.True(t, bytes.Equal(content, got), "cross-device byte equality over iroh")
}

// TestE2E_GlobalP2PRefusesStranger proves the inbound gate: the
// key-value records are the allowlist, so an endpoint of an unrelated
// account — one that shares no space with A and therefore appears in
// nobody's records — is refused before the any-sync handshake even
// though A's relay address is public knowledge.
//
// The stranger is a raw iroh endpoint rather than a third SDK: an SDK
// would need A's ticket in its addr book to dial at all, and the only
// writer of that book is the record consumer. Dialing the endpoint
// directly is the same thing an attacker can do with a leaked ticket.
//
// Needs no external services (the nodeconf is dead), so it never skips.
func TestE2E_GlobalP2PRefusesStranger(t *testing.T) {
	if testing.Short() {
		t.Skip("global p2p e2e takes tens of seconds; rerun without -short")
	}
	dead, err := loadLocalNetwork()
	require.NoError(t, err)
	relayURL := startEmbeddedRelay(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	sdkA, err := anysyncsdk.Open(ctx, globalOnlyConfig(t.TempDir(), dead, relayURL), newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdkA.Close() })

	var ticket string
	require.True(t, waitFor(ctx, 60*time.Second, 200*time.Millisecond, func() bool {
		ticket = sdkA.P2PStatus().Global.Ticket
		return ticket != ""
	}), "device A never published a ticket")

	// A stranger endpoint on the same relay: a different account, no
	// shared space, no record anywhere.
	relayURLParsed, err := netaddr.ParseRelayURL(relayURL)
	require.NoError(t, err)
	stranger, err := goiroh.Bind(ctx,
		goiroh.WithALPNs(anyiroh.ALPN),
		goiroh.WithRelayMode(relay.ModeCustomURLs(relayURLParsed)),
		goiroh.WithoutIPTransports())
	require.NoError(t, err)
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = stranger.Shutdown(shutdownCtx)
	}()
	require.NoError(t, stranger.Online(ctx), "stranger must reach the relay before dialing")

	strangerPeerId, err := anyiroh.PeerIdFromEndpointId(stranger.ID())
	require.NoError(t, err)
	require.Nil(t, globalPeer(sdkA, strangerPeerId), "the stranger is in nobody's records")

	target, err := endpointticket.Decode(ticket)
	require.NoError(t, err)
	dialCtx, dialCancel := context.WithTimeout(ctx, 30*time.Second)
	defer dialCancel()
	conn, err := stranger.Connect(dialCtx, target, anyiroh.ALPN)
	if err == nil {
		// The filter runs after the QUIC handshake, so the refusal may
		// arrive as an immediate close instead of a failed Connect.
		defer func() { _ = conn.CloseWithError(0, "") }()
		select {
		case <-conn.Context().Done():
		case <-time.After(20 * time.Second):
			t.Fatal("A accepted a connection from an unknown peer")
		}
	}

	// And it never became a peer: the filter refuses before the
	// handshake, so nothing reaches the peer store.
	assert.Nil(t, globalPeer(sdkA, strangerPeerId), "a refused peer must not enter the peer store")
	for _, p := range sdkA.P2PStatus().Peers {
		assert.NotEqual(t, strangerPeerId, p.PeerId)
	}
}
