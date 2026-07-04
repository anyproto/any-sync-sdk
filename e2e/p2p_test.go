package e2e

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	sdkp2p "github.com/anyproto/any-sync-sdk/p2p"
	"github.com/anyproto/any-sync-sdk/space"
)

// virtualLAN is an in-process discovery bus standing in for mDNS: two
// SDK instances in one test process can't reliably see each other over
// real multicast (loopback isn't multicast-capable on CI), so the fake
// driver publishes announcements here and browsers get them delivered
// as loopback addresses.
type virtualLAN struct {
	mu      sync.Mutex
	peers   map[string]sdkp2p.Announcement
	subs    map[int]func(sdkp2p.DiscoveredPeer)
	nextSub int
}

func newVirtualLAN() *virtualLAN {
	return &virtualLAN{
		peers: map[string]sdkp2p.Announcement{},
		subs:  map[int]func(sdkp2p.DiscoveredPeer){},
	}
}

func (l *virtualLAN) announce(a sdkp2p.Announcement) {
	l.mu.Lock()
	l.peers[a.PeerId] = a
	subs := make([]func(sdkp2p.DiscoveredPeer), 0, len(l.subs))
	for _, s := range l.subs {
		subs = append(subs, s)
	}
	l.mu.Unlock()
	for _, s := range subs {
		s(discovered(a))
	}
}

func (l *virtualLAN) subscribe(found func(sdkp2p.DiscoveredPeer)) (unsub func()) {
	l.mu.Lock()
	id := l.nextSub
	l.nextSub++
	l.subs[id] = found
	current := make([]sdkp2p.Announcement, 0, len(l.peers))
	for _, a := range l.peers {
		current = append(current, a)
	}
	l.mu.Unlock()
	for _, a := range current {
		found(discovered(a))
	}
	return func() {
		l.mu.Lock()
		delete(l.subs, id)
		l.mu.Unlock()
	}
}

func discovered(a sdkp2p.Announcement) sdkp2p.DiscoveredPeer {
	return sdkp2p.DiscoveredPeer{
		PeerId: a.PeerId,
		Addrs:  []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(a.Port))},
	}
}

// lanDriver adapts the virtualLAN to the sdkp2p.Driver contract.
type lanDriver struct{ lan *virtualLAN }

func (d lanDriver) Announce(ctx context.Context, a sdkp2p.Announcement) error {
	d.lan.announce(a)
	<-ctx.Done()
	return nil
}

func (d lanDriver) Browse(ctx context.Context, _ string, found func(sdkp2p.DiscoveredPeer), _ func(string)) error {
	unsub := d.lan.subscribe(found)
	defer unsub()
	<-ctx.Done()
	return nil
}

// TestE2E_P2POfflineSync proves the whole p2p slice with NO reachable
// network: one account on two devices, an unreachable nodeconf
// (e2e/local.yml points at dead loopback ports), and the fake LAN
// driver. Device A creates a space + object; device B must discover A
// over the virtual LAN, handshake via SpaceExchange, fold A into its
// peer managers, and converge — while sync status reports the p2p
// connection and zero network peers.
//
// Because device B boots with an EMPTY data dir only after A wrote
// everything, this is also the offline COLD-RESTORE path: restoring an
// account on a new device purely from a LAN peer — space list via the
// derived tech space, then each space via SpacePull — with no sync
// node or coordinator reachable at any point.
//
// Unlike the staging suite this test needs no external services at
// all, so it never skips.
func TestE2E_P2POfflineSync(t *testing.T) {
	if testing.Short() {
		t.Skip("p2p e2e takes tens of seconds; rerun without -short")
	}
	yaml, err := loadLocalNetwork()
	require.NoError(t, err)

	lan := newVirtualLAN()
	sdkp2p.SetDriverFactory(func() sdkp2p.Driver { return lanDriver{lan} })
	t.Cleanup(func() { sdkp2p.SetDriverFactory(nil) })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// One account, two devices → distinct peer ids (p2p filters self
	// by peerId, so the device keys MUST differ).
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

	sdkA, err := anysyncsdk.Open(ctx, newCfg(), providerA)
	require.NoError(t, err, "device A: Open")
	t.Cleanup(func() { _ = sdkA.Close() })

	// Device A writes everything before B even boots — a cold p2p pull.
	sp, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "P2P"})
	require.NoError(t, err, "device A: Create must work offline")
	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "P2PType"})
	require.NoError(t, err)
	propId, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Title", XKey: "title", Kind: space.PropertyKindString,
	})
	require.NoError(t, err)
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	_, err = sp.Properties().Set(ctx, objId, typeId, map[string]any{propId: "over-the-lan"})
	require.NoError(t, err)
	spaceId := sp.Id()

	sdkB, err := anysyncsdk.Open(ctx, newCfg(), providerB)
	require.NoError(t, err, "device B: Open")
	t.Cleanup(func() { _ = sdkB.Close() })

	// Same account, but every device MUST have its own peer id — p2p
	// filters "self" by peerId, so devices sharing one can never pair.
	require.Equal(t, sdkA.Account().Id(), sdkB.Account().Id())
	require.NotEqual(t, sdkA.P2PStatus().PeerId, sdkB.P2PStatus().PeerId,
		"devices of one account must have distinct peer ids")

	// Discovery + exchange must record each other.
	require.True(t, waitFor(ctx, 30*time.Second, 100*time.Millisecond, func() bool {
		return len(sdkA.P2PStatus().Peers) >= 1 && len(sdkB.P2PStatus().Peers) >= 1
	}), "devices never handshaked: A=%+v B=%+v", sdkA.P2PStatus(), sdkB.P2PStatus())

	// The space list travels via the tech space — over the LAN only.
	require.True(t, waitFor(ctx, 60*time.Second, 250*time.Millisecond, func() bool {
		_ = sdkB.Spaces().SyncSpaceList(ctx)
		list, lErr := sdkB.Spaces().List(ctx)
		if lErr != nil {
			return false
		}
		return containsString(spaceIdsOnly(list), spaceId)
	}), "device B never learned about the space over p2p")

	var spB space.Space
	require.True(t, waitFor(ctx, 30*time.Second, 100*time.Millisecond, func() bool {
		spB, err = sdkB.Spaces().Get(ctx, spaceId)
		return err == nil
	}), "device B: Spaces().Get(%s): %v", spaceId, err)

	// Object + property must converge over the LAN.
	require.True(t, waitFor(ctx, 60*time.Second, 250*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		rec, rErr := spB.Properties().Get(ctx, objId)
		if rErr != nil || rec == nil {
			return false
		}
		return rec.GetString(typeId, propId) == "over-the-lan"
	}), "device B never converged on the object over p2p")

	// Sync status must expose the p2p slice: connected local peer,
	// zero network peers (everything is unreachable). A learns that B
	// now holds the space via B's post-pull re-handshake, which is
	// async — poll briefly.
	for name, sdk := range map[string]*anysyncsdk.SDK{"A": sdkA, "B": sdkB} {
		var st space.SpaceSyncStatus
		require.True(t, waitFor(ctx, 15*time.Second, 100*time.Millisecond, func() bool {
			st = sdk.Spaces().Status(spaceId)
			return st.P2P == space.P2PStateConnected && st.LocalPeers >= 1
		}), "device %s: p2p never reflected in sync status; last=%+v", name, st)
		assert.Zero(t, st.NetworkPeers, "device %s: NetworkPeers must be 0 offline", name)
	}

	// Debug surface agrees.
	debugA := sdkA.P2PStatus()
	assert.True(t, debugA.Enabled)
	assert.True(t, debugA.ListenerStarted)
	assert.Equal(t, space.P2PStateConnected, debugA.State, "%+v", debugA)
	require.Len(t, debugA.Peers, 1)
	assert.True(t, debugA.Peers[0].Connected)
	assert.Contains(t, debugA.Peers[0].SpaceIds, spaceId)
}

// TestE2E_P2PRealMDNS is the same offline two-device scenario but over
// the real dnssd driver — actual mDNS multicast on this machine's LAN
// interface. Gated behind ANY_SDK_E2E_MDNS=1 because it needs a
// multicast-capable interface (CI loopback won't do) and puts real
// announcements on the local network. A unique per-run service name
// keeps concurrent developers on one LAN from cross-talking.
func TestE2E_P2PRealMDNS(t *testing.T) {
	if os.Getenv("ANY_SDK_E2E_MDNS") == "" {
		t.Skip("set ANY_SDK_E2E_MDNS=1 to run the real-mDNS p2p test")
	}
	yaml, err := loadLocalNetwork()
	require.NoError(t, err)

	var rnd [4]byte
	_, err = rand.Read(rnd[:])
	require.NoError(t, err)
	serviceName := fmt.Sprintf("_anytest%x._tcp", rnd)
	t.Logf("using service name %s", serviceName)

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
			P2P:     config.P2P{ServiceName: serviceName},
		}
	}

	sdkA, err := anysyncsdk.Open(ctx, newCfg(), providerA)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdkA.Close() })
	sp, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "mdns"})
	require.NoError(t, err)
	spaceId := sp.Id()

	sdkB, err := anysyncsdk.Open(ctx, newCfg(), providerB)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdkB.Close() })

	// Real multicast: discovery can take a few seconds.
	require.True(t, waitFor(ctx, 60*time.Second, 250*time.Millisecond, func() bool {
		return len(sdkA.P2PStatus().Peers) >= 1 && len(sdkB.P2PStatus().Peers) >= 1
	}), "mDNS discovery never connected the two instances: A=%+v B=%+v", sdkA.P2PStatus(), sdkB.P2PStatus())

	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = sdkB.Spaces().SyncSpaceList(ctx)
		list, lErr := sdkB.Spaces().List(ctx)
		return lErr == nil && containsString(spaceIdsOnly(list), spaceId)
	}), "device B never learned about the space over real mDNS p2p")
}

// TestE2E_P2PDisabled verifies the opt-out: no listener, no discovery,
// NotPossible in both status surfaces.
func TestE2E_P2PDisabled(t *testing.T) {
	yaml, err := loadLocalNetwork()
	require.NoError(t, err)

	disabled := false
	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		P2P:     config.P2P{Enabled: &disabled},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	st := sdk.P2PStatus()
	assert.False(t, st.Enabled)
	assert.False(t, st.ListenerStarted)
	assert.Zero(t, st.Port)
	assert.Equal(t, space.P2PStateNotPossible, st.State)
	assert.Empty(t, st.Peers)
}

// loadLocalNetwork reads the checked-in unreachable nodeconf (dead
// loopback ports). Unlike staging.yml it is part of the repo, so the
// offline p2p tests never skip.
func loadLocalNetwork() ([]byte, error) {
	return os.ReadFile("local.yml")
}
