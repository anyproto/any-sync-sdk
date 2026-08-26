package e2e

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tmc/go-iroh/dnsserver"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/p2p/account"
	"github.com/anyproto/any-sync-sdk/space"
)

// startEmbeddedPkarrRelay runs a pkarr relay in-process (plain HTTP,
// which InsecurePkarr admits) and returns its URL.
func startEmbeddedPkarrRelay(t *testing.T) string {
	t.Helper()
	ts := httptest.NewServer(dnsserver.New())
	t.Cleanup(ts.Close)
	return ts.URL
}

// accountConfig is a device with the LAN layer off, the global layer on
// and the account record on: own devices are found through the record.
func accountConfig(dataDir string, nodeConf []byte, relayURL, pkarrURL string) config.Config {
	cfg := globalOnlyConfig(dataDir, nodeConf, relayURL)
	cfg.P2P.Global.PkarrRelayURLs = []string{pkarrURL}
	cfg.P2P.Global.InsecurePkarr = true
	return cfg
}

// accountRecordKeys derives the account's discovery keys the way the SDK
// does from the provider's account seed.
func accountRecordKeys(t *testing.T, accountSeed []byte) *account.Keys {
	t.Helper()
	identity, err := crypto.NewSigningEd25519PrivKeyFromBytes(accountSeed)
	require.NoError(t, err)
	sign, enc, err := crypto.DeriveDiscoveryKeys(identity)
	require.NoError(t, err)
	keys, err := account.NewKeys(sign, enc)
	require.NoError(t, err)
	return keys
}

// TestE2E_AccountRecovery proves cold recovery through the account
// record with no node reachable at any point: device A publishes itself
// into the record under the account-derived key; device B — same
// account, empty data dir, dead nodeconf — resolves the record, dials A
// through the relay, is admitted by identity, and pulls the tech space
// and a shared space's objects from A alone. A third account can
// neither read A's record nor appear in A's peers.
//
// Needs no external services (the nodeconf is dead), so it never skips.
func TestE2E_AccountRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("account recovery e2e takes a minute; rerun without -short")
	}
	dead, err := loadLocalNetwork()
	require.NoError(t, err)
	relayURL := startEmbeddedRelay(t)
	pkarrURL := startEmbeddedPkarrRelay(t)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	providerA := newFixedSeedProvider(t)
	_, devB, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	providerB := &fixedSeedProvider{account: providerA.account, device: devB}

	// ---- device A: creates content and registers itself ---------------
	sdkA, err := anysyncsdk.Open(ctx, accountConfig(t.TempDir(), dead, relayURL, pkarrURL), providerA)
	require.NoError(t, err, "device A: Open")
	t.Cleanup(func() { _ = sdkA.Close() })

	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "Recovered"})
	require.NoError(t, err)
	spaceId := spA.Id()
	typeId, err := spA.Types().Create(ctx, space.TypeCreateParams{Name: "RecoveredType"})
	require.NoError(t, err)
	propId, err := spA.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Title", XKey: "title", Kind: space.PropertyKindString,
	})
	require.NoError(t, err)
	objId, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	_, err = spA.Properties().Set(ctx, objId, typeId, map[string]any{propId: "through-a-sibling"})
	require.NoError(t, err)
	assert.True(t, spA.Info().Advertise, "advertising is on by default")

	peerA := sdkA.P2PStatus().PeerId
	keys := accountRecordKeys(t, providerA.account)
	client, err := account.NewClient([]string{pkarrURL})
	require.NoError(t, err)

	// A's record names A with its home relay.
	var rec account.Record
	require.True(t, waitFor(ctx, 60*time.Second, 250*time.Millisecond, func() bool {
		packet, rErr := client.Resolve(ctx, keys.Public())
		if rErr != nil || packet == nil {
			return false
		}
		rec, rErr = account.Open(keys, packet)
		if rErr != nil {
			return false
		}
		for _, d := range rec.Devices {
			if d.PeerId == peerA {
				return true
			}
		}
		return false
	}), "device A never registered itself in the account record")
	for _, d := range rec.Devices {
		if d.PeerId == peerA {
			assert.Equal(t, relayURL+"/", d.Relay)
			assert.WithinDuration(t, time.Now(), d.LastSeen, time.Minute)
		}
	}
	assert.True(t, sdkA.P2PStatus().Global.Account.Enabled)

	// Another account cannot read the record: its derived key is another
	// address, and the packet under A's address does not decrypt for it.
	strangerKeys := accountRecordKeys(t, newFixedSeedProvider(t).account)
	packet, err := client.Resolve(ctx, strangerKeys.Public())
	require.NoError(t, err)
	assert.Nil(t, packet, "nothing under a stranger's address")
	packet, err = client.Resolve(ctx, keys.Public())
	require.NoError(t, err)
	require.NotNil(t, packet)
	_, err = account.Open(strangerKeys, packet)
	assert.ErrorIs(t, err, account.ErrWrongKey)

	// ---- device B: empty data dir, dead nodes, only the record ---------
	sdkB, err := anysyncsdk.Open(ctx, accountConfig(t.TempDir(), dead, relayURL, pkarrURL), providerB)
	require.NoError(t, err, "device B: Open")
	t.Cleanup(func() { _ = sdkB.Close() })
	peerB := sdkB.P2PStatus().PeerId
	require.NotEqual(t, peerA, peerB)

	// B resolves A from the record and connects; A admits B by identity
	// before B's own entry propagates.
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		p := globalPeer(sdkB, peerA)
		return p != nil && p.Connected
	}), "device B never connected to A through the account record: %+v", sdkB.P2PStatus().Global)
	pA := globalPeer(sdkB, peerA)
	require.NotNil(t, pA)
	assert.Contains(t, pA.Sources, "account")
	assert.Equal(t, "active", pA.Tier)

	require.True(t, waitFor(ctx, 60*time.Second, 500*time.Millisecond, func() bool {
		p := globalPeer(sdkA, peerB)
		return p != nil && p.Connected
	}), "device A never saw B: %+v", sdkA.P2PStatus().Global)
	assert.Contains(t, globalPeer(sdkA, peerB).Sources, "account")

	// The tech space converges from A: B learns the space list.
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		list, lErr := sdkB.Spaces().List(ctx)
		return lErr == nil && containsString(spaceIdsOnly(list), spaceId)
	}), "device B never learned the space list from A; status=%+v", sdkB.Spaces().Status(sdkB.TechSpaceId()))

	// The space itself is pulled from A and its object arrives.
	spB, err := sdkB.Spaces().Get(ctx, spaceId)
	require.NoError(t, err, "device B: pull the space from A")
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		row, rErr := spB.Properties().Get(ctx, objId)
		return rErr == nil && row != nil && row.GetString(typeId, propId) == "through-a-sibling"
	}), "device B never converged on the object; status=%+v", sdkB.Spaces().Status(spaceId))

	st := sdkB.Spaces().Status(spaceId)
	assert.Zero(t, st.NetworkPeers, "no node is reachable")
	assert.Zero(t, st.LocalPeers, "the LAN layer is off")
	assert.GreaterOrEqual(t, st.GlobalPeers, 1)
	assert.Equal(t, space.P2PStateConnected, st.P2P)

	// B registers itself too: the record now names both devices.
	require.True(t, waitFor(ctx, 60*time.Second, 500*time.Millisecond, func() bool {
		packet, rErr := client.Resolve(ctx, keys.Public())
		if rErr != nil || packet == nil {
			return false
		}
		rec, rErr := account.Open(keys, packet)
		if rErr != nil {
			return false
		}
		var a, b bool
		for _, d := range rec.Devices {
			a = a || d.PeerId == peerA
			b = b || d.PeerId == peerB
		}
		return a && b
	}), "device B never registered itself")

	// Advertising off hides A from the space's other members, not from
	// its siblings: the switch syncs to B and the record path stays.
	require.NoError(t, spA.SetAdvertise(ctx, false))
	assert.False(t, spA.Info().Advertise)
	require.True(t, waitFor(ctx, 60*time.Second, 500*time.Millisecond, func() bool {
		return !spB.Info().Advertise
	}), "the advertising switch never synced to B")
	assert.True(t, globalPeer(sdkB, peerA).Connected)
	require.NoError(t, spA.SetAdvertise(ctx, true))
	assert.True(t, spA.Info().Advertise)
}
