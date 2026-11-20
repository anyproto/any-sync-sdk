package e2e

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	sdkp2p "github.com/anyproto/any-sync-sdk/p2p"
	"github.com/anyproto/any-sync-sdk/space"
)

// msgCollector accumulates PubSubMessages from a Subscribe callback.
type msgCollector struct {
	mu   sync.Mutex
	msgs []space.PubSubMessage
}

func (c *msgCollector) cb(m space.PubSubMessage) {
	c.mu.Lock()
	c.msgs = append(c.msgs, m)
	c.mu.Unlock()
}

func (c *msgCollector) find(match func(space.PubSubMessage) bool) (space.PubSubMessage, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.msgs {
		if match(m) {
			return m, true
		}
	}
	return space.PubSubMessage{}, false
}

// TestE2E_PubSubLoopbackAndErrors exercises the fully-offline slice of
// the pub/sub surface on one SDK: loopback delivery of own publishes
// (space scope and the account-wide tech-space surface) and every
// synchronous error path — all with an unreachable network.
func TestE2E_PubSubLoopbackAndErrors(t *testing.T) {
	yaml, err := loadLocalNetwork()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "PubSub"})
	require.NoError(t, err, "Create must work offline")

	// Space-scope loopback: own publish delivers to the local
	// subscriber with the verified own identity and Self=true.
	var got msgCollector
	cancelSub, err := sp.PubSub().Subscribe("chat/>", got.cb)
	require.NoError(t, err)
	payload := []byte("hello")
	require.NoError(t, sp.PubSub().Publish(ctx, "chat/room1", payload))
	require.True(t, waitFor(ctx, 10*time.Second, 50*time.Millisecond, func() bool {
		_, ok := got.find(func(m space.PubSubMessage) bool { return m.Topic == "chat/room1" })
		return ok
	}), "own publish never delivered to local subscriber")
	m, _ := got.find(func(m space.PubSubMessage) bool { return m.Topic == "chat/room1" })
	assert.Equal(t, sp.Id(), m.SpaceId)
	assert.Equal(t, sdk.Account().Id(), m.SenderIdentity)
	assert.True(t, m.Self)
	assert.Equal(t, payload, m.Payload)

	// Wildcard scoping: a non-matching topic must not reach the handler.
	require.NoError(t, sp.PubSub().Publish(ctx, "other/room1", nil))
	_, leaked := got.find(func(m space.PubSubMessage) bool { return m.Topic == "other/room1" })
	assert.False(t, leaked, "pattern chat/> must not match other/room1")

	// cancel is idempotent, and a cancelled subscription stops receiving.
	cancelSub()
	cancelSub()

	// Account-wide surface (tech space) loops back the same way.
	var acc msgCollector
	cancelAcc, err := sdk.PubSub().Subscribe("ev/*", acc.cb)
	require.NoError(t, err)
	t.Cleanup(cancelAcc)
	require.NoError(t, sdk.PubSub().Publish(ctx, "ev/ping", []byte("x")))
	require.True(t, waitFor(ctx, 10*time.Second, 50*time.Millisecond, func() bool {
		_, ok := acc.find(func(m space.PubSubMessage) bool { return m.Topic == "ev/ping" })
		return ok
	}), "account-wide publish never looped back")

	// Error paths.
	_, err = sp.PubSub().Subscribe("chat//bad", func(space.PubSubMessage) {})
	require.ErrorIs(t, err, space.ErrPubSubInvalidTopic)
	err = sp.PubSub().Publish(ctx, "chat/*", nil)
	require.ErrorIs(t, err, space.ErrPubSubInvalidTopic, "wildcards are invalid in concrete topics")
	err = sp.PubSub().Publish(ctx, "big/one", make([]byte, 65*1024))
	require.ErrorIs(t, err, space.ErrPubSubPayloadTooLarge)

	// The reserved acc/ namespace: own suffix publishes, foreign is
	// rejected before anything leaves the process.
	require.NoError(t, sp.PubSub().Publish(ctx, "acc/cursor/"+sdk.Account().Id(), []byte("me")))
	_, otherPub, err := anySyncCryptoGenerate()
	require.NoError(t, err)
	err = sp.PubSub().Publish(ctx, "acc/cursor/"+otherPub.Account(), []byte("spoof"))
	require.ErrorIs(t, err, space.ErrPubSubTopicNotOwned)
}

// TestE2E_PubSubLAN proves delivery between two devices of one account
// over the virtual LAN with NO reachable network: account-wide
// (tech-space) fan-out and space-scope messages both flow peer-to-peer.
func TestE2E_PubSubLAN(t *testing.T) {
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
	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "PubSubLAN"})
	require.NoError(t, err)
	spaceId := spA.Id()

	sdkB, err := anysyncsdk.Open(ctx, newCfg(), providerB)
	require.NoError(t, err, "device B: Open")
	t.Cleanup(func() { _ = sdkB.Close() })

	require.True(t, waitFor(ctx, 30*time.Second, 100*time.Millisecond, func() bool {
		return len(sdkA.P2PStatus().Peers) >= 1 && len(sdkB.P2PStatus().Peers) >= 1
	}), "devices never handshaked")

	// Account-wide fan-out over the LAN (tech space). Publish inside
	// the poll: pub/sub is at-most-once with no replay, so early sends
	// racing the interest propagation are simply lost by design.
	var accB msgCollector
	cancelAcc, err := sdkB.PubSub().Subscribe("ev/>", accB.cb)
	require.NoError(t, err)
	t.Cleanup(cancelAcc)
	seq := 0
	require.True(t, waitFor(ctx, 60*time.Second, 500*time.Millisecond, func() bool {
		seq++
		_ = sdkA.PubSub().Publish(ctx, "ev/lan", []byte(fmt.Sprintf("seq-%d", seq)))
		_, ok := accB.find(func(m space.PubSubMessage) bool { return m.Topic == "ev/lan" })
		return ok
	}), "device B never received A's account-wide publish over the LAN")
	// B only subscribed on B, so anything it collected crossed the LAN.
	// Self is account-scoped (both devices sign as the same account), so
	// a cross-device delivery still reads Self=true.
	m, _ := accB.find(func(m space.PubSubMessage) bool { return m.Topic == "ev/lan" })
	assert.Equal(t, sdkA.Account().Id(), m.SenderIdentity)
	assert.True(t, m.Self, "Self is account-scoped, not device-scoped")

	// Space scope: B pulls the space over the LAN first, then messages
	// flow both ways.
	require.True(t, waitFor(ctx, 60*time.Second, 250*time.Millisecond, func() bool {
		_ = sdkB.Spaces().SyncSpaceList(ctx)
		list, lErr := sdkB.Spaces().List(ctx)
		return lErr == nil && containsString(spaceIdsOnly(list), spaceId)
	}), "device B never learned about the space over p2p")
	var spB space.Space
	require.True(t, waitFor(ctx, 30*time.Second, 100*time.Millisecond, func() bool {
		spB, err = sdkB.Spaces().Get(ctx, spaceId)
		return err == nil
	}), "device B: Spaces().Get(%s): %v", spaceId, err)

	var spaceB msgCollector
	cancelSp, err := spB.PubSub().Subscribe("chat/*", spaceB.cb)
	require.NoError(t, err)
	t.Cleanup(cancelSp)
	seq = 0
	require.True(t, waitFor(ctx, 60*time.Second, 500*time.Millisecond, func() bool {
		seq++
		_ = spA.PubSub().Publish(ctx, "chat/lan", []byte(fmt.Sprintf("s-%d", seq)))
		_, ok := spaceB.find(func(m space.PubSubMessage) bool { return m.Topic == "chat/lan" })
		return ok
	}), "device B never received A's space-scope publish over the LAN")
}

// TestE2E_PubSubStagingRelay proves the node-relay path against
// staging: two DISTINCT accounts in a shared space, no LAN driver, so
// every message travels client → responsible node (pubsubrelay) →
// client. Also asserts the verified sender identity crosses accounts
// correctly (Self=false).
func TestE2E_PubSubStagingRelay(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	mkSDK := func(name string) *anysyncsdk.SDK {
		t.Helper()
		cfg := config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
		}
		sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
		require.NoError(t, err, "%s: Open", name)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk
	}

	owner := mkSDK("owner")
	joiner := mkSDK("joiner")
	require.NotEqual(t, owner.Account().Id(), joiner.Account().Id())

	// Owner creates the space + invite, joiner requests, owner accepts,
	// joiner's autonomous loader flips the row to active.
	sp, err := owner.Spaces().Create(ctx, space.CreateRequest{Name: "PubSubRelay"})
	require.NoError(t, err)
	inv, err := sp.ACL().CreateInvite(ctx)
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("skipping e2e: network unreachable: %v", err)
		}
		t.Fatalf("CreateInvite: %v", err)
	}
	token, err := space.EncodeInvite(inv)
	require.NoError(t, err)
	_, err = joiner.Spaces().Join(ctx, space.JoinRequest{Invite: token})
	require.ErrorIs(t, err, spaceimpl.ErrJoinPending)

	var joinReq space.JoinRequestInfo
	require.Eventually(t, func() bool {
		_ = sp.SyncHeads(ctx)
		reqs, rErr := sp.Members().JoinRequests(ctx)
		if rErr != nil || len(reqs) == 0 {
			return false
		}
		joinReq = reqs[0]
		return true
	}, 90*time.Second, 2*time.Second, "owner never saw the join request")
	require.NoError(t, sp.ACL().AcceptRequest(ctx, joinReq.RecordId, space.PermissionWriter))

	require.Eventually(t, func() bool {
		infos, lErr := joiner.Spaces().List(ctx)
		if lErr != nil {
			return false
		}
		for _, info := range infos {
			if info.Id == sp.Id() {
				return info.Status == space.StatusActive
			}
		}
		return false
	}, 90*time.Second, 2*time.Second, "joiner row never flipped to active")
	joinerSp, err := joiner.Spaces().Get(ctx, sp.Id())
	require.NoError(t, err)

	// Owner → joiner via the node relay. Publish inside the poll:
	// at-most-once, sends racing the subscribe/interest propagation
	// (subscribe ramp + engine resync ≤ 20s) are lost by design.
	var atJoiner msgCollector
	cancelJ, err := joinerSp.PubSub().Subscribe("chat/>", atJoiner.cb)
	require.NoError(t, err)
	t.Cleanup(cancelJ)
	seq := 0
	require.True(t, waitFor(ctx, 90*time.Second, time.Second, func() bool {
		seq++
		_ = sp.PubSub().Publish(ctx, "chat/main", []byte(fmt.Sprintf("owner-%d", seq)))
		_, ok := atJoiner.find(func(m space.PubSubMessage) bool { return m.Topic == "chat/main" && !m.Self })
		return ok
	}), "joiner never received the owner's publish via the node relay")
	m, _ := atJoiner.find(func(m space.PubSubMessage) bool { return m.Topic == "chat/main" && !m.Self })
	assert.Equal(t, owner.Account().Id(), m.SenderIdentity)
	assert.False(t, m.Self)

	// And the reverse direction: joiner → owner.
	var atOwner msgCollector
	cancelO, err := sp.PubSub().Subscribe("chat/>", atOwner.cb)
	require.NoError(t, err)
	t.Cleanup(cancelO)
	seq = 0
	require.True(t, waitFor(ctx, 90*time.Second, time.Second, func() bool {
		seq++
		_ = joinerSp.PubSub().Publish(ctx, "chat/main", []byte(fmt.Sprintf("joiner-%d", seq)))
		_, ok := atOwner.find(func(m space.PubSubMessage) bool { return m.SenderIdentity == joiner.Account().Id() })
		return ok
	}), "owner never received the joiner's publish via the node relay")
}
