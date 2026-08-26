package p2p

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/net/transport/iroh"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/require"
	"github.com/tmc/go-iroh/dnsserver"

	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/p2p/account"
)

// accountFixture adds a real pkarr relay and the account keys of the
// fixture's identity to a global fixture.
type accountFixture struct {
	*globalFixture
	keys     *account.Keys
	identity crypto.PrivKey
	client   *countingClient
}

// countingClient counts publishes around the real client.
type countingClient struct {
	*account.Client
	mu        sync.Mutex
	publishes int
}

func (c *countingClient) Publish(ctx context.Context, p *account.SignedPacket) error {
	c.mu.Lock()
	c.publishes++
	c.mu.Unlock()
	return c.Client.Publish(ctx, p)
}

func (c *countingClient) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.publishes
}

func newAccountFixture(t *testing.T, insecure bool) *accountFixture {
	t.Helper()
	ts := httptest.NewServer(dnsserver.New())
	t.Cleanup(ts.Close)
	identity, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	sign, enc, err := crypto.DeriveDiscoveryKeys(identity)
	require.NoError(t, err)
	keys, err := account.NewKeys(sign, enc)
	require.NoError(t, err)
	client, err := account.NewClient([]string{ts.URL})
	require.NoError(t, err)
	fx := &accountFixture{globalFixture: newGlobalFixture(t, config.GlobalP2P{}), keys: keys, identity: identity, client: &countingClient{Client: client}}
	fx.g.selfIdentity = identity.GetPublic().Account()
	fx.g.SetAccount(keys, fx.client, insecure)
	return fx
}

// seed publishes a record naming the devices, as a sibling would.
func (fx *accountFixture) seed(t *testing.T, devices ...account.Device) {
	t.Helper()
	packet, err := account.Seal(fx.keys, account.Record{Devices: devices})
	require.NoError(t, err)
	require.NoError(t, fx.client.Client.Publish(context.Background(), packet))
}

func (fx *accountFixture) record(t *testing.T) account.Record {
	t.Helper()
	packet, err := fx.client.Resolve(context.Background(), fx.keys.Public())
	require.NoError(t, err)
	require.NotNil(t, packet)
	rec, err := account.Open(fx.keys, packet)
	require.NoError(t, err)
	return rec
}

func TestAccountCycleResolvesAndPublishes(t *testing.T) {
	fx := newAccountFixture(t, false)
	fx.g.runCtx, fx.g.runCancel = context.WithCancel(context.Background())
	defer fx.g.runCancel()
	now := fx.clock()
	sibling := newTestPeer(t, "me")
	insecure := newTestPeer(t, "me")
	gone := newTestPeer(t, "me")
	fx.seed(t,
		account.Device{PeerId: sibling.peerId, Relay: "https://relay.test/", LastSeen: now.Add(-time.Minute)},
		account.Device{PeerId: insecure.peerId, Relay: "http://relay.test/", LastSeen: now},
		account.Device{PeerId: gone.peerId, Relay: "https://relay.test/", LastSeen: now.Add(-account.MaxAge - time.Hour)},
		account.Device{PeerId: fx.self.peerId, Relay: "https://old.test/", LastSeen: now.Add(-2 * time.Hour)},
	)

	require.NoError(t, fx.g.accountCycle())

	// the sibling is a global peer of every space, with the record's
	// timestamp and a ticket for its relay
	require.True(t, fx.store.HasAccountPeer(sibling.peerId))
	require.True(t, fx.store.HasGlobalPeer(sibling.peerId))
	require.Contains(t, fx.store.GlobalPeerIds("any-space"), sibling.peerId)
	want, err := iroh.TicketForPeer(sibling.peerId, "https://relay.test/", false)
	require.NoError(t, err)
	require.Equal(t, want, fx.book.Ticket(sibling.peerId))
	require.Equal(t, now.Add(-time.Minute), fx.status.LastSeen(sibling.peerId))
	require.Equal(t, TierActive, fx.status.Tier(sibling.peerId))
	// an http relay is refused without the insecure opt-in; an entry past
	// MaxAge is ignored
	require.False(t, fx.store.HasGlobalPeer(insecure.peerId))
	require.False(t, fx.store.HasGlobalPeer(gone.peerId))
	require.Equal(t, fx.g.selfIdentity, fx.g.peerIdentity(sibling.peerId))

	// the own entry was refreshed with the current relay; the stale
	// entries are gone from the record
	require.Equal(t, 1, fx.client.count())
	rec := fx.record(t)
	byId := map[string]account.Device{}
	for _, d := range rec.Devices {
		byId[d.PeerId] = d
	}
	require.Equal(t, "https://relay.test/", byId[fx.self.peerId].Relay)
	require.Equal(t, now.Unix(), byId[fx.self.peerId].LastSeen.Unix())
	require.Contains(t, byId, sibling.peerId)
	require.NotContains(t, byId, gone.peerId)

	// a fresh own entry is not republished; a stale one is
	require.NoError(t, fx.g.accountCycle())
	require.Equal(t, 1, fx.client.count())
	fx.advance(accountRefreshAfter + time.Minute)
	require.NoError(t, fx.g.accountCycle())
	require.Equal(t, 2, fx.client.count())

	// a sibling that disappears from the record leaves the store
	fx.seed(t, account.Device{PeerId: fx.self.peerId, Relay: "https://relay.test/", LastSeen: fx.clock().Add(time.Second)})
	require.NoError(t, fx.g.accountCycle())
	require.False(t, fx.store.HasAccountPeer(sibling.peerId))
	require.Empty(t, fx.book.Ticket(sibling.peerId))

	st := fx.g.Status()
	require.True(t, st.Account.Enabled)
	require.Equal(t, 0, st.Account.Devices)
	require.False(t, st.Account.LastResolved.IsZero())
}

func TestAccountCycleInsecureRelayAndEmptyTicket(t *testing.T) {
	fx := newAccountFixture(t, true)
	fx.g.runCtx, fx.g.runCancel = context.WithCancel(context.Background())
	defer fx.g.runCancel()
	sibling := newTestPeer(t, "me")
	fx.seed(t, account.Device{PeerId: sibling.peerId, Relay: "http://relay.test/", LastSeen: fx.clock()})
	fx.ep.setTicket("")

	// no own ticket yet: siblings are still learned, nothing published
	require.NoError(t, fx.g.accountCycle())
	require.True(t, fx.store.HasAccountPeer(sibling.peerId))
	require.NotEmpty(t, fx.book.Ticket(sibling.peerId))
	require.Equal(t, 0, fx.client.count())
}

func TestAccountRecordAndSpaceRowAgree(t *testing.T) {
	fx := newAccountFixture(t, false)
	fx.run(t)
	sibling := newTestPeer(t, "me")

	// known from a space row first
	sp := fx.space(sibling)
	sp.kv.put(sibling, sibling.ticket, fx.clock().Add(-time.Hour))
	fx.g.SpaceLoaded("s1", sp)
	fx.drain(t)
	require.Equal(t, sibling.ticket, fx.book.Ticket(sibling.peerId))
	require.Equal(t, []Source{SourceGlobal}, fx.store.Sources(sibling.peerId))

	// a newer account entry names another relay: the ticket follows it
	fx.seed(t, account.Device{PeerId: sibling.peerId, Relay: "https://relay2.test/", LastSeen: fx.clock()})
	require.NoError(t, fx.g.accountCycle())
	want, err := iroh.TicketForPeer(sibling.peerId, "https://relay2.test/", false)
	require.NoError(t, err)
	require.Equal(t, want, fx.book.Ticket(sibling.peerId))
	require.Equal(t, []Source{SourceGlobal, SourceAccount}, fx.store.Sources(sibling.peerId))
	require.Equal(t, []string{"s1"}, fx.store.SpaceIds(sibling.peerId))

	// the space unloads: the sibling stays a global peer of every space
	fx.g.SpaceUnloaded("s1")
	require.True(t, fx.store.HasAccountPeer(sibling.peerId))
	require.Contains(t, fx.store.GlobalPeerIds("s2"), sibling.peerId)
	require.Equal(t, want, fx.book.Ticket(sibling.peerId))
}

func TestAccountAdmission(t *testing.T) {
	fx := newAccountFixture(t, false)
	known := newTestPeer(t, "other")
	fx.store.UpdateGlobalPeer(known.peerId, []string{"s1"})
	fx.status.Seen(known.peerId, fx.clock())
	stranger := newTestPeer(t, "stranger")
	sibling := newTestPeer(t, "me")
	otherIdentity, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)

	// pre-handshake: known peers pass; unknown ones under the budget
	require.True(t, fx.g.allowInbound(known.peerId))
	for i := 0; i < unknownPerMinute; i++ {
		require.True(t, fx.g.allowInbound(stranger.peerId), "attempt %d", i)
	}
	require.False(t, fx.g.allowInbound(stranger.peerId), "budget spent")
	fx.advance(2 * time.Minute)
	require.True(t, fx.g.allowInbound(stranger.peerId), "budget refilled")

	// post-handshake: known peers pass, a stranger never, a device that
	// proved the own identity is admitted and remembered as a sibling
	require.True(t, fx.g.allowHandshake(known.peerId, otherIdentity.GetPublic()))
	require.False(t, fx.g.allowHandshake(stranger.peerId, otherIdentity.GetPublic()))
	require.False(t, fx.g.allowHandshake(stranger.peerId, nil))
	require.False(t, fx.store.HasGlobalPeer(stranger.peerId))
	require.True(t, fx.g.allowHandshake(sibling.peerId, fx.identity.GetPublic()))
	require.True(t, fx.store.HasAccountPeer(sibling.peerId))
	require.Equal(t, fx.clock(), fx.status.LastSeen(sibling.peerId))
	require.Empty(t, fx.book.Ticket(sibling.peerId), "no relay known yet")
	require.True(t, fx.g.allowInbound(sibling.peerId), "known from now on")

	// without the account layer, unknown peers never pass either stage
	plain := newGlobalFixture(t, config.GlobalP2P{})
	require.False(t, plain.g.allowInbound(stranger.peerId))
	require.False(t, plain.g.allowHandshake(stranger.peerId, plain.g.selfIdentityKey(t)))
}

// selfIdentityKey is a public key whose account matches the fixture's
// selfIdentity string only when one was derived from a real key.
func (g *Global) selfIdentityKey(t *testing.T) crypto.PubKey {
	t.Helper()
	priv, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	return priv.GetPublic()
}

func TestAdvertiseGate(t *testing.T) {
	fx := newGlobalFixture(t, config.GlobalP2P{})
	fx.run(t)
	off := map[string]bool{"tech": true}
	fx.g.SetAdvertiseFn(func(spaceId string) bool { return !off[spaceId] })

	tech, sp := fx.space(), fx.space()
	fx.g.SpaceLoaded("tech", tech)
	fx.g.SpaceLoaded("s1", sp)
	fx.drain(t)
	require.Equal(t, 0, tech.kv.setCount(), "a declined space gets no row")
	require.Equal(t, 1, sp.kv.setCount())

	// records of a declined space are still read
	member := newTestPeer(t, "other")
	tech.kv.put(member, member.ticket, fx.clock())
	tech.members[member.identity] = true
	fx.g.handle(task{kind: taskReconcile, spaceId: "tech"})
	require.Contains(t, fx.store.GlobalPeerIds("tech"), member.peerId)

	// switching a space on republishes it
	delete(off, "tech")
	fx.g.Republish("tech")
	fx.drain(t)
	require.Equal(t, 1, tech.kv.setCount())
}
