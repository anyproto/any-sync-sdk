package p2p

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/transport/iroh"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/require"
	"github.com/tmc/go-iroh/dnsserver"

	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/p2p/account"
	sdkp2p "github.com/anyproto/any-sync-sdk/p2p"
)

// accountFixture adds a real pkarr relay and the account keys of the
// fixture's identity to a global fixture.
type accountFixture struct {
	*globalFixture
	keys     *account.Keys
	identity crypto.PrivKey
	client   *countingClient
}

// countingClient counts publishes around the real client; staleOnce
// makes the next publish report a race with a sibling.
type countingClient struct {
	*account.Client
	mu        sync.Mutex
	publishes int
	staleOnce bool
}

func (c *countingClient) Publish(ctx context.Context, p *account.SignedPacket) error {
	c.mu.Lock()
	c.publishes++
	stale := c.staleOnce
	c.staleOnce = false
	c.mu.Unlock()
	if stale {
		return account.ErrStale
	}
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
	fx.g.SetAccount(keys, fx.client, insecure, "")
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

	// a sibling that disappears from the record stays for a grace period
	// (its own entry may be in flight), then leaves the store
	fx.seed(t, account.Device{PeerId: fx.self.peerId, Relay: "https://relay.test/", LastSeen: fx.clock().Add(time.Second)})
	require.NoError(t, fx.g.accountCycle())
	require.True(t, fx.store.HasAccountPeer(sibling.peerId), "kept within the grace period")
	fx.advance(accountKeepUnnamed + time.Minute)
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
	fx.run(t)
	known := newTestPeer(t, "other")
	fx.store.UpdateGlobalPeer(known.peerId, []string{"s1"})
	fx.status.Seen(known.peerId, fx.clock())
	sibling := newTestPeer(t, "me")
	otherIdentity, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)

	// pre-handshake: known peers pass, uncharged; an unknown id gets one
	// attempt per window, two unknowns may be in flight at once
	require.True(t, fx.g.allowInbound(known.peerId))
	s1, s2, s3 := newTestPeer(t, "stranger"), newTestPeer(t, "stranger"), newTestPeer(t, "stranger")
	require.True(t, fx.g.allowInbound(s1.peerId))
	require.False(t, fx.g.allowInbound(s1.peerId), "one attempt per window")
	require.True(t, fx.g.allowInbound(s2.peerId))
	require.False(t, fx.g.allowInbound(s3.peerId), "two in flight")
	require.True(t, fx.g.allowInbound(known.peerId), "known peers are never charged")
	// a verdict frees the slot; a failed identity check bars the id
	require.False(t, fx.g.allowHandshake(s1.peerId, otherIdentity.GetPublic()))
	require.True(t, fx.g.allowInbound(s3.peerId), "slot freed")
	fx.advance(2 * time.Minute)
	require.False(t, fx.g.allowInbound(s1.peerId), "denied after a failed identity check")
	fx.advance(unknownDenyFor)
	require.True(t, fx.g.allowInbound(s1.peerId), "denial expired")
	// the window bounds distinct ids; a stale in-flight slot expires
	fx.advance(2 * time.Minute)
	for i := 0; i < unknownPerMinute; i++ {
		id := newTestPeer(t, "flood").peerId
		require.True(t, fx.g.allowInbound(id), "attempt %d", i)
		fx.g.allowHandshake(id, otherIdentity.GetPublic())
	}
	require.False(t, fx.g.allowInbound(newTestPeer(t, "flood").peerId), "window spent")
	fx.advance(2 * time.Minute)
	require.True(t, fx.g.allowInbound(newTestPeer(t, "flood").peerId), "window refilled")
	fx.advance(unknownInflightTTL + time.Second)
	require.True(t, fx.g.allowInbound(newTestPeer(t, "flood").peerId), "stale in-flight slot expired")

	// post-handshake: a peer a space row names passes with any identity;
	// a stranger never; a device that proved the own identity is
	// admitted and remembered as a sibling
	require.True(t, fx.g.allowHandshake(known.peerId, otherIdentity.GetPublic()))
	stranger := newTestPeer(t, "stranger")
	require.False(t, fx.g.allowHandshake(stranger.peerId, otherIdentity.GetPublic()))
	require.False(t, fx.g.allowHandshake(stranger.peerId, nil))
	require.False(t, fx.store.HasGlobalPeer(stranger.peerId))
	require.True(t, fx.g.allowHandshake(sibling.peerId, fx.identity.GetPublic()))
	fx.drain(t)
	require.True(t, fx.store.HasAccountPeer(sibling.peerId))
	require.Equal(t, fx.clock(), fx.status.LastSeen(sibling.peerId))
	require.Empty(t, fx.book.Ticket(sibling.peerId), "no relay known yet")
	require.True(t, fx.g.allowInbound(sibling.peerId), "known from now on")
	// a peer known through the account record alone still has to prove
	// the identity
	require.False(t, fx.g.allowHandshake(sibling.peerId, otherIdentity.GetPublic()))
	require.True(t, fx.g.allowHandshake(sibling.peerId, fx.identity.GetPublic()))

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

// TestAccountCycleNeverWipes: an absent or unreadable answer keeps the
// siblings and re-seeds the record from the last known one; the last
// known record survives a restart.
func TestAccountCycleNeverWipes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "account_record.json")
	fx := newAccountFixture(t, false)
	fx.g.SetAccount(fx.keys, fx.client, false, path)
	fx.g.runCtx, fx.g.runCancel = context.WithCancel(context.Background())
	defer fx.g.runCancel()
	now := fx.clock()
	sibling := newTestPeer(t, "me")
	fx.seed(t, account.Device{PeerId: sibling.peerId, Relay: "https://relay.test/", LastSeen: now})
	require.NoError(t, fx.g.accountCycle())
	require.True(t, fx.store.HasAccountPeer(sibling.peerId))
	require.FileExists(t, path)

	// every relay forgot the record: the sibling stays, the next publish
	// carries it again
	empty := httptest.NewServer(dnsserver.New())
	t.Cleanup(empty.Close)
	other, err := account.NewClient([]string{empty.URL})
	require.NoError(t, err)
	fx.g.account().client = other
	fx.advance(accountRefreshAfter + time.Minute)
	require.NoError(t, fx.g.accountCycle())
	require.True(t, fx.store.HasAccountPeer(sibling.peerId), "an absent record never drops a sibling")
	packet, err := other.Resolve(context.Background(), fx.keys.Public())
	require.NoError(t, err)
	require.NotNil(t, packet, "re-seeded")
	rec, err := account.Open(fx.keys, packet)
	require.NoError(t, err)
	var names bool
	for _, d := range rec.Devices {
		names = names || d.PeerId == sibling.peerId
	}
	require.True(t, names, "the re-seeded record still names the sibling")

	// an unreadable answer is treated the same way
	garbage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not a packet"))
	}))
	t.Cleanup(garbage.Close)
	bad, err := account.NewClient([]string{garbage.URL})
	require.NoError(t, err)
	fx.g.account().client = bad
	require.Error(t, fx.g.accountCycle(), "an unverifiable answer is a failed resolve")
	require.True(t, fx.store.HasAccountPeer(sibling.peerId))

	// a restart loads the last known record before any relay answers,
	// and the connector dials the sibling from it with every relay down
	again := newAccountFixture(t, false)
	again.g.selfIdentity = fx.g.selfIdentity
	again.g.SetAccount(fx.keys, bad, false, path)
	require.True(t, again.store.HasAccountPeer(sibling.peerId), "persisted record seeds the store")
	require.NotEmpty(t, again.book.Ticket(sibling.peerId))
	again.pool.dialFn = func(context.Context, string) (peer.Peer, error) { return nil, errors.New("unreachable") }
	again.run(t)
	again.g.SpaceLoaded("s1", again.space())
	require.Eventually(t, func() bool {
		again.pool.mu.Lock()
		defer again.pool.mu.Unlock()
		return slices.Contains(again.pool.dials, sibling.peerId)
	}, 5*time.Second, 5*time.Millisecond, "the sibling is dialed from the persisted record")
}

// TestAcceptedPeerIsNotDialedBack: a peer the handshake filter admitted
// counts as connected until the pool hands its connection over, so the
// connector does not dial back into it; the mark expires on its own.
func TestAcceptedPeerIsNotDialedBack(t *testing.T) {
	fx := newAccountFixture(t, false)
	fx.pool.dialFn = func(context.Context, string) (peer.Peer, error) { return nil, errors.New("unreachable") }
	sdkp2p.SetPowerHint(sdkp2p.PowerLow)
	t.Cleanup(func() { sdkp2p.SetPowerHint(sdkp2p.PowerNormal) })
	fx.run(t)
	sibling := newTestPeer(t, "me")
	fx.g.applyAccountRecord(account.Record{Devices: []account.Device{{PeerId: sibling.peerId, Relay: "https://relay.test/", LastSeen: fx.clock()}}}, fx.clock())
	fx.g.SpaceLoaded("s1", fx.space())
	fx.drain(t)
	require.NotEmpty(t, fx.book.Ticket(sibling.peerId))

	// the sibling connects to us first
	require.True(t, fx.g.allowHandshake(sibling.peerId, fx.identity.GetPublic()))
	fx.drain(t)
	require.True(t, fx.g.connected(sibling.peerId), "admitted peers count as connected")
	require.Equal(t, 1, fx.g.liveCount())
	sdkp2p.SetPowerHint(sdkp2p.PowerNormal)
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, 0, fx.pool.dialCount(), "no dial-back into a peer that just connected")

	// no connection arrived: the mark expires and the connector dials
	fx.advance(pendingInbound + time.Second)
	require.False(t, fx.g.connected(sibling.peerId))
	fx.g.conn.wakeUp()
	require.Eventually(t, func() bool { return fx.pool.dialCount() == 1 }, 5*time.Second, 5*time.Millisecond)
}

// TestRefusedAfterHandshakeIsADialFailure: a peer that closes right after
// a successful dial refused us; the connector backs off instead of
// treating it as connected.
func TestRefusedAfterHandshakeIsADialFailure(t *testing.T) {
	fx := newAccountFixture(t, false)
	sibling := newTestPeer(t, "me")
	fx.pool.dialFn = func(_ context.Context, id string) (peer.Peer, error) {
		p := &fakePeer{id: id, ctx: context.Background(), closed: make(chan struct{})}
		go func() {
			time.Sleep(100 * time.Millisecond)
			_ = p.Close()
		}()
		return p, nil
	}
	fx.run(t)
	fx.g.applyAccountRecord(account.Record{Devices: []account.Device{{PeerId: sibling.peerId, Relay: "https://relay.test/", LastSeen: fx.clock()}}}, fx.clock())
	fx.g.SpaceLoaded("s1", fx.space())

	require.Eventually(t, func() bool {
		rec, _ := fx.status.Get(sibling.peerId)
		return rec.Failures == 1
	}, 5*time.Second, 5*time.Millisecond, "the early close counts as a failed dial")
	require.Equal(t, 1, fx.pool.dialCount())
	require.False(t, fx.g.connected(sibling.peerId))
	fx.g.conn.mu.Lock()
	next := fx.g.conn.next[sibling.peerId]
	fx.g.conn.mu.Unlock()
	require.True(t, next.After(fx.clock()), "backoff set")
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, 1, fx.pool.dialCount(), "no burst of re-dials")
}

// TestAccountCycleNewerVersion: a record written by a newer version is
// read but never overwritten.
func TestAccountCycleNewerVersion(t *testing.T) {
	fx := newAccountFixture(t, false)
	fx.g.runCtx, fx.g.runCancel = context.WithCancel(context.Background())
	defer fx.g.runCancel()
	sibling := newTestPeer(t, "me")
	plain, err := account.Encode(account.Record{Devices: []account.Device{{PeerId: sibling.peerId, Relay: "https://relay.test/", LastSeen: fx.clock()}}})
	require.NoError(t, err)
	plain[0] = account.RecordVersion + 1
	packet, err := account.SealRaw(fx.keys, append(plain, 0xaa), 0)
	require.NoError(t, err)
	require.NoError(t, fx.client.Client.Publish(context.Background(), packet))

	require.NoError(t, fx.g.accountCycle())
	require.True(t, fx.store.HasAccountPeer(sibling.peerId), "read as far as this layout goes")
	require.Equal(t, 0, fx.client.count(), "never overwritten")
	st := fx.g.Status()
	require.False(t, st.Account.LastResolved.IsZero())
	require.False(t, st.Account.OwnEntry)
}

// TestAccountCycleClockAhead: a record dated ahead of the local clock is
// signed past, and the lead is reported; a publish that loses a race
// with a sibling merges on top of the sibling's record.
func TestAccountCycleClockAhead(t *testing.T) {
	fx := newAccountFixture(t, false)
	fx.g.runCtx, fx.g.runCancel = context.WithCancel(context.Background())
	defer fx.g.runCancel()
	now := fx.clock()
	sibling := newTestPeer(t, "me")
	rec := account.Record{Devices: []account.Device{{PeerId: sibling.peerId, Relay: "https://relay.test/", LastSeen: now}}}
	ahead, err := account.SealAt(fx.keys, rec, account.Timestamp(now.Add(time.Hour).UnixMicro()))
	require.NoError(t, err)
	require.NoError(t, fx.client.Client.Publish(context.Background(), ahead))

	require.NoError(t, fx.g.accountCycle())
	st := fx.g.Status()
	require.GreaterOrEqual(t, st.Account.ClockAhead, 55*time.Minute)
	require.True(t, st.Account.OwnEntry)
	require.False(t, st.Account.LastPublished.IsZero())
	got := fx.record(t)
	require.Len(t, got.Devices, 2)
	packet, err := fx.client.Resolve(context.Background(), fx.keys.Public())
	require.NoError(t, err)
	require.Greater(t, packet.Timestamp(), ahead.Timestamp(), "signed past the held packet")

	// a sibling publishes between our resolve and publish: the cycle
	// re-resolves, merges on top and succeeds
	late := newTestPeer(t, "me")
	fx.advance(accountRefreshAfter + time.Minute)
	fx.client.mu.Lock()
	fx.client.staleOnce = true
	fx.client.mu.Unlock()
	fx.seed(t, account.Device{PeerId: late.peerId, Relay: "https://relay.test/", LastSeen: fx.clock()},
		account.Device{PeerId: sibling.peerId, Relay: "https://relay.test/", LastSeen: fx.clock()})
	require.NoError(t, fx.g.accountCycle())
	got = fx.record(t)
	require.Len(t, got.Devices, 3, "self, sibling and the late sibling")
	require.True(t, fx.store.HasAccountPeer(late.peerId))
}

// TestSiblingGraceKeepsTicket: a sibling admitted by its handshake keeps
// the ticket a space row supplied and its place until its own entry
// lands.
func TestSiblingGraceKeepsTicket(t *testing.T) {
	fx := newAccountFixture(t, false)
	fx.run(t)
	sibling := newTestPeer(t, "me")
	sp := fx.space(sibling)
	sp.kv.put(sibling, sibling.ticket, fx.clock().Add(-time.Minute))
	fx.g.SpaceLoaded("s1", sp)
	fx.drain(t)
	require.Equal(t, sibling.ticket, fx.book.Ticket(sibling.peerId))

	require.True(t, fx.g.allowHandshake(sibling.peerId, fx.identity.GetPublic()))
	fx.drain(t)
	require.Equal(t, sibling.ticket, fx.book.Ticket(sibling.peerId), "a relay-less entry never clears a row's ticket")
	require.Equal(t, fx.clock(), fx.status.LastSeen(sibling.peerId), "the handshake counts as seen")
	require.True(t, fx.store.HasAccountPeer(sibling.peerId))

	// a record that does not name it yet keeps it for the grace period
	fx.g.applyAccountRecord(account.Record{}, fx.clock().Add(time.Minute))
	require.True(t, fx.store.HasAccountPeer(sibling.peerId))
	require.Equal(t, sibling.ticket, fx.book.Ticket(sibling.peerId))
	fx.advance(accountKeepUnnamed + time.Minute)
	fx.g.applyAccountRecord(account.Record{}, fx.clock())
	require.False(t, fx.store.HasAccountPeer(sibling.peerId), "gone after the grace period")
	require.Equal(t, sibling.ticket, fx.book.Ticket(sibling.peerId), "the row still vouches for it")
}
