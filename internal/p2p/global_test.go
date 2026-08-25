package p2p

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonspace/object/keyvalue/keyvaluestorage"
	"github.com/anyproto/any-sync/commonspace/object/keyvalue/keyvaluestorage/innerstorage"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/peerservice"
	"github.com/anyproto/any-sync/net/transport"
	"github.com/anyproto/any-sync/net/transport/iroh"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/require"
	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/netaddr"

	"github.com/anyproto/any-sync-sdk/config"
	sdkp2p "github.com/anyproto/any-sync-sdk/p2p"
)

// testPeer is a peer id backed by a real ed25519 key, so its iroh
// endpoint id derives and tickets can be minted for it.
type testPeer struct {
	peerId   string
	identity string
	ticket   string
}

func newTestPeer(t *testing.T, identity string) testPeer {
	t.Helper()
	_, pub, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	peerId := pub.PeerId()
	return testPeer{peerId: peerId, identity: identity, ticket: mintTicket(t, peerId, "https://relay.test")}
}

func mintTicket(t *testing.T, peerId, relay string) string {
	t.Helper()
	id, err := iroh.EndpointIdFromPeerId(peerId)
	require.NoError(t, err)
	addr := netaddr.NewEndpointAddr(id)
	if relay != "" {
		u, err := netaddr.ParseRelayURL(relay)
		require.NoError(t, err)
		addr = addr.WithRelayURL(u)
	}
	return endpointticket.Encode(addr)
}

// fakeEndpoint stands in for the iroh transport.
type fakeEndpoint struct {
	mu      sync.Mutex
	ticket  string
	updates chan struct{}
	filter  func(string) bool
}

func newFakeEndpoint(ticket string) *fakeEndpoint {
	return &fakeEndpoint{ticket: ticket, updates: make(chan struct{}, 1)}
}

func (f *fakeEndpoint) Ticket() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ticket
}
func (f *fakeEndpoint) TicketUpdates() <-chan struct{} { return f.updates }
func (f *fakeEndpoint) RelayConnected() bool           { return f.Ticket() != "" }
func (f *fakeEndpoint) SetIncomingFilter(fn func(string) bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.filter = fn
}
func (f *fakeEndpoint) setTicket(ticket string) {
	f.mu.Lock()
	f.ticket = ticket
	f.mu.Unlock()
	select {
	case f.updates <- struct{}{}:
	default:
	}
}

// fakeKV is an in-memory keyvaluestorage.Storage holding one row per
// peer for RecordKey; the decryptor hands the stored bytes back.
type fakeKV struct {
	keyvaluestorage.Storage
	mu   sync.Mutex
	rows map[string]innerstorage.KeyValue
	self testPeer
	now  func() time.Time
	sets int
}

func newFakeKV(self testPeer, now func() time.Time) *fakeKV {
	return &fakeKV{rows: map[string]innerstorage.KeyValue{}, self: self, now: now}
}

func (f *fakeKV) put(p testPeer, ticket string, at time.Time) {
	f.mu.Lock()
	f.rows[p.peerId] = innerstorage.KeyValue{
		KeyPeerId:      RecordKey + "-" + p.peerId,
		Key:            RecordKey,
		PeerId:         p.peerId,
		Identity:       p.identity,
		TimestampMicro: at.UnixMicro(),
		Value:          innerstorage.Value{Value: []byte(ticket)},
	}
	f.mu.Unlock()
}

func decrypt(kv innerstorage.KeyValue) ([]byte, error) { return kv.Value.Value, nil }

func (f *fakeKV) Set(_ context.Context, key string, value []byte) error {
	if key != RecordKey {
		return errors.New("unexpected key")
	}
	f.mu.Lock()
	f.sets++
	f.mu.Unlock()
	f.put(f.self, string(value), f.now())
	return nil
}

func (f *fakeKV) GetAll(_ context.Context, key string, get func(decryptor keyvaluestorage.Decryptor, values []innerstorage.KeyValue) error) error {
	if key != RecordKey {
		return nil
	}
	f.mu.Lock()
	values := make([]innerstorage.KeyValue, 0, len(f.rows))
	for _, kv := range f.rows {
		values = append(values, kv)
	}
	f.mu.Unlock()
	return get(decrypt, values)
}

func (f *fakeKV) setCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sets
}

// fakeSpace is the SpaceKV of one test space.
type fakeSpace struct {
	kv       *fakeKV
	canWrite bool
	members  map[string]bool
}

func (s *fakeSpace) Store() keyvaluestorage.Storage { return s.kv }
func (s *fakeSpace) CanWrite() bool                 { return s.canWrite }
func (s *fakeSpace) IsMember(identity string) bool  { return s.members[identity] }

// fakePeer is a connected peer whose context carries an iroh address.
type fakePeer struct {
	peer.Peer
	id  string
	ctx context.Context
}

func (p fakePeer) Id() string               { return p.id }
func (p fakePeer) Context() context.Context { return p.ctx }

func irohPeer(id string) fakePeer {
	return fakePeer{id: id, ctx: peer.CtxWithPeerAddr(context.Background(), transport.Iroh+"://x")}
}

// fakeGlobalPool: Pick serves the connected map; Get is scripted.
type fakeGlobalPool struct {
	mu        sync.Mutex
	connected map[string]peer.Peer
	getFn     func(ctx context.Context, id string) (peer.Peer, error)
	gets      []string
}

func (f *fakeGlobalPool) Pick(_ context.Context, id string) (peer.Peer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if p, ok := f.connected[id]; ok {
		return p, nil
	}
	return nil, errors.New("not connected")
}

func (f *fakeGlobalPool) Get(ctx context.Context, id string) (peer.Peer, error) {
	f.mu.Lock()
	f.gets = append(f.gets, id)
	fn := f.getFn
	f.mu.Unlock()
	if fn == nil {
		return nil, errors.New("no dial")
	}
	return fn(ctx, id)
}

func (f *fakeGlobalPool) getCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.gets)
}

func (f *fakeGlobalPool) connect(id string) {
	f.mu.Lock()
	if f.connected == nil {
		f.connected = map[string]peer.Peer{}
	}
	f.connected[id] = irohPeer(id)
	f.mu.Unlock()
}

// globalFixture wires a Global with fakes; the subscriber captures
// per-space handlers so tests can feed live rows.
type globalFixture struct {
	g        *Global
	self     testPeer
	ep       *fakeEndpoint
	pool     *fakeGlobalPool
	store    *PeerStore
	status   *StatusBook
	book     *AddrBook
	ps       *fakePeerService
	now      time.Time
	mu       sync.Mutex
	handlers map[string]KVHandler
}

func newGlobalFixture(t *testing.T, cfg config.Global) *globalFixture {
	t.Helper()
	cfg = cfg.WithDefaults()
	fx := &globalFixture{
		self:     newTestPeer(t, "me"),
		pool:     &fakeGlobalPool{},
		store:    NewPeerStore(),
		status:   NewStatusBook("", ThresholdsFrom(cfg)),
		now:      time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC),
		handlers: map[string]KVHandler{},
		ps:       &fakePeerService{},
	}
	fx.ep = newFakeEndpoint(fx.self.ticket)
	fx.book = NewAddrBook()
	fx.book.ps = fx.ps
	fx.store.SetStatus(fx.status)
	fx.status.now = fx.clock
	g := NewGlobal(cfg, fx.self.peerId, fx.self.identity, fx.store, fx.status, fx.book)
	g.now = fx.clock
	g.ep = fx.ep
	g.pool = fx.pool
	g.subscribe = func(spaceId string, h KVHandler) func() {
		fx.mu.Lock()
		fx.handlers[spaceId] = h
		fx.mu.Unlock()
		return func() {
			fx.mu.Lock()
			delete(fx.handlers, spaceId)
			fx.mu.Unlock()
		}
	}
	require.NoError(t, g.Init(nil))
	fx.g = g
	return fx
}

func (fx *globalFixture) clock() time.Time {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	return fx.now
}

func (fx *globalFixture) advance(d time.Duration) {
	fx.mu.Lock()
	fx.now = fx.now.Add(d)
	fx.mu.Unlock()
}

func (fx *globalFixture) run(t *testing.T) {
	t.Helper()
	require.NoError(t, fx.g.Run(context.Background()))
	t.Cleanup(func() { _ = fx.g.Close(context.Background()) })
}

func (fx *globalFixture) space(members ...testPeer) *fakeSpace {
	sp := &fakeSpace{kv: newFakeKV(fx.self, fx.clock), canWrite: true, members: map[string]bool{fx.self.identity: true}}
	for _, m := range members {
		sp.members[m.identity] = true
	}
	return sp
}

// drain waits until the worker has processed everything queued so far.
func (fx *globalFixture) drain(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool { return len(fx.g.tasks) == 0 }, 5*time.Second, 5*time.Millisecond)
	// One more hop: the last task may still be executing.
	time.Sleep(20 * time.Millisecond)
}

func (fx *globalFixture) handler(spaceId string) KVHandler {
	fx.mu.Lock()
	defer fx.mu.Unlock()
	return fx.handlers[spaceId]
}

func TestParseTicket(t *testing.T) {
	a := newTestPeer(t, "a")
	b := newTestPeer(t, "b")

	ticket, addr, err := parseTicket(a.peerId, "self", []byte(a.ticket))
	require.NoError(t, err)
	require.Equal(t, a.ticket, ticket)
	require.Len(t, addr.RelayURLs(), 1)

	_, _, err = parseTicket(a.peerId, "self", []byte(b.ticket))
	require.ErrorIs(t, err, errRecordIdentity, "a row may only carry its signer's own endpoint")
	_, _, err = parseTicket(a.peerId, a.peerId, []byte(a.ticket))
	require.ErrorIs(t, err, errRecordSelf)
	_, _, err = parseTicket(a.peerId, "self", []byte("garbage"))
	require.Error(t, err)
	_, _, err = parseTicket("not-a-peer-id", "self", []byte(a.ticket))
	require.Error(t, err)

	require.Equal(t, "https://relay.test/", homeRelay(a.ticket))
	require.Equal(t, "", homeRelay(mintTicket(t, a.peerId, "")))
	require.Equal(t, "", homeRelay("garbage"))
}

func TestGlobalConsumeRecords(t *testing.T) {
	fx := newGlobalFixture(t, config.Global{})
	a := newTestPeer(t, "me")     // own device on another machine
	b := newTestPeer(t, "bob")    // member
	c := newTestPeer(t, "carol")  // signed rows, but removed from the ACL
	d := newTestPeer(t, "dave")   // row carries somebody else's ticket
	old := newTestPeer(t, "old")  // disabled tier
	skew := newTestPeer(t, "skw") // clock in the future

	sp := fx.space(a, b, d, old, skew)
	sp.kv.put(a, a.ticket, fx.now.Add(-time.Minute))
	sp.kv.put(b, b.ticket, fx.now.Add(-2*time.Hour))
	sp.kv.put(c, c.ticket, fx.now)
	sp.kv.put(d, a.ticket, fx.now)
	sp.kv.put(old, old.ticket, fx.now.Add(-40*24*time.Hour))
	sp.kv.put(skew, skew.ticket, fx.now.Add(48*time.Hour))
	sp.kv.put(fx.self, fx.self.ticket, fx.now)
	fx.run(t)

	fx.g.SpaceLoaded("s1", sp)
	fx.drain(t)

	require.Equal(t, []string{skew.peerId, a.peerId, b.peerId}, fx.store.GlobalPeerIds("s1"), "ranked by lastSeen, disabled hidden")
	require.True(t, fx.store.HasGlobalPeer(old.peerId))
	require.False(t, fx.store.HasGlobalPeer(c.peerId), "removed member")
	require.False(t, fx.store.HasGlobalPeer(d.peerId), "foreign ticket")
	require.False(t, fx.store.HasGlobalPeer(fx.self.peerId), "own row")
	require.Equal(t, []string{"iroh://" + b.ticket}, fx.ps.addrs[b.peerId])
	require.Empty(t, fx.ps.addrs[old.peerId], "disabled peers hold no address")
	require.Equal(t, fx.now, fx.status.LastSeen(skew.peerId), "future timestamps clamp to now")
	require.Equal(t, TierStale, fx.status.Tier(b.peerId))
	require.Equal(t, "me", fx.g.peerIdentity(a.peerId))

	// Own record: unchanged and fresh → no rewrite.
	require.Equal(t, 0, sp.kv.setCount())

	// A live row with a newer timestamp reactivates a disabled peer;
	// an older one changes nothing.
	fx.handler("s1")(decrypt, []innerstorage.KeyValue{
		{Key: RecordKey, PeerId: old.peerId, Identity: old.identity, TimestampMicro: fx.now.UnixMicro(), Value: innerstorage.Value{Value: []byte(old.ticket)}},
		{Key: RecordKey, PeerId: b.peerId, Identity: b.identity, TimestampMicro: fx.now.Add(-3 * time.Hour).UnixMicro(), Value: innerstorage.Value{Value: []byte(b.ticket)}},
		{Key: "read/other", PeerId: b.peerId, Identity: b.identity, TimestampMicro: fx.now.UnixMicro(), Value: innerstorage.Value{Value: []byte("ignored")}},
	})
	fx.drain(t)
	require.Equal(t, TierActive, fx.status.Tier(old.peerId))
	require.Equal(t, []string{"iroh://" + old.ticket}, fx.ps.addrs[old.peerId])
	require.Equal(t, fx.now.Add(-2*time.Hour), fx.status.LastSeen(b.peerId))

	// A second space adds coverage; unloading it drops the peers known
	// only through it.
	e := newTestPeer(t, "eve")
	sp2 := fx.space(b, e)
	sp2.kv.put(b, b.ticket, fx.now)
	sp2.kv.put(e, e.ticket, fx.now)
	fx.g.SpaceLoaded("s2", sp2)
	fx.drain(t)
	require.Equal(t, []string{"s1", "s2"}, fx.store.SpaceIds(b.peerId))
	require.Equal(t, fx.now, fx.status.LastSeen(b.peerId), "newest row across spaces wins")
	require.True(t, fx.store.HasGlobalPeer(e.peerId))

	fx.g.SpaceUnloaded("s2")
	require.False(t, fx.store.HasGlobalPeer(e.peerId))
	require.Empty(t, fx.ps.addrs[e.peerId])
	require.Equal(t, []string{"s1"}, fx.store.SpaceIds(b.peerId))
	require.Nil(t, fx.handler("s2"))
}

func TestGlobalPublishHeartbeat(t *testing.T) {
	fx := newGlobalFixture(t, config.Global{})
	fx.run(t)

	// No own row yet → publish.
	sp := fx.space()
	fx.g.SpaceLoaded("s1", sp)
	fx.drain(t)
	require.Equal(t, 1, sp.kv.setCount())

	// Fresh, unchanged row → a reload publishes nothing.
	fx.g.SpaceUnloaded("s1")
	fx.g.SpaceLoaded("s1", sp)
	fx.drain(t)
	require.Equal(t, 1, sp.kv.setCount())

	// Ticket change → debounced republish into every loaded space.
	other := fx.space()
	fx.g.SpaceLoaded("s2", other)
	fx.drain(t)
	require.Equal(t, 1, other.kv.setCount())
	fx.ep.setTicket(mintTicket(t, fx.self.peerId, "https://relay2.test"))
	require.Eventually(t, func() bool { return sp.kv.setCount() == 2 && other.kv.setCount() == 2 }, 10*time.Second, 10*time.Millisecond)

	// Row older than the heartbeat threshold → re-set on load.
	fx.advance(heartbeatStaleAfter + time.Minute)
	fx.g.SpaceUnloaded("s1")
	fx.g.SpaceLoaded("s1", sp)
	fx.drain(t)
	require.Equal(t, 3, sp.kv.setCount())

	// Readers never write; an empty ticket never publishes.
	ro := fx.space()
	ro.canWrite = false
	fx.g.SpaceLoaded("ro", ro)
	fx.drain(t)
	require.Equal(t, 0, ro.kv.setCount())
	fx.ep.setTicket("")
	time.Sleep(publishDebounce + 100*time.Millisecond)
	fx.drain(t)
	require.Equal(t, 3, sp.kv.setCount())
}

func TestGlobalInboundFilter(t *testing.T) {
	fx := newGlobalFixture(t, config.Global{MaxConnections: 1, MaxInbound: 1})
	require.NotNil(t, fx.ep.filter, "filter installed at Init, before the transport starts")
	a := newTestPeer(t, "a")
	b := newTestPeer(t, "b")
	c := newTestPeer(t, "c")
	old := newTestPeer(t, "old")
	sp := fx.space(a, b, c, old)
	sp.kv.put(a, a.ticket, fx.now)
	sp.kv.put(b, b.ticket, fx.now)
	sp.kv.put(c, c.ticket, fx.now)
	sp.kv.put(old, old.ticket, fx.now.Add(-60*24*time.Hour))
	fx.run(t)
	fx.g.SpaceLoaded("s1", sp)
	fx.drain(t)

	require.False(t, fx.ep.filter("stranger"))
	require.False(t, fx.ep.filter(old.peerId), "disabled tier")
	require.True(t, fx.ep.filter(a.peerId))

	// MaxConnections+MaxInbound live iroh connections → refuse.
	fx.pool.connect(a.peerId)
	require.True(t, fx.ep.filter(b.peerId))
	fx.pool.connect(b.peerId)
	require.False(t, fx.ep.filter(c.peerId))
}

func TestSelectTargets(t *testing.T) {
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	cands := map[string][]string{
		"s1": {"own", "p1"},
		"s2": {"own", "p2"},
		"s3": {"p2", "p3"},
		"s4": {"p3"},
	}
	infos := map[string]peerInfo{
		"own": {lastSeen: now.Add(-time.Hour), own: true},
		"p1":  {lastSeen: now},
		"p2":  {lastSeen: now.Add(-time.Minute)},
		"p3":  {lastSeen: now.Add(-2 * time.Hour)},
	}
	// Greedy cover: own covers s1+s2, then p3 covers s3+s4 (ties on
	// coverage go to p2? p2 covers s2(taken)+s3 = 1, p3 covers 2).
	require.Equal(t, []string{"own", "p3"}, selectTargets(cands, infos, 4))
	// Cap wins over coverage.
	require.Equal(t, []string{"own"}, selectTargets(cands, infos, 1))
	// Same coverage: connected first, then own, then lastSeen, then id.
	tie := map[string][]string{"s": {"a", "b", "c", "d"}}
	tieInfo := map[string]peerInfo{
		"a": {lastSeen: now},
		"b": {lastSeen: now.Add(-time.Hour), own: true},
		"c": {lastSeen: now.Add(-2 * time.Hour), connected: true},
		"d": {lastSeen: now},
	}
	require.Equal(t, []string{"c"}, selectTargets(tie, tieInfo, 4), "one peer covers the only space; no extra dials")
	delete(tieInfo, "c")
	delete(tie, "s")
	tie["s"] = []string{"a", "b", "d"}
	require.Equal(t, []string{"b"}, selectTargets(tie, tieInfo, 4))
	tie["s"] = []string{"a", "d"}
	require.Equal(t, []string{"a"}, selectTargets(tie, tieInfo, 4))
	require.Empty(t, selectTargets(nil, nil, 4))
}

func TestBackoffFor(t *testing.T) {
	require.Equal(t, 30*time.Second, backoffFor(TierActive, 0))
	require.Equal(t, 30*time.Second, backoffFor(TierActive, 1))
	require.Equal(t, time.Minute, backoffFor(TierActive, 2))
	require.Equal(t, 4*time.Minute, backoffFor(TierActive, 4))
	require.Equal(t, 10*time.Minute, backoffFor(TierActive, 40))
	require.Equal(t, staleProbe, backoffFor(TierStale, 3))
	require.Equal(t, dormantProbe, backoffFor(TierDormant, 1))
}

func TestConnectorDialsBoundedAndRateLimited(t *testing.T) {
	fx := newGlobalFixture(t, config.Global{MaxConnections: 2, MaxDialsPerMinute: 2, DialTimeout: 50 * time.Millisecond})
	a := newTestPeer(t, "a")
	b := newTestPeer(t, "b")
	c := newTestPeer(t, "c")
	// s1 is covered by a (fresher than b); s2 only by c.
	sp := fx.space(a, b)
	sp.kv.put(a, a.ticket, fx.now)
	sp.kv.put(b, b.ticket, fx.now.Add(-time.Minute))
	sp2 := fx.space(c)
	sp2.kv.put(c, c.ticket, fx.now.Add(-2*time.Minute))

	// Every dial hangs until its deadline: the connector must give up
	// at DialTimeout, count the failure, back off, and never exceed the
	// per-minute budget. Dials run one at a time.
	var inflight, maxInflight int
	var mu sync.Mutex
	fx.pool.getFn = func(ctx context.Context, id string) (peer.Peer, error) {
		dl, ok := ctx.Deadline()
		require.True(t, ok, "dial ctx carries DialTimeout")
		require.WithinDuration(t, time.Now().Add(50*time.Millisecond), dl, 30*time.Millisecond)
		mu.Lock()
		inflight++
		if inflight > maxInflight {
			maxInflight = inflight
		}
		mu.Unlock()
		<-ctx.Done()
		mu.Lock()
		inflight--
		mu.Unlock()
		return nil, ctx.Err()
	}
	fx.run(t)
	fx.g.SpaceLoaded("s1", sp)
	fx.g.SpaceLoaded("s2", sp2)

	// The cover is a (s1) + c (s2); b is never worth a dial.
	require.Eventually(t, func() bool { return fx.pool.getCount() == 2 }, 5*time.Second, 5*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	fx.pool.mu.Lock()
	require.ElementsMatch(t, []string{a.peerId, c.peerId}, fx.pool.gets)
	fx.pool.mu.Unlock()
	mu.Lock()
	require.Equal(t, 1, maxInflight, "one dial in flight")
	mu.Unlock()
	rec, _ := fx.status.Get(a.peerId)
	require.Equal(t, 1, rec.Failures)

	// Backoff over (30 s) but the per-minute budget is spent → no dial.
	fx.advance(31 * time.Second)
	fx.g.conn.wakeUp()
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 2, fx.pool.getCount(), "rate limit: 2 dials per minute")

	// Budget back → the freshest due target is retried.
	fx.advance(30 * time.Second)
	fx.g.conn.wakeUp()
	require.Eventually(t, func() bool { return fx.pool.getCount() == 3 }, 5*time.Second, 5*time.Millisecond)
	fx.pool.mu.Lock()
	require.Equal(t, a.peerId, fx.pool.gets[2])
	fx.pool.mu.Unlock()

	// A connected peer is never dialed; a LAN-reachable one neither.
	fx.pool.connect(a.peerId)
	fx.book.SetLAN(c.peerId, []string{"yamux://10.0.0.1:1"})
	fx.advance(2 * time.Minute)
	fx.g.conn.reactivate(a.peerId)
	fx.g.conn.reactivate(c.peerId)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 3, fx.pool.getCount())
}

func TestConnectorPowerHintAndAddrsNotFound(t *testing.T) {
	sdkp2p.SetPowerHint(sdkp2p.PowerLow)
	t.Cleanup(func() { sdkp2p.SetPowerHint(sdkp2p.PowerNormal) })

	fx := newGlobalFixture(t, config.Global{})
	a := newTestPeer(t, "a")
	sp := fx.space(a)
	sp.kv.put(a, a.ticket, fx.now)
	fx.pool.getFn = func(context.Context, string) (peer.Peer, error) { return nil, peerservice.ErrAddrsNotFound }
	fx.run(t)
	fx.g.SpaceLoaded("s1", sp)
	fx.drain(t)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 0, fx.pool.getCount(), "no dials while low power")

	sdkp2p.SetPowerHint(sdkp2p.PowerNormal)
	require.Eventually(t, func() bool { return fx.pool.getCount() == 1 }, 5*time.Second, 5*time.Millisecond)
	rec, _ := fx.status.Get(a.peerId)
	require.Equal(t, 0, rec.Failures, "a lost pool load is not a failure")
	require.True(t, rec.LastAttempt.IsZero())
}

func TestGlobalSweepBumpsConnectedPeers(t *testing.T) {
	fx := newGlobalFixture(t, config.Global{})
	a := newTestPeer(t, "a")
	b := newTestPeer(t, "b")
	sp := fx.space(a, b)
	sp.kv.put(a, a.ticket, fx.now.Add(-2*time.Hour))
	sp.kv.put(b, b.ticket, fx.now.Add(-2*time.Hour))
	fx.run(t)
	fx.g.SpaceLoaded("s1", sp)
	fx.drain(t)
	require.Equal(t, TierStale, fx.status.Tier(a.peerId))

	fx.pool.connect(a.peerId)
	fx.g.sweep()
	require.Equal(t, TierActive, fx.status.Tier(a.peerId))
	require.Equal(t, TierStale, fx.status.Tier(b.peerId))

	st := fx.g.Status()
	require.True(t, st.Enabled)
	require.NotEmpty(t, st.EndpointId)
	require.Equal(t, fx.self.ticket, st.Ticket)
	require.Equal(t, "https://relay.test/", st.HomeRelay)
	require.True(t, st.RelayConnected)
	require.Len(t, st.Peers, 2)
	require.Equal(t, a.peerId, st.Peers[0].PeerId)
	require.True(t, st.Peers[0].Connected)
	require.Equal(t, "active", st.Peers[0].Tier)
	require.Equal(t, []string{"global"}, st.Peers[0].Sources)
}
