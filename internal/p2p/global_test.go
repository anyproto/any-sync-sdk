package p2p

import (
	"bytes"
	"context"
	"errors"
	"net/netip"
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
	mu       sync.Mutex
	ticket   string
	updates  chan struct{}
	filter   func(string) bool
	hsFilter func(string, crypto.PubKey) bool
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
func (f *fakeEndpoint) SetHandshakeFilter(fn func(string, crypto.PubKey) bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hsFilter = fn
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
	id     string
	ctx    context.Context
	closed chan struct{}
	once   sync.Once
}

func (p *fakePeer) Id() string               { return p.id }
func (p *fakePeer) Context() context.Context { return p.ctx }
func (p *fakePeer) CloseChan() <-chan struct{} {
	return p.closed
}
func (p *fakePeer) IsClosed() bool {
	select {
	case <-p.closed:
		return true
	default:
		return false
	}
}
func (p *fakePeer) SetTTL(time.Duration) {}
func (p *fakePeer) Close() error {
	p.once.Do(func() { close(p.closed) })
	return nil
}

func irohPeer(id string) *fakePeer {
	return &fakePeer{id: id, ctx: peer.CtxWithPeerAddr(context.Background(), transport.Iroh+"://x"), closed: make(chan struct{})}
}

// fakeGlobalPool stands in for the pool and the peer service: Pick
// serves the connected map, Dial is scripted, AddPeer connects.
type fakeGlobalPool struct {
	mu        sync.Mutex
	connected map[string]peer.Peer
	dialFn    func(ctx context.Context, id string) (peer.Peer, error)
	dials     []string
	// pickBlocks makes Pick wait for ctx: the pool's Pick waits on an
	// in-flight load for the same id.
	pickBlocks bool
}

func (f *fakeGlobalPool) Pick(ctx context.Context, id string) (peer.Peer, error) {
	f.mu.Lock()
	blocks := f.pickBlocks
	p, ok := f.connected[id]
	f.mu.Unlock()
	if blocks {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if ok {
		return p, nil
	}
	return nil, errors.New("not connected")
}

func (f *fakeGlobalPool) Dial(ctx context.Context, id string) (peer.Peer, error) {
	f.mu.Lock()
	f.dials = append(f.dials, id)
	fn := f.dialFn
	f.mu.Unlock()
	if fn == nil {
		return nil, errors.New("no dial")
	}
	return fn(ctx, id)
}

func (f *fakeGlobalPool) AddPeer(_ context.Context, p peer.Peer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.connected == nil {
		f.connected = map[string]peer.Peer{}
	}
	f.connected[p.Id()] = p
	return nil
}

func (f *fakeGlobalPool) dialCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.dials)
}

func (f *fakeGlobalPool) connect(id string) *fakePeer {
	p := irohPeer(id)
	f.mu.Lock()
	if f.connected == nil {
		f.connected = map[string]peer.Peer{}
	}
	f.connected[id] = p
	f.mu.Unlock()
	return p
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

func newGlobalFixture(t *testing.T, cfg config.GlobalP2P) *globalFixture {
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
	g.dialer = fx.pool
	g.publishDebounce = 50 * time.Millisecond
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

// connect puts a live iroh connection to the peer into the pool and the
// layer's live set, as a connector dial or a sweep would.
func (fx *globalFixture) connect(id string) *fakePeer {
	p := fx.pool.connect(id)
	fx.g.addLive(p)
	return p
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

	ticket, err := parseTicket(a.peerId, []byte(a.ticket), true)
	require.NoError(t, err)
	require.Equal(t, a.ticket, ticket)

	_, err = parseTicket(a.peerId, []byte(b.ticket), true)
	require.ErrorIs(t, err, errRecordIdentity, "a row may only carry its signer's own endpoint")
	_, err = parseTicket(a.peerId, []byte("garbage"), true)
	require.Error(t, err)
	_, err = parseTicket("not-a-peer-id", []byte(a.ticket), true)
	require.Error(t, err)
	_, err = parseTicket(a.peerId, bytes.Repeat([]byte("x"), maxTicketLen+1), true)
	require.ErrorIs(t, err, errRecordTooLong)

	// With relays configured a row must be relay-only: a direct address
	// would point our dials at a host of the writer's choosing.
	id, err := iroh.EndpointIdFromPeerId(a.peerId)
	require.NoError(t, err)
	relay, _ := netaddr.ParseRelayURL("https://relay.test")
	direct := endpointticket.Encode(netaddr.NewEndpointAddr(id).WithRelayURL(relay).WithIP(netip.MustParseAddrPort("10.0.0.1:4000")))
	_, err = parseTicket(a.peerId, []byte(direct), true)
	require.ErrorIs(t, err, errRecordDirect)
	noRelay := mintTicket(t, a.peerId, "")
	_, err = parseTicket(a.peerId, []byte(noRelay), true)
	require.ErrorIs(t, err, errRecordNoRelay)
	_, err = parseTicket(a.peerId, []byte(direct), false)
	require.NoError(t, err, "direct mode (no local relays) accepts direct addresses")

	require.Equal(t, "https://relay.test/", homeRelay(a.ticket))
	require.Equal(t, "", homeRelay(mintTicket(t, a.peerId, "")))
	require.Equal(t, "", homeRelay("garbage"))
}

func TestGlobalConsumeRecords(t *testing.T) {
	fx := newGlobalFixture(t, config.GlobalP2P{})
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

	// A live row reactivates a disabled peer; a live row with an older
	// timestamp inside the skew window still proves the device is alive
	// now.
	fx.handler("s1")(decrypt, []innerstorage.KeyValue{
		{Key: RecordKey, PeerId: old.peerId, Identity: old.identity, TimestampMicro: fx.now.UnixMicro(), Value: innerstorage.Value{Value: []byte(old.ticket)}},
		{Key: RecordKey, PeerId: b.peerId, Identity: b.identity, TimestampMicro: fx.now.Add(-3 * time.Hour).UnixMicro(), Value: innerstorage.Value{Value: []byte(b.ticket)}},
		{Key: "read/other", PeerId: b.peerId, Identity: b.identity, TimestampMicro: fx.now.UnixMicro(), Value: innerstorage.Value{Value: []byte("ignored")}},
	})
	fx.drain(t)
	require.Equal(t, TierActive, fx.status.Tier(old.peerId))
	require.Equal(t, []string{"iroh://" + old.ticket}, fx.ps.addrs[old.peerId])
	require.Equal(t, fx.now, fx.status.LastSeen(b.peerId), "a live row inside the skew window reads as now")

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
	fx := newGlobalFixture(t, config.GlobalP2P{})
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
	time.Sleep(fx.g.publishDebounce + 100*time.Millisecond)
	fx.drain(t)
	require.Equal(t, 3, sp.kv.setCount())
}

func TestGlobalInboundFilter(t *testing.T) {
	fx := newGlobalFixture(t, config.GlobalP2P{MaxConnections: 1, MaxInbound: 1})
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

	// MaxConnections+MaxInbound distinct live peers → refuse. Inbound
	// connections live only in the pool until the sweep folds them in.
	fx.pool.connect(a.peerId)
	require.True(t, fx.ep.filter(b.peerId))
	fx.g.sweep()
	require.True(t, fx.ep.filter(b.peerId))
	pb := fx.connect(b.peerId)
	require.False(t, fx.ep.filter(c.peerId))
	// A closed connection frees its slot.
	require.NoError(t, pb.Close())
	require.Eventually(t, func() bool { return fx.ep.filter(c.peerId) }, time.Second, 5*time.Millisecond)
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
	require.Equal(t, activeBackoffMax, backoffFor(TierActive, 40))
	require.Equal(t, staleProbe, backoffFor(TierStale, 3))
	require.Equal(t, dormantProbe, backoffFor(TierDormant, 1))
}

func TestConnectorDialsBoundedAndRateLimited(t *testing.T) {
	fx := newGlobalFixture(t, config.GlobalP2P{MaxConnections: 2, MaxDialsPerMinute: 2, DialTimeout: 50 * time.Millisecond})
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
	fx.pool.dialFn = func(ctx context.Context, id string) (peer.Peer, error) {
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
	require.Eventually(t, func() bool { return fx.pool.dialCount() == 2 }, 5*time.Second, 5*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	fx.pool.mu.Lock()
	require.ElementsMatch(t, []string{a.peerId, c.peerId}, fx.pool.dials)
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
	require.Equal(t, 2, fx.pool.dialCount(), "rate limit: 2 dials per minute")

	// Budget back → the freshest due target is retried.
	fx.advance(30 * time.Second)
	fx.g.conn.wakeUp()
	require.Eventually(t, func() bool { return fx.pool.dialCount() == 3 }, 5*time.Second, 5*time.Millisecond)
	fx.pool.mu.Lock()
	require.Equal(t, a.peerId, fx.pool.dials[2])
	fx.pool.mu.Unlock()

	// A connected peer is never dialed; a LAN-reachable one neither.
	fx.connect(a.peerId)
	fx.book.SetLAN(c.peerId, []string{"yamux://10.0.0.1:1"})
	fx.advance(2 * time.Minute)
	fx.g.conn.reactivate(a.peerId)
	fx.g.conn.reactivate(c.peerId)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 3, fx.pool.dialCount())
}

func TestConnectorPowerHintAndAddrsNotFound(t *testing.T) {
	sdkp2p.SetPowerHint(sdkp2p.PowerLow)
	t.Cleanup(func() { sdkp2p.SetPowerHint(sdkp2p.PowerNormal) })

	fx := newGlobalFixture(t, config.GlobalP2P{})
	a := newTestPeer(t, "a")
	sp := fx.space(a)
	sp.kv.put(a, a.ticket, fx.now)
	fx.pool.dialFn = func(context.Context, string) (peer.Peer, error) { return nil, peerservice.ErrAddrsNotFound }
	fx.run(t)
	fx.g.SpaceLoaded("s1", sp)
	fx.drain(t)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 0, fx.pool.dialCount(), "no dials while low power")

	sdkp2p.SetPowerHint(sdkp2p.PowerNormal)
	require.Eventually(t, func() bool { return fx.pool.dialCount() == 1 }, 5*time.Second, 5*time.Millisecond)
	rec, _ := fx.status.Get(a.peerId)
	require.Equal(t, 0, rec.Failures, "a lost pool load is not a failure")
	require.True(t, rec.LastAttempt.IsZero())
}

func TestGlobalSweepBumpsConnectedPeers(t *testing.T) {
	fx := newGlobalFixture(t, config.GlobalP2P{})
	a := newTestPeer(t, "a")
	b := newTestPeer(t, "b")
	sp := fx.space(a, b)
	sp.kv.put(a, a.ticket, fx.now.Add(-2*time.Hour))
	sp.kv.put(b, b.ticket, fx.now.Add(-2*time.Hour))
	fx.run(t)
	fx.g.SpaceLoaded("s1", sp)
	fx.drain(t)
	require.Equal(t, TierStale, fx.status.Tier(a.peerId))

	// an inbound connection lives only in the pool until the sweep
	fx.pool.connect(a.peerId)
	require.False(t, fx.g.connected(a.peerId))
	fx.g.sweep()
	require.True(t, fx.g.connected(a.peerId))
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

// The layer never waits on the pool: with a Pick that blocks until its
// ctx ends (an in-flight load for the same id), the inbound gate,
// connectivity checks and the sweep still answer promptly.
func TestGlobalNeverWaitsOnPool(t *testing.T) {
	fx := newGlobalFixture(t, config.GlobalP2P{})
	a := newTestPeer(t, "a")
	b := newTestPeer(t, "b")
	sp := fx.space(a, b)
	sp.kv.put(a, a.ticket, fx.now)
	sp.kv.put(b, b.ticket, fx.now)
	fx.run(t)
	fx.g.SpaceLoaded("s1", sp)
	fx.drain(t)
	fx.pool.mu.Lock()
	fx.pool.pickBlocks = true
	fx.pool.mu.Unlock()

	start := time.Now()
	require.True(t, fx.ep.filter(a.peerId))
	require.False(t, fx.g.connected(a.peerId))
	require.Less(t, time.Since(start), 20*time.Millisecond, "filter and connected read the live set, not the pool")

	start = time.Now()
	fx.g.sweep()
	require.Less(t, time.Since(start), 2*PickTimeout+50*time.Millisecond, "sweep bounds each pool lookup")
	require.Equal(t, TierActive, fx.status.Tier(a.peerId), "unchanged: nobody was seen")

	// A connector dial hands the peer to the layer directly.
	fx.connect(a.peerId)
	require.True(t, fx.g.connected(a.peerId))
	require.Equal(t, 1, fx.g.liveCount())
}

func TestCapRecords(t *testing.T) {
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	records := map[string]record{}
	for i := 0; i < maxRowsPerIdentity+3; i++ {
		records["same"+string(rune('a'+i))] = record{identity: "one", seen: now.Add(time.Duration(i) * time.Minute)}
	}
	records["other"] = record{identity: "two", seen: now.Add(-time.Hour)}
	capRecords(records)
	require.Len(t, records, maxRowsPerIdentity+1)
	_, oldestKept := records["samea"]
	require.False(t, oldestKept, "the identity's oldest rows go first")
	_, ok := records["other"]
	require.True(t, ok)

	records = map[string]record{}
	for i := 0; i < maxGlobalPeers+10; i++ {
		records["p"+string(rune(i))] = record{identity: "id" + string(rune(i)), seen: now.Add(time.Duration(i) * time.Second)}
	}
	capRecords(records)
	require.Len(t, records, maxGlobalPeers)
}

// A live row for a new device of an identity at its cap evicts that
// identity's oldest device; a new peer past the space cap is ignored.
func TestGlobalApplyHoldsCaps(t *testing.T) {
	fx := newGlobalFixture(t, config.GlobalP2P{})
	devices := make([]testPeer, 0, maxRowsPerIdentity+1)
	for i := 0; i <= maxRowsPerIdentity; i++ {
		devices = append(devices, newTestPeer(t, "multi"))
	}
	sp := fx.space(devices...)
	fx.run(t)
	fx.g.SpaceLoaded("s1", sp)
	fx.drain(t)
	// oldest row first: device 0 is the identity's oldest device. Rows
	// sit beyond the live skew window so each keeps its own timestamp.
	for i, d := range devices {
		at := fx.now.Add(-time.Duration(maxRowsPerIdentity+1-i) * 25 * time.Hour)
		fx.handler("s1")(decrypt, []innerstorage.KeyValue{{Key: RecordKey, PeerId: d.peerId, Identity: d.identity, TimestampMicro: at.UnixMicro(), Value: innerstorage.Value{Value: []byte(d.ticket)}}})
	}
	fx.drain(t)
	require.False(t, fx.store.HasGlobalPeer(devices[0].peerId), "oldest device of the identity evicted")
	require.True(t, fx.store.HasGlobalPeer(devices[maxRowsPerIdentity].peerId))
	require.Empty(t, fx.ps.addrs[devices[0].peerId])
}

// A peer that left every space is forgotten once past the disable
// threshold; one still fresh keeps its record.
func TestGlobalForgetsDisabledOrphans(t *testing.T) {
	fx := newGlobalFixture(t, config.GlobalP2P{})
	a := newTestPeer(t, "a")
	old := newTestPeer(t, "old")
	sp := fx.space(a, old)
	sp.kv.put(a, a.ticket, fx.now)
	sp.kv.put(old, old.ticket, fx.now.Add(-40*24*time.Hour))
	fx.run(t)
	fx.g.SpaceLoaded("s1", sp)
	fx.drain(t)
	_, ok := fx.status.Get(old.peerId)
	require.True(t, ok)
	fx.g.SpaceUnloaded("s1")
	_, ok = fx.status.Get(old.peerId)
	require.False(t, ok, "disabled orphan forgotten")
	_, ok = fx.status.Get(a.peerId)
	require.True(t, ok, "fresh orphan kept for its next row")
}

// A row arriving through the apply path is a sign of life regardless of
// the writer's clock, as long as its timestamp is within a day of ours;
// older rows keep their own timestamp and far-future ones clamp to now.
// Reconcile keeps row timestamps.
func TestGlobalLiveRowClockSkew(t *testing.T) {
	fx := newGlobalFixture(t, config.GlobalP2P{})
	behind := newTestPeer(t, "behind") // clock 2 h behind, row arrives live
	stale := newTestPeer(t, "stale")   // row 3 days old, arrives live
	ahead := newTestPeer(t, "ahead")   // clock far ahead, arrives live
	stored := newTestPeer(t, "stored") // same 2 h skew, but read by reconcile
	sp := fx.space(behind, stale, ahead, stored)
	sp.kv.put(stored, stored.ticket, fx.now.Add(-2*time.Hour))
	fx.run(t)
	fx.g.SpaceLoaded("s1", sp)
	fx.drain(t)
	require.Equal(t, fx.now.Add(-2*time.Hour), fx.status.LastSeen(stored.peerId), "reconcile keeps the row timestamp")
	require.Equal(t, TierStale, fx.status.Tier(stored.peerId))

	row := func(p testPeer, at time.Time) innerstorage.KeyValue {
		return innerstorage.KeyValue{Key: RecordKey, PeerId: p.peerId, Identity: p.identity, TimestampMicro: at.UnixMicro(), Value: innerstorage.Value{Value: []byte(p.ticket)}}
	}
	fx.handler("s1")(decrypt, []innerstorage.KeyValue{
		row(behind, fx.now.Add(-2*time.Hour)),
		row(stale, fx.now.Add(-3*24*time.Hour)),
		row(ahead, fx.now.Add(30*24*time.Hour)),
	})
	fx.drain(t)
	require.Equal(t, fx.now, fx.status.LastSeen(behind.peerId))
	require.Equal(t, TierActive, fx.status.Tier(behind.peerId))
	require.Equal(t, fx.now.Add(-3*24*time.Hour), fx.status.LastSeen(stale.peerId))
	require.Equal(t, TierStale, fx.status.Tier(stale.peerId))
	require.Equal(t, fx.now, fx.status.LastSeen(ahead.peerId), "far-future rows clamp to now")
}
