package p2p

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/keyvalue/keyvaluestorage"
	"github.com/anyproto/any-sync/commonspace/object/keyvalue/keyvaluestorage/innerstorage"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/peerservice"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/transport"
	"github.com/anyproto/any-sync/net/transport/iroh"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/config"
	sdkp2p "github.com/anyproto/any-sync-sdk/p2p"
)

const (
	globalCName = "sdk.p2p.global"

	// heartbeatInterval paces the periodic re-set of the own record in
	// every loaded space; heartbeatStaleAfter is the row age past which
	// a space load re-sets it right away. Row TimestampMicro is the
	// remote liveness marker, so a stable ticket alone is not enough.
	heartbeatInterval   = 24 * time.Hour
	heartbeatStaleAfter = 12 * time.Hour
	// reconcileInterval re-reads every loaded space's records, catching
	// ACL removals and tier changes that no row write announces. It is
	// the only signal for removals: syncacl's single AclUpdater slot is
	// owned by the space layer above.
	reconcileInterval = 5 * time.Minute
	// sweepInterval bumps LastSeen of every global peer that is still
	// connected — cheap, no network.
	sweepInterval = 10 * time.Second
	// publishDebounce coalesces ticket changes before republishing.
	publishDebounce = 3 * time.Second
	// opTimeout bounds one publish or reconcile of one space on the
	// single worker.
	opTimeout = 30 * time.Second
	// closeTimeout bounds Close's wait for the workers.
	globalCloseTimeout = 2 * time.Second
	taskQueueSize      = 1024
	// maxRowsPerIdentity caps the devices one identity may announce in
	// one space (newest win); maxGlobalPeers caps the peers one space
	// contributes. Both bound what a member can make us track.
	maxRowsPerIdentity = 8
	maxGlobalPeers     = 256
	// globalPeerTTL keeps a connected global peer in the pool through
	// idle periods; the pool's default minute would churn relay
	// connections.
	globalPeerTTL = 30 * time.Minute
)

// SpaceKV is the slice of a loaded space the global layer reads and
// writes: its default key-value store and two ACL answers.
type SpaceKV interface {
	Store() keyvaluestorage.Storage
	// CanWrite reports whether this device may Set rows (writer+).
	CanWrite() bool
	// IsMember reports whether identity (account address) still holds
	// any permission in the space.
	IsMember(identity string) bool
}

// KVHandler receives applied key-value writes of one space. Runs on
// any-sync's apply path — decode and enqueue only.
type KVHandler func(decryptor keyvaluestorage.Decryptor, kvs []innerstorage.KeyValue)

// KVSubscriber registers a KVHandler for a space; the app layer wires
// its per-space dispatcher here.
type KVSubscriber func(spaceId string, h KVHandler) (cancel func())

// endpoint is the slice of the iroh transport the layer uses.
type endpoint interface {
	Ticket() string
	TicketUpdates() <-chan struct{}
	RelayConnected() bool
	SetIncomingFilter(f func(peerId string) bool)
}

// globalPool is the slice of the any-sync pool the layer uses. The
// connector never calls Get: it dials through globalDialer and hands
// the peer to AddPeer, so no in-flight pool load ever exists for a
// global peer and Pick answers at once.
type globalPool interface {
	Pick(ctx context.Context, id string) (peer.Peer, error)
	AddPeer(ctx context.Context, p peer.Peer) error
}

// globalDialer is the peer service: the only dial path of the layer.
type globalDialer interface {
	Dial(ctx context.Context, peerId string) (peer.Peer, error)
}

type taskKind uint8

const (
	taskPublish taskKind = iota
	taskReconcile
	taskApply
)

// task is one unit of worker work. Empty spaceId with publish /
// reconcile means every loaded space.
type task struct {
	kind    taskKind
	spaceId string
	peerId  string
	rec     record
}

// globalSpace is a loaded, non-local-only, non-guest space.
type globalSpace struct {
	kv      SpaceKV
	cancel  func()
	records map[string]record // by peer id
}

// Global is the internet-wide p2p layer: it publishes this device's
// endpoint ticket into every loaded space's key-value store, learns the
// other members' tickets from the same rows, keeps the peer store /
// addr book / status book in sync with them, gates inbound connections
// to known members, and runs the connector that maintains a bounded
// set of global connections. Everything network-facing happens on the
// connector; the key-value side never dials.
type Global struct {
	cfg          config.GlobalP2P
	selfPeerId   string
	selfIdentity string
	store        *PeerStore
	status       *StatusBook
	book         *AddrBook
	ep           endpoint
	pool         globalPool
	dialer       globalDialer
	subscribe    KVSubscriber
	now          func() time.Time
	// publishDebounce coalesces ticket changes before republishing.
	publishDebounce time.Duration

	mu     sync.Mutex
	spaces map[string]*globalSpace

	// live holds the global peers with a connection this layer knows
	// about: connector dials, plus inbound connections the sweep folds
	// in. The inbound gate and the connector read it — never the pool.
	liveMu sync.Mutex
	live   map[string]peer.Peer
	// onLive is told about every newly live global peer.
	onLive func(peerId string)

	tasks chan task
	conn  *connector

	runCtx    context.Context
	runCancel context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// NewGlobal builds the layer; zero budget fields take their defaults.
func NewGlobal(cfg config.GlobalP2P, selfPeerId, selfIdentity string, store *PeerStore, status *StatusBook, book *AddrBook) *Global {
	g := &Global{
		cfg:             cfg.WithDefaults(),
		selfPeerId:      selfPeerId,
		selfIdentity:    selfIdentity,
		store:           store,
		status:          status,
		book:            book,
		now:             time.Now,
		publishDebounce: publishDebounce,
		spaces:          map[string]*globalSpace{},
		live:            map[string]peer.Peer{},
		tasks:           make(chan task, taskQueueSize),
	}
	g.conn = newConnector(g)
	return g
}

func (g *Global) Init(a *app.App) error {
	if g.ep == nil {
		g.ep = a.MustComponent(transport.IrohCName).(iroh.Iroh)
	}
	if g.pool == nil {
		g.pool = a.MustComponent(pool.CName).(pool.Pool)
	}
	if g.dialer == nil {
		g.dialer = a.MustComponent(peerservice.CName).(peerservice.PeerService)
	}
	// The transport refuses to start without a filter: nobody is let in
	// before the allowlist exists.
	g.ep.SetIncomingFilter(g.allowInbound)
	return nil
}

func (g *Global) Name() string { return globalCName }

// SetKVSubscriber wires the per-space key-value dispatcher. Set during
// app assembly, before any space loads.
func (g *Global) SetKVSubscriber(fn KVSubscriber) { g.subscribe = fn }

// SetOnLive registers a callback for every global peer that becomes
// live (dialed or accepted). Set during app assembly.
func (g *Global) SetOnLive(fn func(peerId string)) { g.onLive = fn }

func (g *Global) Run(_ context.Context) error {
	if err := g.status.Load(); err != nil {
		log.Warn("load peer status", zap.Error(err))
	}
	g.status.SetOnAdvance(g.conn.reactivate)
	g.runCtx, g.runCancel = context.WithCancel(context.Background())
	g.wg.Add(4)
	go g.worker()
	go g.watchTicket()
	go g.tickers()
	go g.conn.loop(g.runCtx, &g.wg)
	return nil
}

// Close stops the workers (bounded wait), then flushes and closes the
// status book.
func (g *Global) Close(_ context.Context) error {
	g.closeOnce.Do(func() {
		if g.runCancel != nil {
			g.runCancel()
		}
		done := make(chan struct{})
		go func() {
			g.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(globalCloseTimeout):
			log.Warn("global p2p workers did not stop in time")
		}
		g.mu.Lock()
		for _, sp := range g.spaces {
			if sp.cancel != nil {
				sp.cancel()
			}
		}
		g.mu.Unlock()
	})
	return g.status.Close()
}

// SpaceLoaded registers a loaded space: its records are read, its
// applied writes followed, and the own record published (or re-set
// when stale). Local-only and guest spaces must not be registered.
func (g *Global) SpaceLoaded(spaceId string, kv SpaceKV) {
	g.mu.Lock()
	if _, ok := g.spaces[spaceId]; ok {
		g.mu.Unlock()
		return
	}
	sp := &globalSpace{kv: kv, records: map[string]record{}}
	g.spaces[spaceId] = sp
	g.mu.Unlock()
	if g.subscribe != nil {
		cancel := g.subscribe(spaceId, g.handlerFor(spaceId))
		g.mu.Lock()
		// an unload that raced the subscription finds no cancel to call
		if g.spaces[spaceId] != sp {
			g.mu.Unlock()
			cancel()
			return
		}
		sp.cancel = cancel
		g.mu.Unlock()
	}
	g.enqueue(task{kind: taskReconcile, spaceId: spaceId})
	g.enqueue(task{kind: taskPublish, spaceId: spaceId})
}

// SpaceUnloaded drops a space: its records stop contributing to the
// peer store, and peers known only through it disappear.
func (g *Global) SpaceUnloaded(spaceId string) {
	g.mu.Lock()
	sp, ok := g.spaces[spaceId]
	if !ok {
		g.mu.Unlock()
		return
	}
	delete(g.spaces, spaceId)
	cancel := sp.cancel
	peers := make([]string, 0, len(sp.records))
	for id := range sp.records {
		peers = append(peers, id)
	}
	g.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	g.recompute(peers...)
}

// LoadedSpaceIds lists the registered spaces.
func (g *Global) LoadedSpaceIds() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]string, 0, len(g.spaces))
	for id := range g.spaces {
		out = append(out, id)
	}
	return out
}

func (g *Global) space(spaceId string) *globalSpace {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.spaces[spaceId]
}

// requireRelay reports whether consumed tickets must be relay-only: with
// relays configured, a direct address in a row is a member pointing our
// dials at a host of its choosing.
func (g *Global) requireRelay() bool { return len(g.cfg.RelayURLs) > 0 }

// clamp bounds a publisher timestamp to now.
func (g *Global) clamp(at time.Time) time.Time {
	if now := g.now(); at.After(now) {
		return now
	}
	return at
}

// enqueue hands a task to the worker; a full queue drops it — the
// periodic reconcile / heartbeat replays whatever was missed.
func (g *Global) enqueue(t task) {
	select {
	case g.tasks <- t:
	default:
		log.Warn("global p2p task queue full; dropping", zap.Int("kind", int(t.kind)), zap.String("spaceId", t.spaceId))
	}
}

// handlerFor is the apply-path hook of one space: decode the row,
// verify the ticket belongs to its signer, enqueue. The membership
// check needs the ACL lock and runs on the worker instead.
func (g *Global) handlerFor(spaceId string) KVHandler {
	return func(decryptor keyvaluestorage.Decryptor, kvs []innerstorage.KeyValue) {
		for _, kv := range kvs {
			if kv.Key != RecordKey || kv.PeerId == g.selfPeerId {
				continue
			}
			raw, err := decryptor(kv)
			if err != nil {
				log.Debug("global p2p record decrypt", zap.String("peerId", kv.PeerId), zap.Error(err))
				continue
			}
			ticket, err := parseTicket(kv.PeerId, raw, g.requireRelay())
			if err != nil {
				log.Debug("global p2p record rejected", zap.String("spaceId", spaceId), zap.String("peerId", kv.PeerId), zap.Error(err))
				continue
			}
			g.enqueue(task{kind: taskApply, spaceId: spaceId, peerId: kv.PeerId, rec: record{
				ticket:   ticket,
				identity: kv.Identity,
				seen:     g.clamp(time.UnixMicro(kv.TimestampMicro).UTC()),
			}})
		}
	}
}

func (g *Global) worker() {
	defer g.wg.Done()
	for {
		select {
		case <-g.runCtx.Done():
			return
		case t := <-g.tasks:
			g.handle(t)
		}
	}
}

func (g *Global) handle(t task) {
	switch t.kind {
	case taskPublish:
		g.forSpaces(t.spaceId, g.publish)
	case taskReconcile:
		g.forSpaces(t.spaceId, g.reconcile)
	case taskApply:
		g.apply(t.spaceId, t.peerId, t.rec)
	}
}

// forSpaces runs fn for one space, or for every loaded space when
// spaceId is empty.
func (g *Global) forSpaces(spaceId string, fn func(spaceId string)) {
	if spaceId != "" {
		fn(spaceId)
		return
	}
	for _, id := range g.LoadedSpaceIds() {
		select {
		case <-g.runCtx.Done():
			return
		default:
		}
		fn(id)
	}
}

// publish (re-)sets the own record in a space when the ticket changed
// or the row is older than heartbeatStaleAfter. Readers can't write
// rows and stay dial-only.
func (g *Global) publish(spaceId string) {
	ticket := g.ep.Ticket()
	sp := g.space(spaceId)
	if ticket == "" || sp == nil || !sp.kv.CanWrite() {
		return
	}
	ctx, cancel := context.WithTimeout(g.runCtx, opTimeout)
	defer cancel()
	var (
		cur     string
		curSeen time.Time
	)
	err := sp.kv.Store().GetAll(ctx, RecordKey, func(decryptor keyvaluestorage.Decryptor, values []innerstorage.KeyValue) error {
		for _, kv := range values {
			if kv.Key != RecordKey || kv.PeerId != g.selfPeerId {
				continue
			}
			raw, decErr := decryptor(kv)
			if decErr != nil {
				continue
			}
			cur = string(raw)
			curSeen = time.UnixMicro(kv.TimestampMicro).UTC()
		}
		return nil
	})
	if err != nil {
		log.Warn("global p2p own record", zap.String("spaceId", spaceId), zap.Error(err))
		return
	}
	if cur == ticket && g.now().Sub(curSeen) < heartbeatStaleAfter {
		return
	}
	if err = sp.kv.Store().Set(ctx, RecordKey, []byte(ticket)); err != nil {
		log.Info("global p2p publish", zap.String("spaceId", spaceId), zap.Error(err))
		return
	}
	log.Debug("global p2p record published", zap.String("spaceId", spaceId))
}

// reconcile re-reads a space's records from the store and replaces the
// in-memory set, so ACL removals and rows applied while no handler was
// registered are picked up.
func (g *Global) reconcile(spaceId string) {
	sp := g.space(spaceId)
	if sp == nil {
		return
	}
	ctx, cancel := context.WithTimeout(g.runCtx, opTimeout)
	defer cancel()
	fresh := map[string]record{}
	err := sp.kv.Store().GetAll(ctx, RecordKey, func(decryptor keyvaluestorage.Decryptor, values []innerstorage.KeyValue) error {
		for _, kv := range values {
			if kv.Key != RecordKey || kv.PeerId == g.selfPeerId {
				continue
			}
			raw, decErr := decryptor(kv)
			if decErr != nil {
				continue
			}
			ticket, pErr := parseTicket(kv.PeerId, raw, g.requireRelay())
			if pErr != nil {
				log.Debug("global p2p record rejected", zap.String("spaceId", spaceId), zap.String("peerId", kv.PeerId), zap.Error(pErr))
				continue
			}
			fresh[kv.PeerId] = record{ticket: ticket, identity: kv.Identity, seen: g.clamp(time.UnixMicro(kv.TimestampMicro).UTC())}
		}
		return nil
	})
	if err != nil {
		log.Warn("global p2p reconcile", zap.String("spaceId", spaceId), zap.Error(err))
		return
	}
	for id, rec := range fresh {
		if !sp.kv.IsMember(rec.identity) {
			delete(fresh, id)
		}
	}
	capRecords(fresh)
	g.mu.Lock()
	touched := make([]string, 0, len(sp.records)+len(fresh))
	for id := range sp.records {
		touched = append(touched, id)
	}
	for id := range fresh {
		touched = append(touched, id)
	}
	sp.records = fresh
	g.mu.Unlock()
	g.recompute(touched...)
}

// capRecords trims a space's record set to the caps: at most
// maxRowsPerIdentity newest rows per identity, at most maxGlobalPeers
// newest rows overall.
func capRecords(records map[string]record) {
	type row struct {
		id  string
		rec record
	}
	byIdentity := map[string][]row{}
	for id, rec := range records {
		byIdentity[rec.identity] = append(byIdentity[rec.identity], row{id: id, rec: rec})
	}
	newestFirst := func(a, b row) int { return b.rec.seen.Compare(a.rec.seen) }
	var all []row
	for _, rows := range byIdentity {
		slices.SortFunc(rows, newestFirst)
		for i, r := range rows {
			if i >= maxRowsPerIdentity {
				delete(records, r.id)
				continue
			}
			all = append(all, r)
		}
	}
	if len(all) <= maxGlobalPeers {
		return
	}
	slices.SortFunc(all, newestFirst)
	for _, r := range all[maxGlobalPeers:] {
		delete(records, r.id)
	}
}

// apply upserts one live row, holding the per-identity and per-space
// caps: a new device of an identity at its cap evicts that identity's
// oldest row; a new peer past the space cap is ignored.
func (g *Global) apply(spaceId, peerId string, rec record) {
	sp := g.space(spaceId)
	if sp == nil {
		return
	}
	if !sp.kv.IsMember(rec.identity) {
		g.mu.Lock()
		delete(sp.records, peerId)
		g.mu.Unlock()
		g.recompute(peerId)
		return
	}
	g.mu.Lock()
	cur, known := sp.records[peerId]
	if known && cur.ticket == rec.ticket && !rec.seen.After(cur.seen) {
		g.mu.Unlock()
		return
	}
	var evicted []string
	if !known {
		if len(sp.records) >= maxGlobalPeers {
			g.mu.Unlock()
			log.Debug("global p2p record ignored: space at peer cap", zap.String("spaceId", spaceId), zap.String("peerId", peerId))
			return
		}
		var oldestId string
		var oldest record
		n := 0
		for id, r := range sp.records {
			if r.identity != rec.identity {
				continue
			}
			n++
			if oldestId == "" || r.seen.Before(oldest.seen) {
				oldestId, oldest = id, r
			}
		}
		if n >= maxRowsPerIdentity {
			delete(sp.records, oldestId)
			evicted = append(evicted, oldestId)
		}
	}
	sp.records[peerId] = rec
	g.mu.Unlock()
	g.recompute(append(evicted, peerId)...)
}

// recompute folds a peer's records across loaded spaces into the peer
// store, addr book and status book: the newest row wins the ticket,
// its timestamp is liveness evidence, disabled peers lose their
// ticket. A peer gone from every space and every source is forgotten
// once its record is past the disable threshold. Wakes the connector.
func (g *Global) recompute(peerIds ...string) {
	for _, peerId := range peerIds {
		var (
			spaceIds []string
			latest   record
		)
		g.mu.Lock()
		for spaceId, sp := range g.spaces {
			rec, ok := sp.records[peerId]
			if !ok {
				continue
			}
			spaceIds = append(spaceIds, spaceId)
			if rec.seen.After(latest.seen) {
				latest = rec
			}
		}
		g.mu.Unlock()
		if len(spaceIds) == 0 {
			g.store.RemoveGlobalPeer(peerId)
			g.book.ClearTicket(peerId)
			g.conn.forget(peerId)
			if len(g.store.Sources(peerId)) == 0 && g.status.Tier(peerId) == TierDisabled {
				g.status.Forget(peerId)
			}
			continue
		}
		g.status.Seen(peerId, latest.seen)
		if g.status.Tier(peerId) == TierDisabled {
			g.book.ClearTicket(peerId)
		} else {
			g.book.SetTicket(peerId, latest.ticket)
		}
		g.store.UpdateGlobalPeer(peerId, spaceIds)
	}
	g.conn.wakeUp()
}

// peerIdentity returns the identity behind a global peer's newest
// record; empty when unknown.
func (g *Global) peerIdentity(peerId string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	var latest record
	for _, sp := range g.spaces {
		if rec, ok := sp.records[peerId]; ok && rec.seen.After(latest.seen) {
			latest = rec
		}
	}
	return latest.identity
}

// watchTicket republishes into every loaded space when the own ticket
// changes, debounced.
func (g *Global) watchTicket() {
	defer g.wg.Done()
	var (
		pending bool
		due     time.Time
	)
	for {
		var wait <-chan time.Time
		if pending {
			wait = time.After(time.Until(due))
		}
		select {
		case <-g.runCtx.Done():
			return
		case <-g.ep.TicketUpdates():
			pending = true
			due = time.Now().Add(g.publishDebounce)
		case <-wait:
			pending = false
			g.enqueue(task{kind: taskPublish})
		}
	}
}

// tickers drive the heartbeat, the reconcile and the liveness sweep.
func (g *Global) tickers() {
	defer g.wg.Done()
	heartbeat := time.NewTicker(heartbeatInterval)
	reconcile := time.NewTicker(reconcileInterval)
	sweep := time.NewTicker(sweepInterval)
	defer heartbeat.Stop()
	defer reconcile.Stop()
	defer sweep.Stop()
	for {
		select {
		case <-g.runCtx.Done():
			return
		case <-heartbeat.C:
			g.enqueue(task{kind: taskPublish})
		case <-reconcile.C:
			g.enqueue(task{kind: taskReconcile})
		case <-sweep.C:
			g.sweep()
		}
	}
}

// sweep folds inbound-accepted global peers into the live set and bumps
// LastSeen of every global peer with a live connection. Pool lookups are
// bounded by PickTimeout and never dial.
func (g *Global) sweep() {
	now := g.now()
	for _, id := range g.store.AllGlobalPeers() {
		if g.runCtx != nil && g.runCtx.Err() != nil {
			return
		}
		if !g.connected(id) {
			p := g.pickIroh(id)
			if p == nil {
				continue
			}
			g.addLive(p)
		}
		g.status.Seen(id, now)
	}
}

// pickIroh returns the pool's live iroh connection to a peer, nil when
// there is none within PickTimeout.
func (g *Global) pickIroh(peerId string) peer.Peer {
	ctx := g.runCtx
	if ctx == nil {
		ctx = context.Background()
	}
	p, err := PickLive(ctx, g.pool, peerId)
	if err != nil || !strings.HasPrefix(peer.CtxPeerAddr(p.Context()), transport.Iroh+"://") {
		return nil
	}
	return p
}

// addLive records a live global connection and drops it when the
// connection closes.
func (g *Global) addLive(p peer.Peer) {
	id := p.Id()
	g.liveMu.Lock()
	if cur, ok := g.live[id]; ok && cur == p {
		g.liveMu.Unlock()
		return
	}
	g.live[id] = p
	g.liveMu.Unlock()
	if g.onLive != nil {
		g.onLive(id)
	}
	done := g.runCtx
	if done == nil {
		done = context.Background()
	}
	go func() {
		select {
		case <-p.CloseChan():
		case <-done.Done():
		}
		g.liveMu.Lock()
		if g.live[id] == p {
			delete(g.live, id)
		}
		g.liveMu.Unlock()
	}()
}

// connected reports a live global connection to the peer. Reads the
// layer's own set — never the pool, never blocks.
func (g *Global) connected(peerId string) bool {
	g.liveMu.Lock()
	p, ok := g.live[peerId]
	g.liveMu.Unlock()
	return ok && !p.IsClosed()
}

// liveCount is the number of distinct global peers with a live
// connection.
func (g *Global) liveCount() int {
	g.liveMu.Lock()
	defer g.liveMu.Unlock()
	n := 0
	for _, p := range g.live {
		if !p.IsClosed() {
			n++
		}
	}
	return n
}

// allowInbound is the iroh incoming filter: only members known through
// key-value records and not disabled, and only while fewer than
// MaxConnections+MaxInbound distinct global peers are connected. O(1):
// it runs on the accept path before any handshake.
func (g *Global) allowInbound(peerId string) bool {
	if !g.store.HasGlobalPeer(peerId) || g.status.Tier(peerId) == TierDisabled {
		return false
	}
	return g.liveCount() < g.cfg.MaxConnections+g.cfg.MaxInbound
}

// Status is the debug snapshot of the layer.
func (g *Global) Status() sdkp2p.GlobalStatus {
	st := sdkp2p.GlobalStatus{Enabled: true, Ticket: g.ep.Ticket(), RelayConnected: g.ep.RelayConnected()}
	if id, err := iroh.EndpointIdFromPeerId(g.selfPeerId); err == nil {
		st.EndpointId = id.String()
	}
	st.HomeRelay = homeRelay(st.Ticket)
	for _, id := range g.store.AllGlobalPeers() {
		st.Peers = append(st.Peers, g.PeerStatus(id))
	}
	return st
}

// PeerStatus fills the liveness fields of one peer.
func (g *Global) PeerStatus(peerId string) sdkp2p.PeerStatus {
	rec, _ := g.status.Get(peerId)
	ps := sdkp2p.PeerStatus{
		PeerId:    peerId,
		SpaceIds:  g.store.SpaceIds(peerId),
		Connected: g.connected(peerId),
		LastSeen:  rec.LastSeen,
		Tier:      g.status.Tier(peerId).String(),
		Failures:  rec.Failures,
	}
	for _, src := range g.store.Sources(peerId) {
		ps.Sources = append(ps.Sources, src.String())
	}
	return ps
}
