package p2p

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/keyvalue/keyvaluestorage"
	"github.com/anyproto/any-sync/commonspace/object/keyvalue/keyvaluestorage/innerstorage"
	"github.com/anyproto/any-sync/net/peer"
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
	// ACL removals and tier changes that no row write announces.
	reconcileInterval = 30 * time.Minute
	// sweepInterval bumps LastSeen of every global peer that is still
	// connected — cheap, no network.
	sweepInterval = time.Minute
	// publishDebounce coalesces ticket changes before republishing.
	publishDebounce = 3 * time.Second
	// closeTimeout bounds Close's wait for the workers.
	globalCloseTimeout = 2 * time.Second
	taskQueueSize      = 1024
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

// globalPool is the slice of the any-sync pool the layer uses: Get for
// the connector only, Pick everywhere else.
type globalPool interface {
	Get(ctx context.Context, id string) (peer.Peer, error)
	Pick(ctx context.Context, id string) (peer.Peer, error)
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
	cfg          config.Global
	selfPeerId   string
	selfIdentity string
	store        *PeerStore
	status       *StatusBook
	book         *AddrBook
	ep           endpoint
	pool         globalPool
	subscribe    KVSubscriber
	now          func() time.Time

	mu     sync.Mutex
	spaces map[string]*globalSpace

	tasks chan task
	conn  *connector

	runCtx    context.Context
	runCancel context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// NewGlobal builds the layer. cfg must have its defaults applied.
func NewGlobal(cfg config.Global, selfPeerId, selfIdentity string, store *PeerStore, status *StatusBook, book *AddrBook) *Global {
	g := &Global{
		cfg:          cfg,
		selfPeerId:   selfPeerId,
		selfIdentity: selfIdentity,
		store:        store,
		status:       status,
		book:         book,
		now:          time.Now,
		spaces:       map[string]*globalSpace{},
		tasks:        make(chan task, taskQueueSize),
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
	// The transport refuses to start without a filter: nobody is let in
	// before the allowlist exists.
	g.ep.SetIncomingFilter(g.allowInbound)
	return nil
}

func (g *Global) Name() string { return globalCName }

// SetKVSubscriber wires the per-space key-value dispatcher. Set during
// app assembly, before any space loads.
func (g *Global) SetKVSubscriber(fn KVSubscriber) { g.subscribe = fn }

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

// Close stops the workers (bounded wait) and flushes the status book.
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
	peers := make([]string, 0, len(sp.records))
	for id := range sp.records {
		peers = append(peers, id)
	}
	g.mu.Unlock()
	if sp.cancel != nil {
		sp.cancel()
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
			ticket, _, err := parseTicket(kv.PeerId, g.selfPeerId, raw)
			if err != nil {
				log.Info("global p2p record rejected", zap.String("spaceId", spaceId), zap.String("peerId", kv.PeerId), zap.Error(err))
				continue
			}
			g.enqueue(task{kind: taskApply, spaceId: spaceId, peerId: kv.PeerId, rec: record{
				ticket:   ticket,
				identity: kv.Identity,
				seen:     time.UnixMicro(kv.TimestampMicro).UTC(),
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
	var (
		cur     string
		curSeen time.Time
	)
	err := sp.kv.Store().GetAll(g.runCtx, RecordKey, func(decryptor keyvaluestorage.Decryptor, values []innerstorage.KeyValue) error {
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
	if err = sp.kv.Store().Set(g.runCtx, RecordKey, []byte(ticket)); err != nil {
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
	fresh := map[string]record{}
	err := sp.kv.Store().GetAll(g.runCtx, RecordKey, func(decryptor keyvaluestorage.Decryptor, values []innerstorage.KeyValue) error {
		for _, kv := range values {
			if kv.Key != RecordKey || kv.PeerId == g.selfPeerId {
				continue
			}
			raw, decErr := decryptor(kv)
			if decErr != nil {
				continue
			}
			ticket, _, pErr := parseTicket(kv.PeerId, g.selfPeerId, raw)
			if pErr != nil {
				log.Info("global p2p record rejected", zap.String("spaceId", spaceId), zap.String("peerId", kv.PeerId), zap.Error(pErr))
				continue
			}
			fresh[kv.PeerId] = record{ticket: ticket, identity: kv.Identity, seen: time.UnixMicro(kv.TimestampMicro).UTC()}
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

// apply upserts one live row.
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
	if cur, ok := sp.records[peerId]; ok && cur.ticket == rec.ticket && !rec.seen.After(cur.seen) {
		g.mu.Unlock()
		return
	}
	sp.records[peerId] = rec
	g.mu.Unlock()
	g.recompute(peerId)
}

// recompute folds a peer's records across loaded spaces into the peer
// store, addr book and status book: the newest row wins the ticket,
// its timestamp is liveness evidence, disabled peers lose their
// ticket. Wakes the connector.
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
			due = time.Now().Add(publishDebounce)
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

// sweep bumps LastSeen of every global peer that still has a live
// connection. pool.Pick never dials.
func (g *Global) sweep() {
	now := g.now()
	for _, id := range g.store.AllGlobalPeers() {
		if g.connected(id) {
			g.status.Seen(id, now)
		}
	}
}

// connected reports a live connection to the peer, without dialing.
// Pick never blocks, so no lifetime ctx is needed (the inbound filter
// calls this before Run).
func (g *Global) connected(peerId string) bool {
	_, err := g.pool.Pick(context.Background(), peerId)
	return err == nil
}

// allowInbound is the iroh incoming filter: only members known through
// key-value records and not disabled, and only while the live global
// connection count is under MaxConnections+MaxInbound.
func (g *Global) allowInbound(peerId string) bool {
	if !g.store.HasGlobalPeer(peerId) || g.status.Tier(peerId) == TierDisabled {
		return false
	}
	return g.liveGlobalConns() < g.cfg.MaxConnections+g.cfg.MaxInbound
}

// liveGlobalConns counts known global peers connected over iroh.
func (g *Global) liveGlobalConns() int {
	n := 0
	for _, id := range g.store.AllGlobalPeers() {
		p, err := g.pool.Pick(context.Background(), id)
		if err != nil {
			continue
		}
		if strings.HasPrefix(peer.CtxPeerAddr(p.Context()), transport.Iroh+"://") {
			n++
		}
	}
	return n
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
