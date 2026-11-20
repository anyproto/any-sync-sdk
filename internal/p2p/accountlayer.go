package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/anyproto/any-sync/net/transport/iroh"
	"github.com/anyproto/any-sync/util/crypto"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/p2p/account"
)

const (
	// accountInterval paces the record cycle: resolve (a GET per relay,
	// cheap), and publish when the own entry is missing, moved or stale.
	// It is also how fast a relay wipe or a sibling's fresh entry heals.
	accountInterval = 10 * time.Minute
	// accountRefreshAfter is the own-entry age past which a cycle
	// republishes even if nothing else changed; the entry's timestamp is
	// the sibling devices' liveness evidence, like a space row's
	// heartbeat.
	accountRefreshAfter = 30 * time.Minute
	// accountRetry is the pause after the first failed cycle; it doubles
	// per consecutive failure up to accountRetryMax.
	accountRetry    = time.Minute
	accountRetryMax = accountInterval
	// accountKeepUnnamed is how long a sibling the record does not name
	// stays known: a device admitted by its handshake keeps its place
	// until its own entry lands or it falls silent.
	accountKeepUnnamed = time.Hour
	// accountSkewWarn is the lead a resolved packet's timestamp may have
	// over the local clock before the skew is reported.
	accountSkewWarn = 5 * time.Minute
	// accountRecordFileVersion is the persisted last-read record's
	// format version.
	accountRecordFileVersion = 1

	// unknownInflight bounds the unknown peers between the incoming
	// filter and the handshake verdict: each one costs an any-sync
	// handshake, and concurrency is that cost's only lever.
	unknownInflight = 2
	// unknownInflightTTL frees a slot whose handshake never reported a
	// verdict (the transport gave up on it).
	unknownInflightTTL = 20 * time.Second
	// unknownPerMinute bounds the DISTINCT unknown peer ids admitted per
	// minute; an id counted in the window is refused on repeat, so a
	// retrying stranger cannot recharge.
	unknownPerMinute = 30
	// unknownDenyFor is how long a peer that failed the identity check
	// is refused before the handshake, without spending anything.
	unknownDenyFor = 10 * time.Minute
	// unknownTracked bounds the budget's bookkeeping.
	unknownTracked = 1024
)

// accountEntry is one sibling device from the account record.
type accountEntry struct {
	relay string
	seen  time.Time
}

// accountLayer is the account-level discovery: the record every device
// of the account registers itself in and resolves its siblings from.
// Siblings are global peers of every space, so they need no space row;
// a fresh device with nothing but the mnemonic finds them the same way.
type accountLayer struct {
	keys     *account.Keys
	client   accountClient
	insecure bool
	path     string

	mu    sync.Mutex
	peers map[string]accountEntry
	// last is the newest record this device decoded (or loaded from
	// disk): the base every publish merges onto, so an answer that is
	// missing or unreadable never drops a sibling the account knows.
	last          *account.Record
	lastRead      time.Time
	wake          chan struct{}
	lastOK        time.Time
	lastPublished time.Time
	lastErr       string
	ownEntry      bool
	ahead         time.Duration
	failures      int
	newerLogged   bool
	skewLogged    bool
	unknown       unknownBudget
}

// accountClient is the slice of account.Client the layer uses.
type accountClient interface {
	Publish(ctx context.Context, p *account.SignedPacket) error
	Resolve(ctx context.Context, pub account.PublicKey) (*account.SignedPacket, error)
	Relays() []string
}

// persistedRecord is the on-disk form of the last decoded record.
type persistedRecord struct {
	Version int               `json:"v"`
	ReadAt  time.Time         `json:"readAt"`
	Devices []persistedDevice `json:"devices"`
}

type persistedDevice struct {
	PeerId   string    `json:"peerId"`
	Relay    string    `json:"relay"`
	LastSeen time.Time `json:"lastSeen"`
}

// SetAccount turns the account layer on. Set before Run. insecure
// admits http:// relays named by sibling entries, for test relays; path
// is where the last decoded record is kept across restarts (empty
// keeps it in memory only).
func (g *Global) SetAccount(keys *account.Keys, client accountClient, insecure bool, path string) {
	a := &accountLayer{
		keys:     keys,
		client:   client,
		insecure: insecure,
		path:     path,
		peers:    map[string]accountEntry{},
		wake:     make(chan struct{}, 1),
	}
	if rec, readAt, ok := a.loadRecord(); ok {
		a.last, a.lastRead = &rec, readAt
	}
	g.acct.Store(a)
	if a.last != nil {
		g.applyAccountRecord(*a.last, g.now())
	}
}

// account returns the layer, nil when off.
func (g *Global) account() *accountLayer { return g.acct.Load() }

// AccountEnabled reports whether the account layer is on.
func (g *Global) AccountEnabled() bool { return g.account() != nil }

// RepublishAccount runs a record cycle soon (ticket change, own
// relay change).
func (g *Global) RepublishAccount() {
	a := g.account()
	if a == nil {
		return
	}
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

// accountLoop drives the record cycle: at start, every accountInterval, on wake, and
// after a failure with exponential backoff.
func (g *Global) accountLoop() {
	defer g.wg.Done()
	a := g.account()
	delay := time.Duration(0)
	for {
		select {
		case <-g.runCtx.Done():
			return
		case <-a.wake:
		case <-time.After(delay):
		}
		err := g.accountCycle()
		delay = a.noteCycle(err)
	}
}

// noteCycle records a cycle's outcome and returns the pause before the
// next one; the first failure in a run is logged at Info, repeats at
// Debug. Errors never carry the record address: the client strips URLs.
func (a *accountLayer) noteCycle(err error) time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err == nil {
		a.failures = 0
		a.lastErr = ""
		return accountInterval
	}
	a.failures++
	a.lastErr = err.Error()
	delay := accountRetry
	for i := 1; i < a.failures && delay < accountRetryMax; i++ {
		delay *= 2
	}
	delay = min(delay, accountRetryMax)
	if a.failures == 1 {
		log.Info("account record cycle", zap.Error(err), zap.Duration("retry", delay))
	} else {
		log.Debug("account record cycle", zap.Error(err), zap.Int("failures", a.failures), zap.Duration("retry", delay))
	}
	return delay
}

// accountCycle resolves the record, folds its devices into the peer
// store, and publishes the own entry when it is missing, names another
// relay, or is older than accountRefreshAfter.
//
// Publishing always merges onto the newest record this device decoded:
// an answer that is missing (a relay restart is not a sign the
// siblings are gone) or unreadable keeps the known siblings and is
// overwritten with them; a record written by a newer version is read
// as far as this layout goes and never overwritten.
func (g *Global) accountCycle() error {
	a := g.account()
	packet, err := a.resolve(g.runCtx)
	if err != nil {
		return err
	}
	now := g.now()
	a.mu.Lock()
	a.ahead = 0
	a.mu.Unlock()
	if packet != nil {
		a.noteSkew(packet.Timestamp(), now)
		rec, oerr := account.Open(a.keys, packet)
		switch {
		case oerr == nil:
			g.applyAccountRecord(rec, now)
			a.setLast(rec, now)
			if rec.Version > account.RecordVersion {
				a.mu.Lock()
				logIt := !a.newerLogged
				a.newerLogged = true
				a.mu.Unlock()
				if logIt {
					log.Warn("account record written by a newer version; not publishing", zap.Uint8("version", rec.Version))
				}
				return nil
			}
		default:
			log.Warn("account record unreadable; publishing from the last known one", zap.Error(oerr))
		}
	}
	base := a.knownRecord()
	relay := homeRelay(g.ep.Ticket())
	if relay == "" {
		return nil
	}
	if i := slices.IndexFunc(base.Devices, func(d account.Device) bool { return d.PeerId == g.selfPeerId }); i >= 0 {
		own := base.Devices[i]
		if own.Relay == relay && now.Sub(own.LastSeen) < accountRefreshAfter {
			a.mu.Lock()
			a.ownEntry = true
			a.mu.Unlock()
			return nil
		}
	}
	self := account.Device{PeerId: g.selfPeerId, Relay: relay, LastSeen: now}
	err = a.publishMerged(g.runCtx, base, self, now, packet)
	if !errors.Is(err, account.ErrStale) {
		return err
	}
	// a sibling published in between, or its clock runs ahead of ours:
	// merge on top of what the relays hold now and sign past it
	packet, rerr := a.resolve(g.runCtx)
	if rerr != nil || packet == nil {
		return err
	}
	a.noteSkew(packet.Timestamp(), now)
	if rec, oerr := account.Open(a.keys, packet); oerr == nil && rec.Version <= account.RecordVersion {
		g.applyAccountRecord(rec, now)
		a.setLast(rec, now)
	}
	return a.publishMerged(g.runCtx, a.knownRecord(), self, now, packet)
}

// resolve reads the record from the relays, bounded by opTimeout.
func (a *accountLayer) resolve(parent context.Context) (*account.SignedPacket, error) {
	ctx, cancel := context.WithTimeout(parent, opTimeout)
	defer cancel()
	return a.client.Resolve(ctx, a.keys.Public())
}

// publishMerged writes base with self merged in, signed past the packet
// it was resolved from, bounded by its own opTimeout. The published
// record becomes the last known one.
func (a *accountLayer) publishMerged(parent context.Context, base account.Record, self account.Device, now time.Time, after *account.SignedPacket) error {
	merged := account.Merge(base, self, now, account.MaxAge)
	var minTs account.Timestamp
	if after != nil {
		minTs = after.Timestamp() + 1
	}
	sealed, err := account.SealAt(a.keys, merged, minTs)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, opTimeout)
	defer cancel()
	if err = a.client.Publish(ctx, sealed); err != nil {
		return err
	}
	a.setLast(merged, now)
	a.mu.Lock()
	a.lastPublished = now
	a.ownEntry = true
	a.mu.Unlock()
	log.Debug("account record published", zap.Int("devices", len(merged.Devices)))
	return nil
}

// noteSkew reports the lead of a packet the relays hold over the local
// clock: every publish then has to sign past a sibling's clock. Logged
// once, shown in the status for the cycle that saw it.
func (a *accountLayer) noteSkew(ts account.Timestamp, now time.Time) {
	lead := time.Duration(int64(ts.Micros())-now.UnixMicro()) * time.Microsecond
	if lead < accountSkewWarn {
		return
	}
	a.mu.Lock()
	a.ahead = lead
	logIt := !a.skewLogged
	a.skewLogged = true
	a.mu.Unlock()
	if logIt {
		log.Warn("account record timestamp runs ahead of the local clock", zap.Duration("lead", lead))
	}
}

// setLast records the newest decoded or published record and persists
// it.
func (a *accountLayer) setLast(rec account.Record, at time.Time) {
	a.mu.Lock()
	a.last = &rec
	a.lastRead = at
	a.lastOK = at
	a.mu.Unlock()
	a.saveRecord(rec, at)
}

// knownRecord is the base a publish merges onto: the last decoded
// record, or one rebuilt from the siblings this device knows a relay
// for.
func (a *accountLayer) knownRecord() account.Record {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.last != nil {
		return account.Record{Version: account.RecordVersion, Devices: slices.Clone(a.last.Devices)}
	}
	rec := account.Record{Version: account.RecordVersion}
	for id, e := range a.peers {
		if e.relay == "" {
			continue
		}
		rec.Devices = append(rec.Devices, account.Device{PeerId: id, Relay: e.relay, LastSeen: e.seen})
	}
	return rec
}

func (a *accountLayer) saveRecord(rec account.Record, at time.Time) {
	if a.path == "" {
		return
	}
	out := persistedRecord{Version: accountRecordFileVersion, ReadAt: at}
	for _, d := range rec.Devices {
		out.Devices = append(out.Devices, persistedDevice{PeerId: d.PeerId, Relay: d.Relay, LastSeen: d.LastSeen})
	}
	data, err := json.Marshal(out)
	if err != nil {
		return
	}
	tmp := a.path + ".tmp"
	if err = os.WriteFile(tmp, data, 0o600); err != nil {
		log.Debug("account record file", zap.Error(err))
		return
	}
	if err = os.Rename(tmp, a.path); err != nil {
		log.Debug("account record file", zap.Error(err))
		_ = os.Remove(tmp)
	}
}

func (a *accountLayer) loadRecord() (account.Record, time.Time, bool) {
	if a.path == "" {
		return account.Record{}, time.Time{}, false
	}
	data, err := os.ReadFile(a.path)
	if err != nil {
		return account.Record{}, time.Time{}, false
	}
	var in persistedRecord
	if err = json.Unmarshal(data, &in); err != nil || in.Version != accountRecordFileVersion {
		log.Warn("account record file unreadable; starting without it", zap.String("file", filepath.Base(a.path)))
		return account.Record{}, time.Time{}, false
	}
	rec := account.Record{Version: account.RecordVersion}
	for _, d := range in.Devices {
		rec.Devices = append(rec.Devices, account.Device{PeerId: d.PeerId, Relay: d.Relay, LastSeen: d.LastSeen})
	}
	return rec, in.ReadAt, true
}

// applyAccountRecord merges the record's devices into the sibling set
// (own entry skipped, duplicates collapsed to the newest, timestamps
// clamped, relays validated). A sibling the record does not name stays
// while it is connected or was seen within accountKeepUnnamed — a
// device admitted by its handshake keeps its place until its own entry
// lands. Every peer that entered, left or changed is recomputed.
func (g *Global) applyAccountRecord(rec account.Record, now time.Time) {
	a := g.account()
	fresh := map[string]accountEntry{}
	for _, d := range rec.Devices {
		if d.PeerId == g.selfPeerId || now.Sub(d.LastSeen) > account.MaxAge {
			continue
		}
		if err := validRelay(d.Relay, a.insecure); err != nil {
			log.Debug("account record relay rejected", zap.String("peerId", d.PeerId), zap.Error(err))
			continue
		}
		if prev, ok := fresh[d.PeerId]; ok && !d.LastSeen.After(prev.seen) {
			continue
		}
		fresh[d.PeerId] = accountEntry{relay: d.Relay, seen: g.clamp(d.LastSeen)}
	}
	a.mu.Lock()
	touched := make([]string, 0, len(a.peers)+len(fresh))
	for id, e := range a.peers {
		touched = append(touched, id)
		if _, named := fresh[id]; named {
			continue
		}
		if g.connected(id) || now.Sub(e.seen) < accountKeepUnnamed {
			fresh[id] = e
		}
	}
	for id := range fresh {
		if _, known := a.peers[id]; !known {
			touched = append(touched, id)
		}
	}
	a.peers = fresh
	a.mu.Unlock()
	g.recompute(touched...)
}

// accountEntryFor returns a sibling's entry, if the record names it.
func (g *Global) accountEntryFor(peerId string) (accountEntry, bool) {
	a := g.account()
	if a == nil {
		return accountEntry{}, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	e, ok := a.peers[peerId]
	return e, ok
}

// admitSibling records a device that proved the account identity in
// the handshake before the record named it: a sibling seen now,
// dialable once the record carries its relay. The stores are updated
// on the worker; the handshake goroutine only takes the layer's lock.
func (g *Global) admitSibling(peerId string, now time.Time) {
	a := g.account()
	a.mu.Lock()
	if e, ok := a.peers[peerId]; ok {
		e.seen = now
		a.peers[peerId] = e
	} else {
		a.peers[peerId] = accountEntry{seen: now}
	}
	a.mu.Unlock()
	g.enqueue(task{kind: taskRecompute, peerId: peerId})
}

// accountTicket builds a sibling's relay-only ticket.
func (g *Global) accountTicket(peerId string, e accountEntry) string {
	if e.relay == "" {
		return ""
	}
	ticket, err := iroh.TicketForPeer(peerId, e.relay, g.account().insecure)
	if err != nil {
		return ""
	}
	return ticket
}

// allowHandshake is the iroh handshake-stage filter. A peer a space
// row names passes (the handshake counts as a sighting); a peer known
// only through the account record, or unknown altogether, passes only
// when the handshake proved the account's own identity. Any peer that
// proved it is remembered as a sibling. A failed identity check bars an
// unknown peer id before the handshake for a while.
func (g *Global) allowHandshake(peerId string, identity crypto.PubKey) bool {
	a := g.account()
	now := g.now()
	known := g.store.HasGlobalPeer(peerId) && g.status.Tier(peerId) != TierDisabled
	own := a != nil && identity != nil && identity.Account() == g.selfIdentity
	if known && !g.store.accountOnly(peerId) {
		g.markPending(peerId)
		if own {
			g.admitSibling(peerId, now)
		} else {
			g.status.Seen(peerId, now)
		}
		return true
	}
	if a == nil {
		if known {
			g.markPending(peerId)
		}
		return known
	}
	if !known {
		a.unknown.settle(now, peerId, own)
	}
	if !own {
		return false
	}
	g.markPending(peerId)
	g.admitSibling(peerId, now)
	return true
}

// status is the debug snapshot of the layer.
func (a *accountLayer) status() (devices int, resolved, published time.Time, lastErr string, own bool, ahead time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.peers), a.lastOK, a.lastPublished, a.lastErr, a.ownEntry, a.ahead
}

// validRelay accepts https relay URLs with a host, http only when
// insecure: a sibling's entry is remote input and names where this
// device will dial.
func validRelay(raw string, insecure bool) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if u.Host == "" {
		return errors.New("relay url without host")
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		if insecure {
			return nil
		}
		return errors.New("http relay url")
	}
	return errors.New("relay url scheme must be https")
}

// unknownBudget rations the handshakes unknown inbound peers may buy: a
// few in flight at once (the handshake is the cost), a bounded number of
// distinct ids per minute (a retrying id is refused, so it cannot
// recharge), and a denial period for ids that failed the identity
// check. A flood of fresh ids cannot starve a genuine sibling for more
// than a window.
type unknownBudget struct {
	mu       sync.Mutex
	inflight map[string]time.Time
	seen     map[string]time.Time
	denied   map[string]time.Time
}

// allow admits one attempt by an unknown peer id.
func (b *unknownBudget) allow(now time.Time, peerId string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pruneLocked(now)
	if until, ok := b.denied[peerId]; ok && now.Before(until) {
		return false
	}
	if _, dup := b.seen[peerId]; dup {
		return false
	}
	if len(b.inflight) >= unknownInflight || len(b.seen) >= unknownPerMinute {
		return false
	}
	if b.inflight == nil {
		b.inflight = map[string]time.Time{}
		b.seen = map[string]time.Time{}
	}
	b.inflight[peerId] = now
	b.seen[peerId] = now
	return true
}

// settle frees the attempt's slot once the handshake reported a
// verdict; a failed identity check denies the id for a while.
func (b *unknownBudget) settle(now time.Time, peerId string, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.inflight, peerId)
	if !ok {
		if b.denied == nil {
			b.denied = map[string]time.Time{}
		}
		b.denied[peerId] = now.Add(unknownDenyFor)
	}
}

// pruneLocked drops stale bookkeeping: a window-old distinct id, an
// in-flight slot past its TTL, an expired denial; oversized tables are
// reset.
func (b *unknownBudget) pruneLocked(now time.Time) {
	for id, at := range b.seen {
		if now.Sub(at) >= time.Minute {
			delete(b.seen, id)
		}
	}
	for id, at := range b.inflight {
		if now.Sub(at) >= unknownInflightTTL {
			delete(b.inflight, id)
		}
	}
	for id, until := range b.denied {
		if !now.Before(until) {
			delete(b.denied, id)
		}
	}
	if len(b.seen) > unknownTracked {
		b.seen = map[string]time.Time{}
	}
	if len(b.denied) > unknownTracked {
		b.denied = map[string]time.Time{}
	}
}
