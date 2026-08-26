package p2p

import (
	"context"
	"errors"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/anyproto/any-sync/net/transport/iroh"
	"github.com/anyproto/any-sync/util/crypto"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/p2p/account"
)

const (
	// accountInterval paces the record cycle: resolve, merge the own
	// entry, publish. The entry's timestamp is the sibling devices'
	// liveness evidence, like a space row's heartbeat.
	accountInterval = time.Hour
	// accountRefreshAfter is the own-entry age past which a cycle
	// republishes even if nothing else changed.
	accountRefreshAfter = 30 * time.Minute
	// accountRetry is the pause after a failed cycle (relay down,
	// stale packet from a racing sibling).
	accountRetry = time.Minute
	// unknownPerMinute bounds inbound peers the store does not know:
	// they cost one any-sync handshake each before the identity check
	// admits or drops them.
	unknownPerMinute = 6
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

	mu      sync.Mutex
	peers   map[string]accountEntry
	wake    chan struct{}
	lastOK  time.Time
	unknown rateBudget
}

// accountClient is the slice of account.Client the layer uses.
type accountClient interface {
	Publish(ctx context.Context, p *account.SignedPacket) error
	Resolve(ctx context.Context, pub account.PublicKey) (*account.SignedPacket, error)
}

// SetAccount turns the account layer on. Set before Run. insecure
// admits http:// relays named by sibling entries, for test relays.
func (g *Global) SetAccount(keys *account.Keys, client accountClient, insecure bool) {
	g.acct = &accountLayer{
		keys:     keys,
		client:   client,
		insecure: insecure,
		peers:    map[string]accountEntry{},
		wake:     make(chan struct{}, 1),
	}
}

// AccountEnabled reports whether the account layer is on.
func (g *Global) AccountEnabled() bool { return g.acct != nil }

// RepublishAccount runs a record cycle soon (ticket change, own
// relay change).
func (g *Global) RepublishAccount() {
	if g.acct == nil {
		return
	}
	select {
	case g.acct.wake <- struct{}{}:
	default:
	}
}

// accountLoop drives the record cycle: at start, hourly, and on wake.
func (g *Global) accountLoop() {
	defer g.wg.Done()
	delay := time.Duration(0)
	for {
		select {
		case <-g.runCtx.Done():
			return
		case <-g.acct.wake:
		case <-time.After(delay):
		}
		if err := g.accountCycle(); err != nil {
			log.Info("account record cycle", zap.Error(err))
			delay = accountRetry
		} else {
			delay = accountInterval
		}
	}
}

// accountCycle resolves the record, folds its devices into the peer
// store, and publishes the own entry when it is missing, names another
// relay, or is older than accountRefreshAfter.
func (g *Global) accountCycle() error {
	a := g.acct
	ctx, cancel := context.WithTimeout(g.runCtx, opTimeout)
	defer cancel()
	packet, err := a.client.Resolve(ctx, a.keys.Public())
	if err != nil {
		return err
	}
	var cur account.Record
	if packet != nil {
		if cur, err = account.Open(a.keys, packet); err != nil {
			// a record we cannot read is one we overwrite
			log.Warn("account record unreadable", zap.Error(err))
			cur = account.Record{}
		}
	}
	now := g.now()
	g.applyAccountRecord(cur, now)
	a.mu.Lock()
	a.lastOK = now
	a.mu.Unlock()

	relay := homeRelay(g.ep.Ticket())
	if relay == "" {
		return nil
	}
	if i := slices.IndexFunc(cur.Devices, func(d account.Device) bool { return d.PeerId == g.selfPeerId }); i >= 0 {
		own := cur.Devices[i]
		if own.Relay == relay && now.Sub(own.LastSeen) < accountRefreshAfter {
			return nil
		}
	}
	merged := account.Merge(cur, account.Device{PeerId: g.selfPeerId, Relay: relay, LastSeen: now}, now, account.MaxAge)
	sealed, err := account.Seal(a.keys, merged)
	if err != nil {
		return err
	}
	if err = a.client.Publish(ctx, sealed); err != nil {
		return err
	}
	log.Debug("account record published", zap.Int("devices", len(merged.Devices)))
	return nil
}

// applyAccountRecord replaces the sibling set with the record's
// devices (own entry skipped, timestamps clamped, relays validated) and
// recomputes every peer that entered or left it.
func (g *Global) applyAccountRecord(rec account.Record, now time.Time) {
	fresh := map[string]accountEntry{}
	for _, d := range rec.Devices {
		if d.PeerId == g.selfPeerId || now.Sub(d.LastSeen) > account.MaxAge {
			continue
		}
		if err := validRelay(d.Relay, g.acct.insecure); err != nil {
			log.Debug("account record relay rejected", zap.String("peerId", d.PeerId), zap.Error(err))
			continue
		}
		fresh[d.PeerId] = accountEntry{relay: d.Relay, seen: g.clamp(d.LastSeen)}
	}
	a := g.acct
	a.mu.Lock()
	touched := make([]string, 0, len(a.peers)+len(fresh))
	for id := range a.peers {
		touched = append(touched, id)
	}
	for id := range fresh {
		touched = append(touched, id)
	}
	a.peers = fresh
	a.mu.Unlock()
	g.recompute(touched...)
}

// accountEntryFor returns a sibling's entry, if the record names it.
func (g *Global) accountEntryFor(peerId string) (accountEntry, bool) {
	if g.acct == nil {
		return accountEntry{}, false
	}
	g.acct.mu.Lock()
	defer g.acct.mu.Unlock()
	e, ok := g.acct.peers[peerId]
	return e, ok
}

// admitSibling records a device that proved the account identity in
// the handshake before the record named it: it is a sibling seen now,
// dialable once the record carries its relay.
func (g *Global) admitSibling(peerId string, now time.Time) {
	a := g.acct
	a.mu.Lock()
	if e, ok := a.peers[peerId]; ok {
		e.seen = now
		a.peers[peerId] = e
	} else {
		a.peers[peerId] = accountEntry{seen: now}
	}
	a.mu.Unlock()
	g.recompute(peerId)
}

// accountTicket builds a sibling's relay-only ticket.
func (g *Global) accountTicket(peerId string, e accountEntry) string {
	if e.relay == "" {
		return ""
	}
	ticket, err := iroh.TicketForPeer(peerId, e.relay, g.acct.insecure)
	if err != nil {
		return ""
	}
	return ticket
}

// allowHandshake is the iroh handshake-stage filter: peers the records
// name pass; an unknown peer passes only when the handshake proved the
// account's own identity, and is remembered as a sibling.
func (g *Global) allowHandshake(peerId string, identity crypto.PubKey) bool {
	if g.store.HasGlobalPeer(peerId) && g.status.Tier(peerId) != TierDisabled {
		return true
	}
	if g.acct == nil || identity == nil || identity.Account() != g.selfIdentity {
		return false
	}
	g.admitSibling(peerId, g.now())
	return true
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

// rateBudget is a per-minute token bucket.
type rateBudget struct {
	mu    sync.Mutex
	stamp []time.Time
}

func (b *rateBudget) allow(now time.Time, perMinute int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	cut := now.Add(-time.Minute)
	b.stamp = slices.DeleteFunc(b.stamp, func(t time.Time) bool { return t.Before(cut) })
	if len(b.stamp) >= perMinute {
		return false
	}
	b.stamp = append(b.stamp, now)
	return true
}
