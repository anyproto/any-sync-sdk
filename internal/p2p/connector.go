package p2p

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anyproto/any-sync/net/peerservice"
	"go.uber.org/zap"

	sdkp2p "github.com/anyproto/any-sync-sdk/p2p"
)

const (
	// activeBackoffMin / activeBackoffMax bound the exponential backoff
	// of an active peer that fails to answer.
	activeBackoffMin = 30 * time.Second
	activeBackoffMax = 10 * time.Minute
	// staleProbe / dormantProbe are the fixed cadences of the slower
	// tiers.
	staleProbe   = 30 * time.Minute
	dormantProbe = 6 * time.Hour
	// idleCheck bounds how long the loop sleeps without a wake-up.
	idleCheck  = time.Minute
	rateWindow = time.Minute
	// addrsNotFoundRetry is the pause after a dial that lost its pool
	// load to a concurrent plain Get.
	addrsNotFoundRetry = 5 * time.Second
	// minSleep keeps a clock hiccup from turning the loop hot.
	minSleep = 10 * time.Millisecond
)

// connector is the only dialer of global peers. It keeps at most
// MaxConnections peers connected, chosen to cover the loaded spaces
// (own devices first, then most recently seen), one dial at a time,
// rate-limited, with per-peer backoff by tier. Every other consumer of
// global peers only picks live connections from the pool.
type connector struct {
	g *Global

	mu       sync.Mutex
	next     map[string]time.Time
	attempts []time.Time
	wake     chan struct{}
	power    atomic.Int32
}

func newConnector(g *Global) *connector {
	return &connector{g: g, next: map[string]time.Time{}, wake: make(chan struct{}, 1)}
}

// wakeUp asks the loop to re-plan now.
func (c *connector) wakeUp() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// reactivate clears a peer's backoff after fresh liveness evidence.
func (c *connector) reactivate(peerId string) {
	c.mu.Lock()
	delete(c.next, peerId)
	c.mu.Unlock()
	c.wakeUp()
}

func (c *connector) loop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	c.power.Store(int32(sdkp2p.CurrentPowerHint()))
	stop := sdkp2p.SubscribePowerHint(func(h sdkp2p.PowerHint) {
		c.power.Store(int32(h))
		c.wakeUp()
	})
	defer stop()
	for {
		delay := c.step(ctx)
		select {
		case <-ctx.Done():
			return
		case <-c.wake:
		case <-time.After(delay):
		}
	}
}

// peerInfo is what target selection knows about a candidate.
type peerInfo struct {
	lastSeen  time.Time
	own       bool
	connected bool
}

// step plans once and dials at most one peer. Returns how long to
// sleep without a wake-up.
func (c *connector) step(ctx context.Context) time.Duration {
	if sdkp2p.PowerHint(c.power.Load()) == sdkp2p.PowerLow {
		return idleCheck
	}
	g := c.g
	candidates := c.candidates()
	if len(candidates) == 0 {
		return idleCheck
	}
	infos := map[string]peerInfo{}
	for _, ids := range candidates {
		for _, id := range ids {
			if _, ok := infos[id]; ok {
				continue
			}
			infos[id] = peerInfo{
				lastSeen:  g.status.LastSeen(id),
				own:       g.selfIdentity != "" && g.peerIdentity(id) == g.selfIdentity,
				connected: g.connected(id),
			}
		}
	}
	targets := selectTargets(candidates, infos, g.cfg.MaxConnections)
	now := g.now()
	var (
		dial   string
		wakeAt time.Time
	)
	c.mu.Lock()
	for _, id := range targets {
		if infos[id].connected {
			continue
		}
		at := c.next[id]
		if !at.After(now) {
			dial = id
			break
		}
		if wakeAt.IsZero() || at.Before(wakeAt) {
			wakeAt = at
		}
	}
	c.mu.Unlock()
	if dial == "" {
		if wakeAt.IsZero() {
			return idleCheck
		}
		return max(min(wakeAt.Sub(now), idleCheck), minSleep)
	}
	if wait := c.rateLimitWait(now); wait > 0 {
		return wait
	}
	c.dial(ctx, dial)
	return 0
}

// candidates lists, per loaded space, the global peers worth dialing:
// not disabled, not reachable over the LAN, with a ticket.
func (c *connector) candidates() map[string][]string {
	g := c.g
	out := map[string][]string{}
	for _, spaceId := range g.LoadedSpaceIds() {
		var ids []string
		for _, id := range g.store.GlobalPeerIds(spaceId) {
			if g.book.HasLAN(id) || g.book.Ticket(id) == "" {
				continue
			}
			ids = append(ids, id)
		}
		if len(ids) > 0 {
			out[spaceId] = ids
		}
	}
	return out
}

// selectTargets is the greedy space cover. Connected candidates are
// taken first and the spaces they cover need no dial; then the peer
// covering the most still-uncovered spaces is taken repeatedly — ties to
// own devices, then the most recently seen, then by id — until every
// space is covered or max peers are chosen.
func selectTargets(candidates map[string][]string, infos map[string]peerInfo, max int) []string {
	uncovered := map[string]struct{}{}
	spacesOf := map[string][]string{}
	for spaceId, ids := range candidates {
		uncovered[spaceId] = struct{}{}
		for _, id := range ids {
			spacesOf[id] = append(spacesOf[id], spaceId)
		}
	}
	var targets []string
	chosen := map[string]struct{}{}
	connectedIds := make([]string, 0, len(spacesOf))
	for id := range spacesOf {
		if infos[id].connected {
			connectedIds = append(connectedIds, id)
		}
	}
	slices.Sort(connectedIds)
	for _, id := range connectedIds {
		targets = append(targets, id)
		chosen[id] = struct{}{}
		for _, s := range spacesOf[id] {
			delete(uncovered, s)
		}
	}
	for len(targets) < max && len(uncovered) > 0 {
		best, bestCov := "", 0
		for id, spaces := range spacesOf {
			if _, ok := chosen[id]; ok {
				continue
			}
			cov := 0
			for _, s := range spaces {
				if _, ok := uncovered[s]; ok {
					cov++
				}
			}
			if cov == 0 {
				continue
			}
			if best == "" || cov > bestCov || (cov == bestCov && betterPeer(infos[id], id, infos[best], best)) {
				best, bestCov = id, cov
			}
		}
		if best == "" {
			break
		}
		targets = append(targets, best)
		chosen[best] = struct{}{}
		for _, s := range spacesOf[best] {
			delete(uncovered, s)
		}
	}
	return targets
}

// betterPeer orders two equally covering peers.
func betterPeer(a peerInfo, aId string, b peerInfo, bId string) bool {
	if a.own != b.own {
		return a.own
	}
	if !a.lastSeen.Equal(b.lastSeen) {
		return a.lastSeen.After(b.lastSeen)
	}
	return aId < bId
}

// rateLimitWait returns how long to wait before another dial fits the
// per-minute budget; zero when a dial may go now.
func (c *connector) rateLimitWait(now time.Time) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	cut := now.Add(-rateWindow)
	c.attempts = slices.DeleteFunc(c.attempts, func(t time.Time) bool { return t.Before(cut) })
	if limit := c.g.cfg.MaxDialsPerMinute; limit <= 0 || len(c.attempts) < limit {
		return 0
	}
	return c.attempts[0].Add(rateWindow).Sub(now)
}

// forget drops a peer's backoff entry once it left every space.
func (c *connector) forget(peerId string) {
	c.mu.Lock()
	delete(c.next, peerId)
	c.mu.Unlock()
}

// dial is the single global dial path: the peer service is called
// directly (the only ctx carrying CtxWithGlobalDial, bounded by
// DialTimeout) and the connection is handed to the pool as an
// incoming-style peer. Going around pool.Get means no in-flight pool
// load ever exists for a global peer, so every Pick answers at once.
func (c *connector) dial(ctx context.Context, peerId string) {
	g := c.g
	now := g.now()
	c.mu.Lock()
	c.attempts = append(c.attempts, now)
	c.mu.Unlock()

	dctx, cancel := context.WithTimeout(peerservice.CtxWithGlobalDial(ctx), g.cfg.DialTimeout)
	p, err := g.dialer.Dial(dctx, peerId)
	cancel()
	if errors.Is(err, peerservice.ErrAddrsNotFound) {
		// The addr book handed the peer to the LAN or dropped its ticket
		// between planning and dialing. Not evidence about the peer —
		// retry shortly.
		c.mu.Lock()
		c.next[peerId] = now.Add(addrsNotFoundRetry)
		c.mu.Unlock()
		return
	}
	if err == nil {
		p.SetTTL(globalPeerTTL)
		if err = g.pool.AddPeer(ctx, p); err != nil {
			_ = p.Close()
		} else {
			g.addLive(p)
		}
	}
	ok := err == nil
	g.status.Attempt(peerId, ok)
	rec, _ := g.status.Get(peerId)
	c.mu.Lock()
	if ok {
		delete(c.next, peerId)
	} else {
		c.next[peerId] = now.Add(backoffFor(g.status.Tier(peerId), rec.Failures))
	}
	c.mu.Unlock()
	if ok {
		log.Info("global peer connected", zap.String("peerId", peerId))
	} else {
		log.Debug("global peer dial failed", zap.String("peerId", peerId), zap.Int("failures", rec.Failures), zap.Error(err))
	}
}

// backoffFor is the wait after a failed dial, by tier: exponential for
// active peers, a fixed slow cadence otherwise.
func backoffFor(tier Tier, failures int) time.Duration {
	switch tier {
	case TierStale:
		return staleProbe
	case TierDormant, TierDisabled:
		return dormantProbe
	}
	if failures < 1 {
		failures = 1
	}
	d := activeBackoffMin
	for i := 1; i < failures && d < activeBackoffMax; i++ {
		d *= 2
	}
	return min(d, activeBackoffMax)
}
