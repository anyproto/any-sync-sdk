package p2p

import (
	"context"
	"errors"
	"math/rand/v2"
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
	// of an active peer that fails to answer; a retry comes up to
	// retryJitter of its backoff early (see jittered).
	activeBackoffMin = 30 * time.Second
	activeBackoffMax = 30 * time.Minute
	// staleProbe / dormantProbe are the fixed cadences of the slower
	// tiers.
	staleProbe   = 2 * time.Hour
	dormantProbe = 6 * time.Hour
	// idleCheck bounds how long the loop sleeps without a wake-up.
	idleCheck  = time.Minute
	rateWindow = time.Minute
	// addrsNotFoundRetry is the pause after a dial that lost its pool
	// load to a concurrent plain Get.
	addrsNotFoundRetry = 5 * time.Second
	// minSleep keeps a clock hiccup from turning the loop hot.
	minSleep = 10 * time.Millisecond
	// refusedWindow is how long after a successful dial a close still
	// counts as the peer refusing us.
	refusedWindow = 2 * time.Second
	// retryJitter is the largest share of a failed dial's wait a retry
	// comes early by, at random, so peers that failed together do not
	// retry in lockstep.
	retryJitter = 0.25
	// demandPeerTTL is the pool TTL of a connection dialed on demand: it
	// is outside the cover, so it closes soon after the fetch that
	// wanted it goes idle.
	demandPeerTTL = 5 * time.Minute
	// demandRetry is how long a peer whose dial failed is left alone by
	// demands, whatever its backoff: a demand is a caller waiting now,
	// and the cover's backoff of a stale peer runs to hours.
	demandRetry = activeBackoffMin
)

// connector is the only dialer of global peers. It keeps at most
// MaxConnections peers connected, chosen to cover the loaded spaces
// (own devices first, then most recently seen), one dial at a time,
// rate-limited, with per-peer backoff by tier. Beyond the cover it dials
// only on demand (see demand). Every other consumer of global peers
// only picks live connections from the pool.
type connector struct {
	g *Global

	mu       sync.Mutex
	next     map[string]time.Time
	attempts []time.Time
	demands  []*demand
	wake     chan struct{}
	power    atomic.Int32
}

// demand is a caller waiting for one more live peer of a space: a file
// fetch whose holder is not among the connected peers. The connector
// owns it once queued; the caller only reads res.
type demand struct {
	ctx     context.Context
	spaceId string
	// prefer is the account identity whose devices are dialed first.
	prefer string
	// skip holds the peers the caller already asked, plus the ones this
	// demand failed to dial.
	skip map[string]struct{}
	// res receives the peer id, empty when no candidate is left.
	res chan string
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
	// backoff marks a peer whose last dial failed and whose retry is not
	// due: the cover looks past it, so one dead sibling does not block a
	// space's other peers for the length of its backoff.
	backoff bool
}

// step plans once and dials at most one peer. Returns how long to
// sleep without a wake-up. A demand goes ahead of the cover and of the
// power hint: it exists only while a caller waits.
func (c *connector) step(ctx context.Context) time.Duration {
	if d := c.nextDemand(); d != nil {
		return c.serveDemand(ctx, d)
	}
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
	now := g.now()
	c.mu.Lock()
	for id, info := range infos {
		info.backoff = c.next[id].After(now)
		infos[id] = info
	}
	c.mu.Unlock()
	targets := selectTargets(candidates, infos, g.cfg.MaxConnections)
	var (
		dial   string
		wakeAt time.Time
	)
	for _, id := range targets {
		if !infos[id].connected {
			dial = id
			break
		}
	}
	if dial == "" {
		// nothing dialable now: wake when the earliest backoff ends
		c.mu.Lock()
		for id, info := range infos {
			if !info.backoff || info.connected {
				continue
			}
			if at := c.next[id]; wakeAt.IsZero() || at.Before(wakeAt) {
				wakeAt = at
			}
		}
		c.mu.Unlock()
		if wakeAt.IsZero() {
			return idleCheck
		}
		return max(min(wakeAt.Sub(now), idleCheck), minSleep)
	}
	if wait := c.rateLimitWait(now); wait > 0 {
		return wait
	}
	c.dial(ctx, dial, globalPeerTTL, nil)
	return 0
}

// connectOne queues a demand and waits for its answer: the id of a live
// peer of the space outside skip, ok=false when no candidate is left or
// ctx ends first. A dial in flight when ctx ends still completes.
func (c *connector) connectOne(ctx context.Context, spaceId, prefer string, skip []string) (string, bool) {
	d := &demand{ctx: ctx, spaceId: spaceId, prefer: prefer, skip: make(map[string]struct{}, len(skip)), res: make(chan string, 1)}
	for _, id := range skip {
		d.skip[id] = struct{}{}
	}
	c.mu.Lock()
	c.demands = append(c.demands, d)
	c.mu.Unlock()
	c.wakeUp()
	select {
	case id := <-d.res:
		return id, id != ""
	case <-ctx.Done():
		return "", false
	}
}

// nextDemand returns the oldest demand somebody still waits for.
func (c *connector) nextDemand() *demand {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.demands = slices.DeleteFunc(c.demands, func(d *demand) bool { return d.ctx.Err() != nil })
	if len(c.demands) == 0 {
		return nil
	}
	return c.demands[0]
}

// answer takes a demand off the queue and hands its result over.
func (c *connector) answer(d *demand, peerId string) {
	c.mu.Lock()
	c.demands = slices.DeleteFunc(c.demands, func(q *demand) bool { return q == d })
	c.mu.Unlock()
	d.res <- peerId
}

// serveDemand advances one demand by at most one dial: it answers with
// a connected peer the caller has not asked, dials the next candidate,
// or answers empty when none is left. A failed dial keeps the demand
// queued for the candidate after it.
func (c *connector) serveDemand(ctx context.Context, d *demand) time.Duration {
	g := c.g
	now := g.now()
	var target string
	for _, id := range c.demandOrder(d) {
		if _, ok := d.skip[id]; ok {
			continue
		}
		if g.connected(id) {
			c.answer(d, id)
			return 0
		}
		if rec, ok := g.status.Get(id); ok && rec.Failures > 0 && now.Sub(rec.LastAttempt) < demandRetry {
			continue
		}
		if target == "" {
			target = id
		}
	}
	// a demand never takes the headroom left for inbound connections
	if target == "" || g.liveCount() >= 2*g.cfg.MaxConnections {
		c.answer(d, "")
		return 0
	}
	if wait := c.rateLimitWait(now); wait > 0 {
		return wait
	}
	answered := false
	live := c.dial(ctx, target, demandPeerTTL, func() {
		answered = true
		c.answer(d, target)
	})
	switch {
	case answered:
	case live:
		c.answer(d, target)
	default:
		d.skip[target] = struct{}{}
	}
	return 0
}

// demandOrder ranks the dialable peers of a demand's space: devices of
// the preferred identity, then own devices, then the rest, each group
// most recently seen first.
func (c *connector) demandOrder(d *demand) []string {
	g := c.g
	ids := c.dialable(g.store.GlobalPeerIds(d.spaceId))
	rank := func(id string) int {
		switch identity := g.peerIdentity(id); {
		case d.prefer != "" && identity == d.prefer:
			return 0
		case g.selfIdentity != "" && identity == g.selfIdentity:
			return 1
		}
		return 2
	}
	ranks := make(map[string]int, len(ids))
	for _, id := range ids {
		ranks[id] = rank(id)
	}
	slices.SortStableFunc(ids, func(a, b string) int { return ranks[a] - ranks[b] })
	return ids
}

// accountCover is the bucket the account's own devices are covered
// under while no space is loaded. It cannot collide with a space id.
const accountCover = "\x00account"

// candidates lists, per loaded space, the global peers worth dialing:
// not disabled, not reachable over the LAN, with a ticket. A device
// that holds no space yet — a restore — covers its own devices
// instead, otherwise it would wait for a space it can only get from
// them.
func (c *connector) candidates() map[string][]string {
	g := c.g
	out := map[string][]string{}
	for _, spaceId := range g.LoadedSpaceIds() {
		if ids := c.dialable(g.store.GlobalPeerIds(spaceId)); len(ids) > 0 {
			out[spaceId] = ids
		}
	}
	if len(out) == 0 {
		if ids := c.dialable(g.store.AccountPeerIds()); len(ids) > 0 {
			out[accountCover] = ids
		}
	}
	return out
}

// dialable keeps the peers the connector may dial: reachable only
// through a ticket, not over the LAN.
func (c *connector) dialable(ids []string) []string {
	g := c.g
	out := ids[:0:0]
	for _, id := range ids {
		if g.book.HasLAN(id) || g.book.Ticket(id) == "" {
			continue
		}
		out = append(out, id)
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
			if infos[id].backoff && !infos[id].connected {
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
// ttl is the connection's pool TTL. onLive, when set, runs as soon as a
// dialed connection is usable, before the wait that tells a refusal
// from a connection. Reports whether the peer is connected.
func (c *connector) dial(ctx context.Context, peerId string, ttl time.Duration, onLive func()) bool {
	g := c.g
	now := g.now()
	// the plan is stale by now if the peer connected to us in between:
	// dialing back would replace the accepted connection and kill both
	if g.connected(peerId) {
		return true
	}
	if p := g.pickIroh(peerId); p != nil {
		g.addLive(p)
		return true
	}
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
		return false
	}
	if err == nil {
		p.SetTTL(ttl)
		if err = g.pool.AddPeer(ctx, p); err != nil {
			_ = p.Close()
		} else {
			g.addLive(p)
			if onLive != nil {
				onLive()
			}
			// a peer that refuses us after the handshake closes at once:
			// that is a failed dial, not a connection
			select {
			case <-p.CloseChan():
				err = errRefusedAfterHandshake
			case <-time.After(refusedWindow):
			case <-ctx.Done():
			}
		}
	}
	ok := err == nil
	g.status.Attempt(peerId, ok)
	rec, _ := g.status.Get(peerId)
	c.mu.Lock()
	if ok {
		delete(c.next, peerId)
	} else {
		c.next[peerId] = now.Add(jittered(backoffFor(g.status.Tier(peerId), rec.Failures)))
	}
	c.mu.Unlock()
	if ok {
		log.Info("global peer connected", zap.String("peerId", peerId))
	} else {
		log.Debug("global peer dial failed", zap.String("peerId", peerId), zap.Int("failures", rec.Failures), zap.Error(err))
	}
	return ok
}

// errRefusedAfterHandshake is a dial the peer closed within
// refusedWindow of success: its handshake filter turned us down.
var errRefusedAfterHandshake = errors.New("peer closed the connection after the handshake")

// jittered takes a random share of up to retryJitter off d.
func jittered(d time.Duration) time.Duration {
	return d - time.Duration(rand.Int64N(int64(float64(d)*retryJitter)+1))
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
