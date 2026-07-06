package p2p

import (
	"context"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/anyproto/any-sync/app"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/config"
	sdkp2p "github.com/anyproto/any-sync-sdk/p2p"
)

const (
	discoveryCName = "sdk.p2p.discovery"

	// DefaultServiceType is the DNS-SD service type SDK peers announce.
	// Deliberately NOT anytype-heart's "_anytype._tcp" — the protocols
	// are separate networks.
	DefaultServiceType = "_any._tcp"

	// resweepInterval paces the session watcher: interface fingerprint
	// check (restart announce/browse on change) and re-handshake of all
	// known peers (refreshes their space lists; re-dials dropped ones).
	resweepInterval = 60 * time.Second

	// sessionRetryDelay backs off between discovery sessions so a
	// driver that errors instantly can't hot-loop.
	sessionRetryDelay = 5 * time.Second

	// notifyTimeout bounds one PeerDiscovered handshake so a
	// half-reachable peer can't wedge the consumer.
	notifyTimeout = 15 * time.Second

	// closeTimeout bounds Close's wait for driver goroutines. A driver
	// that ignores cancellation past this only leaks dead sockets —
	// nothing is reused after Close.
	closeTimeout = 2 * time.Second
)

// Notifier consumes discovery results; implemented by Exchange.
type Notifier interface {
	PeerDiscovered(ctx context.Context, peer sdkp2p.DiscoveredPeer, own sdkp2p.OwnAddresses)
}

type discoveryEvent struct {
	peer       sdkp2p.DiscoveredPeer
	lostPeerId string
	resweep    bool
}

// Discovery announces this device on the LAN and browses for other SDK
// peers, feeding each sighting to the Notifier (the SpaceExchangeV2
// handshake).
//
// Concurrency model, deliberately simpler than anytype-heart's:
//   - one supervisor goroutine runs announce+browse "sessions",
//     restarting them (scoped child context) when the interface set
//     changes or the driver dies — never tearing down the component;
//   - driver callbacks only enqueue onto a bounded channel;
//   - one consumer goroutine owns the known-peer map and performs all
//     notifier calls serially.
//
// Nothing is ever reassigned after Run; Close cancels one context and
// waits (bounded) on one WaitGroup.
type Discovery struct {
	cfg         config.P2P
	serviceType string
	peerId      string
	portFn      func() (port int, started bool)
	notifier    Notifier
	driver      sdkp2p.Driver

	events chan discoveryEvent
	cancel context.CancelFunc
	wg     sync.WaitGroup
	port   int

	// timing knobs — the constants above, overridable in tests.
	resweepEvery time.Duration
	retryDelay   time.Duration

	mu          sync.Mutex
	possibility sdkp2p.Possibility
	hooks       []func(sdkp2p.Possibility)
}

func NewDiscovery(cfg config.P2P, peerId string, portFn func() (int, bool), notifier Notifier) *Discovery {
	serviceType := cfg.ServiceName
	if serviceType == "" {
		serviceType = DefaultServiceType
	}
	return &Discovery{
		cfg:          cfg,
		serviceType:  serviceType,
		peerId:       peerId,
		portFn:       portFn,
		notifier:     notifier,
		events:       make(chan discoveryEvent, 64),
		resweepEvery: resweepInterval,
		retryDelay:   sessionRetryDelay,
	}
}

func (d *Discovery) Init(_ *app.App) error {
	if d.driver == nil {
		if f := sdkp2p.DriverFactory(); f != nil {
			d.driver = f()
		} else {
			d.driver = dnssdDriver{}
		}
	}
	return nil
}

func (d *Discovery) Name() string { return discoveryCName }

func (d *Discovery) Run(_ context.Context) error {
	if !d.cfg.IsEnabled() {
		return nil
	}
	port, started := d.portFn()
	if !started {
		// No inbound listener (e.g. a forced p2p.Port was already taken
		// at boot) — browsing is pointless because no peer could dial
		// back. p2p reports NotPossible for the process lifetime; the
		// listener is bound once at startup and not retried, so this is
		// terminal until restart. Logged loudly so the cause is clear
		// (the possibility enum has no dedicated "listener failed").
		log.Warn("p2p discovery not starting: inbound listener is down (check p2p.Port for a conflict)")
		d.setPossibility(sdkp2p.PossibilityNoInterfaces)
		return nil
	}
	d.port = port
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	d.wg.Add(2)
	go func() { defer d.wg.Done(); d.consumeLoop(ctx) }()
	go func() { defer d.wg.Done(); d.superviseLoop(ctx) }()
	return nil
}

func (d *Discovery) Close(_ context.Context) error {
	if d.cancel == nil {
		return nil
	}
	d.cancel()
	done := make(chan struct{})
	go func() { d.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(closeTimeout):
		log.Warn("discovery close timed out waiting for driver goroutines")
	}
	return nil
}

// Possibility is the current discovery-possibility state.
func (d *Discovery) Possibility() sdkp2p.Possibility {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.possibility
}

// RegisterPossibilityHook adds a callback fired (outside locks) on
// every possibility change. Used by sync status.
func (d *Discovery) RegisterPossibilityHook(fn func(sdkp2p.Possibility)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.hooks = append(d.hooks, fn)
}

// Port is the announced QUIC listen port (zero before Run).
func (d *Discovery) Port() int { return d.port }

func (d *Discovery) setPossibility(p sdkp2p.Possibility) {
	d.mu.Lock()
	if d.possibility == p {
		d.mu.Unlock()
		return
	}
	d.possibility = p
	hooks := d.hooks
	d.mu.Unlock()
	log.Info("discovery possibility changed", zap.Uint8("state", uint8(p)))
	for _, fn := range hooks {
		fn(p)
	}
}

// superviseLoop re-runs discovery sessions until the component closes.
// Each iteration re-probes possibility, so plugging in a cable or
// granting the iOS permission is picked up within a retry cycle.
func (d *Discovery) superviseLoop(ctx context.Context) {
	for ctx.Err() == nil {
		d.setPossibility(d.probe(ctx))
		if d.Possibility() == sdkp2p.PossibilityPossible {
			d.runSession(ctx)
		}
		if !sleepCtx(ctx, d.retryDelay) {
			return
		}
	}
}

// runSession runs one announce+browse pair under a child context and
// blocks until it ends: driver death, interface-set change (watcher
// cancels for a clean restart), or component close.
func (d *Discovery) runSession(ctx context.Context) {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fingerprint := interfaceFingerprint()
	var swg sync.WaitGroup
	swg.Add(2)
	go func() {
		defer swg.Done()
		defer cancel() // announce died → end the session
		if err := d.driver.Announce(sctx, sdkp2p.Announcement{
			PeerId:      d.peerId,
			Port:        d.port,
			ServiceType: d.serviceType,
		}); err != nil && sctx.Err() == nil {
			log.Warn("discovery announce", zap.Error(err))
		}
	}()
	go func() {
		defer swg.Done()
		defer cancel() // browse died → end the session
		if err := d.driver.Browse(sctx, d.serviceType, d.onFound, d.onLost); err != nil && sctx.Err() == nil {
			log.Warn("discovery browse", zap.Error(err))
		}
	}()

	ticker := time.NewTicker(d.resweepEvery)
	defer ticker.Stop()
	for {
		select {
		case <-sctx.Done():
			swg.Wait()
			return
		case <-ticker.C:
			if fp := interfaceFingerprint(); fp != fingerprint {
				log.Info("network interfaces changed, restarting discovery session")
				cancel()
				swg.Wait()
				return
			}
			d.emit(discoveryEvent{resweep: true})
		}
	}
}

// consumeLoop is the single goroutine that owns the known-peer map and
// calls the notifier. Serial by design: one handshake at a time keeps
// dial storms and lock interplay out of the picture.
func (d *Discovery) consumeLoop(ctx context.Context) {
	known := map[string]sdkp2p.DiscoveredPeer{}
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-d.events:
			switch {
			case ev.resweep:
				for _, p := range known {
					d.notify(ctx, p)
				}
			case ev.lostPeerId != "":
				delete(known, ev.lostPeerId)
			default:
				if ev.peer.PeerId == "" || ev.peer.PeerId == d.peerId {
					continue
				}
				known[ev.peer.PeerId] = ev.peer
				d.notify(ctx, ev.peer)
			}
		}
	}
}

func (d *Discovery) notify(ctx context.Context, p sdkp2p.DiscoveredPeer) {
	nctx, cancel := context.WithTimeout(ctx, notifyTimeout)
	defer cancel()
	d.notifier.PeerDiscovered(nctx, p, sdkp2p.OwnAddresses{Addrs: ownIPv4s(), Port: d.port})
}

// onFound / onLost run on driver goroutines — enqueue only, never
// block. A full channel drops the event; the periodic resweep and
// mDNS re-announces recover anything missed.
func (d *Discovery) onFound(p sdkp2p.DiscoveredPeer) { d.emit(discoveryEvent{peer: p}) }
func (d *Discovery) onLost(peerId string)            { d.emit(discoveryEvent{lostPeerId: peerId}) }

func (d *Discovery) emit(ev discoveryEvent) {
	select {
	case d.events <- ev:
	default:
		log.Debug("discovery event dropped, queue full")
	}
}

// probe determines whether discovery can work right now: the injected
// platform probe if any (iOS), otherwise "is there a usable multicast
// interface with an IPv4 address".
func (d *Discovery) probe(ctx context.Context) sdkp2p.Possibility {
	if f := sdkp2p.PossibilityProbe(); f != nil {
		return f(ctx, d.port)
	}
	if len(ownIPv4s()) == 0 {
		return sdkp2p.PossibilityNoInterfaces
	}
	return sdkp2p.PossibilityPossible
}

// CurrentOwnAddresses is this device's announce right now: LAN IPv4s
// plus the given listen port. Used by the exchange's proactive
// re-handshake.
func CurrentOwnAddresses(port int) sdkp2p.OwnAddresses {
	return sdkp2p.OwnAddresses{Addrs: ownIPv4s(), Port: port}
}

// ownIPv4s enumerates this device's LAN IPv4 addresses on usable
// (up, multicast, non-loopback) interfaces.
func ownIPv4s() []string {
	ifaces, err := sdkp2p.InterfaceProvider()()
	if err != nil {
		return nil
	}
	var out []string
	for _, iface := range ifaces {
		if !usableInterface(iface) {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			if ip4 := ipNet.IP.To4(); ip4 != nil {
				out = append(out, ip4.String())
			}
		}
	}
	return out
}

func usableInterface(iface net.Interface) bool {
	return iface.Flags&net.FlagUp != 0 &&
		iface.Flags&net.FlagMulticast != 0 &&
		iface.Flags&net.FlagLoopback == 0
}

// interfaceFingerprint summarizes the usable-interface set; sessions
// restart when it changes. Names+flags+addrs, order-independent.
func interfaceFingerprint() string {
	ifaces, err := sdkp2p.InterfaceProvider()()
	if err != nil {
		return "err:" + err.Error()
	}
	parts := make([]string, 0, len(ifaces))
	for _, iface := range ifaces {
		if !usableInterface(iface) {
			continue
		}
		p := iface.Name
		if addrs, aErr := iface.Addrs(); aErr == nil {
			for _, a := range addrs {
				p += "|" + a.String()
			}
		}
		parts = append(parts, p)
	}
	sort.Strings(parts)
	return strings.Join(parts, ";")
}

// sleepCtx sleeps d or until ctx is done; false when ctx ended.
func sleepCtx(ctx context.Context, dur time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(dur):
		return true
	}
}
