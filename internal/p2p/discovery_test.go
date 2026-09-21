package p2p

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/config"
	sdkp2p "github.com/anyproto/any-sync-sdk/p2p"
)

// fakeDriver lets the test inject found/lost events and observe
// announce lifecycles.
type fakeDriver struct {
	mu        sync.Mutex
	announced []sdkp2p.Announcement
	found     func(sdkp2p.DiscoveredPeer)
	lost      func(string)
	ready     chan struct{}
	die       chan struct{} // close to make the current Browse return early
}

func newFakeDriver() *fakeDriver {
	return &fakeDriver{ready: make(chan struct{}, 8), die: make(chan struct{})}
}

func (f *fakeDriver) Announce(ctx context.Context, a sdkp2p.Announcement) error {
	f.mu.Lock()
	f.announced = append(f.announced, a)
	f.mu.Unlock()
	<-ctx.Done()
	return nil
}

func (f *fakeDriver) Browse(ctx context.Context, _ string, found func(sdkp2p.DiscoveredPeer), lost func(string)) error {
	f.mu.Lock()
	f.found, f.lost = found, lost
	die := f.die
	f.mu.Unlock()
	f.ready <- struct{}{}
	select {
	case <-ctx.Done():
	case <-die:
	}
	return nil
}

func (f *fakeDriver) announceCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.announced)
}

func (f *fakeDriver) emitFound(p sdkp2p.DiscoveredPeer) {
	f.mu.Lock()
	found := f.found
	f.mu.Unlock()
	found(p)
}

type recordingNotifier struct {
	mu    sync.Mutex
	peers []sdkp2p.DiscoveredPeer
	own   []sdkp2p.OwnAddresses
	lost  []string
}

func (r *recordingNotifier) PeerLost(peerId string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lost = append(r.lost, peerId)
}

func (r *recordingNotifier) PeerDiscovered(_ context.Context, p sdkp2p.DiscoveredPeer, own sdkp2p.OwnAddresses) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers = append(r.peers, p)
	r.own = append(r.own, own)
}

func (r *recordingNotifier) seen() []sdkp2p.DiscoveredPeer {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]sdkp2p.DiscoveredPeer(nil), r.peers...)
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func TestDiscoveryNotifiesAndFiltersSelf(t *testing.T) {
	drv := newFakeDriver()
	notifier := &recordingNotifier{}
	d := NewDiscovery(config.P2P{}, "self-peer", func() (int, bool) { return 4242, true }, notifier)
	d.driver = drv

	require.NoError(t, d.Run(context.Background()))
	defer func() { require.NoError(t, d.Close(context.Background())) }()
	<-drv.ready

	// Announce runs concurrently with Browse — wait for it, and never
	// assert while holding the driver mutex (a failed require would
	// exit with it locked).
	waitFor(t, func() bool { return drv.announceCount() == 1 })
	drv.mu.Lock()
	a := drv.announced[0]
	drv.mu.Unlock()
	require.Equal(t, "self-peer", a.PeerId)
	require.Equal(t, 4242, a.Port)
	require.Equal(t, DefaultServiceType, a.ServiceType)

	drv.emitFound(sdkp2p.DiscoveredPeer{PeerId: "self-peer", Addrs: []string{"10.0.0.1:1"}})
	drv.emitFound(sdkp2p.DiscoveredPeer{PeerId: "other", Addrs: []string{"10.0.0.2:2"}})

	waitFor(t, func() bool { return len(notifier.seen()) == 1 })
	require.Equal(t, "other", notifier.seen()[0].PeerId)
	notifier.mu.Lock()
	ownPort := notifier.own[0].Port
	notifier.mu.Unlock()
	require.Equal(t, 4242, ownPort)

	// Same peer again → handshake retried (idempotent downstream).
	drv.emitFound(sdkp2p.DiscoveredPeer{PeerId: "other", Addrs: []string{"10.0.0.2:2"}})
	waitFor(t, func() bool { return len(notifier.seen()) == 2 })
}

func TestDiscoveryDisabledOrNoListener(t *testing.T) {
	disabled := false
	d := NewDiscovery(config.P2P{Enabled: &disabled}, "p", func() (int, bool) { return 0, false }, &recordingNotifier{})
	d.driver = newFakeDriver()
	require.NoError(t, d.Run(context.Background()))
	require.NoError(t, d.Close(context.Background()))

	// Listener down: discovery stays off and reports NoInterfaces.
	d2 := NewDiscovery(config.P2P{}, "p", func() (int, bool) { return 0, false }, &recordingNotifier{})
	d2.driver = newFakeDriver()
	require.NoError(t, d2.Run(context.Background()))
	require.Equal(t, sdkp2p.PossibilityNoInterfaces, d2.Possibility())
	require.NoError(t, d2.Close(context.Background()))
}

func TestDiscoverySessionRestartsAfterDriverDeath(t *testing.T) {
	drv := newFakeDriver()
	d := NewDiscovery(config.P2P{ServiceName: "_t._tcp"}, "self", func() (int, bool) { return 1, true }, &recordingNotifier{})
	d.driver = drv
	d.retryDelay = 20 * time.Millisecond

	require.NoError(t, d.Run(context.Background()))
	defer func() { require.NoError(t, d.Close(context.Background())) }()
	<-drv.ready
	waitFor(t, func() bool { return drv.announceCount() == 1 })

	// Browse dying ends the session; the supervisor must bring up a
	// fresh announce+browse pair after the retry delay.
	drv.mu.Lock()
	close(drv.die)
	drv.die = make(chan struct{})
	drv.mu.Unlock()

	<-drv.ready
	waitFor(t, func() bool { return drv.announceCount() == 2 })
}

// awaitSession waits for the driver's next Browse. Bounded: a bare
// receive would turn a regression into a test-binary timeout rather
// than a named failure.
func awaitSession(t *testing.T, drv *fakeDriver) {
	t.Helper()
	select {
	case <-drv.ready:
	case <-time.After(3 * time.Second):
		t.Fatal("no discovery session started in time")
	}
}

// setProbe installs a possibility probe whose answer the test controls,
// and restores the process global afterwards.
func setProbe(t *testing.T, restricted *atomic.Bool) {
	t.Helper()
	sdkp2p.SetPossibilityProbe(func(context.Context, int) sdkp2p.Possibility {
		if restricted.Load() {
			return sdkp2p.PossibilityRestricted
		}
		return sdkp2p.PossibilityPossible
	})
	t.Cleanup(func() { sdkp2p.SetPossibilityProbe(nil) })
}

func TestDiscoverySwitchEndsAndResumesSession(t *testing.T) {
	drv := newFakeDriver()
	d := NewDiscovery(config.P2P{}, "self", func() (int, bool) { return 1, true }, &recordingNotifier{})
	d.driver = drv
	// Long on purpose: Disabled must be recorded as the session ends,
	// not after this backoff, and the switch turning on must cut it
	// short. Neither path may be paced by it.
	d.retryDelay = 10 * time.Second

	require.NoError(t, d.Run(context.Background()))
	defer func() { require.NoError(t, d.Close(context.Background())) }()
	awaitSession(t, drv)
	waitFor(t, func() bool { return drv.announceCount() == 1 })
	require.True(t, d.Enabled())

	// Restating the current value must not tear down a healthy session.
	d.SetEnabled(true)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 1, drv.announceCount())

	// Off: the LIVE session must end. The supervisor only re-probes
	// between sessions, and a session on a healthy LAN never ends on
	// its own, so without the nudge the device announces indefinitely.
	d.SetEnabled(false)
	waitFor(t, func() bool { return d.Possibility() == sdkp2p.PossibilityDisabled })
	require.False(t, d.Enabled())

	// And stays ended — no new session while the switch is off.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 1, drv.announceCount())

	// On again: discovery comes back with no SDK restart.
	d.SetEnabled(true)
	awaitSession(t, drv)
	waitFor(t, func() bool { return drv.announceCount() == 2 })
	require.Equal(t, sdkp2p.PossibilityPossible, d.Possibility())
}

func TestDiscoveryEnabledReadsOffWhileP2PDisabled(t *testing.T) {
	off := false
	d := NewDiscovery(config.P2P{Enabled: &off}, "self", func() (int, bool) { return 1, true }, &recordingNotifier{})
	require.False(t, d.Enabled())
	d.SetEnabled(true)
	require.False(t, d.Enabled(), "the switch cannot turn on what p2p.enabled keeps off")
}

func TestDiscoveryStartsOffFromConfigAndSwitchCutsShortTheBackoff(t *testing.T) {
	off := false
	drv := newFakeDriver()
	d := NewDiscovery(config.P2P{LocalDiscovery: &off}, "self", func() (int, bool) { return 1, true }, &recordingNotifier{})
	d.driver = drv
	// Only the switch can get the supervisor out of this: a host that
	// has just been granted the permission must not sit out the backoff.
	d.retryDelay = time.Hour

	require.NoError(t, d.Run(context.Background()))
	defer func() { require.NoError(t, d.Close(context.Background())) }()
	// Not a single multicast send before the host says so: this is what
	// keeps the macOS Local Network prompt from firing at boot.
	waitFor(t, func() bool { return d.Possibility() == sdkp2p.PossibilityDisabled })
	require.Equal(t, 0, drv.announceCount())

	d.SetEnabled(true)
	awaitSession(t, drv)
	waitFor(t, func() bool { return drv.announceCount() == 1 })
}

func TestDiscoveryProbeReadBetweenSessionsAndSwitchWinsOverIt(t *testing.T) {
	var restricted atomic.Bool
	restricted.Store(true)
	setProbe(t, &restricted)

	drv := newFakeDriver()
	d := NewDiscovery(config.P2P{}, "self", func() (int, bool) { return 1, true }, &recordingNotifier{})
	d.driver = drv
	d.retryDelay = 20 * time.Millisecond

	require.NoError(t, d.Run(context.Background()))
	defer func() { require.NoError(t, d.Close(context.Background())) }()
	waitFor(t, func() bool { return d.Possibility() == sdkp2p.PossibilityRestricted })
	require.Equal(t, 0, drv.announceCount())

	// The switch wins over the probe: off is Disabled, not Restricted,
	// and the probe is not consulted at all while off.
	d.SetEnabled(false)
	waitFor(t, func() bool { return d.Possibility() == sdkp2p.PossibilityDisabled })

	// Granted after the fact: the probe is re-read before every session,
	// so switching on with a positive probe starts one within a cycle.
	restricted.Store(false)
	d.SetEnabled(true)
	awaitSession(t, drv)
	waitFor(t, func() bool { return drv.announceCount() == 1 })
	require.Equal(t, sdkp2p.PossibilityPossible, d.Possibility())
}
