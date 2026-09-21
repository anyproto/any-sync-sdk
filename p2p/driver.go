package p2p

import (
	"context"
	"net"
	"sync"
)

// Announcement is what a Driver publishes on the local network.
type Announcement struct {
	// PeerId is this device's any-sync peer id — the mDNS instance name.
	PeerId string
	// Port is the QUIC listen port to advertise.
	Port int
	// ServiceType is the DNS-SD service type (e.g. "_any._tcp").
	ServiceType string
}

// Driver is the pluggable discovery backend. The SDK ships an mDNS
// driver; platform bridges may inject a native one (e.g. Android NSD,
// where in-process multicast is unreliable) via SetDriverFactory.
//
// Both methods block until ctx is done and are called from dedicated
// goroutines. The SDK may cancel and re-call them on network changes,
// so implementations must be restartable.
type Driver interface {
	// Announce publishes the given announcement until ctx is done.
	Announce(ctx context.Context, a Announcement) error
	// Browse watches for peers of the given service type until ctx is
	// done. found fires when a peer appears or its records change;
	// lost fires when a peer's announcement expires or is withdrawn.
	Browse(ctx context.Context, serviceType string, found func(DiscoveredPeer), lost func(peerId string)) error
}

// Possibility is whether local discovery can work on this device right
// now. Mirrors the states surfaced in sync status.
type Possibility uint8

const (
	PossibilityUnknown Possibility = iota
	// PossibilityPossible — discovery can run.
	PossibilityPossible
	// PossibilityNoInterfaces — no multicast-capable network interface.
	PossibilityNoInterfaces
	// PossibilityRestricted — the OS denies local-network access
	// (e.g. iOS Local Network permission).
	PossibilityRestricted
)

// String returns a stable lowercase token for logging / wire mapping.
func (p Possibility) String() string {
	switch p {
	case PossibilityPossible:
		return "possible"
	case PossibilityNoInterfaces:
		return "nointerfaces"
	case PossibilityRestricted:
		return "restricted"
	default:
		return "unknown"
	}
}

// The injection points below exist for platform bridges (gomobile)
// that must plug native behavior in before sdk.Open. They are process
// globals — same trade-off as anytype-heart's notifier provider: the
// SDK config is pure data and Open's signature stays platform-free.
// Regular embedders never touch these.
var (
	injectMu          sync.Mutex
	driverFactory     func() Driver
	interfaceProvider func() ([]net.Interface, error) = net.Interfaces
	possibilityProbe  func(ctx context.Context, port int) Possibility
)

// SetDriverFactory installs a custom discovery-driver factory, called
// once per SDK instance. Pass nil to restore the built-in mDNS driver.
func SetDriverFactory(f func() Driver) {
	injectMu.Lock()
	defer injectMu.Unlock()
	driverFactory = f
}

// DriverFactory returns the injected factory, or nil when the SDK
// should use its built-in driver. SDK-internal.
func DriverFactory() func() Driver {
	injectMu.Lock()
	defer injectMu.Unlock()
	return driverFactory
}

// SetInterfaceProvider overrides how the SDK enumerates network
// interfaces — Android bridges inject the host's view here, where
// net.Interfaces is unreliable. Pass nil to restore net.Interfaces.
func SetInterfaceProvider(f func() ([]net.Interface, error)) {
	injectMu.Lock()
	defer injectMu.Unlock()
	if f == nil {
		f = net.Interfaces
	}
	interfaceProvider = f
}

// InterfaceProvider returns the current interface enumerator. SDK-internal.
func InterfaceProvider() func() ([]net.Interface, error) {
	injectMu.Lock()
	defer injectMu.Unlock()
	return interfaceProvider
}

// SetPossibilityProbe overrides the discovery-possibility check — iOS
// bridges inject a self-connection probe here to detect the Local
// Network permission being denied. Pass nil to restore the default
// (interface-based) check.
//
// A probe may return PossibilityUnknown to mean "nothing to add right
// now"; the SDK then falls back to its own interface check. That is how
// a host overrides only the half it knows — whether the OS permits
// local-network access — without reimplementing the rest.
//
// The probe is re-read on every cycle and mid-session, so it may change
// its answer over the life of the process; see SDK.RefreshP2PPossibility
// for applying a change at once.
func SetPossibilityProbe(f func(ctx context.Context, port int) Possibility) {
	injectMu.Lock()
	defer injectMu.Unlock()
	possibilityProbe = f
}

// PossibilityProbe returns the injected probe, or nil when the SDK
// should use its interface-based default. SDK-internal.
func PossibilityProbe() func(ctx context.Context, port int) Possibility {
	injectMu.Lock()
	defer injectMu.Unlock()
	return possibilityProbe
}

// PowerHint tells the global p2p connector how eager to be. Platform
// bridges report the app moving to the background or the OS entering a
// low-power mode; the connector then stops probing and lets idle
// connections lapse until PowerNormal is reported again.
type PowerHint uint8

const (
	// PowerNormal — foreground, no power constraint.
	PowerNormal PowerHint = iota
	// PowerLow — background or low-power mode: no global dials.
	PowerLow
)

// String returns a stable lowercase token for logging.
func (p PowerHint) String() string {
	if p == PowerLow {
		return "low"
	}
	return "normal"
}

var (
	powerHint     PowerHint
	powerSubs     = map[int]func(PowerHint){}
	powerSubsNext int
)

// SetPowerHint reports the current power state. Process-global like the
// other injection points; safe to call from any goroutine at any time.
func SetPowerHint(h PowerHint) {
	injectMu.Lock()
	powerHint = h
	subs := make([]func(PowerHint), 0, len(powerSubs))
	for _, fn := range powerSubs {
		subs = append(subs, fn)
	}
	injectMu.Unlock()
	for _, fn := range subs {
		fn(h)
	}
}

// CurrentPowerHint returns the last reported power state.
func CurrentPowerHint() PowerHint {
	injectMu.Lock()
	defer injectMu.Unlock()
	return powerHint
}

// SubscribePowerHint registers fn for future SetPowerHint calls. The
// returned cancel is idempotent. SDK-internal.
func SubscribePowerHint(fn func(PowerHint)) (cancel func()) {
	injectMu.Lock()
	id := powerSubsNext
	powerSubsNext++
	powerSubs[id] = fn
	injectMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			injectMu.Lock()
			delete(powerSubs, id)
			injectMu.Unlock()
		})
	}
}
