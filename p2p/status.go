package p2p

import (
	"time"

	"github.com/anyproto/any-sync-sdk/space"
)

// PeerStatus describes one known peer (LAN or global) for the debug
// surface.
type PeerStatus struct {
	// PeerId of the peer.
	PeerId string
	// SpaceIds it is known to share with this device, over any source.
	SpaceIds []string
	// Connected — a live connection exists right now.
	Connected bool
	// Sources that know the peer: "lan" (space exchange), "global"
	// (space key-value records), "account" (the account's device
	// record), in any combination.
	Sources []string
	// LastSeen is the newest liveness evidence: a key-value heartbeat
	// or a local connection. Zero for LAN-only peers.
	LastSeen time.Time
	// Tier is the liveness tier derived from LastSeen ("active",
	// "stale", "dormant", "disabled"); empty for LAN-only peers.
	Tier string
	// Failures counts consecutive failed global dials.
	Failures int
}

// GlobalStatus is the snapshot of the internet-wide p2p layer.
type GlobalStatus struct {
	// Enabled is the config opt-in state.
	Enabled bool
	// EndpointId is this device's iroh endpoint id (its peer key).
	EndpointId string
	// Ticket is the endpoint ticket this device publishes; empty until
	// the relay session is up.
	Ticket string
	// HomeRelay is the relay URL carried by Ticket, if any.
	HomeRelay string
	// RelayConnected — the session to the home relay is up.
	RelayConnected bool
	// Peers lists every peer known through records: space rows or the
	// account record.
	Peers []PeerStatus
	// Account is the account-level discovery record.
	Account AccountStatus
}

// AccountStatus is the snapshot of the account-level discovery record:
// the pkarr record every device of the account registers itself in.
type AccountStatus struct {
	// Enabled — pkarr relays are configured.
	Enabled bool
	// Relays are the configured pkarr relay hosts.
	Relays []string
	// Devices is the number of sibling devices the record names.
	Devices int
	// OwnEntry — the record names this device with its current relay.
	OwnEntry bool
	// LastResolved is when a record was last decoded from a relay.
	LastResolved time.Time
	// LastPublished is when this device last stored the record.
	LastPublished time.Time
	// LastError is the last failed cycle's error; empty after a
	// successful one.
	LastError string
	// ClockAhead is how far the record the relays hold was dated past
	// this device's clock at the last cycle (a sibling's clock runs
	// ahead; this device signs past it); zero when it was not.
	ClockAhead time.Duration
}

// Status is the account-wide snapshot of the p2p layers, returned by
// SDK.P2PStatus. A debug surface: cheap to read, not a stable wire
// contract.
type Status struct {
	// PeerId is THIS device's peer id — what it announces on the LAN.
	// Every device of an account must have a distinct one (the device
	// key is per-device); two devices sharing a peerId cannot pair.
	PeerId string
	// Enabled is the LAN config opt-out state.
	Enabled bool
	// LocalDiscovery is the mDNS switch state (config p2p.localDiscovery,
	// SDK.SetLocalDiscoveryEnabled). False reads as PossibilityDisabled.
	LocalDiscovery bool
	// ListenerStarted — the inbound QUIC listener is up.
	ListenerStarted bool
	// Port is the QUIC listen port (zero when not started).
	Port int
	// Possibility is the discovery-possibility state.
	Possibility Possibility
	// State is the account-wide p2p state (Connected when any direct
	// peer, LAN or global, has a live connection).
	State space.P2PState
	// Peers lists every peer currently in the LAN peer store.
	Peers []PeerStatus
	// Global is the internet-wide layer.
	Global GlobalStatus
}
