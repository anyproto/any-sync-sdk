package p2p

import "github.com/anyproto/any-sync-sdk/space"

// PeerStatus describes one known local-network peer for the debug
// surface.
type PeerStatus struct {
	// PeerId of the LAN peer.
	PeerId string
	// SpaceIds it reported in the space exchange.
	SpaceIds []string
	// Connected — a live connection exists right now.
	Connected bool
}

// Status is the account-wide snapshot of the local-network layer,
// returned by SDK.P2PStatus. A debug surface: cheap to read, not a
// stable wire contract.
type Status struct {
	// PeerId is THIS device's peer id — what it announces on the LAN.
	// Every device of an account must have a distinct one (the device
	// key is per-device); two devices sharing a peerId cannot pair.
	PeerId string
	// Enabled is the config opt-out state.
	Enabled bool
	// ListenerStarted — the inbound QUIC listener is up.
	ListenerStarted bool
	// Port is the QUIC listen port (zero when not started).
	Port int
	// Possibility is the discovery-possibility state.
	Possibility Possibility
	// State is the account-wide p2p state (Connected when any local
	// peer has a live connection).
	State space.P2PState
	// Peers lists every peer currently in the local peer store.
	Peers []PeerStatus
}
