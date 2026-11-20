// Package p2p is the public surface of the SDK's local-network layer:
// the types exchanged with discovery drivers and the injection points
// platform embedders (gomobile bridges) use to plug native behavior in.
// Regular embedders never need this package — the SDK wires a default
// mDNS driver on its own.
package p2p

// DiscoveredPeer is one device found on the local network.
type DiscoveredPeer struct {
	// PeerId is the any-sync peer id the device announced.
	PeerId string
	// Addrs are "ip:port" endpoints the peer listens on (QUIC).
	Addrs []string
}

// OwnAddresses describes this device's own local-network listener,
// sent to discovered peers so they can dial back.
type OwnAddresses struct {
	// Addrs are this device's LAN IPs (no port).
	Addrs []string
	// Port is the local QUIC listen port.
	Port int
}
