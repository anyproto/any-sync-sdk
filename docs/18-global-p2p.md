# Global p2p

Direct device-to-device sync across the internet, next to the LAN layer
(docs/03-space.md § Sync). Transport: any-sync's iroh transport (QUIC,
relay fallback, hole punching — `github.com/tmc/go-iroh`). Discovery:
each space's key-value store.

## Model

- **Identity.** The iroh endpoint id is the device peer key. A peer id
  converts to an endpoint id and back, so a key-value row signed by a
  peer already names its endpoint; the transport verifies the remote
  key against the expected peer id on every connection, and the
  any-sync handshake (identity, protocol version, credentials) runs on
  the first stream exactly as over QUIC.
- **Record.** Key `p2p/iroh`, one row per device per space (the store
  keys rows by `key-peerId`, newest wins), value = the device's endpoint
  ticket, relay-only (no direct IPs; direct paths come from
  relay-mediated NAT traversal). Rows are encrypted with the space read
  key and signed by peer and identity, so only members learn addresses
  and a member can publish only its own. The row is re-set every 24 h
  while the space is loaded (on load when older than 12 h): its
  timestamp is the remote liveness marker.
- **Scope.** The tech space is shared by every device of the account —
  account-wide device discovery. Shared spaces add the other members'
  devices. Readers cannot write rows and stay dial-only; local-only and
  guest-mode spaces take no part.
- **Never blocking.** No sync path waits for a global dial. Global
  peers are used only while already connected (`pool.Pick`); one
  background connector is the sole dialer, the only caller of
  `pool.Get` for a global peer and the only ctx that opts in to the
  iroh scheme (`peerservice.CtxWithGlobalDial`). The addr book gives a
  peer either its LAN addresses or its ticket, never both, so a LAN dial
  can never fall through into a relay dial. Cold restore stays a LAN
  affair: a fresh device has no rows and nobody's allowlist knows it.
- **Inbound gate.** The transport accepts a connection only from a
  peer id present in the key-value records of a loaded space and not in
  the disabled tier, and only while live global connections are under
  `MaxConnections + MaxInbound`. The records are the allowlist.

## Liveness tiers

Every peer carries a persisted record (`p2p_peers.json` under DataDir):
`lastSeen`, `lastAttempt`, `failures`. `lastSeen` is the newest of the
key-value row timestamps (publisher clock, clamped to now), a
successful dial, an accepted connection, or the one-minute sweep over
peers still connected. Age selects the tier:

| tier     | age (defaults) | dialing                              | in peer sets    |
|----------|----------------|--------------------------------------|-----------------|
| active   | < 1 h          | kept connected, 30 s → 10 min backoff | yes            |
| stale    | 1 h – 7 d      | probed every 30 min, after active     | while connected |
| dormant  | 7 d – 30 d     | probed at startup and every 6 h       | while connected |
| disabled | > 30 d         | never; no address, refused inbound    | no              |

A newer row or any local success re-tiers the peer and wakes the
connector. A disabled peer's row stays (the store has no delete) and is
ignored until its timestamp moves; identities removed from the ACL are
dropped on the next reconcile.

## Budget

Device-wide, in `config.P2P.Global`:

- `MaxConnections` (4): maintained global connections, chosen as a
  greedy cover of the loaded spaces — most spaces covered first, ties to
  already connected peers, own devices, most recently seen. A space
  already covered by a connected peer triggers no further dial.
- `MaxInbound` (8): headroom above `MaxConnections` for connections
  the other side opened.
- `MaxDialsPerMinute` (6), one dial in flight, `DialTimeout` (15 s),
  per-peer backoff by tier. Nothing loaded or everything covered means
  zero dials.
- `KeepAlive` (60 s); the QUIC idle timeout is three periods. The relay
  session is the floor cost of being reachable.
- `p2p.SetPowerHint(p2p.PowerLow)` (platform bridges) stops probing;
  `PowerNormal` resumes it.

## Head updates and head-sync

With a node stream up, global peers receive nothing directly: the nodes
propagate, and the connections serve pubsub, files p2p, inbound sync
and failover. Without a node, global peers take over: head updates are
coalesced per object over a 500 ms window and sent to at most
`MaxConnections` connected peers; the periodic diff runs against one
connected global peer per tick, rotating.

## Configuration

```go
config.P2P{
	Global: config.Global{
		Enabled:   &on,
		RelayURLs: []string{"https://relay.example"},
	},
}
```

Off by default until a relay is deployed for the network. Independent
of the LAN layer (`P2P.Enabled`). `InsecureRelay` admits `http://`
relays for development. `Port` pins the UDP port. The remaining fields
are the budget above.

## Status

`SDK.P2PStatus().Global` reports the endpoint id, the published ticket,
the home relay and its session state, and every known global peer with
`LastSeen`, `Tier`, `Failures`, `Sources`. Per space,
`SpaceSyncStatus.GlobalPeers` counts connected global peers and `P2P`
is `Connected` when any direct peer — LAN or global — is live.
