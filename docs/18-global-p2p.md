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
  keys rows by `key-peerId` and binds that id to the signed key and
  peer, newest wins), value = the device's endpoint ticket, relay-only
  (no direct IPs; direct paths come from relay-mediated NAT traversal).
  Rows are encrypted with the space read key and signed by peer and
  identity, so only members learn addresses and a member can publish
  only its own. Consumers enforce the shape: a value over 512 bytes, a
  ticket whose endpoint is not the row's peer, a ticket carrying a
  direct address or no relay, a row of an identity no longer in the ACL
  — all ignored. One identity announces at most 8 devices per space and
  a space contributes at most 256 peers (newest rows win). The row is
  re-set every 24 h while the space is loaded (on load when older than
  12 h): its timestamp, clamped to the reader's clock, is the remote
  liveness marker.
- **Scope.** The tech space is shared by every device of the account —
  account-wide device discovery. Shared spaces add the other members'
  devices. Readers cannot write rows and stay dial-only; local-only and
  guest-mode spaces take no part.
- **Never blocking.** No sync path waits for a global dial. Global
  peers are used only while already connected; one background connector
  is the sole dialer and the only ctx that opts in to the iroh scheme
  (`peerservice.CtxWithGlobalDial`). It dials through the peer service
  and hands the connection to the pool as an incoming peer instead of
  going through `pool.Get`: the pool's `Pick` waits on an in-flight
  load for the same peer, so a `Get`-driven dial would make every
  "is it connected" lookup wait for the relay. Every remaining pool
  lookup on a global peer is bounded (`p2p.PickTimeout`, 50 ms). The
  addr book gives a peer either its LAN addresses or its ticket, never
  both (the push node's addresses are the one exception, registered by
  the SDK directly and never in the peer store), so a LAN dial can never
  fall through into a relay dial. A LAN peer whose first handshake fails
  or that mDNS reports lost releases its addresses, so its ticket can
  take over. Cold restore stays a LAN affair: a fresh device has no rows
  and nobody's allowlist knows it.
- **Inbound gate.** The transport accepts a connection only from a
  peer id present in the key-value records of a loaded space and not in
  the disabled tier, and only while fewer than
  `MaxConnections + MaxInbound` distinct global peers hold a live
  connection (the layer's own count: connector dials plus inbound
  connections folded in by the minute sweep). The records are the
  allowlist.

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
dropped on the next reconcile (every 5 minutes — the ACL update slot is
owned by the space layer). A peer gone from every space and every source
is forgotten once past the disable threshold; records past it are also
pruned when the file is loaded.

## Budget

Device-wide, in `config.P2P.Global` (type `config.GlobalP2P`):

- `MaxConnections` (4): maintained global connections, chosen as a
  greedy cover of the loaded spaces — connected peers first (the spaces
  they cover need no dial), then most spaces covered, ties to own
  devices, then most recently seen. Every loaded space covered by a
  connected peer means zero dials.
- `MaxInbound` (8): headroom above `MaxConnections` for connections
  the other side opened, counted as distinct peers with a live
  connection.
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
and failover. Without a node, global peers take over: tree head updates are
coalesced per object over a 500 ms window (a tree receiver fetches
whatever it misses; key-value and ACL updates are queued in order, their
payloads are not cumulative) and sent to at most `MaxConnections`
connected peers; the periodic diff runs against one connected global
peer per tick, rotating. A space with no known global peer queues
nothing.

## Configuration

```go
config.P2P{
	Global: config.GlobalP2P{
		Enabled:   &on,
		RelayURLs: []string{"https://relay.example"},
	},
}
```

Off by default until a relay is deployed for the network. `RelayURLs`
is required when enabled (`Open` refuses the combination: a relay-less
endpoint would publish its IP addresses into every space's records).
Independent of the LAN layer (`P2P.Enabled`). `InsecureRelay` admits
`http://` relays for development. `Port` pins the UDP port (dual-stack).
The remaining fields are the budget above.

## Status

`SDK.P2PStatus().Global` reports the endpoint id, the published ticket,
the home relay and its session state, and every known global peer with
`LastSeen`, `Tier`, `Failures`, `Sources`. LAN-only peers in
`Status.Peers` carry no liveness fields. Per space,
`SpaceSyncStatus.GlobalPeers` counts connected global peers and `P2P`
is `Connected` when any direct peer — LAN or global — is live; with
nobody live the LAN verdicts (`Restricted`, `NotPossible`) still
surface while the LAN layer is on, so a denied local-network permission
is never hidden by the global layer.
