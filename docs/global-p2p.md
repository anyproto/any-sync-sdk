# Global p2p

Direct device-to-device sync across the internet, next to the LAN layer
(docs/space.md § Sync). Transport: any-sync's iroh transport (QUIC, relay
fallback, hole punching; `github.com/tmc/go-iroh`). Discovery: each space's
key-value store, plus the account record for the account's own devices
([19-account-discovery](account-discovery.md)).

## Model

- **Identity.** The iroh endpoint id is the device peer key. A peer id
  converts to an endpoint id and back, so a key-value row signed by a peer
  already names its endpoint. The transport verifies the remote key against
  the expected peer id on every connection, and the any-sync handshake
  (identity, protocol version, credentials) runs on the first stream as over
  QUIC.
- **Record.** Key `p2p/iroh`, one row per device per space (the store keys
  rows by `key-peerId` and binds that id to the signed key and peer, newest
  wins). The value is the device's endpoint ticket, relay-only: no direct
  IPs, since direct paths come from relay-mediated NAT traversal. Rows are
  encrypted with the space read key and signed by peer and identity, so only
  members learn addresses and a member can publish only its own. Readers
  ignore a value over 512 bytes, a ticket whose endpoint is not the row's
  peer, a ticket with a direct address or no relay, and rows of identities no
  longer in the ACL. One identity announces at most 8 devices per space; a
  space contributes at most 256 peers (newest rows win). The row is re-set
  every 24 h while the space is loaded, and on load when older than 12 h; its
  timestamp, clamped to the reader's clock, is the remote liveness marker.
- **Scope.** Space rows name the other members' devices. Own devices come
  from the account record and count as global peers of every space, so the
  tech space carries no device row. A space's row is written only while its
  `P2PAdvertise` switch is on (default on). Readers cannot write rows and
  stay dial-only; local-only and guest-mode spaces take no part.
- **Never blocking.** No sync path waits for a global dial. Global peers are
  used only while already connected. One background connector is the sole
  dialer and the only ctx that opts in to the iroh scheme
  (`peerservice.CtxWithGlobalDial`). It dials through the peer service and
  hands the connection to the pool as an incoming peer rather than calling
  `pool.Get`: the pool's `Pick` waits on an in-flight load for the same
  peer, so a `Get`-driven dial would make every "is it connected" lookup
  wait for the relay. Every other pool lookup on a global peer is bounded
  (`p2p.PickTimeout`, 50 ms). The addr book gives a peer either its LAN
  addresses or its ticket, never both, so a LAN dial never falls through
  into a relay dial. (The push node's addresses are the one exception; the
  SDK registers them directly, outside the peer store.) A LAN peer whose
  first handshake fails, or that mDNS reports lost, releases its addresses
  so its ticket can take over.
- **Inbound gate.** The transport accepts a connection only from a peer id
  present in the records (a loaded space's rows or the account record), not
  in the disabled tier, and only while fewer than
  `MaxConnections + MaxInbound` distinct global peers hold a live
  connection (connector dials plus inbound connections folded in by the
  10 s sweep). With the account layer on, an unknown peer passes the
  pre-handshake filter under a small budget and is admitted after the
  handshake only if it proves this account's identity.

## Liveness tiers

Every peer has a persisted record (`p2p_peers.json` under DataDir):
`lastSeen`, `lastAttempt`, `failures`. `lastSeen` is the newest of: the
key-value row timestamps (publisher clock, clamped to now), a successful
dial, an accepted connection, or the 10 s sweep over peers still connected.
A row arriving live through the store's apply path counts as "seen now" when
its timestamp is within a day of the local clock, so a device whose clock
runs hours off is not demoted. A row further off keeps its own timestamp (it
replays old state), and the periodic reconcile always uses row timestamps.
Age selects the tier:

| tier     | age (defaults) | dialing                               | in peer sets    |
|----------|----------------|---------------------------------------|-----------------|
| active   | < 1 h          | kept connected, 30 s → 30 min backoff | yes             |
| stale    | 1 h – 7 d      | probed every 2 h, after active        | while connected |
| dormant  | 7 d – 30 d     | probed at startup and every 6 h       | while connected |
| disabled | > 30 d         | never; no address, refused inbound    | no              |

A newer row or any local success re-tiers the peer and wakes the connector.
A disabled peer's row stays (the store has no delete) and is ignored until
its timestamp moves. Identities removed from the ACL are dropped on the next
reconcile, every 5 minutes (the space layer owns the ACL update slot). A
peer gone from every space and every source is forgotten once past the
disable threshold; such records are also pruned when the file is loaded.

## Budget

Device-wide, in `config.P2P.Global` (`config.GlobalP2P`):

- `MaxConnections` (4): maintained global connections, chosen as a greedy
  cover of the loaded spaces. Connected peers come first (the spaces they
  cover need no dial), then the peer covering the most uncovered spaces,
  ties to own devices, then most recently seen. When connected peers cover
  every loaded space, nothing is dialed.
- `MaxInbound` (8): headroom above `MaxConnections` for connections the
  other side opened, counted as distinct peers with a live connection.
- `MaxDialsPerMinute` (6), one dial in flight, `DialTimeout` (6 s), per-peer
  backoff by tier. A relay dial completes in about a round trip or dies at
  QUIC's 5 s handshake timeout, so a longer `DialTimeout` only holds the dial
  slot. A dial that finds nobody home still costs about 13 KB of uplink in
  retransmitted handshake packets; the rate limit and backoff bound that.
- The pool keeps an idle relay connection for the transport's peer TTL
  (30 min from creation), then closes it and the connector dials again: an
  always-open idle connection would never be re-evaluated against the
  budget. That is one handshake per half hour per held peer.
- `KeepAlive` (60 s); the QUIC idle timeout is three periods. Over the
  relay, an idle connection costs ~170 B/min at 60 s (~480 B/min at 25 s).
  The relay session itself costs more, ~315 B/min per endpoint (four relay
  pings a minute), whether or not anything is connected.
- `p2p.SetPowerHint(p2p.PowerLow)` (platform bridges) stops probing;
  `PowerNormal` resumes it. The endpoint stays bound, so a backgrounded
  device still pays the relay session cost.

## Head updates and head-sync

Pushes to global peers follow subscriptions. A device with no node stream
asks every connected global peer for pushes with a `SpaceSubscription`:
immediately when the node stream drops or a global peer connects, then every
30 s. It withdraws the ask once a node stream is back, and pushes to all its
connected global peers itself, since they are its only path. A device with a
node stream pushes only to the global peers that asked; everyone else
receives through the nodes.

The receiver keeps asks in its own registry, not on the stream. An ask is
honoured only from a peer the space's records name and that is not
reachable over the LAN (the LAN path already pushes to it), with at most 64
well-formed space ids per message, and it expires three cadences (90 s)
after the last refresh. A lost withdrawal, a peer removed from the records,
or a dead stream therefore stops pushes on its own, and a stranger can
neither obtain an ask nor keep a space pushing with an empty audience.

Pushed head updates are coalesced per object over a 1 s window and sent to
at most `MaxConnections` peers. A tree receiver fetches whatever it misses;
key-value and ACL updates are queued in order, since their payloads are not
cumulative. Over the relay, an edit costs ~3.1 KB when it shares a window
with others and ~9.2 KB alone; with a node stream up, nothing goes to global
peers. The periodic diff adds one connected global peer per tick, rotating,
only while no node stream is up. A space with no known global peer queues
nothing.

A tree fetched whole from a peer, during a diff round or because that
peer's head update named a tree this device lacked, counts as synced with
that peer, so a space that converges through pulls or pushes alone reports
`synced` without waiting for an empty round. As with the nodes, a peer that
itself lags is not detected until its own diff.

## Configuration

```go
config.P2P{
	Global: config.GlobalP2P{
		Enabled:   &on,
		RelayURLs: []string{"https://relay.example"},
	},
}
```

Off by default. `RelayURLs` is required when enabled; `Open` refuses an
enabled layer without one, because a relay-less endpoint would publish its IP
addresses into every space's records. Independent of the LAN layer
(`P2P.Enabled`). `InsecureRelay` admits `http://` relays for development.
`Port` pins the UDP port (dual-stack). `PkarrRelayURLs` turns on the account
layer ([19-account-discovery](account-discovery.md)). The remaining fields
are the budget above.

## Status

`SDK.P2PStatus().Global` reports the endpoint id, the published ticket, the
home relay and its session state, and every known global peer with
`LastSeen`, `Tier`, `Failures` and `Sources` (`lan`, `global`, `account`);
`Global.Account` is the account record's state. LAN-only peers in
`Status.Peers` carry no liveness fields. Per space,
`SpaceSyncStatus.GlobalPeers` counts connected global peers, and `P2P` is
`Connected` when any direct peer, LAN or global, is live. With nobody live,
the LAN verdicts (`Restricted`, `NotPossible`) still surface while the LAN
layer is on, so the global layer never hides a denied local-network
permission.

## Operational notes

- Space records are not a cold-restore path: a device with an empty data dir
  has no rows to dial from, and nobody's allowlist knows it yet. A first
  restore needs the nodes, the LAN, or the account layer. Once a device holds
  its spaces, it reconnects to its global peers from the persisted records
  without any node.
- One device key means one endpoint. The relay keeps a single session per
  endpoint id, so two processes sharing a device key evict each other and
  neither stays reachable. Every process needs its own key.
