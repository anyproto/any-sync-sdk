# 19. Account-level device discovery

Follow-up to [18-global-p2p](global-p2p.md). Own devices find each other
through a record addressed by a key derived from the identity key; space
records become an optional advertisement to other members. Cold recovery is
the same path as everyday discovery.

## Model

| Layer | Peers come from | Who can find a device | On by default |
|---|---|---|---|
| Account | pkarr record under the derived p2p key; every device registers itself, every device resolves it | holders of the identity key only | whenever the global layer is on and pkarr relays are configured |
| Space | KV row `p2p/iroh` in the space (18-global-p2p) | members of that space | yes, per space (`advertise`) |

Account peers are candidates for every loaded space except local-only ones, so
own devices sync a space even when advertising is off there. Turning
advertising off in a space hides the device from the other members; it can
still dial members that advertise.

## Keys

All derived from the identity key, so a fresh device recomputes them from the
mnemonic alone:

- `discoveryKey` (ed25519): slip-10 `m/99999'/2'` rooted in the identity
  key's 32-byte ed25519 seed (`crypto.AnysyncDiscoveryKeyPath`, frozen). It
  addresses and signs the pkarr record. The identity key itself never signs
  the record: the account id is known to every member of every shared space
  and would let them enumerate devices and relays.
- `discoveryEncKey` (symmetric): slip-21 `m/SLIP-0021/anysync/discovery`
  from the same seed (`crypto.AnysyncDiscoveryEncPath`); AEAD over the
  record payload.

Both come from `crypto.DeriveDiscoveryKeys(identity)`; the SDK wraps them in
`internal/p2p/account.Keys`. The LAN probe-token key (`discoverykeys.go`) is
a third label in the same family.

## Record

One pkarr packet addressed by `discoveryKey.pub`: `<pubkey><signature><timestamp><DNS packet>`,
signature over timestamp + packet, so only key holders write and a relay
rejects older packets. One TXT record `_anydev` (relative to the key), value
= base64url of `AEAD(discoveryEncKey, payload)`; the payload
(`internal/p2p/account`):

```
u8  version = 1
u8  relay count, then per relay: u16 length + URL          (≤ 8)
u8  device count, then per device:                          (≤ 12)
    32 B endpoint key (= the peer id), u8 relay index, u32 unix seconds
```

An entry is 37 B. The plaintext is padded to 256-byte buckets (the
ciphertext length then only reveals the device count to within a bucket),
so twelve devices fit the ~1 KB pkarr packet with one relay in the table
(measured); a record that still does not fit loses its oldest entries, and
one naming more than eight relays keeps the devices on the eight most used.
Entries whose stamp is ahead of the writer's clock are clamped; entries
without a usable relay URL are dropped.

Publish (`accountCycle`): every online device, at start, every 10 minutes
and on ticket change, resolves (a GET per relay) and, when its own entry is
missing, names another relay or is older than 30 minutes, merges its entry
(peer id, home relay, now) → drops entries unseen for 30 days → seals → PUTs
to every configured relay. Ten minutes is also how fast a relay wipe or a
sibling's fresh entry heals.
The own entry is rewritten only when missing, naming another relay, or
older than 30 min. Every publish merges onto the newest record this device
decoded (kept in memory and in `account_record.json` next to
`p2p_peers.json`): an answer that is missing (a relay restart is not a sign
the siblings are gone) or unreadable keeps the known siblings and is
overwritten with them; a record written by a newer version is read as far
as this layout goes and never overwritten. Any device holds the key, so
last-writer-wins races heal on the next cycle: a relay answering "stale"
means a sibling published first, or its clock runs ahead — the writer
re-resolves, merges on top, and signs past the held packet's timestamp
(`Status().Global.Account.ClockAhead` shows the lead). Failed cycles back
off from 1 to 15 minutes.

Resolve: GET from every configured relay, keep the newest packet whose
signature verifies, decrypt, validate each relay URL (https, or http with
`InsecureRelay`), feed entries to the peer store as source `account` with
`lastSeen` from the entry (clamped to now) and the ticket built from peer id
+ relay. A peer named by both a space row and the record takes the newer of
the two.

## Admission

- Pre-handshake (iroh incoming filter): peer ids known from either source
  and not disabled, under the live-connection cap. With the account layer on,
  unknown peers pass under a budget of 6 per minute.
- Post-handshake (iroh handshake filter, identity proven by the any-sync
  handshake): a peer a space row names passes; a peer known only through the
  account record, or unknown altogether, passes only when its identity is
  this account's, and is then remembered as a sibling seen now — so a device
  whose entry has not propagated yet, a fresh restore, connects after one
  handshake. Everyone else is dropped there, at the cost of one handshake;
  unknown peers get at most two handshakes in flight, thirty distinct ids a
  minute, and a failed identity check bars the id for ten minutes.
- An admitted peer counts as connected for the next 15 s even before the
  pool hands its connection to the layer, so the connector never dials back
  into a device that just connected (a dial-back replaces the accepted
  connection and kills both). A peer that closes within 2 s of a successful
  dial refused us: that is a failed dial with backoff, not a connection.

## Cold recovery

`Open` on an empty data dir: the app starts, the account cycle resolves the
record at once and the connector dials the siblings; the tech space is
derived locally (its id and root follow from the identity key) and, as a
loaded space with no node stream, asks the connected sibling for pushes and
head-syncs against it, so the space index fills from the sibling; the
bootstrap then pulls each listed space from the same peer (`NewSpace` with
the global peer as responsible peer). No recovery mode, no flag; a device
that is the account's only device simply finds an empty record. Measured in
the e2e: a fresh device converges on a sibling's space and object in
seconds with every node dead.

Needs a pkarr relay and an iroh relay reachable and one other device online.

## Per-space advertising

`SpaceInfo.P2PAdvertise` / `Space.SetP2PAdvertise` is the tech-space row's
`p2pAdvertise` field (synced account-wide, absent = on; written through
`Space.SetP2PAdvertise`). Off stops the row
heartbeat on this device at once and on the account's other devices within
a day (their next heartbeat or space load reads the switch); the old rows age out on other
members' devices through the 30-day rule (KV has no delete). On republishes
at once. The tech space never carries a device row. Own devices are
unaffected either way.

## Config

`P2P.Global.PkarrRelayURLs` (https; `InsecurePkarr` admits http for test
relays) turns the account layer on; empty leaves devices known to each other
only through the records of shared spaces. The pkarr relay is
`iroh-dns-server` (or go-iroh's `dnsserver` in tests), co-hosted with the
iroh relays: it stores a derived public key, ciphertext and publisher IPs,
and can withhold but never forge. `SDK.P2PStatus().Global.Account` reports
whether the layer is on, how many siblings the record names and when it was
last read.

## Costs

One PUT per device per hour (~1 KB) and one GET per resolve, to every
configured pkarr relay; the relay session
and connection costs are those of 18-global-p2p. The tech-space row is no
longer written for own devices.

## Threats

- Enumeration: needs the derived public key, i.e. the mnemonic.
- Forgery / replay: signature + monotonic timestamp at the relay.
- Withholding: several relays; the space layer still works for advertised
  spaces.
- A lost device keeps a valid entry until it ages out; revocation is an ACL
  matter, the record is discovery only.

## Where it lives

`internal/p2p/account` (record codec, pkarr relay client),
`internal/p2p/accountlayer.go` (cycle, sibling set, admission),
`PeerStore` source `account`, `SpaceIndexRecord.P2PAdvertise` /
`Space.SetP2PAdvertise`, `config.GlobalP2P.PkarrRelayURLs`; e2e
`TestE2E_AccountRecovery` (embedded `dnsserver` + relay, dead nodes).

Later: Mainline DHT as a second record store (no infra; needs a Go BEP44
client).
