# 19. Account-level device discovery

Follow-up to [18-global-p2p](18-global-p2p.md). Own devices find each other
through a record addressed by a key derived from the identity key; space
records become an optional advertisement to other members. Cold recovery is
the same path as everyday discovery.

## Model

| Layer | Peers come from | Who can find a device | On by default |
|---|---|---|---|
| Account | pkarr record under the derived p2p key; every device registers itself, every device resolves it | holders of the identity key only | always |
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
u8  device count, then per device:                          (≤ 64)
    32 B endpoint key (= the peer id), u8 relay index, u32 unix seconds
```

An entry is 37 B, so dozens of devices fit the ~1 KB pkarr limit; a record
that still does not fit loses its oldest entries.

Publish (`accountCycle`): every online device, at start, hourly and on
ticket change, resolves → merges its own entry (peer id, home relay, now) →
drops entries unseen for 30 days → seals → PUTs to every configured relay.
The own entry is rewritten only when missing, naming another relay, or
older than 30 min. Any device holds the key, so last-writer-wins races heal
on the next cycle (a relay answering "stale" means a sibling published
first; the next cycle merges on top).

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
  handshake): known peers pass; an unknown peer passes only when its identity
  is this account's, and is then remembered as a sibling seen now — so a
  device whose entry has not propagated yet, a fresh restore, connects after
  one handshake. Everyone else is dropped there, at the cost of one
  handshake.

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

`SpaceInfo.Advertise` / `Space.SetAdvertise` is the tech-space row's
`p2pAdvertise` field (synced account-wide, absent = on). Off stops the row
heartbeat for every device of the account; the old rows age out on other
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

One PUT per device per hour (~1 KB) and one GET per resolve; the relay session
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
`PeerStore` source `account`, `SpaceIndexRecord.Advertise` /
`Space.SetAdvertise`, `config.GlobalP2P.PkarrRelayURLs`; e2e
`TestE2E_AccountRecovery` (embedded `dnsserver` + relay, dead nodes).

Later: Mainline DHT as a second record store (no infra; needs a Go BEP44
client).
