# Account-level device discovery

The account layer of global p2p ([18-global-p2p](global-p2p.md)). A
device finds the account's other devices through one record addressed by a
key derived from the identity key; a space's `p2p/iroh` rows only advertise
the device to other members. Cold recovery uses the same path as everyday
discovery.

## Model

| Layer | Peers come from | Who can find a device | On by default |
|---|---|---|---|
| Account | pkarr record under the derived discovery key; every device registers itself and resolves the rest | holders of the identity key | whenever the global layer is on and pkarr relays are configured |
| Space | KV row `p2p/iroh` in the space ([18-global-p2p](global-p2p.md)) | members of that space | yes, per space (`P2PAdvertise`) |

Account peers are candidates for every loaded space except local-only ones,
so own devices sync a space even with advertising off. Turning advertising
off hides the device from the space's other members; it can still dial
members that advertise.

## Keys

Both keys derive from the identity key, so a fresh device recomputes them
from the mnemonic alone:

- `discoveryKey` (ed25519): slip-10 `m/99999'/2'` rooted in the identity
  key's 32-byte ed25519 seed (`crypto.AnysyncDiscoveryKeyPath`, frozen). It
  addresses and signs the pkarr record. The identity key never signs the
  record: the account id is known to every member of every shared space,
  and would let them enumerate devices and relays.
- `discoveryEncKey` (symmetric): slip-21 `m/SLIP-0021/anysync/discovery`
  from the same seed (`crypto.AnysyncDiscoveryEncPath`); AEAD over the
  record payload.

`crypto.DeriveDiscoveryKeys(identity)` returns both; the SDK wraps them in
`internal/p2p/account.Keys`. The LAN probe-token keys
(`internal/anysyncx/discoverykeys.go`, HKDF over the account key per space
id) are a separate derivation from the same identity key.

## Record

One pkarr packet addressed by `discoveryKey.pub`:
`<pubkey><signature><timestamp><DNS packet>`, signed over timestamp and
packet, so only key holders write and a relay rejects older packets. It
holds one TXT record `_anydev` (relative to the key) whose value is
base64url of `AEAD(discoveryEncKey, payload)`. Payload
(`internal/p2p/account`):

```
u8  version = 1
u8  relay count, then per relay: u16 length + URL          (≤ 8)
u8  device count, then per device:                          (≤ 12)
    32 B endpoint key (= the peer id), u8 relay index, u32 unix seconds
```

An entry is 37 B. The plaintext is padded to 256-byte buckets, so the
ciphertext length reveals the device count only to within a bucket. Twelve
devices fit the ~1 KB pkarr packet with one relay in the table; a record
that still does not fit loses its oldest entries, and one naming more than
eight relays keeps the devices on the eight most used. Entries dated ahead
of the writer's clock are clamped; entries without a usable relay URL are
dropped. A reader parses a newer payload version as far as its layout goes.

### Publish

`accountCycle` runs on every online device at start, every 10 minutes, and
on ticket change:

1. Resolve: GET from every configured relay.
2. If the own entry is present, names the current home relay, and is younger
   than 30 minutes, stop.
3. Otherwise merge the own entry (peer id, home relay, now) into the newest
   record this device has decoded, drop entries unseen for 30 days, seal, and
   PUT to every relay.

The newest decoded record is kept in memory and in `account_record.json`
next to `p2p_peers.json`. A missing answer (a relay restart is not a sign
the siblings are gone) or an unreadable one keeps the known siblings and is
overwritten with them. A record written by a newer version is never
overwritten.

Every device holds the key, so concurrent writes race last-writer-wins and
heal on the next cycle. A relay answering "stale" means a sibling published
first or its clock runs ahead: the writer re-resolves, merges on top, and
signs past the held packet's timestamp (`Status().Global.Account.ClockAhead`
shows the lead). A failed cycle retries after 1 minute, doubling up to 10
minutes. The 10-minute cycle also bounds how long a relay wipe or a
sibling's fresh entry takes to heal.

### Resolve

GET from every configured relay, keep the newest packet whose signature
verifies, decrypt, validate each relay URL (https, or http with
`InsecureRelay`), and feed the entries to the peer store as source
`account`, with `lastSeen` from the entry (clamped to now) and a ticket
built from peer id and relay. A peer named by both a space row and the
record takes the newer of the two.

## Admission

- **Pre-handshake** (iroh incoming filter): a peer id known from either
  source and not disabled passes, under the live-connection cap. With the
  account layer on, an unknown peer id also passes under a budget: at most
  two in flight, thirty distinct ids a minute, and an id that failed the
  identity check is refused for ten minutes.
- **Post-handshake** (iroh handshake filter, identity proven by the any-sync
  handshake): a peer a space row names passes. A peer known only through the
  account record, or not known at all, passes only when its identity is this
  account's; it is then remembered as a sibling seen now, and stays known for
  an hour even if the record does not name it. A device whose entry has not
  propagated yet, or a fresh restore, therefore connects after one
  handshake. Everyone else is dropped at the cost of that handshake.
- An admitted peer counts as connected for 15 s before the pool hands its
  connection to the layer, so the connector never dials back into a device
  that just connected (a dial-back replaces the accepted connection and
  kills both). A peer that closes within 2 s of a successful dial refused
  the connection: that counts as a failed dial with backoff.

## Cold recovery

`Open` on an empty data dir needs no recovery mode:

1. The account cycle resolves the record at once and the connector dials the
   siblings.
2. The tech space is derived locally (its id and root follow from the
   identity key). As a loaded space with no node stream, it asks the
   connected sibling for pushes and head-syncs against it, so the space index
   fills from the sibling.
3. The bootstrap pulls each listed space from the same peer (`NewSpace` with
   the global peer as responsible peer).

This works with every node unreachable (`TestE2E_AccountRecovery`). It needs
a reachable pkarr relay, a reachable iroh relay, and one other device of the
account online. The account's only device finds an empty record.

## Per-space advertising

`SpaceInfo.P2PAdvertise` reads the tech-space row's `p2pAdvertise` field
(synced account-wide, absent = on); `Space.SetP2PAdvertise` writes it. Off
stops the row heartbeat on this device at once, and on the account's other
devices at their next heartbeat or space load (within a day). Rows already
published age out on other members' devices through the 30-day rule, since
KV has no delete. On republishes at once. The tech space never carries a
device row. Own devices are unaffected either way.

## Config

`P2P.Global.PkarrRelayURLs` (https; `InsecurePkarr` admits http for test
relays) turns the account layer on; empty leaves devices known to each other
only through the records of shared spaces. The pkarr relay is
`iroh-dns-server` (go-iroh's `dnsserver` in tests), co-hosted with the iroh
relays. It stores a derived public key, ciphertext and publisher IPs, and
can withhold a record but never forge one. `SDK.P2PStatus().Global.Account`
reports whether the layer is on, how many siblings the record names, and
when it was last read and published.

## Costs

Per device, to every configured pkarr relay: a GET every 10 minutes and a
PUT (~1 KB) whenever the own entry is refreshed, about every 30–40 minutes.
Relay session and connection costs are those of
[18-global-p2p](global-p2p.md).

## Threats

- **Enumeration** needs the derived public key, i.e. the mnemonic.
- **Forgery / replay**: signature plus monotonic timestamp at the relay.
- **Withholding**: several relays; the space layer still works for
  advertised spaces.
- **A lost device** keeps a valid entry until it ages out. Revocation is an
  ACL matter; the record is discovery only.

## Where it lives

- `internal/p2p/account`: record codec, pkarr relay client
- `internal/p2p/accountlayer.go`: cycle, sibling set, admission
- `PeerStore` source `account`
- `SpaceIndexRecord.P2PAdvertise` / `Space.SetP2PAdvertise`
- `config.GlobalP2P.PkarrRelayURLs`
- e2e `TestE2E_AccountRecovery` (embedded `dnsserver` and relay, dead nodes)

Not implemented: Mainline DHT as a second record store (no infra; needs a Go
BEP44 client).
