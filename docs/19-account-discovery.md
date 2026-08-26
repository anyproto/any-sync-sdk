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

- `discoveryKey` (ed25519): a hardened child of the identity node at a
  reserved index (any-sync constant, frozen once shipped). It addresses and
  signs the pkarr record. The identity key itself is never used: the account
  id is known to every member of every shared space and would let them
  enumerate devices and relays.
- `discoveryEncKey` (symmetric): slip-21 `m/SLIP-0021/anytype/p2p/discovery`
  from the identity key; AEAD over the record payload.

The LAN probe-token key (`discoverykeys.go`) is a third label in the same
family.

## Record

One pkarr packet addressed by `discoveryKey.pub`: `<pubkey><signature><timestamp><DNS packet>`,
signature over timestamp + packet, so only key holders write and a relay
rejects older packets. TXT set `_anydev`, values = base64 of
`AEAD(discoveryEncKey, payload)`:

```
payload {
  relays  []string          // relay URL table
  devices []{ peerId, relay uint8, lastSeen unixSec }
}
```

An entry is ~40 B (the endpoint id derives from the peer id, the ticket from
relay + endpoint id), so dozens of devices fit the ~1 KB pkarr limit.

Publish: every online device, hourly and on ticket change, resolves → merges
its own entry → drops entries older than 30 days → signs → publishes. Any
device holds the key, so last-writer-wins races heal on the next cycle.

Resolve: GET from every configured pkarr relay, keep the newest packet whose
signature verifies, decrypt, feed entries to the peer store as source
`account` with `lastSeen` from the entry.

## Admission

- Pre-handshake (iroh transport filter): peer ids known from either source.
  Unknown peers pass under a small rate budget.
- Post-handshake (identity from the any-sync handshake): identity == own
  account admits regardless of the pre-filter, so a device whose entry has not
  propagated yet — a fresh restore — connects after one handshake. Everyone
  else is dropped there.

## Cold recovery

`Open` on an empty data dir: derive keys → resolve the record → start the
connector with the account peers → pull the tech space from the first
connected device (the LAN cold-restore pull over an iroh connection) → the
space layer takes over. No recovery mode, no flag; a device that is the
account's only device simply finds an empty record.

Needs a pkarr relay and an iroh relay reachable and one other device online.

## Per-space advertising

`advertise` is a persisted space setting (tech-space index row), default on,
exposed on the `Space` API. Off stops the row heartbeat; the old row ages out
on other members' devices through the 30-day rule (KV has no delete). Own
devices are unaffected.

## Config

`P2P.Global.PkarrRelayURLs` (https; an insecure flag admits http for test
relays), baked-in defaults next to the iroh relay list. The pkarr relay is
`iroh-dns-server` (or go-iroh's `dnsserver` in tests), co-hosted with the iroh
relays: it stores a derived public key, ciphertext and publisher IPs, and can
withhold but never forge.

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

## Plan

1. Keys, record codec, publisher/resolver, `account` peer-store source,
   handshake-stage admission (any-sync: filter with identity; the derivation
   index constant). No user-visible change yet.
2. Per-space `advertise` setting and API, cold recovery in `Open`, e2e with
   embedded `dnsserver` and relay (fresh device with dead nodes recovers
   through a peer).

Later: Mainline DHT as a second record store (no infra; needs a Go BEP44
client).
