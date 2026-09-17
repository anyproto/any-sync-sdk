# One-to-One Spaces

A 1-1 (direct) space is shared by exactly two identities and derived
deterministically from their keys: both peers compute the same space id,
ACL and read key with no owner or invite handshake. It is the substrate for
direct messaging and any two-party shared state.

Two requirements shape the design:

1. **Incoming requests need approval.** An incoming 1-1 stays pending until
   the local user accepts it. Declining is sticky.
2. **No hard server dependency.** Derive, materialize and approve work with
   no coordinator and no sync nodes. Server-backed discovery is an optional
   layer on top.

## What any-sync provides

any-sync implements the 1-1 primitive; the SDK drives existing calls and
adds no crypto or ACL shapes.

`spacepayloads.StoragePayloadForOneToOneSpaceWithType(aSk, bPk, spaceType)`
builds the whole space locally and offline:

- A shared secret comes from ECDH, `crypto.GenerateSharedKey(myPriv,
  otherPub, path)`. Since `ECDH(aSk, bPk) == ECDH(bSk, aPk)`, each peer
  derives identical key material from only the other party's public
  account identity.
- Derivation paths (`util/crypto/derived.go`): `AnysyncOneToOneSpacePath`
  (owner/space key), `AnysyncReadOneToOneSpacePath` (read key),
  `AnysyncMetadataOneToOnePath` (metadata key).
- The ACL root (`BuildOneToOneRoot`) embeds `AclOneToOneInfo`:
  - `Owner = sharedPk`: a synthetic identity no member holds. It exists
    only to satisfy any-sync's "ACL must have an owner" invariant and is
    ignored by business logic.
  - `Writers = [aPk, bPk]`: the two real identities, sorted so the space id
    is order-independent, both granted `AclPermissionsWriter` at the root.
- Replication key = `fnv64(sharedPubKey)`, independent of either account's
  replication key.
- On-wire header `SpaceType = "any.onetoone"` (coordinator allow-list, see
  `docs/space.md`).

The ACL is **immutable**: a root with two writers and no
invite/request/accept records. Either party who knows the other's identity
can derive and join unilaterally. **"Approve incoming" is therefore a local
SDK gate, not an ACL operation.** It decides whether this device
materializes storage and syncs the space; it cannot gate membership.

## Architecture: two layers

```
┌─────────────────────────────────────────────────────────────┐
│  Layer 2 — Discovery / notification  (OPTIONAL)              │
│  "Bob, Alice wants a 1-1 with you."                          │
│  Coordinator inbox (InboxFetch / InboxAddMessage)            │
│  Without it: out-of-band identity exchange                   │
└─────────────────────────────────────────────────────────────┘
                              │ surfaces "incoming" → pending row
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  Layer 1 — 1-1 primitive  (REQUIRED, zero server deps)       │
│  derive → local state machine (pending/active/declined)      │
│  → materialize + sync + seed (accept) / marker (decline)     │
└─────────────────────────────────────────────────────────────┘
```

Layer 2 uses the coordinator inbox when present; Layer 1 never depends on
it. Without Layer 2 the app supplies the peer identity out-of-band and
calls `OneToOne` / `RegisterIncoming` directly.

## Layer 1 — the 1-1 primitive

### Local state machine

State lives on the tech-space index row for the derived space id. The row's
synced `FieldOneToOnePeer` carries the other identity, the one thing a
space id doesn't encode invertibly; accept and every device of the account
need it to materialize storage.

| State              | Field / scope                         | Materialized? | Crosses devices? |
|--------------------|---------------------------------------|---------------|------------------|
| `oneToOnePending`  | `LocalStatus` (device-local)          | No, row only  | No: each device discovers through the shared inbox cursor or its own `RegisterIncoming` |
| `active`           | `RemoteStatus=active` (synced)        | Yes           | Yes: accept is account-scoped |
| `oneToOneDeclined` | `RemoteStatus` (synced, non-terminal) | No            | Yes: decline silences every device |
| `oneToOneDeleted`  | `RemoteStatus` (synced, non-terminal) | No, offloaded | Yes: every device offloads |

Pending is device-local because it is only this device's unresolved view.
Accept and decline are account-wide decisions, so both are synced. Decline
and delete use their own non-terminal status values rather than the regular
`deleted`, which is terminal (the index handler refuses to move out of it):
a later explicit `OneToOne(peer)` must be able to override both.

Transitions:

```
(none)            ── OneToOne(peer) ─────────────────► active
(none)            ── inbox / RegisterIncoming(peer) ─► oneToOnePending
oneToOnePending   ── Accept(id) / OneToOne(peer) ────► active
oneToOnePending   ── Decline(id) ────────────────────► oneToOneDeclined
oneToOneDeclined  ── Accept(id) / OneToOne(peer) ────► active
active            ── Delete(id) ─────────────────────► oneToOneDeleted
oneToOneDeleted   ── OneToOne(peer) ─────────────────► active
```

Inbox re-delivery onto an existing row changes nothing.

Rules:

- **Self-initiated is implicit approval.** `OneToOne(peer)` goes straight to
  active. There is no "has the peer accepted?" signal: the immutable ACL
  carries none, and inferring it from the peer's first write is left to a
  chat layer.
- **Incoming starts pending.** The inbox notifier, or `RegisterIncoming`
  for the out-of-band path, creates the row with device-local
  `oneToOnePending`, without deriving storage and without writing a synced
  status. The row carries the peer's identity (surfaced as
  `SpaceInfo.Author`); name and icon resolve from identityRepo via the
  symkey the invite carried, or from an out-of-band `displayHint`.
- **Accept is account-scoped.** `AcceptOneToOne(spaceId)`, or
  `OneToOne(peer)` again, writes synced `RemoteStatus=active` and
  materializes. On the account's other devices the synced active status
  wins over a stale device-local pending (or a bare row with no status
  yet), and the next `Get` adopts the 1-1 through the accept path. No
  per-device re-accept. A device-local delete is never adopted over.
- **Decline** (`DeclineOneToOne(spaceId)`) writes synced `oneToOneDeclined`.
  Inbox re-deliveries on any device never re-surface the request. An
  explicit `OneToOne(peer)` or `AcceptOneToOne` overrides it.

### Materialization (accept / self-initiate)

`OneToOne` builds the storage payload and passes it to `activateOneToOne`,
which both entry points share:

1. `app.GetSpace` loads the space, creating storage from the payload when
   absent.
2. The index row is upserted: `RemoteStatus = active`, `LocalStatus =
   active` (or a new row with `Type = SpaceTypeOneToOne` and the peer).
3. `ensureSpaceIndexWiring`, `newSpace`, `goSeed`.
4. The peer's name resolves onto the row in the background.

`deriveOneToOneId` computes the id without creating storage;
`RegisterIncoming` uses it for the pending row. Every step is server-free;
sync runs over whatever transport is configured.

### Delete

A 1-1 is derived and not node-owned, so `Delete` never removes it from the
nodes and never sends `coordinator.SpaceDelete`:

- `Delete` writes the synced `techspace.OneToOneDeletedStatus`
  (`oneToOneDeleted`) and offloads local state.
- The marker syncs to the account's other devices. The deletion
  reconciler's coordinator-independent scan (`offloadDeletedOneToOnes`, run
  first on every pass) offloads any such 1-1 that still has local storage;
  it works offline. The boot eager-loader skips these rows via
  `SpaceIndexRecord.IsDeleted()`.
- `mapStatus` reports `space.StatusDeleted`, and `Subscribe` emits
  `Removed`.
- The marker is non-terminal, so a later `OneToOne(peer)` re-derives storage
  and flips the row back to active.

### Surfacing to callers

`List` and `Subscribe` return every row; `SpaceInfo.Status` distinguishes
them:

- `oneToOnePending` → `space.StatusOneToOnePending`. A 1-1 row with no
  active, declined or pending signal (synced in from the registering device
  before this device set a status) also maps here.
- `oneToOneDeclined` → `space.StatusOneToOneDeclined`.
- `oneToOneDeleted` → `space.StatusDeleted`.

`Get` refuses pending and declined rows with `ErrSpaceNotAccepted` and
deleted rows with `ErrSpaceDeleted`. Callers find incoming requests by
filtering on `StatusOneToOnePending`, then call `AcceptOneToOne` or
`DeclineOneToOne`.

Members of an active 1-1 come from the normal members collection
(`docs/space.md`), which reads the two writers from the ACL. The
synthetic `sharedPk` owner is filtered out of every member view (`List`,
`Get`, `Query`, `Subscribe`) in `collectMembers`, via
`AclState.IsOneToOne()` and `OwnerPubKey()`.

## Layer 2 — discovery via coordinator inbox

The SDK reuses any-sync's inbox transport and crypto and owns only the
dispatch, cursor and dedup wrapper (`internal/inbox`), which is where the
correctness rules in "Heart bugs we fix" apply.

### What any-sync provides

`coordinator/inboxclient` and `coordinator/subscribeclient`, registered in
the anysyncx app next to the coordinator client:

- `InboxAddMessage(ctx, receiverPubKey, message)` ECIES-encrypts the body
  to the receiver's account key and signs the ciphertext.
- `InboxFetch(ctx, offset) → []InboxMessage, hasMore` returns bodies still
  encrypted; verify and decrypt are the caller's job.
- `SetMessageReceiver(cb)` subscribes to
  `NotifyEventType_InboxNewMessageEvent`; `subscribeclient` owns stream
  open and reconnect.

`App.InboxClient()` exposes the transport; nil means no inbox, and the
subsystem stays off.

### Inbox message shape

```
InboxPacket{ keyType, senderIdentity, receiverIdentity, senderSignature, payload }
InboxPayload{ payloadType = InboxPayloadOneToOneInvite, timestamp, body }
```

The 1-1 invite body is the sender's metadata symkey. Direct-add invites
(`InboxPayloadRegularInvite`) share the transport, see
`docs/direct-add-invites.md`.

### Subscription: push + poll, one worker

The push event carries no body: it only says "you have mail", so it
triggers a fetch. Push and a periodic poll (60s) both kick one serialized
worker:

```
push InboxNewMessageEvent ─┐
                           ├─► kick (buffered 1) ─► notifier worker (one goroutine)
periodic tick (fallback) ──┘                          └─ fetch → process → advance cursor
```

The worker is the only fetcher and the only cursor writer, so no lock is
needed. The poll covers a missed or disconnected stream; push provides
latency. A push forwarder is installed before `app.Start` (the inbox client
rejects a nil receiver) and delegates to the handler the notifier installs
via `App.OnInboxMessage`.

### Send

`OneToOne` (the initiate path only; accept never notifies back) stamps the
device-local `FieldOneToOneInviteState = "toSend"` and kicks the send-retry
loop. The loop posts `InboxPayloadOneToOneInvite` to the peer and clears the
marker on confirmed delivery; a transient failure keeps the marker for the
next pass (60s tick or kick), so the marker survives restarts and an
offline initiate delivers once online. Re-sends are harmless because
receive is idempotent. Without an inbox transport no marker is written:
the space is re-derivable, so sending matters only for notification.

The body is the sender's metadata symkey (the key that decrypts the sender's
identityRepo profile, see `docs/identities.md`). The receiver caches it
and resolves name and icon from identityRepo; the body carries no name or
icon.

### Key exchange inside the space

The inbox carries the symkey one way only (`OneToOne` posts it, accept
sends nothing back), and the immutable 1-1 ACL root has no per-writer
metadata. The invite alone therefore leaves the initiator unable to decrypt
the acceptor's profile, and a missed or absent inbox leaves the acceptor
unable to decrypt the initiator's. The space itself is the symmetric
channel: its read key is held by exactly the two participants.

- **Dataset `identityKeys`** on the space's derived spaceIndex object
  (`internal/types/spaceindex/identitykeys.go`, registered next to
  `bundles`): row id = the participant's account identity, one synced field
  `symKey` in the `space.MarshalSymKey` string form the ACL blob and the
  inbox body use. The handler admits a row only from a change whose signer
  is the row id, refuses a change without a creator, and refuses deletes (a
  tombstone would ban that participant's key for good).
- **Publish** (`spaceImpl.publishOneToOneKey`, from the post-load seed
  goroutine, so on `OneToOne`, `AcceptOneToOne` and every later
  `Spaces().Get`; the boot eager-loader does not run it): for a 1-1 whose
  own row is absent or differs, upsert it, one publish per space at a time.
  The key is a pure function of the account key, so every device of the
  account writes the same bytes and an equal row is left alone; a differing
  row can only mean the derivation changed. Existing 1-1s heal on the first
  `Get`.
- **Watch** (`oneToOneKeysWatcher`, wired by `ensureSpaceIndexWiring` for
  1-1 spaces only): a subscription on `(spaceIndexObjectId, identityKeys)`
  with a one-shot reconcile on start. Only the row keyed by the row's
  `OneToOnePeer` goes to the identities directory (`SetIdentityMetaKey`,
  no-op when equal). Each reconcile triggers `resolveOneToOnePeerName` in
  the background while the directory holds no profile for the peer, whoever
  cached the key: the inbox invite and the synced directory deliver the key
  with no resolve of their own, and the resolves on `RegisterIncoming` and
  accept can run before any key has arrived. The fetch itself is one-shot;
  a space whose member watcher runs (started by the first members query or
  subscribe) also refreshes every member's profile each
  `identityRepoPollInterval` (60s) with the same directory key. Cold devices
  receive the key through the synced directory, as for any contact.
- **The inbox invite still carries the key.** It is the only pre-accept
  channel: a pending row shows the initiator's name before the space is
  materialized, and the in-space row is readable only after. The row is the
  durable source; the invite is a notification with a display hint.
- **Visibility.** The rows are refused on the public write surface
  (`Modify` / `Delete` / `Upsert`, like `bundles`) and stay readable through
  `Query`, `Aggregate` and history like any dataset: a reader is one of the
  two key holders, and the identities directory already keeps the peer's key
  for that reader, so the row grants nothing new. The tech-space
  `identities` dataset differs: it holds every contact's key in one place
  and stays off the generic read surface. A wrapper serving local clients
  over HTTP is a lower trust tier and refuses the rows on its read routes,
  as `any` does.
- Regular spaces don't need this: their key rides the ACL join record.

### Receive

Per fetched message:

1. **Verify** `senderSignature` over the still-encrypted body against
   `senderIdentity`.
2. **Decrypt** the body with the account key. For a 1-1 invite that yields
   the sender's metadata symkey, cached with `SetIdentityMetaKey`. The body
   carries no identity; `packet.SenderIdentity` (verified by the
   coordinator) is the peer identity.
3. **Dispatch** (`handleInboxMessage`, by payload type; unknown types are
   skipped). A 1-1 invite calls `RegisterIncoming(senderIdentity, {})`,
   which derives the id and checks the row:
   - an existing row (active, pending, synced decline, delete) → no-op; a
     still-pending row retries the name resolution;
   - no row → a device-local `oneToOnePending` row, identity-only until the
     name resolves from identityRepo.
4. **Advance the cursor after handling, once per batch**, to the furthest
   handled message id. The cursor is the synced, account-scoped
   `InboxCursor` in the tech space (`SetInboxCursor`, monotonic-forward).
   Per batch keeps synced writes low; idempotent receive makes it
   crash-safe (a crash re-runs the batch, which dedups against the rows).
5. **Failure split.** A content failure (nil packet, bad sender, bad
   signature, decrypt failure, non-retry handler error) is permanent: log
   it and count the message as handled. A transient failure (`Fetch` error,
   or a handler `ErrRetry` for a tech-space write) halts the cursor before
   that message; the next pass retries.

An empty cursor passes through a **replay guard** first. On an established
account an empty cursor may only mean the synced cursor hasn't reached this
device, and replaying the whole inbox would resurrect resolved invites as
pending rows. The guard runs one tech-space head-sync round and requires
zero parked trees; until then the pass is deferred. The first success is
latched for the notifier's lifetime.

### Lifecycle

`Service.StartOneToOneInbox`, called from `sdk.Open` after the tech space
opens, installs the push handler, starts the send-retry loop, then runs the
notifier. It is a no-op without an inbox transport. `Close` stops both loops
before the tech space is torn down. Both loops run on
`context.Background()`-derived contexts, so the `Open` context expiring
doesn't stop them.

## Heart bugs we fix

anytype-heart's inbox (`core/inbox/inboxclient`, `core/inbox/inboxservice`)
has defects that must not be ported. Each rule below is a guarantee of this
implementation.

1. **The cursor advances only after the side effect commits.** Heart
   persists the batch-tail offset before running handlers, so a crash or
   handler error in between loses messages for good.
2. **A bad message costs nothing but itself.** Heart skips a bad message
   while the offset already sits at the batch tail, losing the good messages
   after it. Here every message up to the cursor was handled; a content
   failure is skipped individually, a transient failure halts the cursor
   before the message, and nothing wedges.
3. **Receive is idempotent.** The synced 1-1 row is the account-scoped
   "handled" marker, so `RegisterIncoming` on an existing row is a no-op and
   a re-delivered or replayed message is harmless. No processed-id ledger is
   needed. This is also what makes a synced cursor safe (rule 8). Heart
   re-runs side effects on every replay.
4. **One worker serves both triggers.** Heart's push callback and poll run
   fetch and dispatch concurrently and double-process.
5. **No self-declared identity.** The body holds only the symkey; the peer
   identity is the coordinator-verified `SenderIdentity`. Heart trusts an
   identity embedded in the body, which lets a sender impersonate a third
   party.
6. **The send marker clears only after confirmed delivery.** A crash
   re-sends, which the idempotent receiver absorbs.
7. **The handler set is fixed at construction** and the wrapper takes no
   unlocked map reads or unbounded blocking sends.
8. **The cursor is synced and account-scoped.** Heart's synced offset
   diverges only because its processing isn't idempotent. The coordinator
   inbox is per receiver, and its messages are immutable and ordered by
   ObjectID (the offset is an ObjectID hex; the server fetches `_id $gt
   offset` sorted ascending), so the furthest-processed offset is one shared
   high-water mark. Writes are monotonic-forward (lexical hex `max` in
   `SetInboxCursor`), everything below the cursor is covered by synced 1-1
   rows, and rule 3 makes a cross-device regression a harmless re-fetch. A
   fresh device seeds from the cursor instead of replaying the inbox, behind
   the replay guard described under Receive.

## Out-of-band discovery (no server)

With no coordinator, discovery is the app's job (QR, link, username lookup,
an existing space's member list). The app calls:

- `OneToOne(peerIdentity)` to reach out (active immediately), or
- `RegisterIncoming(peerIdentity, displayHint)` to record an incoming
  request the app learned of through its own channel.

The state machine, accept and decline are the same.

## API surface

On `space.Service` (`space/service.go`):

```go
// OneToOne reaches out to — or explicitly accepts / un-declines — the
// derived 1-1 space shared with otherIdentity. Materializes and activates
// it locally. Same id regardless of key order. Idempotent.
OneToOne(ctx context.Context, otherIdentity string) (Space, error)

// AcceptOneToOne approves an incoming pending 1-1 by space id. The peer
// identity is read off the row. Equivalent to OneToOne(peer).
AcceptOneToOne(ctx context.Context, spaceId string) (Space, error)

// DeclineOneToOne writes a synced sticky marker so the request is
// suppressed on all the account's devices; a later OneToOne(peer)
// overrides it.
DeclineOneToOne(ctx context.Context, spaceId string) error

// RegisterIncoming records an incoming 1-1 learned out-of-band as a
// pending row, without materializing storage. displayHint is an optional
// name/icon snapshot. No-op if a row for the derived space exists.
RegisterIncoming(ctx context.Context, peerIdentity string, displayHint AccountMetadata) error
```

`OneToOne` and `RegisterIncoming` return `ErrSelfPair` for the caller's own
identity.

## Security & abuse

- **Sender authenticity.** The notifier verifies `senderSignature`, and the
  peer identity is the coordinator-verified `SenderIdentity`; nothing in the
  body identifies the sender. In the out-of-band path the app vouches for
  the identity it passes to `RegisterIncoming`.
- **Privacy.** `InboxAddMessage` ECIES-encrypts the body to the receiver's
  account key.
- **Spam.** A pending row is inert (no storage, no sync), so an unsolicited
  request costs one tech-space row, and decline is sticky. An allow/block
  list (for example, surfacing only known contacts) would sit on top of the
  notifier without touching Layer 1.
- **Self-pairing** is rejected.

## Open questions

- **Un-decline surface.** `OneToOne(peer)` overriding a synced decline is
  the mechanism. Whether the SDK also needs an explicit `UndoDecline` or a
  list-of-declined surface waits for a client that needs it.
- **Dead-letter escape hatch.** A permanently bad message at the cursor
  head is skipped, never wedged. Open whether any scenario should instead
  hard-stop and alert.
