# One-to-One Spaces

## Vision

A 1-1 (direct) space is a space shared by **exactly two identities**, derived
deterministically from their keys — both peers compute the same space id,
the same ACL, the same read key, with **no owner/invite handshake**. It is the
substrate for direct messaging and any two-party shared state.

Requirements driving this design:

1. **Approve incoming.** Unlike anytype-heart (which auto-materializes a 1-1 the
   moment an invite lands), an incoming 1-1 must sit in a *pending* state until
   the local user explicitly accepts. Declining is sticky.
2. **No hard server dependency.** The core mechanism — derive, materialize,
   approve — must work with **zero server infrastructure** (no coordinator, no
   sync nodes). Server-backed discovery is an *optional layer*, never a
   prerequisite. (We do not ship a p2p transport here; the constraint is that
   the design must not be *architecturally* coupled to servers.)

## What any-sync already gives us

any-sync ships the full 1-1 primitive. We do **not** invent crypto or ACL
shapes — we drive existing calls.

**Symmetric derivation** (`commonspace/spaceservice.go`):
```go
DeriveOneToOneSpace(ctx, aSk crypto.PrivKey, bPk crypto.PubKey) (id string, err error)
```
The whole space is built locally and offline by
`spacepayloads.StoragePayloadForOneToOneSpace(aSk, bPk)`:

- A **shared secret** is computed by ECDH:
  `crypto.GenerateSharedKey(myPriv, otherPub, path)`. Since
  `ECDH(aSk, bPk) == ECDH(bSk, aPk)`, both peers derive the *identical* key
  material from **only the other party's public account identity**.
- Derivation paths (`util/crypto/derived.go`):
  `AnysyncOneToOneSpacePath` (owner/space key), `AnysyncReadOneToOneSpacePath`
  (read key), `AnysyncMetadataOneToOnePath` (metadata key).
- The ACL root (`BuildOneToOneRoot`) embeds `AclOneToOneInfo`:
  - `Owner = sharedPk` — a **synthetic** identity nobody holds as a member;
    exists only to satisfy any-sync's "ACL must have an owner" invariant. It is
    ignored in business logic.
  - `Writers = [aPk, bPk]` — the two real identities, **sorted** for an
    idempotent space id, both granted `AclPermissionsWriter` from the root.
- Replication key = `fnv64(sharedPubKey)` — self-contained, independent of the
  account's replication key.
- On-wire header `SpaceType = "anytype.onetoone"` (coordinator-gated allow-list,
  see `docs/03-space.md`; irrelevant in a no-coordinator deployment).

**Consequence that shapes everything below:** the ACL is **immutable** — just a
root with two writers, no invite/request/accept records. There is *nothing to
accept cryptographically*. Either party who knows the other's account identity
can derive and join unilaterally.

Therefore **"approve incoming" is a local SDK gate, not an ACL operation.** It
governs whether *this device* materializes storage and participates in syncing
the derived space — it cannot and need not gate ACL membership.

## Architecture: two layers

```
┌─────────────────────────────────────────────────────────────┐
│  Layer 2 — Discovery / notification  (OPTIONAL, pluggable)   │
│  "Bob, Alice wants a 1-1 with you."                          │
│  Shipped impl: coordinator Inbox (InboxFetch/InboxAddMessage)│
│  Fallbacks: out-of-band identity exchange; future p2p        │
└─────────────────────────────────────────────────────────────┘
                              │ surfaces "incoming" → creates Pending row
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  Layer 1 — 1-1 primitive  (REQUIRED, zero server deps)       │
│  derive → local state machine (Pending/Active/Declined)      │
│  → materialize + sync + seed   (Accept) / tombstone (Decline)│
└─────────────────────────────────────────────────────────────┘
```

This split is exactly how the two requirements reconcile: discovery *uses* the
coordinator inbox when present, but Layer 1 never depends on it. Strip Layer 2
and the primitive still works — the app just supplies the peer identity
out-of-band and calls `OneToOne` / `Accept` directly.

## Layer 1 — the 1-1 primitive

### Local state machine

State lives in the **tech-space index row** for the derived space id. The split
between device-local and synced fields is deliberate and is what makes the
multi-device behavior (groomed below) fall out correctly:

| State              | Field / scope                          | Materialized? | Crosses devices? |
|--------------------|----------------------------------------|---------------|------------------|
| `oneToOnePending`  | `LocalStatus` (**device-local**)       | No — row only | No — each device discovers via its own inbox |
| `active`           | synced space membership (normal)       | Yes           | Yes — accept propagates as a real space |
| `oneToOneDeclined` | dedicated **synced** sticky marker     | No            | Yes — decline silences all devices |
| `deleted`          | `RemoteStatus=deleted` (synced, local offload) | No   | Yes (sticky) |

Why pending is device-local but declined is synced: discovery is **per-device**
(each device runs its own inbox notifier with its own cursor), so the *pending*
prompt naturally exists independently on each device. **Accept** materializes a
real space whose membership syncs through the normal tech-space list, so it
reaches the other devices for free. **Decline** has no space to sync, so to
silence the prompt account-wide (groom decision) it writes its own synced
marker. This marker is **distinct from `RemoteStatus=deleted`** (which is
hard-sticky — the index handler refuses to move out of it): `oneToOneDeclined`
must stay overridable by a later explicit `OneToOne(peer)` (un-decline), so it
is a separate synced status value, not a reuse of `deleted`.

Transitions:

```
                 OneToOne(peer)            Accept(id)
   (none) ───────────────────────► active ◄──────────── oneToOnePending
      │  self-initiated, implicit self-approve   ▲          ▲
      │            OneToOne(peer) un-decline ─────┘          │ inbox notifier /
      │                                                      │ RegisterIncoming(peer)
      └──────────────────────────────────────────────────────
                                                             │
   oneToOnePending ─ Decline(id) ─► oneToOneDeclined (synced) ┘ (re-delivery: ignored, sticky)
   active ──── Delete(id) ──► deleted  (local offload, synced tombstone)
```

Rules:

- **Self-initiated is implicit approval.** `OneToOne(peer)` (I reached out, or I
  pasted Alice's invite link) goes straight to `active`. The row I create is
  *my* decision; there is no one to ask. v1 surfaces no "has the peer accepted?"
  signal — the immutable ACL carries none; acceptance is inferable only from the
  peer's first write, deferred to a later chat layer.
- **Incoming starts Pending (device-local).** The inbox notifier (Layer 2) — or
  an explicit `RegisterIncoming(peerIdentity, displayHint)` for the out-of-band
  path — creates the row as `oneToOnePending` **without** deriving storage and
  **without** writing any synced field. The pending row carries the peer's
  identity and a display snapshot (name/icon) from the notification payload, so
  the UI renders "Alice wants to chat" without syncing anything.
- **Accept** (`AcceptOneToOne(spaceId)`, or equivalently `OneToOne(peer)` again)
  flips Pending→active and runs materialization. Idempotent. The space's
  membership syncs through the normal tech-space list, so the account's other
  devices materialize it on their own.
- **Decline** (`DeclineOneToOne(spaceId)`) writes the **synced** `oneToOneDeclined`
  marker, sticky against the automatic discovery path on **every** device:
  later inbox re-deliveries / peer writes never re-surface it as incoming. A
  notifier that finds a synced `oneToOneDeclined` for a derived id suppresses
  its local pending row. Decline is automatic-path sticky only — a deliberate
  `OneToOne(peer)` later **overrides** the marker to `active` (un-decline).

### Materialization (Accept / self-initiate)

Reuses the existing path in `OneToOne` (`service.go:720`), refactored so the
**activation** half is callable from both the initiate and the accept entry
points:

1. `DeriveOneToOneSpace(myKey, peerPub)` → `spaceId` (pure, offline).
2. Upsert tech-space row: `Type = SpaceTypeOneToOne`, `LocalStatus = active`.
3. `app.GetSpace(ctx, spaceId)` — materializes storage via
   `StoragePayloadForOneToOneSpace` if absent; loads if present.
4. `ensureSpaceIndexWiring` + `newSpace` + `goSeed`.

All four steps are server-free. Sync (over nodes today, p2p later) is whatever
transport is configured; the primitive doesn't care.

### Decline / Delete semantics

- **Decline** → synced `oneToOneDeclined` marker, no storage ever created.
  Account-wide sticky against the automatic discovery path; overridable only by
  an explicit `OneToOne(peer)`.
- **Delete** of an *active* 1-1 → local-only offload (per `docs/03-space.md`:
  1-1 spaces are "not removable from the network, derived, always
  re-creatable"). Sets `LocalStatus = deleted`, offloads local state, but does
  **not** send `coordinator.SpaceDelete`. A subsequent `OneToOne(peer)`
  re-derives and re-materializes from scratch.

### Surfacing to callers

The space list / `SpaceInfo.Status` gains mapped values (extend `mapStatus`,
`service.go:948`):

- `oneToOnePending` → a **dedicated** `space.StatusOneToOnePending` (groom
  decision: not a generic "incoming request" status — clearer for clients
  filtering "1-1 chats to approve"; a future regular-invite-via-inbox would add
  its own status).
- `oneToOneDeclined` → filtered out of the default list (like `deleted`);
  surfaced only under an explicit filter.

Callers discover incoming requests by `Subscribe`/`List` filtering on
`Status == OneToOnePending`, then call `AcceptOneToOne` / `DeclineOneToOne`.
Members of an active 1-1 are read through the normal members collection
(`docs/03-space.md`), which reads the two Writers from the immutable ACL.

## Layer 2 — discovery via coordinator inbox

The shipped notifier. We **reuse any-sync's transport and crypto** and build
only the dispatch/cursor/dedup wrapper on top — the wrapper is exactly where
every heart bug lives (see "Heart bugs we fix" below), so it is the part worth
owning.

### What any-sync gives us (verified in 0.12.11)

`coordinator/inboxclient` (`inboxclient.New()`, registered as a component):

- `InboxAddMessage(ctx, receiverPubKey, message)` — **ECIES-encrypts the body to
  the receiver's account pubkey and signs the ciphertext** (`client.go:122`).
  So the body is encrypted on the wire for free; we do *not* hand-roll
  encryption. (This already improves on heart's `create.go:65` TODO — though
  note the *profile bytes inside* the body still carry whatever the sender put
  there; see authenticity below.)
- `InboxFetch(ctx, offset) → []InboxMessage, hasMore` — returns bodies **still
  encrypted**; decrypt + verify is the caller's job (`client.go:104`).
- `SetMessageReceiver(cb)` — registers a push callback; on `Run` it subscribes
  to `coordinatorproto.NotifyEventType_InboxNewMessageEvent` via
  `coordinator/subscribeclient`, whose `streamWatcher` owns stream open +
  reconnect (linear backoff to 60s) and is torn down on `Close`.

We register `subscribeclient` + `inboxclient` into the anysyncx app alongside
the existing coordinator client.

### Inbox message shape (any-sync proto, already defined)

```
InboxPacket{ keyType, senderIdentity, receiverIdentity, senderSignature, payload }
InboxPayload{ payloadType = InboxPayloadOneToOneInvite, timestamp, body }
```

### Subscription layer: push + poll, one funnel

The push event is **body-less** — `InboxNewMessageEvent` only says "you have
mail", so it is a *wake-up* that triggers a fetch, never a delivery. Heart runs
the push callback and an independent 30s poll, and they both call the same
fetch+dispatch with no mutual exclusion — the source of its double-process race
(see fix #1, #4). Our shape instead funnels both triggers into a **single
serialized worker**:

```
push InboxNewMessageEvent ─┐
                           ├─► kick(buffered 1) ─► notifier worker (one goroutine)
periodic tick (fallback) ──┘                         └─ fetch → process → advance cursor
```

- One worker goroutine, own cancellable context (the `joinController` /
  `deletionController` shape: ticker + buffered kick, bound to `Close`).
- Push and poll both just `kick`; the worker is the only thing that fetches and
  the only thing that touches the cursor — no lock needed because there is one
  writer. The poll is a safety net (missed/disconnected stream); push is for
  latency.

### Send (outbound, optional)

When `OneToOne(peer)` runs *and a coordinator is configured*, after local
activation the SDK posts one `InboxAddMessage(peerPubKey, …)`:

- `payloadType = InboxPayloadOneToOneInvite`
- `body` = the sender's identity profile snapshot (name/icon). any-sync encrypts
  it to `peerPubKey` on send.

Send is **best-effort** and gated by an idempotent local send-status (fix #6):
flip the row to a `oneToOneInviteSent` marker only *after* a confirmed send, and
treat re-sends as harmless because the receiver path is fully idempotent (fix
#3). If no coordinator is configured, send is skipped; discovery falls to the
out-of-band path. Sending is never required for correctness — the space is
re-derivable — only for notification.

### Receive (the notifier worker)

Per fetched `OneToOneInvite` message, **process first, advance cursor after**
(fix #1):

1. **Verify** `senderSignature` over the (still-encrypted) body against
   `senderIdentity` (`senderPub.Verify(body, sig)`).
2. **Decrypt** body with my account sign key → display-hint profile
   (name/icon). The body carries **no identity** — `packet.SenderIdentity`
   (coordinator-verified) is the authoritative peer identity, so there is
   nothing self-declared to spoof (fix #5; nothing to "bind").
3. **Dispatch** to the handler: `RegisterIncoming(senderIdentity, hint)` →
   `deriveOneToOneId` then reconcile the (possibly synced) row:
   - synced `oneToOneDeclined` present → **ignore** (sticky, account-wide —
     could have been declined on another device);
   - no row → add device-local `oneToOnePending` row + profile snapshot;
   - `active` / `oneToOnePending` → idempotent (no-op / refresh). This
     idempotence is why no separate processed-id ledger is needed (fix #3).
4. **Advance the cursor per-message, AFTER the handler returns** (fix #1) — the
   device-local cursor moves to this message's id only once it's handled, so a
   crash mid-pass reprocesses the in-flight message (harmless: idempotent).
5. **Failure split** (fix #2): a *content* failure (nil packet / bad sender /
   bad signature / decrypt failure) is non-transient → log + advance past **that
   message only** (skip; does NOT wedge). A *transient* failure (offline
   `Fetch`, or the handler returning `ErrRetry` for a transient tech-space
   write) → halt the cursor and retry the next pass.

The worker only runs when a coordinator is present (`app.Coordinator() != nil`).
Absent one, Layer 2 is simply off and the primitive stands alone.

## Heart bugs we fix (do not port)

A close read of heart's inbox (`core/inbox/inboxclient/inboxclient.go`,
`core/inbox/inboxservice/onetoone.go`) surfaced concrete defects. Each maps to a
design rule above:

1. **Cursor advances before processing → permanent message loss.** Heart
   persists the batch-tail offset in `fetchMessages`, *then* runs handlers in
   `handleMessages`; a crash or a handler error between them loses the message
   forever (the coordinator won't re-deliver). **Fix:** advance the cursor only
   after the message's side effect commits, in the same transaction.
2. **Batch-tail advance loses good messages.** Heart `continue`s over a bad
   message while the offset is already the *batch tail*, so the good messages
   between a bad one and the tail are skipped along with it (never re-fetched).
   **Fix:** advance the cursor **per-message**, and split by failure type — a
   *content* failure (nil packet / bad sender / bad signature / decrypt) is
   non-transient, so log it and skip past **that one message only** (advance);
   a *transient* failure (offline fetch, or a handler `ErrRetry`) halts the
   cursor and retries the next pass. This deliberately does NOT wedge on a
   permanently-bad message: it is skipped, not looped forever. (Heart's other
   sin — advancing before processing — is fix #1.)
3. **Replay re-runs side effects.** Heart relies on deterministic space-id
   derivation + the space ocache to avoid duplicate spaces, but still re-runs
   side effects (`SpaceInitChat`, `AddIdentityProfile`, `SpaceViewSetData`) on
   every replay. **Fix:** the receive handler is **fully idempotent**
   (`RegisterIncoming` is a no-op on an existing row and honors a sticky
   decline), so a re-delivered message is harmless. Combined with the
   per-message cursor (fix #1) this needs no separate processed-id ledger — the
   only replay is the single in-flight message after a crash, which the
   idempotent handler absorbs.
4. **Two triggers, no mutual exclusion.** Push callback and 30s poll both call
   the dispatch with the offset read released between fetch and process →
   double-process. **Fix:** single serialized worker; both triggers only kick.
5. **Self-declared identity in the body.** Heart embeds the sender's identity
   inside the (decrypted) profile body and trusts it, so a sender could
   impersonate a third party in the surfaced "incoming from X". **Fix:** carry
   **no identity in the body** — it is a display-hint-only profile; the peer
   identity is solely the coordinator-verified `packet.SenderIdentity`. Nothing
   to spoof, nothing to bind.
6. **Send→status-flip not atomic.** Heart sends then writes `Sent` as a separate
   CRDT op; a crash between re-sends the invite. **Fix:** idempotent receiver +
   send-marker written only post-confirmation; re-send is harmless.
7. **Unlocked receivers-map read** and a `context.TODO()` in the subscribe
   stream mailbox `Add` (unbounded block) — both in any-sync/heart shared code;
   we don't reintroduce them in our wrapper, and we freeze our handler set at
   construction.
8. **Offset stored in a *synced* CRDT account object.** Heart keeps the inbox
   offset in a synced account object. The cursor is per-device *processing
   position*, so a single shared value can't represent two devices at different
   positions: CRDT last-writer-wins either jumps the lagging device past
   messages it never processed (loss) or rewinds the leading device (re-delivery
   ping-pong). **Fix:** the inbox cursor is **device-local** — a plain any-store
   collection (`inbox_cursor`) in sdk.db that never enters the DAG and never
   syncs. (This is the *cursor*; the *decline* marker is deliberately synced
   because it is an account-wide user decision, not per-device state — a
   separate concern, see groom decisions.)

### Out-of-band / p2p fallback (no server)

With no coordinator, discovery is the app's job (exchange identities via QR,
link, username lookup, an existing space's member list, …). The app then calls:

- `OneToOne(peerIdentity)` — to reach out (active immediately), or
- `RegisterIncoming(peerIdentity, displayHint)` — to drop an incoming request
  into `oneToOnePending` for the user to approve, when the app learned of the
  intent through its own channel.

Same Layer-1 state machine, same Accept/Decline. This is the path that
satisfies "must work with no server infra."

## API surface

Additions to `space.Service` (`space/service.go`); `OneToOne` already exists and
keeps its meaning (self-initiate / explicit accept):

```go
// OneToOne reaches out to / accepts a 1-1 with otherIdentity. Derives the
// shared space and activates it locally (implicit self-approval). Idempotent;
// clears any pending/declined state for that peer. (existing — unchanged shape)
OneToOne(ctx, otherIdentity string) (Space, error)

// AcceptOneToOne approves an incoming pending 1-1 by space id: materializes
// and syncs it. Equivalent to OneToOne(peer) but keyed by the id surfaced in
// the space list, so the caller needn't re-derive the peer identity.
AcceptOneToOne(ctx, spaceId string) (Space, error)

// DeclineOneToOne rejects an incoming pending 1-1. Writes a synced sticky
// marker so the request is suppressed on all the account's devices; never
// auto-materialized or re-surfaced from the discovery layer again (a
// deliberate OneToOne(peer) still overrides it).
DeclineOneToOne(ctx, spaceId string) error

// RegisterIncoming records an incoming 1-1 request learned out-of-band (no
// coordinator) as a pending row for the user to approve. displayHint is an
// optional name/icon snapshot for the UI. No-op if the row already exists.
RegisterIncoming(ctx, peerIdentity string, displayHint AccountMetadata) error
```

`SpaceInfo.Status` gains a pending-1-1 value; `List`/`Subscribe` are the
discovery surface for the UI.

## What changes in existing code

- `internal/spaceimpl/service.go:720 OneToOne` — split into `deriveOneToOneId` +
  `activateOneToOne` so Accept and Initiate share the activation half. Stop
  unconditionally writing `active`; Initiate writes `active`, the notifier
  writes `oneToOnePending`.
- `internal/spaceimpl/service.go:948 mapStatus` — map `oneToOnePending` /
  `oneToOneDeclined`.
- `internal/techspace/spaceindex.go:113` — add `oneToOnePending` (device-local
  status value, like `joiningLocalStatus`) and `oneToOneDeclined` (a **synced**
  status value, distinct from the hard-sticky `deleted`, kept overridable).
- `space/service.go` + `space/types.go` — new methods + `SpaceInfo.Status` value.
- New `internal/inbox/` — a notifier built on any-sync's `inboxclient` +
  `subscribeclient` (registered in `internal/anysyncx/app.go`): single
  serialized worker, push+poll funnel, verify/decrypt, per-message cursor
  advance, idempotent dispatch (no processed-id ledger needed). Started from
  `sdk.Open` (guarded by coordinator presence), torn down in `Close`.
- New `internal/inbox` device-local cursor (a plain any-store collection in
  sdk.db, never synced).

The existing `joinController` (ACL-waiter based) is **not** reused — derived 1-1
has an immutable ACL with no acceptance record for a waiter to observe. The
notifier is the 1-1 analogue and is structurally similar but watches the inbox,
not an ACL head.

## Security & abuse

- **Sender authenticity:** the inbox body is signed; the notifier verifies
  `senderSignature` *and* binds the decrypted profile identity to
  `SenderIdentity` (fix #5). In the no-coordinator path the app vouches for the
  identity it passes to `RegisterIncoming`.
- **Profile privacy:** the body is ECIES-encrypted to the receiver's account key
  by any-sync's `InboxAddMessage` — handled, not an open item.
- **Spam / unsolicited requests:** Pending rows are inert (no storage, no sync)
  until accepted, so an unsolicited request costs only a tech-space row. Decline
  is sticky. A future allow/block list (e.g. only surface incoming from known
  contacts) layers on top of the notifier without touching Layer 1.
- **Self-pairing guard:** reject `OneToOne(myOwnIdentity)`.

## Groom decisions (2026-06-19)

1. **Pending status value → dedicated `space.StatusOneToOnePending`.** Not a
   generic "incoming request" status; clearer for client filtering. A future
   regular-invite-via-inbox gets its own status.
2. **Cross-device decline → synced.** `DeclineOneToOne` writes a synced sticky
   marker keyed by the derived id; every device's notifier honors it and
   suppresses its local pending row. Distinct from `deleted` so an explicit
   later `OneToOne(peer)` can un-decline. (Pending stays device-local; accept
   propagates via normal space-membership sync — see the state-machine table.)
3. **Initiator delivery state → none in v1.** Initiator's side is just `active`;
   no "has the peer accepted?" signal (the immutable ACL carries none).
   Read-receipt / presence is a later chat-layer feature.
4. **Send-failure handling → background retry loop.** A pending-send marker +
   retry (heart's `ToSend` shape, our `deletionController` structure) re-sends
   until the coordinator confirms; idempotent receiver makes re-sends harmless.
   Phase 2.
5. **Dead-letter policy → split by failure type.** Infra/transient failure
   (offline `Fetch`, or a handler `ErrRetry` for a tech-space write) → retry, do
   **not** advance the cursor. Content failure (nil packet / bad sender / bad
   signature / decrypt) is non-transient → log loudly and advance the cursor
   past **that one message** so the queue can't wedge (no infinite loop). No
   app-facing surface in v1.
6. **Inbox cursor → device-local plain any-store collection** (`inbox_cursor`
   in sdk.db, never synced — explicitly NOT account-values, which is synced).
   Each device fetches and tracks its own position independently; the idempotent
   receive path makes that safe. (Fixes heart's synced-CRDT-offset divergence,
   fix #8.)
7. **Profile freshness.** Pending row caches name/icon from the invite payload;
   always (re)resolve via the existing identityRepo background fetch
   (`members.go`) once active. Out-of-band `RegisterIncoming` with no snapshot
   shows identity-only until identityRepo resolves.

## Open questions

- **Un-decline UX surface.** `OneToOne(peer)` overriding a synced
  `oneToOneDeclined` is the mechanism; whether the SDK also exposes an explicit
  `UndoDecline`/list-of-declined surface is deferred until a client needs it.
- **Dead-letter escape hatch.** If a permanently-bad message ever sits at the
  cursor head before any good message, the split policy (5) advances past it;
  confirm there's no scenario where we'd rather hard-stop and alert instead of
  dropping. Revisit if it bites in Phase 2 testing.

## Phasing

1. **Primitive + approval gate (no server):** state machine, `OneToOne` split,
   `AcceptOneToOne` / `DeclineOneToOne` / `RegisterIncoming`, status mapping,
   `SpaceInfo` surface, e2e test driving two in-process accounts via
   out-of-band identity exchange. Satisfies requirement 1 and the
   "no server infra" half of requirement 2 entirely.
2. **Inbox notifier (coordinator discovery):** register any-sync
   `inboxclient`/`subscribeclient`; build the single-worker notifier
   (push+poll funnel, verify/decrypt, per-message cursor advance, idempotent
   dispatch); send-on-initiate; wiring + lifecycle. Encryption is inherited from
   any-sync, not built here. e2e test against the local coordinator, including
   the bug-regression cases from "Heart bugs we fix" (crash-between-fetch-and-
   process, double-trigger replay, impersonated profile). Layered on top;
   Phase 1 stays valid if Phase 2 is absent.

## Phase 1 — implementation status (landed)

Serverless primitive + approval gate, shipped and tested:

- **States.** Pending → device-local `oneToOnePending` (`FieldLocalStatus`);
  declined → synced non-terminal `oneToOneDeclined` (`FieldRemoteStatus`, so
  `OneToOne` can un-decline); active → synced via the normal space row. Public
  `space.StatusOneToOnePending` / `StatusOneToOneDeclined`.
- **Peer identity.** A new synced `FieldOneToOnePeer` on the index row (declared
  in `SpaceIndexSchema`) carries the other identity — the one thing a spaceId
  doesn't encode invertibly, needed to materialize storage on accept and on any
  of the account's devices.
- **API.** `OneToOne` split into `deriveOneToOneId` (pure id, no storage — used
  by `RegisterIncoming`) + `activateOneToOne` (shared by initiate/accept).
  `AcceptOneToOne(spaceId)`, `DeclineOneToOne(spaceId)`,
  `RegisterIncoming(peerIdentity, displayHint)` on `space.Service`. Self-pairing
  guarded.
- **mapStatus** now takes `Type`: a bare synced 1-1 row (no active/declined/
  pending signal, e.g. synced from the registering device) surfaces as pending
  rather than `Unknown`.
- **Tests.** `e2e/onetoone_test.go`: `DeclineSticky` (fast, serverless —
  pending→decline→sticky-no-reprompt→un-decline) and `ApproveIncoming` (two
  accounts, out-of-band identity, symmetric-derivation + accept + content
  convergence).

Deferred to follow-up: cross-device active materialization of a 1-1 initiated
elsewhere. (1-1 `Delete` semantics landed — see "1-1 deletion" below.)

### 1-1 deletion (landed)

A 1-1 is derived and not node-owned, so deleting one must **not** remove it from
the nodes — only offload it, and propagate that to the account's other devices:

- **`techspace.OneToOneDeletedStatus`** ("oneToOneDeleted") is a **synced**
  `remoteStatus` value that is **non-terminal** (distinct from the regular,
  terminal `StatusDeleted`). `Delete` branches on `Type == SpaceTypeOneToOne`:
  it writes this marker and offloads locally, and **never** kicks the deletion
  reconciler's coordinator path (no `SpaceDelete` RPC — the space stays on the
  nodes).
- **Propagation + offload on other devices.** The marker rides tech-space sync
  to the account's other devices. A coordinator-independent scan in the deletion
  reconciler (`offloadDeletedOneToOnes`, run first each pass) offloads any
  oneToOneDeleted 1-1 that still has local storage — works offline, and on the
  boot eager-loader via `SpaceIndexRecord.IsDeleted()`.
- **Surfacing.** `mapStatus` and the `Subscribe` translator map oneToOneDeleted
  to `space.StatusDeleted` / a `Removed` event — clients see it as deleted.
- **Re-creatable.** Because the marker is non-terminal, a later `OneToOne(peer)`
  re-derives storage and flips the row back to active (the handler only blocks
  moves out of the terminal regular `deleted`). This is the property that
  reusing `StatusDeleted` would have broken.
- **Tests.** `e2e/onetoone_test.go`: `DeleteAndRecreate` (serverless: delete →
  Deleted → re-create → active) and `DeleteSyncsToOtherDevice` (two devices, one
  account: A deletes, B converges on Deleted and offloads, no node removal —
  passed against staging).

## Phase 2 — implementation status (landed)

Coordinator-inbox notifier (Layer-2 discovery), shipped and tested end-to-end
against the live staging coordinator:

- **Transport reused from any-sync.** `coordinator/inboxclient` +
  `subscribeclient` are registered in the anysyncx app. `InboxAddMessage`
  ECIES-encrypts the body to the receiver's account key and signs it;
  `InboxFetch` returns still-encrypted bodies; `subscribeclient` provides the
  `InboxNewMessageEvent` push stream. We build only the dispatch wrapper.
- **`App` wiring.** A push forwarder is installed *before* `app.Start`
  (`inboxClient.Run` rejects a nil receiver), delegating to an atomic handler
  the notifier installs via `App.OnInboxMessage`. `App.InboxClient()` exposes the
  transport; nil-return is treated as "inbox unavailable" (degrades to
  out-of-band).
- **`internal/inbox` notifier** (`notifier.go`). Single serialized worker;
  coordinator push and a poll fallback both just `Notify()` (buffered-1 kick) —
  one writer, no lock. Per message: verify signature over the ciphertext against
  the coordinator-supplied `SenderIdentity`, decrypt with our account key,
  dispatch. **Cursor advances per-message AFTER the handler commits** (device-
  local plain any-store collection `inbox_cursor` in sdk.db, never synced). The
  dead-letter split is realized: a `Fetch` error or a handler `ErrRetry` halts
  the cursor (transient → retry next pass); a content failure (nil packet / bad
  sender / bad signature / decrypt failure) is logged and skipped past (non-
  transient → no wedge). This is the concrete fix for heart bugs #1, #2, #4, #6,
  #8 from "Heart bugs we fix".
- **Sender↔body trust (fix #5).** The body carries only a display-hint profile;
  the authoritative peer identity is the coordinator-verified `SenderIdentity`,
  never anything self-declared in the body — so there is nothing to spoof.
- **Receive → pending.** `spaceimpl.handleInboxMessage` decodes the hint and
  calls `RegisterIncoming(senderIdentity, hint)` → device-local pending row,
  honoring a synced `oneToOneDeclined`. Idempotent, so re-delivery is harmless.
- **Send-on-initiate + retry (decision 4 / fix #6).** `OneToOne` (initiate path
  only — `Accept` never notifies back) stamps a device-local
  `FieldOneToOneInviteState = "toSend"` and kicks a send-retry loop
  (`deletionController` shape) that posts the `InboxPayloadOneToOneInvite` and
  clears the marker on confirmed delivery. Durable across restart; offline at
  initiate just retries.
- **Lifecycle.** `Service.StartOneToOneInbox` (called from `sdk.Open` after the
  tech space opens) wires the push handler, runs the notifier, and starts the
  retry loop; `Close` drains both before tearing down the tech space. Both loops
  own `context.Background()`-derived contexts, so the `Open` context expiring
  doesn't kill them.
- **Tests.** `internal/inbox/notifier_test.go` (5 unit tests, no network):
  verify/decrypt/advance, bad-signature skip-and-advance, `ErrRetry` halt +
  reprocess, fetch-error no-advance, Run/Notify/Close lifecycle.
  `e2e/onetoone_inbox_test.go`: Alice `OneToOne(bob)` → Bob's notifier surfaces
  the pending row with **no** out-of-band `RegisterIncoming`, display hint
  intact, then accepts. Passed against staging (the coordinator implements the
  inbox RPCs); skips gracefully if a network lacks inbox support.

Open question 2 (dead-letter hard-stop escape hatch) remains as designed: a
permanently-bad message at the cursor head is skipped past, never wedges; revisit
only if a scenario wants a hard-stop-and-alert instead.
