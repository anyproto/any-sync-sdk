# Direct-Add Invites (SYN-46)

Add users to a regular space by account identity alone — no invite token,
no join-request round-trip. The receiver sees the space as pending in
their list and accepts or declines, like an incoming 1-1 (docs/13).

## Model

Two independent layers, mirroring the 1-1 design:

1. **Membership (ACL, required).** The owner/admin calls
   `ACL().AddAccounts([]MemberAdd)` — one ACL `AccountsAdd` record for
   the whole batch. The added accounts are full members immediately:
   the record carries the read key encrypted to each identity. There is
   nothing for the receiver to accept cryptographically — **approval is
   a local materialization gate**: nothing is downloaded until the user
   says yes. No AnyoneCanJoin, no invite keys.

2. **Discovery (coordinator inbox, optional).** After the ACL write,
   the SDK queues one `InboxPayloadRegularInvite` message per added
   identity. The body (encrypted to the receiver, signed, sender
   verified by the coordinator) is versioned JSON:

   ```json
   {"v":1, "spaceId":"...", "symKey":"...", "name":"...", "spaceType":"..."}
   ```

   - `symKey` — the sender's metadata symkey, so the receiver resolves
     the sender's identityRepo profile (same convention as 1-1).
   - `name` / `spaceType` — unauthenticated display hints for the
     pending row, replaced by the synced in-space values after accept.
   - The header `type` of the pending row is hardcoded
     `SpaceTypeRegular` on the receiver (the field is pinned for life;
     a sender-supplied value must not reach it).

   Without the inbox transport (headless deployments) the ACL write
   still happens; no notification is sent.

## Sender-side delivery (durable outbox)

`AddAccounts` → `FieldInviteNotifyPending` (device-local array of
identities on the space row) → the shared invite send-retry loop
(`reconcileInviteOutbox`, same loop as the 1-1 `toSend` marker) sends
and clears per identity. Transient failures (offline, coordinator down)
keep the entry for the next pass — crash- and offline-safe; permanent
failures (undecodable identity) drop the entry with a loud log. The
marker is device-local: only the device that ran AddAccounts owes the
notification.

## Receiver-side states

The pending state is **synced** (`remoteStatus`), unlike the 1-1
device-local pending: the account-scoped inbox cursor means only one of
the receiver's devices processes the message, so the row itself must
carry pending to the other devices.

| remoteStatus (synced)   | public Status         | transition |
|-------------------------|-----------------------|------------|
| `invitePending`         | `StatusInvitePending` | written by the inbox handler on a fresh row |
| `active`                | `StatusActive`        | `AcceptInvite` (any device; converges everywhere) |
| `inviteDeclined`        | `StatusInviteDeclined`| `DeclineInvite`; sticky, NON-terminal — `AcceptInvite` overrides |

- The inbox handler **no-clobbers**: an existing row (active
  membership, sticky decline, terminal delete, duplicate delivery)
  means no-op. It pulls the tech space current (`SyncHeads`) before the
  check so a duplicate delivery on a lagging device sees another
  device's registration/accept instead of re-writing pending over it.
  No storage is materialized while pending — `Service.Get` refuses to
  load rows in the invite-pending/declined states, so no read path
  (List handle probes, direct Get) can defeat the gate.
- `AcceptInvite` stamps the device-local `inviteLoading` marker FIRST,
  then flips the synced status to active, then tries one bounded load.
  The order is load-bearing: a crash between the writes leaves the
  resumable marker, and the background load finishes the interrupted
  synced flip — never a durable accept nobody completes. If content
  isn't pullable it returns `ErrInviteAcceptPending` and the join
  controller finishes in the background (`loadAcceptedInvite`, resumed
  at boot; no ACL waiter — the account is already a member). The loop
  re-reads the row each attempt and stops on a synced decline or a
  delete, so `Delete` is the cleanup path for an accept that can never
  complete (spoofed spaceId, membership revoked before accept).
- `DeclineInvite` writes the sticky marker only. **No ACL write** — the
  account stays a member on paper (self-remove is a follow-up); the
  sender gets no signal (v1, same as 1-1).
- A spoofed `spaceId` (spam-level trust, same as the 1-1 inbox) shows a
  pending row whose accept never completes — membership is the real
  validation.

## Caveats

- Re-adding a declined member has no sender-side trigger: the ACL
  already lists them, so `AddAccounts` for them again is a no-op /
  error. The declined user self-serves via `AcceptInvite`.
- The accepting device reports `StatusActive` while content still
  pulls (`inviteLoading` is a device detail); the account's other
  devices lazy-load on `Get`, as with any synced space.
