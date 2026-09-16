# Direct-Add Invites

An owner or admin adds accounts to a regular space by identity alone, with
no invite token and no join request. The receiver sees the space as pending
and accepts or declines it, like an incoming 1-1 (docs/one-to-one-spaces.md).

## Model

Two independent layers, as in the 1-1 design:

1. **Membership (ACL, required).** `ACL().AddAccounts([]MemberAdd)` writes
   one ACL `AccountsAdd` record for the whole batch. The added accounts are
   full members immediately: the record carries the read key encrypted to
   each identity. Approval is therefore a local materialization gate, not a
   cryptographic step: nothing is downloaded until the user accepts.

2. **Discovery (coordinator inbox, optional).** After the ACL write the SDK
   queues one `InboxPayloadRegularInvite` message per added identity. The
   body is encrypted to the receiver, signed, and its sender verified by
   the coordinator. It is versioned JSON:

   ```json
   {"v":1, "spaceId":"...", "symKey":"...", "name":"...", "spaceType":"..."}
   ```

   - `symKey`: the sender's metadata symkey, so the receiver can resolve
     the sender's identityRepo profile (same convention as 1-1).
   - `name` / `spaceType`: unauthenticated display hints for the pending
     row, replaced by the synced in-space values after accept.
   - The row's header `type` is not taken from the body. It stays empty
     while pending (the field is set-once) and the post-accept load fills
     it from the space header.

   Without the inbox transport (headless deployments) the ACL write still
   happens and no notification is sent.

## Sender-side delivery

`AddAccounts` appends each identity to `FieldInviteNotifyPending`, a
device-local array on the space row. The invite send-retry loop
(`reconcileInviteOutbox`, shared with the 1-1 `toSend` marker) sends and
clears each entry. A transient failure (offline, coordinator down) keeps
the entry for the next pass; a permanent failure (undecodable identity)
drops it with a log. Only the device that ran `AddAccounts` owes the
notification.

## Receiver-side states

Pending is **synced** (`remoteStatus`), unlike the device-local 1-1
pending: the inbox cursor is account-scoped, so only one of the receiver's
devices processes the message, and the row must carry pending to the rest.

| remoteStatus (synced)   | public Status          | transition |
|-------------------------|------------------------|------------|
| `invitePending`         | `StatusInvitePending`  | written by the inbox handler on a fresh row, or over an ended join (`joinEnded`, docs/space.md § Join lifecycle) |
| `active`                | `StatusActive`         | `AcceptInvite` (any device; converges everywhere) |
| `inviteDeclined`        | `StatusInviteDeclined` | `DeclineInvite`; sticky, non-terminal: `AcceptInvite` overrides it |

- **No clobbering.** The inbox handler leaves an existing row alone: active
  membership, a pending join, a sticky decline, a terminal delete, or a
  duplicate delivery. The one exception is an ended join. That row records
  "not a member" account-wide, the add just made the account one, and
  nothing else watches the ACL of a space the account never loaded, so the
  invite registers over it (the sender's name hint fills the empty name).
  The handler runs `SyncHeads` on the tech space before the check, so a
  duplicate delivery on a lagging device sees another device's registration
  or accept instead of writing pending over it.
- **Nothing materializes while pending.** `Service.Get` refuses rows in the
  invite-pending and invite-declined states (`ErrSpaceNotAccepted`), so no
  read path can bypass the gate.
- **`AcceptInvite`** stamps the device-local `inviteLoading` marker first,
  then flips the synced status to active, then tries one load. The order
  matters: a crash between the writes leaves the resumable marker, and the
  background load completes the synced flip. If the content isn't pullable
  yet, it returns `ErrInviteAcceptPending` and the join controller finishes
  the load in the background (`loadAcceptedInvite`, resumed at boot; no ACL
  waiter, since the account is already a member). Each attempt re-reads the
  row and stops on a synced decline or a delete, so `Delete` is the cleanup
  for an accept that can never complete (spoofed `spaceId`, membership
  revoked before accept).
- **`DeclineInvite`** writes the sticky marker only. There is no ACL write:
  the account stays a member, and the sender gets no signal.
- **Spoofed `spaceId`.** Trust is spam-level, as for the 1-1 inbox: a bogus
  id shows a pending row whose accept never completes. ACL membership is
  the real validation.

## Caveats

- Re-adding a declined member has no sender-side trigger: the ACL already
  lists them, so `AddAccounts` fails for them (`ErrDuplicateAccounts` from
  any-sync). The declined user self-serves with `AcceptInvite`.
- The accepting device reports `StatusActive` while content still pulls
  (`inviteLoading` is a device-local detail). The account's other devices
  load the space lazily on `Get`, as for any synced space.
