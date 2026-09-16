# Space

A user creates a space or joins someone else's through an invite. One
space = one ACL + many objects, files, and optionally key-value data.

## any-sync background

### Space interface

```go
type Space interface {
    Id() string
    Acl() syncacl.SyncAcl
    Storage() spacestorage.SpaceStorage
    TreeBuilder() objecttreebuilder.TreeBuilder
    AclClient() aclclient.AclSpaceClient
    KeyValue() kvinterfaces.KeyValueService
    SyncStatus() syncstatus.StatusUpdater
    // + sync handlers, lifecycle
}
```

### Creation

`SpaceService.CreateSpace(SpaceCreatePayload)` → space id. Steps:
signed space header (content-addressed id) → ACL root → settings tree →
storage.

```go
type SpaceCreatePayload struct {
    SigningKey      crypto.PrivKey
    SpaceType       string
    ReplicationKey  uint64          // selects the nodes that store the space
    SpacePayload    []byte
    MasterKey       crypto.PrivKey
    ReadKey         crypto.SymKey   // first read key
    MetadataKey     crypto.PrivKey
    Metadata        []byte          // the owner's metadata symkey, not name/icon (identities.md)
    Options         *AclSpaceOptions
}
```

### Joining

- **Request to join** (approval): invite key → `RequestJoin` → owner
  `AcceptRequest` / `DeclineRequest`.
- **Anyone can join**: invite key → `InviteJoin` → immediate access with
  the invite's permissions.

```go
type AclJoiningClient interface {
    RequestJoin(ctx, spaceId string, payload RequestJoinPayload) (aclHeadId string, err error)
    InviteJoin(ctx, spaceId string, payload InviteJoinPayload) (aclHeadId string, err error)
    CancelJoin(ctx, spaceId string) error
    RequestSelfRemove(ctx, spaceId string, aclList AclList) error
}
```

### ACL

Permissions: `None`, `Reader`, `Guest` (read-only, can't request
removal), `Writer`, `Admin` (write + manage accounts), `Owner`.

Record types: `AclRoot`, `AclAccountInvite`, `AclAccountRequestJoin`,
`AclAccountInviteJoin`, `AclAccountRequestAccept`,
`AclAccountRequestDecline`, `AclPermissionChange`, `AclAccountRemove`,
`AclOwnershipChange`.

```go
type AclSpaceClient interface {
    ReplaceInvite(ctx, permissions) error
    AcceptRequest(ctx, identity) error
    DeclineRequest(ctx, identity) error
    RemoveAccounts(ctx, identities) error
    ChangePermissions(ctx, identity, perms) error
    OwnershipChange(ctx, newOwner) error
    // ...
}
```

### Derivation

- `DeriveSpace()` — deterministic from keys (used for the tech space).
- `DeriveOneToOneSpace()` — shared space between two identities, same id
  regardless of key order.

### Source files

- `any-sync/commonspace/space.go` — `Space`
- `any-sync/commonspace/spaceservice.go` — `SpaceService`
- `any-sync/commonspace/spacepayloads/payloads.go` — create/derive payloads
- `any-sync/commonspace/acl/aclclient/aclspaceclient.go` — owner ACL operations
- `any-sync/commonspace/acl/aclclient/acjoiningclient.go` — join operations
- `any-sync/commonspace/object/acl/list/list.go` — `AclList`
- `any-sync/commonspace/object/acl/list/models.go` — permissions model
- `any-sync/commonspace/object/acl/aclrecordproto/` — proto definitions

## Key Decisions

### ACL

- **Full feature set.** `Space.ACL()` exposes every any-sync ACL
  operation with all six permission levels: invites (`CreateInvite`,
  `RevokeInvite`, `RevokeAllInvites`), requests (`AcceptRequest`,
  `DeclineRequest`, `CancelJoinRequest`), membership (`AddAccounts`,
  `RemoveAccounts`, `ChangePermissions`, `OwnershipChange`,
  `RequestSelfRemove`, `StopSharing`) and guest access
  (`CreateGuestKey`, `RevokeGuestKey`).
- **Invite format.** SDK-defined: one base58 token carrying the space
  id, the invite private key minted by any-sync's `BuildInvite`, and the
  kind (member request-to-join, or guest). Not compatible with
  anytype-heart's invite blob.
- **Metadata and member names.** Symkey only (`identities.md`): the
  ACL `RequestMetadata` carries each account's metadata symkey, and
  member names resolve from the encrypted identityRepo profile with that
  key.
- **identityRepo integration** is internal: background fetch and
  decrypt, written through to the account-global identities directory.
  Public surface: `account.UpdateMetadata` (own profile) and
  `sdk.Identities()`.
- **Members as a collection.** `Space.Members()` offers `List`, `Get`,
  `Me`, `JoinRequests`, `Invites`, `Subscribe`, and `Query()` over a
  materialized members system collection with the same query rules as
  any other data. Pending join requests appear in the same view with
  status `joining`.

### Join lifecycle

A request to join is account-wide state carried by the tech-space row's
synced `remoteStatus`, so every device of the joiner's account classifies
the row the same way and a verdict seen on any device converges the rest.
Nothing is materialized until membership: `Service.Get` refuses a joining
row (`ErrSpaceNotAccepted`), the boot loader and the index follower skip
it, and an unloaded space's ACL is watched only through the joining
client (no storage, no pull).

| remoteStatus (synced) | public Status   | written by |
|-----------------------|-----------------|------------|
| `joining`             | `StatusJoining` | `Join`: request posted, or already on the chain (the joining client dedupes a repeat request from the same identity) |
| `active`              | `StatusActive`  | the device whose waiter observes the acceptance, after its own load succeeds; other devices converge and load lazily |
| `joinEnded`           | `StatusDeleted` | a waiter observing the owner's decline (any device holding a head), or `CancelJoin` (any device). Non-terminal and classified as deleted: `Get` refuses it, `Subscribe` emits `Removed`, `Join` revives it, a direct add (`AddAccounts`) registers over it as `invitePending` (docs/direct-add-invites.md) |
| `deleted`             | `StatusDeleted` | `Delete`; terminal |

**Waiters.** The join controller runs one any-sync ACL waiter per joining
row on every device. A waiter reports acceptance whenever the ACL grants
the identity, but a decline only when the request is gone and the head it
was built on exists on the chain. A head is therefore a per-device
credential for one request (device-local `aclHeadId`), cleared whenever
the row leaves joining: a head from the previous request would satisfy
the decline test on a replica that hasn't seen the next one.

- The requesting device stores the head `RequestJoin` returned.
- A device that learned of the join from the synced row snapshots the
  chain first (`AclSnapshot` through the joining client, 20s bound):
  - membership granted → load, no waiter;
  - request pending → store the chain head and start a waiter on it;
  - neither → no waiter. The requesting device syncs its verdict, or a
    local `CancelJoin` / `Join` settles the row. The tick pass re-probes
    such a row indefinitely (one chain read per tick) and never ends it
    on its own;
  - chain unreadable → an acceptance-only waiter the next tick tries to
    upgrade.
- Kick-driven passes (every tech-space index change) throttle chain
  probes per row; the tick pass does not.

**`Join` always stamps `joining`**, even when the row already reads it.
The fresh version keeps a verdict for the previous request, still in
flight from another device, from landing over the new one under
last-writer-wins. Two devices acting at the same instant (a re-request
here, a withdrawal there) still resolve by DAG order; a winning
withdrawal leaves the new request on the chain with the row ended, and
another `Join` heals it.

**`CancelJoin` works from any device.** The withdrawal is identity-based
on the chain, so a device with no head can post it; the row is marked
ended synchronously. When the chain holds no request (the owner resolved
it first, or another device withdrew), one snapshot decides:

- membership granted → the space loads; `ErrJoinNotPending`;
- request still there → the waiter settles it; `ErrJoinNotPending`;
- neither → the row is marked ended; nil;
- chain unreadable → the transport error; row untouched.

**`Delete` on a joining row is a withdrawal**, routed through
`CancelJoin`. A tombstone would leave the request on the chain, and the
terminal `deleted` would make the space unjoinable for the account. If
the owner accepted meanwhile, the delete proceeds as for a member.

**Heals.**

- A loaded space whose row reads joining or ended while the live ACL
  grants membership (e.g. a withdrawal from another device landing over
  the accept) is flipped to active by `Info()` and the members watcher.
- An ended row that still holds local storage (the same race, seen at
  boot) is probed by the controller's tick pass and reloaded when the ACL
  grants membership; the boot pass and the orphan-collection sweep keep
  its storage.
- `Join` with a valid invite on any non-active row the ACL already grants
  loads the space without a new request.

**No handler transition rules for join values.** The space-index handler
sees the local pre-op state at arrival and the CRDT is per-path LWW, so a
"refuse X after Y" rule would diverge across devices. Only the monotone
terminal `deleted` is enforced, and it covers `joining` / `joinEnded`.

**Known limit.** The terminal `deleted` rule is checked against local
pre-op state at arrival, while the value is per-path LWW. A `Delete`
racing a synced `active` flip (join acceptance, `AcceptInvite`,
`JoinGuest`, a 1-1 accept) can converge to `active` on one device and
`deleted` on another. Carrying the tombstone on its own set-once field
would make it order-independent.

**Legacy rows.** Device-local join markers in `localStatus` (`joining`;
`deleted` over a synced `active` for an ended join) are still read with
the same meaning, and the controller runs a waiter for a legacy joining
row (it holds its own head). They are never written: the next verdict,
or a `Join` revival, lands in the synced form.

### Derived spaces are permanent

`Spaces().Delete` refuses seed-derived spaces with
`space.ErrIsDerivedSpace`. The id is deterministic, so delete + re-derive
would recreate the space with fresh history under the same id, and the
sticky tombstone would block the account's well-known id forever.

- The deriving account's row carries a synced set-once `derived` flag
  (`SpaceInfo.Derived`; re-running `Derive` adds it to an unflagged row).
- A joiner of someone else's derived space never gets the flag; they
  can't re-derive it, so removal stays allowed.
- Enforcement: `Delete` refuses flagged rows (and the tech space id,
  `ErrIsTechSpace`); the space-index handler rejects
  `remoteStatus=deleted` on flagged rows from any writer; the deletion
  reconciler skips them (a coordinator `NotExists` for a space derived
  offline must not tombstone it).
- 1-1 spaces keep their own re-derivable local-delete path.

### Space Lifecycle

**Deletion.** `Spaces().Delete(spaceId)` is offline-first: a synchronous
local half and a deferred network half.

1. Writes the synced `remoteStatus=deleted` tombstone to the tech-space
   index. This propagates the delete to the account's other devices and
   is the durable intent the reconciler scans on restart.
2. Offloads all local state immediately: closes the per-space watchers
   and Store, evicts the any-sync space, drops every SDK CRDT collection
   for the space, removes the any-sync per-space DB file, and GCs the
   account-values carrier. Disk is reclaimed even offline.
3. Kicks the background deletion reconciler, which sends the signed
   `coordinator.SpaceDelete` when online. Owner-only: the coordinator
   rejects non-owners, so deleting a space you don't own only offloads
   locally.

The reconciler also polls the coordinator (`StatusCheckMany`); a space it
reports gone (deleted on another device, or the owner deleted a space you
joined) is marked `deleted` and offloaded. The row is never physically
removed: it stays in `List` with `StatusDeleted`.

**1-1 and guest spaces** delete locally only. `Delete` writes a synced,
non-terminal marker (`oneToOneDeleted` / `guestDeleted`) and offloads;
there is no coordinator `SpaceDelete`, and the space stays on the nodes.
Other devices offload when the marker arrives. `OneToOne(peer)` or
`JoinGuest` re-adds the space.

**Offload is retryable; the startup sweep is the guarantee.**

- Each offload step tolerates already-gone state.
- The CRDT collection sweep commits in chunks (bounding the single-writer
  stall and persisting progress) and purges `_meta` watermarks before any
  collection drop.
- A real sweep failure (meta purge, failed drop, chunk commit) skips the
  any-sync storage removal, so `SpaceExists` keeps the boot retry armed;
  the independent file, account-values and identities cleanups still
  run.
- `<spaceId>_objects` is dropped last, after the per-object sweep, so an
  interrupted run can re-enumerate.
- A crash can still leave partial local state. Every `SDK.Open` runs an
  orphan-collection sweep (after the tech space opens, before other
  spaces load): it drops CRDT collections whose owner, the `<spaceId>_` /
  `<objectId>_` name prefix, resolves to a tombstoned space or to an
  object no kept space claims, and purges the matching `_meta` rows
  (sticky purge markers stay as the change-feed deletion signal).
  Absence of evidence is not deletion: a space-shaped owner with no
  tech-space row is kept, and its roster shields its objects.
- Fixed collections (`_meta`, `files_*`, `_history_*`, `_read_*`) are
  exempt: their prefixes are never valid content ids.

**Consumer collections.** `SDK.Store()` exposes `sdk.db` so a consumer
can keep its own non-CRDT collections in the same file (one snapshot
across both sides for `$lookup`; rollups via `$out` / `$merge`). Rules:

- Every consumer collection sits under a tag whose leading segment is not
  a content id. `l_` is reserved for the any server's local store.
- Such collections are never swept, re-indexed or rebuilt; a wiped
  `sdk.db` loses them.
- Consumers never write an SDK collection (a direct write bypasses the
  DAG and is reverted by re-index) and never open a write tx spanning a
  consumer collection and an SDK one.

**Consumer index wipe.** To drop derived indexes (search, UI caches) for
a deleted space, watch `Spaces().Subscribe` and purge per-space state
when the `spaceId` appears in `SpaceListEvent.Removed`. Every deleted
shape (tombstone, 1-1 / guest markers, ended join) surfaces as
`Removed`. Delivery is asynchronous and not ordered with the SDK's own
offload.

**Offline-first.** Everything works offline except account recovery on
a new device, which needs reachable peers.

**Direct peers.** Besides sync nodes, a space syncs with devices that
share it: LAN peers found over mDNS and, with `P2P.Global` on,
internet-wide peers discovered through the space's key-value records and
reached over iroh (relay fallback, hole punching). Global peers are never
dialed on a sync path; see `global-p2p.md`.

### Sync

- **Space loading.** Every space loads at boot, off the `Open` path.
  `Open` returns after local wiring (any-sync app, `sdk.db`, tech space,
  files/push, read sync, pending-join resume, 1-1 inbox). One background
  goroutine then runs, strictly serially: tombstone offloads, space load,
  offline catch-up replay (`spacesync.Run`), deletion reconcile, profile
  republish and read-state reconcile. Serial because concurrent
  commonspace builds spike RAM and CPU on constrained devices.
  `SDK.BootstrapDone()` closes when the pass finishes; `Close` cancels
  and joins it before teardown.
  - **Contract:** once `Open` returns, local reads are safe
    (`Spaces().List`, queries on loaded spaces, Create / Get / Modify).
    A query against a space not yet caught up serves its pre-offline
    state until its turn; per-object lazy cold restore still covers
    direct Gets. Headless mode skips the pass (`BootstrapDone`
    pre-closed).
  - Loaded spaces stay resident for the SDK's lifetime, so headsync and
    ACL sync stay subscribed without a caller touching the space.
- **Watermark persist on Close.** On clean `Close`, the per-space
  catch-up watermark (head-store `MaxLastAddSeq`) is snapshotted for
  every open space that is known clean, so the next boot's replay is a
  no-op instead of force-loading every tree the session touched. A crash
  skips the snapshot; the boot replay remains the fallback.
  - Allowlist-gated, dirty by default: a skipped snapshot only costs a
    replay, a wrong one loses records. A space qualifies through a
    successful boot catch-up `Run`, or by being created or derived this
    session. Joins and accepts stay dirty until their first boot `Run`.
  - A space with parked trees in the tree syncer is skipped
    (storage-committed but never materialized, e.g. `ErrNoReadKey` during
    join key propagation; the park-retry set is in memory, so the boot
    replay is their only cross-restart recovery).
  - The write never regresses the watermark.
- **Replication key.** One per account, read from the tech space id. The
  tech space is derived from the account key, so the key is known before
  any space loads, including on a new device.
- **Sync status.** Per-space, per-object and peer state: see
  `sync-status-proposal.md`.

### Files

See `files.md`.

### Storage topology

`Config.Storage.Topology`: `StorageShared` (default, one `sdk.db` for
all spaces) or `StoragePerSpace` (isolates writers, cheap deletion, no
cross-space transactions).

### Tech Space Integration

- Create, join and derive write the space's row in the tech-space index.
- Delete updates the row and offloads (see Space Lifecycle).
- All index writes go through SDK methods.

### Space type strings

- The on-wire `header.SpaceType` is gated by the any-sync coordinator
  (`spacestatus/changeverifier.go`). Accepted: the `any` product's
  `any.space`, `any.techspace`, `any.onetoone` (all require
  `fileprotoVersion=2` in the header) and anytype's `anytype.space`,
  `anytype.techspace`, `anytype.chatspace`, `anytype.onetoone`. Anything
  else fails periodic headsync with `unknown space type: <value>`, and
  because the type is content-addressed into the immutable header, a
  rejected value breaks the space permanently.
- The SDK mints only `any.*`, all with fileproto v2: created and derived
  spaces use `any.space` (`space.SpaceTypeAny`, the only non-empty value
  `CreateRequest.SpaceType` accepts), the tech space `any.techspace`, 1-1s
  `any.onetoone` (`space.SpaceTypeOneToOne`). `anytype.*` spaces belong
  to anytype-heart clients; the SDK can join or track them but never
  creates them.
- The 1-1 type feeds the symmetric derived 1-1 id, so an any↔anytype 1-1
  is structurally impossible.
- Rows registered before the header is readable (join, track, invite
  pending) carry an empty `type`, filled once from the header on first
  load.

## Open questions

- **Metadata before membership.** An invite token carries only the space
  id and key, so a request-to-join joiner sees no name or icon until the
  space loads. (Direct-add inbox invites carry an unauthenticated name
  hint; see direct-add-invites.md.)
