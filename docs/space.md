# Space

## Vision
A user creates a space or joins someone's space via invite. One space = one ACL + many objects, files, and optionally key-value data.

## Current any-sync Implementation

### Space Interface (core)
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

### Space Creation
`SpaceService.CreateSpace(SpaceCreatePayload)` → space ID

```go
type SpaceCreatePayload struct {
    SigningKey      crypto.PrivKey
    SpaceType       string
    ReplicationKey  uint64          // determines which nodes store this space
    SpacePayload    []byte
    MasterKey       crypto.PrivKey
    ReadKey         crypto.SymKey   // first read key for encryption
    MetadataKey     crypto.PrivKey
    Metadata        []byte          // owner metadata: the owner's metadata symkey
                                    // (NOT inline name/icon) — see docs/identities.md
    Options         *AclSpaceOptions
}
```

Steps: create space header (signed, produces CID-based space ID) → build ACL root → create settings tree → init storage.

### Space Joining

Two mechanisms:

**A. Request-to-Join (approval required)**
```
Invitee has invite key → RequestJoin() → owner receives request → AcceptRequest() / DeclineRequest()
```

**B. AnyoneCanJoin (no approval)**
```
Invitee has invite key → InviteJoin() → immediate access with specified permissions
```

```go
type AclJoiningClient interface {
    RequestJoin(ctx, spaceId string, payload RequestJoinPayload) (aclHeadId string, err error)
    InviteJoin(ctx, spaceId string, payload InviteJoinPayload) (aclHeadId string, err error)
    CancelJoin(ctx, spaceId string) error
    RequestSelfRemove(ctx, spaceId string, aclList AclList) error
}
```

#### Join lifecycle (SDK)

A request-to-join is account-wide state: the tech-space row's synced
`remoteStatus` carries it, so every device of the joiner's account
classifies the row the same way and a verdict observed on any device
converges the rest. Nothing is materialized until membership: `Service.Get`
refuses a joining row (`ErrSpaceNotAccepted`), the boot eager-loader and the
index follower skip it, and an unloaded space's ACL is watched only through
the joining client (no storage, no pull).

| remoteStatus (synced) | public Status   | written by |
|-----------------------|-----------------|------------|
| `joining`             | `StatusJoining` | `Join` — request posted, or found already on the chain (the joining client dedupes a repeat request from the same identity) |
| `active`              | `StatusActive`  | the device whose waiter observes the acceptance, after its own load succeeds; the account's other devices converge and load lazily |
| `joinEnded`           | `StatusDeleted` | a waiter observing the owner's decline (any device holding a head); `CancelJoin` (any device). Non-terminal, `IsDeleted`-classified: `Get` refuses it, `Subscribe` emits `Removed`, `Join` revives it, a direct add (`AddAccounts`) registers over it as `invitePending` (docs/direct-add-invites.md) |
| `deleted`             | `StatusDeleted` | `Delete`; terminal, as everywhere |

- **Waiters.** The join controller runs one any-sync ACL waiter per
  joining row on every device. A waiter reports acceptance whenever the
  ACL grants the identity, but a decline only when the request is gone
  AND the head it was built on exists on the chain — so a head is a
  per-device credential for ONE request (device-local `aclHeadId`),
  cleared whenever the row leaves joining: kept across requests, a head
  from the previous one would satisfy the decline test on a replica that
  has not seen the next request yet. The requesting device stores the
  head `RequestJoin` returned; a device that learned of the join from
  the synced row snapshots the chain first (`AclSnapshot`, through the
  joining client, bounded to 20s): membership already granted → load, no
  waiter; a pending request → the chain head, stored, waiter built on
  it; neither → no waiter (nothing to wait for — the requesting device
  syncs its verdict, or a local `CancelJoin` / `Join` settles the row;
  the tick pass re-probes such a row indefinitely, one chain read per
  tick, and never ends it on its own); chain unreadable → a head-less,
  acceptance-only waiter the next tick tries to upgrade. Kick-driven
  passes (every tech-space index change) throttle chain probes per row;
  the tick pass never does.
- **`Join` always stamps `joining`**, even when the cached row already
  reads it: the fresh version keeps a verdict for the PREVIOUS request,
  still in flight from another device, from landing over the new one
  under last-writer-wins. Residual: two devices acting at the same
  instant (a re-request here, a withdrawal there) still resolve by DAG
  order, and a withdrawal that wins leaves the new request on the chain
  with the row ended — `Join` again heals it (membership granted → load).
- **`CancelJoin` from any device.** The withdrawal is identity-based on
  the chain, so a device with no head can post it; the row is marked
  ended synchronously. When the chain holds no request (the owner
  resolved it first, or another device withdrew it), one snapshot
  decides: membership granted → the space loads, `ErrJoinNotPending`;
  request still there → the waiter settles it, `ErrJoinNotPending`;
  neither → the request is gone and the row is marked ended, nil; chain
  unreadable → the transport error, row untouched.
- **`Delete` on a joining row is a withdrawal**, routed through
  `CancelJoin`: a tombstone would leave the request on the chain and the
  terminal `deleted` would make the space unjoinable for the account
  forever. If the owner accepted meanwhile, the delete proceeds as for a
  member's space.
- **Heals.** A loaded space whose row still reads joining or ended while
  the live ACL grants membership (a withdrawal from another device
  landing over the accept; a legacy device-local marker) is flipped to
  active by `Info()` and the members watcher. An ended row that still
  holds local storage — the same race, seen at boot — is probed by the
  controller's tick pass and reloaded when the ACL grants membership;
  the boot pass and the orphan-collection sweep keep its storage instead
  of reclaiming it. `Join` with a valid invite on any non-active row the
  ACL already grants loads the space without a new request.
- **Known limit, pre-existing.** The terminal `deleted` rule is enforced
  on the LOCAL pre-op state at arrival, while the value itself is
  per-path LWW: a `Delete` racing a synced `active` flip (this join
  flip, `AcceptInvite`, `JoinGuest`, a 1-1 accept) can converge to
  `active` on one device and `deleted` on another. Carrying the tombstone
  on its own set-once field would make it order-independent.
- **No handler transition rules for the new values.** The space-index
  handler sees the local pre-op state at arrival and the CRDT is per-path
  LWW, so a "refuse X after Y" rule would diverge across devices; only
  the monotone terminal `deleted` is enforced, and it already covers
  `joining` / `joinEnded`.
- **Legacy rows.** Rows written before the move carry the join in the
  device-local `localStatus` (`joining`; `deleted` over a synced
  `active` for an ended join). They are still read — the controller runs
  a waiter for a legacy joining row (it holds its own head), mapStatus
  keeps their meaning — never written: the next verdict lands in the
  synced form, and `Join` revives a legacy ended row in the synced form.
  An un-upgraded device reads the new values through its default branch
  as active, the same misread it has today for every pending row.

### ACL Permissions
```go
const (
    AclPermissionsNone
    AclPermissionsReader   // read-only
    AclPermissionsGuest    // read-only, can't request removal
    AclPermissionsWriter   // read + write
    AclPermissionsAdmin    // write + manage accounts
    AclPermissionsOwner    // full control
)
```

### ACL Record Types
`AclRoot` → `AclAccountInvite` → `AclAccountRequestJoin` / `AclAccountInviteJoin` → `AclAccountRequestAccept` / `AclAccountRequestDecline` → `AclPermissionChange` → `AclAccountRemove` → `AclOwnershipChange`

### ACL Space Client (owner operations)
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

### Derived Spaces
- `DeriveSpace()` — deterministic from keys (used for tech space)
- `DeriveOneToOneSpace()` — shared space between two users, same ID regardless of key order
- **Seed-derived spaces are permanent** — `Spaces().Delete` refuses them with
  `space.ErrIsDerivedSpace`. The deterministic id means delete + re-derive would
  recreate the space with fresh history under the same id (history replacement),
  and the sticky deleted tombstone would wedge the account's well-known derived
  id forever. The deriving account's row carries a synced set-once `derived`
  flag (surfaced as `SpaceInfo.Derived`; healed onto pre-flag rows by
  re-running Derive); a joiner of someone else's derived space never gets the
  flag — they cannot re-derive it, so removal stays allowed. 1-1 spaces keep
  their own re-derivable local-delete path. Enforcement is layered: `Delete`
  refuses flagged rows (and the tech-space id — `ErrIsTechSpace`), the
  space-index handler rejects `remoteStatus=deleted` on flagged rows from any
  writer, and the deletion reconciler exempts them (a coordinator `NotExists`
  for a space derived offline must not tombstone it).

### Source files
- `any-sync/commonspace/space.go` — Space interface
- `any-sync/commonspace/spaceservice.go` — SpaceService
- `any-sync/commonspace/spacepayloads/payloads.go` — create/derive payloads
- `any-sync/commonspace/acl/aclclient/aclspaceclient.go` — owner ACL operations
- `any-sync/commonspace/acl/aclclient/acjoiningclient.go` — join operations
- `any-sync/commonspace/object/acl/list/list.go` — AclList
- `any-sync/commonspace/object/acl/list/models.go` — permissions model
- `any-sync/commonspace/object/acl/aclrecordproto/` — proto definitions

## Key Decisions

### ACL
- **Full ACL feature set** — SDK exposes all ACL operations available in any-sync (invite, accept/decline, remove, change permissions, ownership transfer, etc.). All 6 permission levels.
- **API shape** — mirror `AclClient` one-to-one
- **Invite format** — whatever any-sync requires (inherits format from any-sync, not reinvented)
- **Metadata & member names** — symkey-only model (see `docs/identities.md`): the ACL `RequestMetadata` carries each account's **metadata symkey**, not an inline name/icon; member names resolve from the encrypted `identityRepo` profile using that key. SDK wraps any-sync's metadata-key mechanism, doesn't invent its own
- **identityRepo integration** — handled internally by the SDK (background fetch + decrypt, write-through to the account-global identities directory). Public surface: `account.UpdateMetadata` (own profile) and `sdk.Identities()` (the resolved directory)
- **Members as a collection** — members are exposed as an any-store system collection with the same query/subscription rules as any other data. Callers read members via `Find()` + event flow
- **Join request notifications** — a new ACL record is added; SDK reacts to ACL changes via event flow and surfaces pending requests to the caller (likely as `status=pending` in the members collection, TBD)

### Space Lifecycle
- **Deletion (v1)** — `Spaces().Delete(spaceId)` is offline-first and splits into a local half (synchronous) and a network half (deferred):
  1. Writes the synced `remoteStatus=deleted` tombstone to the tech-space index — propagates the delete to the account's other devices and is the durable intent the deletion reconciler scans on restart.
  2. **Offloads all local state immediately** — closes the per-space watchers and Store, evicts the any-sync space, drops every SDK CRDT collection for the space, removes the any-sync per-space DB file, and GCs the account-values carrier. Disk is reclaimed even offline.
  3. Kicks the background **deletion reconciler**, which sends the signed `coordinator.SpaceDelete` confirmation — now if online, or on a later tick when connectivity returns. Owner-only: the coordinator rejects deletes from non-owners, so deleting a non-owned space offloads locally and the reconciler no-ops on the network call.
  - The reconciler also runs the **inbound** direction: it polls the coordinator (`StatusCheckMany`) and, for any space the coordinator reports gone (deleted on another device, or an owner deleted a space you joined), marks it `deleted` locally and offloads.
  - The tech-space row is never physically removed; it stays in `List` with `Status = StatusDeleted` (sticky tombstone).
- **Offload is retryable; the startup sweep is the guarantee** — each offload step tolerates already-gone state, but best-effort stops at the already-gone class: the CRDT collection sweep commits in chunks (bounding the single-writer stall and persisting incremental progress), purging `_meta` watermarks *before* any collection drop (the GC sweep's meta-dies-first invariant), and a real sweep failure (meta purge, failed drop, chunk commit) skips the any-sync storage removal — the independent file/account-values/identities cleanups still run — so `SpaceExists` keeps the boot retry gate armed and the next attempt finishes the remainder. A crash can still leave partial local state behind. The shared-DB half is self-healing: every `SDK.Open` runs an **orphan-collection sweep** (after the tech space opens, before any other space loads) that drops CRDT collections whose owner — the `<spaceId>_` / `<objectId>_` name prefix — resolves to a positively tombstoned (deleted/offloaded) space or an object no kept space claims, and purges the matching `_meta` watermark rows (except sticky purge markers, which stay as the change-feed deletion signal). Absence of evidence is not deletion: a space-shaped owner with no tech-space row is kept, and its roster shields its objects. Offload itself drops `<spaceId>_objects` last, after the per-object sweep, so an interrupted run can always re-enumerate. Fixed collections (`_meta`, `files_*`, `_history_*`, `_read_*`) are structurally exempt: their prefixes are never valid content ids. The same structural exemption is the **consumer-collection contract**: `SDK.Store()` exposes sdk.db so a consumer can keep its own non-CRDT collections in the same file (one snapshot across both sides for `$lookup`; rollups via `$out`/`$merge`), provided every such collection sits under a tag whose leading segment is not a content id — `l_` is reserved for the any server's local store. Those collections are never swept, never re-indexed, and never rebuilt: a wiped sdk.db loses them. Consumers never write an SDK collection (a direct write bypasses the DAG and is reverted by re-index) and never open a write tx spanning a consumer collection and an SDK one.
- **Consumer index wipe** — to drop your own derived indexes (search, UI caches) when a space is deleted, watch `Spaces().Subscribe` and purge your per-space state when a `spaceId` appears in `SpaceListEvent.Removed`. The SDK guarantees `Removed` fires for every offload (local delete and inbound-detected) because both set `remoteStatus=deleted`, which the subscription classifies as `Removed`. This is advisory/async — not ordered with the SDK's own offload.
- **1-1 spaces** — in v1 scope, but with separate logic:
  - Not removable from the network (derived, always re-creatable)
  - Can only be deleted locally
  - Still appear in the space index like regular spaces
- **Offline-first** — everything works offline. Only exception: account recovery still requires p2p peers
- **Direct peers** — besides the sync nodes, a space syncs with the devices that share it: LAN peers found over mDNS, and — with `P2P.Global` on — internet-wide peers discovered through the space's key-value records and reached over iroh (relay fallback, hole punching). Global peers are never dialed on a sync path; see docs/global-p2p.md.

### Sync
- **Space loading** — every space still loads at boot, but OFF the `Open` path: `Open` returns after local wiring (any-sync app, sdk.db, tech space, files/push, readSync, pending-join resume, 1-1 inbox) and one SDK-owned background goroutine then runs the eager loop — tombstone offloads, space load, offline catch-up replay (`spacesync.Run`), deletion reconcile — plus profile republish and the read-state reconcile, **strictly serial** (concurrent commonspace builds spike RAM/CPU exactly on the constrained devices this targets). `SDK.BootstrapDone()` closes when the pass finishes; `Close` cancels+joins it before any teardown.
  - **Contract**: `Open` returns ⇒ local reads are safe (`Spaces().List`, queries against loaded spaces, Create/Get/Modify). Full offline catch-up completes in the background; a query against a not-yet-caught-up space serves the pre-offline state until its turn (per-object lazy ColdRestore still covers direct Gets). Headless mode skips the pass entirely (`BootstrapDone` pre-closed).
  - **Watermark persist on Close** — the per-space catch-up watermark (head-store `MaxLastAddSeq`) is also snapshotted on clean `Close` for every open space whose catch-up completed that session: session-live applies already materialized everything, so the next boot's replay no-ops instead of force-loading every tree the session touched. A crash skips the snapshot and the boot replay remains the fallback. The snapshot is ALLOWLIST-gated (dirty is the default — a skipped snapshot only costs a replay; a wrong one loses records): a space qualifies via a successful boot catch-up `Run` or by being created/derived this session (born clean — the author device materializes its own writes). Joins/accepts are never allowlisted; they stay dirty until their first boot Run. Additionally, a space with **parked trees** in the treesyncer adapter is skipped (storage-committed but never materialized — e.g. `ErrNoReadKey` during join key propagation; the park-retry set is in-memory, so the boot replay is their only cross-restart recovery), and the write itself never regresses.
- **Replication key** — one per account. SDK reads it from the tech space ID (tech space is derived, its replication key is the canonical one for the account)
- **Sync status** — a separate SDK subsystem that tracks per-space, per-object, and peer connection status. Will be designed separately

### Files
- **Deferred** — will be covered in a separate section (files.md). Open question: keep the existing filenode system or move files to the any-sync level

### Storage Topology
- **One vs many any-store DBs** — deferred to Data Structure section

### Tech Space Integration
- Space creation/join → SDK writes to tech space index (internal, atomic)
- Space deletion → SDK updates tech space record to `status=deleted`, offloads local data, and the deletion reconciler sends the signed `coordinator.SpaceDelete` (see Space Lifecycle)
- All writes go through SDK methods, never direct

### Space type strings
- The on-the-wire `header.SpaceType` value is gated by the any-sync-coordinator (`spacestatus/changeverifier.go`). Accepted: the `any` product's own `any.space` / `any.techspace` / `any.onetoone` (all require `fileprotoVersion=2` in the header) plus the anytype names `anytype.space`, `anytype.techspace`, `anytype.chatspace`, `anytype.onetoone`. Anything else fails periodic headsync with `unknown space type: <value>` — and since the type is content-addressed into the immutable header, a rejected value bricks the space permanently.
- The SDK mints only the `any.*` family, all with fileproto v2: created AND derived spaces stamp `any.space` (`space.SpaceTypeAny`; the only value `CreateRequest.SpaceType` accepts besides empty), the tech space `any.techspace`, 1-1s `any.onetoone` (`space.SpaceTypeOneToOne`). The anytype.* names belong to anytype-heart clients — the SDK can join/track such spaces but never creates them.
- The 1-1 type feeds the symmetric derived 1-1 id, and 1-1s pair only within a product — the distinct type makes an any↔anytype 1-1 structurally impossible.
- Rows registered before the space's header is readable (join/track/invite-pending) carry an unknown (empty) `type`, backfilled set-once from the header on the first successful load.

## Grooming Questions (open)

### Space Metadata (deferred)
1. For spaces you haven't joined yet: metadata comes from the invite key + identityRepo. Confirm this is the model.
2. For spaces you have access to: maybe a derived object holding space metadata (name, icon, description). TBD.
3. When to groom: after we know more about derived objects and metadata flow.

### Replication Key
4. First login on a new device — do we know the replication key before tech space is loaded? Current any-sync flow needs checking. Probably derive from account key as a fallback.

### 1-1 Spaces
5. API for creating/opening a 1-1 space — `sdk.OneToOne(otherIdentity)`? Returns the derived space if it already exists locally.
6. "Delete only locally" semantics — what does this look like in the space index? `localStatus=deleted, remoteStatus=active`?

### Members Collection
7. Exact record schema — deferred until we start implementing.
8. `status=pending` vs separate join-requests collection — decide during implementation.

### Dependencies
9. Files → separate section (files.md)
10. Sync status → separate subsystem, designed later
