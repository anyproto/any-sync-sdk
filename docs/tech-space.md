# Tech Space

A derived space, deterministic from the account key, that stores
account-level data. One per account. It never appears in the space
list; callers reach it through dedicated methods, or as a restricted
`Space` handle via `Spaces().Get(SDK.TechSpaceId())`.

## Key Decisions

- **Derived.** Created on first login if absent on the device;
  deterministic id, always re-derivable.
- **Owner-only ACL.** The network refuses new ACL changes, so the tech
  space can never be shared.
- **Same CRDT, same Store.** It runs the regular `spaceobjects.Store`
  path: type registry, `<techSpaceId>_objects` collection, built-in
  handlers, runtime dataset catalog, schema gate. Its own datasets
  (`spaces`, `profile`, `inboxCursor`, `identities`, `devices`,
  `account_values`, `crdtVersion`) are type-less system built-ins
  (`techspace.SystemDatasets()`), ungated like `payloads` / `bundles`.
  The index object and the account-values carriers have no `objects`
  row. History indexing is off; there is no tech-space history surface.
- **any-store first.** All data lives in any-store; the SDK reads the DB
  and listens to its event flow, not in-memory state.
- **Loads first, stays loaded.** The tech space opens before regular
  spaces (the space list comes from it). Like every loaded space it
  stays resident for the SDK's lifetime; sync is continuous.
- **SpaceType** is always `any.techspace` (`techspace.TechSpaceType`)
  with `fileprotoVersion=2`, for every account and derivation index. It
  differs from anytype-heart's `anytype.techspace`, so an SDK account
  never shares a tech space with a heart client. The header feeds the
  derived id (pinned by `TestDeriveCfg_Stable`): changing it orphans
  every account's tech space. The coordinator's allow-list rejects
  any-sync's default `any-sync.space`. See
  `space.md § Space type strings`.

### Restricted handle

`Spaces().Get(SDK.TechSpaceId())` returns a `Space` backed by the tech
Store. It never appears in `List` / `Subscribe`; `Delete` and `Track`
refuse it with `ErrIsTechSpace`.

Supported:

- Reads: `Query`, `QueryObjects`, `Aggregate`, `Datasets`,
  `Objects().Get`, `Types()` / `Collections()` reads. `Query` on the
  index object's `identities` dataset returns `ErrUnsupported` (rows
  carry the profile-decryption `symKey`); use `SDK.Identities()`.
- `Bundles()`: a request must declare a type or collection (`Parts`,
  `Properties` or `XKey`); roots are minted by `Ensure` (derived or
  SDK-created); `ResolveLoser`. No `NewRoot`, `RootType`,
  `RootCollections`, `RootProperties`.
- Dataset declarations on bundle roots only: `Types().AddDataset`,
  `AddDatasetField`, `RemoveDataset*`, `PatchDataset` refuse any other
  `typeId`.
- Generic record writes (`Modify`, `ModifyMany`, `Delete`, `Upsert`) to
  datasets a bundle root declared. Type-system built-ins (`objects`,
  `properties`, `shortIds`, `datasets`) and the system datasets are
  readable but refuse generic writes.
- `Objects().Delete` on bundle roots only (uninstalls a created winner;
  derived roots refuse).
- `Info()` (synthetic: owner-only, derived, `any.techspace`),
  `SyncStatus`, `Debug`, `SyncHeads`, `TreeHeads`, `WaitIndexSynced`
  (same as `WaitListSynced`).

Everything else returns `space.ErrUnsupported`: `Objects().Create` /
`Derive`, `Types().Create` / `Delete`, `Collections().Create` /
`Delete` / `Patch`, property definitions outside bundle roots,
`Properties()`, `Members()`, `ACL()`, `Files()`, `History()`,
`ReadState()`, `PubSub()`, `Changes()`, `SetMetadata`. `Subscribe`-style
methods with no error return (`Members`, `Files`, `ReadState`,
`Changes`) are no-ops. See `bundles.md § Tech-space bundles`.

Compatibility: a device on a build without the Store-backed tech space
cannot apply a `bundles` change on the tech index object; its index
replay stops there and its space list freezes until it upgrades. Install
the first tech bundle only once every device of the account runs a
supporting build.

## What Tech Space Stores

### Space Index

The `spaces` dataset on the tech index object: one row per space the
account knows. Written only through SDK methods. `Spaces().List` reads
it; `Spaces().Subscribe` is a live subscription on it, and every deleted
shape (tombstone, offload markers, ended join) surfaces as `Removed`.

| Field | Scope | Meaning |
| --- | --- | --- |
| `id` | synced | Space id |
| `type` | synced | On-wire header type. Set once; empty until the header is readable (join/track rows), then filled from the loaded header |
| `name`, `description`, `icon` | synced | Display metadata, mirrored from the space's `spaceIndex` object |
| `spaceType` | synced | App-level tag mirrored from `spaceIndex.spaceType`; not pinned |
| `localStatus` | local | Per-device lifecycle, below |
| `remoteStatus` | synced | Account-wide lifecycle, below |
| `aclHeadId` | local | Join-request head credential, below |
| `createdAt` | derived | Added-to-account time, below |
| `push` | local | Push-notification keys, below |
| `ownRole` | local | This account's ACL permission, below |
| `guestKey` | synced | Guest identity private key on a guest-mode row; presence marks guest mode |
| `issuedInviteKeys` | synced | Invite private keys this account issued, below |
| `derived` | synced | Row written by this account's `Derive`, below |
| `oneToOnePeer` | synced | The other identity on a 1-1 row (needed to materialize storage; a spaceId doesn't encode it invertibly) |
| `oneToOneInviteState` | local | `toSend` while this device owes the peer a 1-1 inbox notification |
| `inviteNotifyPending` | local | Identities this device still owes a direct-add inbox notification after `AddAccounts` |
| `settings` | synced | Client-owned free-form object, written per key via `SetSettings`, so edits to different keys merge |

**`localStatus`** — `active` (or absent), `oneToOnePending` (incoming 1-1
prompt), `inviteLoading` / `guestLoading`, `guestRevoked`. Local because
a space offloaded on one device must stay loaded on another. Legacy join
markers (`joining`, and `deleted` over a synced `active`) are still read
as a pending / ended join, never written.

**`remoteStatus`** — `active`; the terminal tombstone `deleted`; the
non-terminal offload markers `oneToOneDeleted` / `guestDeleted`; the
direct-add pair `invitePending` / `inviteDeclined` (docs/direct-add-invites.md); the
request-to-join pair `joining` / `joinEnded`
(`space.md § Join lifecycle`). Pending states are synced so every
device classifies a row the same way: none materializes a space the
account is not a member of, and a verdict observed on one device
converges the others.

**`aclHeadId`** — an ACL head at or after the account's current join
request. The any-sync ACL waiter reports a decline only when the request
is gone and this head exists on the chain. A device that learned of the
join from the synced row resolves a head from the chain before starting a
waiter. Every device clears it when the row leaves joining: a head kept
across requests would decline the next request on a lagging replica.

**`createdAt`** — a `datetime` instant stamped by
`SpaceIndexHandler.BeforeCreate` from the creating change's timestamp
(Create for the author, Join for a joiner): per account, immutable, the
same on every device. 0 means unknown.

**`push`** — key material for decrypting push payloads while the SDK
process is down (mobile notification extensions). Mirrored from ACL
state by the per-space ACL mirror (`aclmirror_watcher.go`) at space load
and on every applied ACL record. Local because every device derives the
same values from the converged ACL. Encodings match anytype-heart's
space-view push keys:

- `spaceKey` — base64 (std) of the protobuf-marshalled ed25519 private
  key identifying the space on the push server (SLIP-10 `m/99999'/1'`
  off the ACL's first metadata key; fixed for the space's life).
- `encKey` — base64 (std) of the raw AES payload key (SLIP-21
  `m/SLIP-0021/anytype/space/key` off the current read key; rotates with
  it).
- `encKeyId` — hex(sha256(raw `encKey`)) = `pushapi.Message.KeyId`.

Clients read it from `SpaceInfo.PushKeys` and cache
`{encKeyId → encKey}` append-only in the OS keystore. Absent until the
mirror first runs, e.g. on a joiner with no read key yet.

**`ownRole`** — `owner` / `admin` / `writer` / `reader` / `guest` /
`none`, surfaced as `SpaceInfo.OwnRole` so a space list needs no
`Members().Me` per space. Mirrored by the same ACL mirror, independently
of `push` (a keyless reader still gets a role). Absent until the mirror
first runs (e.g. a space this device never loaded) and means "unknown
yet", not no access; `Members().Me` is authoritative. On a 1-1 the ACL
owner is the synthetic shared key, so participants read `writer`.

**`issuedInviteKeys`** — per-kind subkeys `member` (written by
`ACL.CreateInvite`) and `guest` (`ACL.CreateGuestKey`). ACL invite
records carry only the public key; this custody lets every device of the
issuing account re-show or revoke the same token (`Members.Invites`
returns it; `CreateGuestKey`'s idempotent path reads it). Per-path merge
keeps kinds minted on different devices apart. Revoke paths clear it,
and read paths verify it against live ACL state, hiding custody that
went stale. Kept apart from `guestKey` so an issuer's row never reads as
guest mode.

**`derived`** — set-once bool stamped at row create by `Spaces().Derive`
(re-running Derive adds it to an unflagged row). It makes derived spaces
permanent (`space.ErrIsDerivedSpace`): `Service.Delete` refuses flagged
rows, the handler rejects `remoteStatus=deleted` on them from any writer,
and the deletion reconciler skips them (a coordinator `NotExists` is
expected for a space derived offline before its first push). Absent on
created, joined, tracked and 1-1 rows; surfaced as `SpaceInfo.Derived`.

### Identities directory (`identities` dataset)

Account-global directory of every identity seen across spaces, 1-1s and
inbox invites; backs `sdk.Identities()`. One row per identity with mixed
scopes: `symKey` is synced (needed to decrypt that identity's
identityRepo profile); `name`, `description`, `iconCID` and the
`spaceIds` sighting set are device-local. See `identities.md`.

### Profile and inbox cursor

`profile` holds the account's own profile, republished to identityRepo
on boot. `inboxCursor` is the synced 1-1 inbox read position
(`one-to-one-spaces.md`).

### Devices registry (`devices` dataset)

One row per device of the account, keyed by libp2p peer id
(`SDK.PeerId()`). All fields synced: `name`, `os`, `version`, `apps`
(object keyed by app slug; presence = installed; slugs are an open set)
and `activeClaims` (per-slug `{seq, at, target?}`, the claims this
device made). Online status is not stored
here.

Reads go through the generic dataset surface
(`Spaces().Query(SpaceIndexObjectId(), "devices")`, `ListDevices`).
Writes go only through `SetDevice`, `ClaimActive` and `DeleteDevice`.
`SetDevice` and `ClaimActive` write only the local peer id's row;
`DeleteDevice` is the one write to another device's row. With no peer
id, `ClaimActive` claims for the local device and also marks the app
installed. Given a peer id, it requires that row in this replica's
registry (`ErrDeviceUnknown`) carrying the app
(`ErrDeviceAppNotInstalled`) and never writes `apps`; another device's
id becomes the claim's `target`. A pruned device's claim is refused
before anything is written (`ErrDevicePruned`). Claims from one device
are serialized, so the device's own claim `seq` never goes backwards.

**Active-app election** is resolved by readers with one rule,
`space.ActiveDevice`: the claims of all live rows rank by highest `seq`,
then highest `at`, then largest claimer peer id. The winner is the
target (the claimer when `target` is absent) of the best claim whose
target is a live row with the app installed; the claimer needs no app.
A claim is writer-supplied `{seq: max+1, at: now}`, not a CRDT version
id (those are peer-local and not comparable across devices). There is
no un-claim: the winner changes when a better claim appears, or when a
claim starts or stops qualifying — its claimer or target is deleted,
or its target uninstalls or reinstalls the app.

A device holds one claim per app, so handing an app away replaces the
device's own claim. Two consequences, both repaired by claiming again:
deleting the device that made the winning hand-off moves the app back
to the best remaining claim, and when a hand-off's target stops
qualifying, the fallback skips the device that handed it away.

- `DeleteDevice` tombstones are sticky, so a pruned peer id can never
  re-register. Deleting the local device's own row is refused
  (`ErrDeviceSelfDelete`); a pruned device's later `SetDevice` /
  `ClaimActive` returns `ErrDevicePruned`.
- Claims decode strictly (integer `seq >= 1`, at most 2^53; a `target`,
  when present, a non-empty string); a malformed claim reads as absent
  on every architecture.
- Known limit: `seq` comes from the claiming replica's view, so a claim
  made on a stale device can lose to an older unseen claim once heads
  converge. Re-claim after sync.

### CRDT version mark (`crdtVersion` dataset)

One record, `crdtVersion`, with one synced field `version`: the newest
CRDT data-model version (`space.CRDTVersion`) any SDK has written this
account with. Monotonic by handler rule. `Open` stamps it when absent or
lower; a higher stored value refuses `Open` with
`space.ErrCRDTVersionNewer`, and one arriving through sync at runtime
makes every synced write read-only (`SDK.CRDTVersion()`). Readable via
`Query(SpaceIndexObjectId(), "crdtVersion")`; no write surface. Contract:
`versioning.md`.

### Read state (key-value store)

Each tracked object's read frontier is published to the tech space's
key-value store under `read/<spaceId>/<objectId>`, one row per device,
and merged into the other devices' read-state engines. The owner-only ACL
keeps read positions invisible to other members. See
`read-tracking-proposal.md`.

### Account-values carriers

For every space the account participates in, the tech space hosts one
derived carrier object (seed `builtin:accountValues/<spaceId>`, dataset
`account_values`). It transports account-scoped values (declared
`scope: account`): synced across this account's devices, invisible to
other space members.

- One carrier record per target `(objectId, dataset, recordId)`, key
  `<objectId>:<dataset>:<recordId>`; the objects row is the common case.
  Record fields are the account-scoped paths verbatim
  (`{typeId}.{propId}`), so the record's own `_ver`, including entries
  retained by `$unset`, is the version source the per-space mirror
  replays into target rows.
- Lifecycle matches the target space: derived on first use (mirror start
  or first account write). Records of a deleted target object are
  tombstoned by whichever device sees the deletion first (idempotent).
- Space offload (leave or delete) drops the carrier tree, best-effort
  (`DropAccountValues`). Known issue: any-sync forbids deleting derived
  trees (delete + re-derive replaces history under the same id); this
  should become record-level GC that tombstones every carrier record and
  keeps the empty tree.
- Contract: `scoped-properties-proposal.md § "Account transport"`;
  mirror: `internal/spaceimpl/accountmirror.go`.

## Key Decisions (continued)

- **Write access.** The space index is written only through SDK methods.
  Clients store their own per-space data through `SetSettings`.
- **Deletion.** Deleted spaces stay in the index as tombstones
  (`remoteStatus=deleted`), never physically removed.
- **Validation on open.** `Spaces().Get` refuses rows the account is not
  a member of (`ErrSpaceNotAccepted`) and deleted rows
  (`ErrSpaceDeleted`).

## any-sync reference

`SpaceService.DeriveSpace(ctx, SpaceDerivePayload)` derives the space
from the signing key and master key, so the same account always gets
the same tech space id.

- `any-sync/commonspace/spaceservice.go` — `DeriveSpace`, `DeriveId`
- `any-sync/commonspace/spacestorage/spacestorage.go` — `SpaceStorage`
- `any-sync/commonspace/spacestorage/statestorage/` — `StateStorage`
- `any-sync/commonspace/spacestorage/headstorage/` — `HeadStorage`

## Open questions

- **Account preferences.** Not built; no schema yet.
