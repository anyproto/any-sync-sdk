# Tech Space

## Vision
A derived space (deterministic from account key) that stores account-level data. One tech space per account. Never listed; reachable as a restricted `Space` handle through `Spaces().Get(SDK.TechSpaceId())` for account-level bundles, otherwise used through dedicated methods.

## Key Decisions
- **Derived** — created on first login if not on device; deterministic ID from account key, always re-derivable
- **Derived ACL** — owner-only, network denies any new ACL changes. No shared accounts possible
- **Restricted handle** — `Spaces().Get(SDK.TechSpaceId())` returns a `Space` (a dedicated `techSpace` type delegating explicitly to the regular implementation over the tech Store) that supports reads (`Query` — except `identities` on the index object, whose rows carry the profile-decryption `symKey`: `ErrUnsupported`, read them via `SDK.Identities()` — `QueryObjects`, `Aggregate`, `Datasets`, `Objects().Get`, `Types()` reads), dataset declarations on bundle roots only (`Types().AddDataset` / `AddDatasetField` / `RemoveDataset*` / `PatchDataset` refuse any `typeId` that is not a self-typed bundle root), `Bundles()` (`Datasets` required, roots minted by `Ensure` — derived or SDK-created; no `NewRoot`, no `RootTypes` / `RootProperties`; `ResolveLoser` wired), generic record writes (`Modify` / `ModifyMany` / `Delete` / `Upsert`) to bundle datasets only — the datasets a bundle root declared; the type-system built-ins (`objects`, `properties`, `shortIds`, `datasets`) and the system datasets (`spaces`, `profile`, `inboxCursor`, `identities`, `devices`, `account_values`) are readable but refuse generic writes — `Info()` (synthetic: owner-only, derived, `any.techspace`), `SyncStatus`, `Debug`, `SyncHeads`, `TreeHeads`, `WaitIndexSynced` (= `WaitListSynced`). `Objects().Delete` works on bundle roots only (uninstall of a created winner; derived roots refuse at the object layer). Everything else — `Objects().Create/Derive`, `Types().Create/Delete` and property definitions, `Properties()`, `Members()`, `ACL()`, `Files()`, `History()`, `ReadState()`, `PubSub()`, `Changes()`, `SetMetadata` — returns `space.ErrUnsupported`; the four `Subscribe`-style methods without an error return (`Members` / `Files` / `ReadState` / `Changes`) are inert no-ops. The handle never appears in `List` / `Subscribe`; `Delete` / `Track` refuse it (`ErrIsTechSpace`). See `bundles.md § Tech-space bundles`
- **Rollout** — a device on an SDK build that still ran the raw tech Store (no gate, six handlers) cannot apply a `bundles` change on the tech index object: its index replay stops at that change and its space list freezes until it upgrades. Install the first tech bundle only once every device of the account runs a build with this Store path
- **Same CRDT, same Store** — the tech space runs the regular `spaceobjects.Store` path (type registry, `<techSpaceId>_objects` collection, built-in handlers, runtime dataset catalog, schema gate). Its own datasets (`spaces`, `profile`, `inboxCursor`, `identities`, `devices`, `account_values`) are registered as type-less system built-ins (`StoreConfig.SystemDatasets`, `techspace.SystemDatasets()`), ungated like `payloads`/`bundles`. The index object and the account-values carriers carry no `objects` row. History indexing is off (`DisableHistory`): there is no tech-space history surface
- **any-store first** — all data lives in any-store, SDK reads DB + listens to event flow, not in-memory state
- **ocache pattern** — any-sync `CommonSpace` managed via ocache (like any-sync-node), init/close by activity. SDK doesn't depend on space being loaded in memory
- **Sync priority** — tech space syncs first on startup, but sync is continuous (decentralized, never "done")
- **Tech space loads before regular spaces** — space list comes from tech space
- **SpaceType** — always `any.techspace` (`techspace.TechSpaceType`) with `fileprotoVersion=2`, for every account regardless of derivation index. Distinct from anytype-heart's `anytype.techspace`, so an SDK account never shares a tech space with a heart client even at index 0. The header feeds the derived tech-space id (pinned by `TestDeriveCfg_Stable`) — changing it orphans every account's tech space. Accepted pre-production break: accounts created before this shipped derive a new (empty) tech-space id on upgrade. The coordinator gates the allow-list (`spacestatus/changeverifier.go`); the library default `spacepayloads.SpaceReserved` (`any-sync.space`) is rejected. See `03-space.md § Space type strings`.

## What Tech Space Stores

### Space Index (derived object, record set)
Records with fields:
- `id` — space ID
- `type` — space type
- `name` — display name
- `icon` — space icon
- `localStatus` — device-local (`ScopeLocal`) lifecycle: `active`, the
  incoming-1-1 prompt (`oneToOnePending`), the loading markers
  (`inviteLoading` / `guestLoading`), `guestRevoked`. Absent means
  active. Legacy join markers — `joining`, and `deleted` over a synced
  `active` — are still read as a pending / ended join (mapStatus,
  `SpaceIndexRecord.JoinEnded`) but no longer written; a synced
  lifecycle state landing over the `deleted` marker reads through it
  (`LocalDeleteStands`).
- `remoteStatus` — synced (account-wide) state: `active`; the terminal
  tombstone `deleted`; the non-terminal offload markers `oneToOneDeleted`
  / `guestDeleted`; the direct-add invite pair `invitePending` /
  `inviteDeclined` (docs/15); and the request-to-join pair `joining` /
  `joinEnded` (docs/03-space.md § Join lifecycle). Pending states are
  synced so every device classifies a row the same way: none
  materializes a space the account is not a member of, and a verdict
  observed on one device converges the others.
- `aclHeadId` — device-local (`ScopeLocal`): an ACL head at-or-after the
  account's CURRENT join request. A per-device credential, not lifecycle
  state: the any-sync ACL waiter reports a decline only when the request
  is gone AND the head it was given exists on the chain, so a device that
  learned of the join from the synced row resolves one from the chain
  before it starts a waiter, and every device clears it when the row
  leaves joining (a head kept across requests would decline the next one
  on a lagging replica).
- `createdAt` — added-to-account time, a `datetime` instant. Handler-derived
  (`ScopeDerived`): `SpaceIndexHandler.BeforeCreate` stamps it from the
  creating change's timestamp when the row first lands — Create for the
  author, Join for a joiner — so it's per-account, immutable, and
  identical across the account's devices. Absent on rows created before
  the field existed; readers treat 0 as "unknown".
- `push` — device-local (`ScopeLocal`) push-notification key material,
  mirrored from ACL state by spaceimpl's per-space ACL mirror watcher
  (`aclmirror_watcher.go`; one mirror pass at space load + a pass per
  applied ACL record via the `aclKickMux` fan-out). Local, not synced,
  because every device derives identical values from the same converged
  ACL. Subfields (encodings byte-compatible with anytype-heart's
  `spacePushNotificationKey` / `spacePushNotificationEncryptionKey`
  space-view details):
  - `spaceKey` — base64(std) of the protobuf-marshalled ed25519 private
    key identifying the space on the push server (SLIP-10 `m/99999'/1'`
    off the ACL's first metadata key; fixed for the space's life).
  - `encKey` — base64(std) of the raw AES payload key (SLIP-21
    `m/SLIP-0021/anytype/space/key` off the CURRENT read key; rotates
    with it).
  - `encKeyId` — hex(sha256(raw encKey bytes)) = `pushapi.Message.KeyId`.
  Purpose: clients read it off `SpaceInfo.PushKeys` (or the raw row /
  its subscribe stream), cache `{encKeyId → encKey}` append-only in the
  OS keystore, and decrypt push payloads while the SDK process is down
  (mobile notification extensions). Absent until the mirror first runs —
  e.g. on a joiner whose access is still pending (no read key yet).
- `ownRole` — device-local (`ScopeLocal`) string: this account's own ACL
  permission in the space, in the canonical `space.Permission` wire
  vocabulary (`owner` / `admin` / `writer` / `reader` / `guest` /
  `none`). Mirrored by the same ACL mirror watcher as `push` (and local
  for the same reason), but independently of it: a keyless reader or
  pending joiner still gets a definite role mirrored while push-key
  derivation errors. Surfaced as `SpaceInfo.OwnRole` on List/Subscribe so
  clients read the caller's role off the space list without one
  `Members().Me` call per space. Absent until the mirror first runs —
  notably on rows whose space was never loaded by this device — which
  readers must treat as "unknown yet", not as no-access; `Members().Me`
  stays the authoritative per-space read. On a 1-1 space the ACL owner is
  the synthetic shared key, so participants mirror the role the ACL
  grants them (`writer`), never `owner`.
- `guestKey` — synced (`ScopeSynced`) string on a guest-mode row: the shared
  read-only guest identity's private key this account joined with (JoinGuest).
  Presence is the guest-mode discriminator; empty everywhere else.
- `issuedInviteKeys` — synced (`ScopeSynced`) object: this account's custody
  of the invite private keys it ISSUED for the space, one subkey per kind —
  `member` (RequestToJoin invite, written by `ACL.CreateInvite`) and `guest`
  (shared guest identity, written by `ACL.CreateGuestKey`). ACL invite
  records carry only the public key, so this custody is what lets every
  device of the issuing account re-show or revoke the same invite token
  (`Members.Invites` returns the key on the matching row; the idempotent
  `CreateGuestKey` fast path reads it). Per-kind subkeys merge per-path, so
  kinds minted on different devices never clobber each other. Cleared by the
  revoke paths; custody that goes stale (invite replaced/revoked elsewhere
  before the clear synced) is hidden by the read paths, which verify it
  against live ACL state before returning it. Distinct from `guestKey` so an
  issuer's own row never reads as guest-mode.
- `derived` — synced (`ScopeSynced`) set-once bool: marks a row written by the
  account's own `Spaces().Derive` (stamped at row create; healed onto a
  pre-existing unflagged row by re-running Derive — `SetDerived`). Gates every
  delete refusal for derived spaces (why they are permanent:
  `space.ErrIsDerivedSpace`): `Service.Delete` refuses flagged rows, the
  handler additionally rejects `remoteStatus=deleted` on them from ANY writer
  (so a pre-flag peer's synced tombstone is dropped on apply), and the
  deletion reconciler skips them entirely — a coordinator `NotExists` is
  expected for a space derived offline before its first push and must not
  tombstone the row. Pinned once true (the handler drops later edits, like
  `type`). Absent on created / joined / tracked / 1-1 rows; surfaced as
  `SpaceInfo.Derived`.
- etc.

### Account Preferences (derived object, postponed)
Probably a separate derived object with multiple record sets (namespaces) for different purposes. Schema TBD.

### Chat Read Tracking (KV namespace)
Read positions for chats, stored as key-value in tech space.

### Identities directory (`identities` dataset)
Account-global directory of every account identity seen across spaces, 1-1s,
and inbox invites — backs `sdk.Identities()`. One row per identity, **mixing
sync classes**: `symKey` is **synced** (a device needs it to decrypt that
contact's identityRepo profile), while the resolved `name`/`description`/
`iconCID` and the `spaceIds` sighting set are **device-local** (re-derived per
device). See `docs/14-identities.md`.

The tech space also hosts two account-scoped helper datasets: `profile` (the
account's own profile, republished to identityRepo on boot) and `inboxCursor`
(the synced 1-1 inbox read position — see `docs/13-one-to-one-spaces.md`).

### Devices registry (`devices` dataset, SYN-165)
One row per device of the account, keyed by the device's libp2p **peer id**
(`SDK.PeerId()`). All fields **synced**: `name`, `os`, `version`, `apps`
(free-form object keyed by app slug — presence = installed; slugs are an open
set, nothing app-specific is hardcoded) and `activeClaims` (per-slug
`{seq, at}` claims). Online status deliberately does not live here (KV /
event-bus territory).

System-owned like the space list: reads go through the generic dataset surface
(`Spaces().Query(SpaceIndexObjectId(), "devices")` / `ListDevices`), writes
only through the typed methods — `SetDevice` (self-row-only by construction:
the row id is always the local peer id), `ClaimActive`, `DeleteDevice`.

**Active-app election** — semantics live in the reader, not the write. A claim
is writer-supplied `{seq: max+1, at: now}` data, NOT a CRDT version id
(version ids are peer-locally allocated and not comparable across devices).
Every consumer resolves the winner with the single rule implementation,
`space.ActiveDevice`: among live rows with the app installed, highest `seq`
wins, ties broken by highest `at`, then largest peer id. There is no un-claim;
only a higher claim or a row deletion moves the winner. `DeleteDevice`
tombstones are sticky — a pruned peer id can never re-register — so the local
device's own row is refused (`ErrDeviceSelfDelete`: prune from another device),
and a pruned device's later `SetDevice`/`ClaimActive` writes surface
`ErrDevicePruned` instead of silently no-oping into the tombstone. Claims are
decoded strictly (numeric integer `seq >= 1`, at most 2^53) so a malformed or
out-of-range claim reads as absent on every architecture instead of electing
different winners. Known v1 limit: `seq` is minted from the claiming replica's
view, so a claim made on a stale (not-yet-synced) device can lose to an older
unseen claim once heads converge — claims are cheap, re-claim after sync.

## Current any-sync Implementation

### Derivation
```go
func (s *spaceService) DeriveSpace(ctx context.Context, payload SpaceDerivePayload) (string, error)
```
Deterministically derived from signing key + master key → same account always produces the same tech space ID.

### SpaceStorage internals
```go
type spaceStorage struct {
    spaceId       string
    headStorage   headstorage.HeadStorage
    stateStorage  statestorage.StateStorage
    aclStorage    list.Storage
    store         anystore.DB              // underlying any-store database
    addSeq        atomic.Uint64
}
```

### Source files
- `any-sync/commonspace/spaceservice.go` — DeriveSpace, DeriveId
- `any-sync/commonspace/spacestorage/spacestorage.go` — SpaceStorage interface & implementation
- `any-sync/commonspace/spacestorage/statestorage/` — StateStorage
- `any-sync/commonspace/spacestorage/headstorage/` — HeadStorage

## Key Decisions (continued)
- **Write access** — space index is written only via SDK methods, never direct writes. Caller can add custom metadata but through SDK API
- **Deletion** — deleted spaces stay in the index with `status=deleted`, never physically removed
- **Space validation on open** — when opening a space from the index, SDK verifies it's still valid (ACL intact, not deleted remotely). Probably at a higher level than tech space itself
- **Chat read tracking** — separate API surface, but internally accesses tech space KV namespace. Chat is not a first-class tech space concept

## Grooming Questions (open)

### Space Index (schema deferred)
1. Record schema will be finalized later. Space loading is complex (statuses like `waitingApprove`, 1-1 spaces with separate logic, etc.) — schema evolves with the space lifecycle design

### Account Preferences (deferred)
2. Postponed — will be groomed separately when we know the SDK API shape

### Data Flow
3. SDK reads any-store + listens events. What exactly triggers re-indexing of the space list? (any-sync push → any-store write → event → SDK notifies caller)
4. How does the caller subscribe to space list changes? Same event flow as regular collections, or dedicated `OnSpaceListChange` callback?

### Dependencies
5. Depends on Auth. Regular spaces depend on tech space for the space index. Opening a space triggers validation at a higher level.

## Account-values carriers

For every target space the account participates in, the tech space
hosts one derived **account-values carrier object** (seed
`builtin:accountValues/<spaceId>`, dataset `account_values`). It is
the transport for account-scoped values — properties (and, later,
dataset fields) declared `scope: account`, which sync across this
account's devices but stay invisible to other space members.

- One carrier record per target `(objectId, dataset, recordId)`,
  record key `<objectId>:<dataset>:<recordId>`; the objects row is the
  degenerate case. Record fields are the account-scoped paths VERBATIM
  (`{typeId}.{propId}`), so the record's own `_ver` — including
  entries retained by $unset — is the version source the per-space
  mirror replays into target rows (injected applies, grouped by
  carrier version).
- Lifecycle is 1:1 with the target space: derived on first use
  (mirror start or first account write), deleted whole on space
  leave/delete. Records of a deleted target object are tombstoned by
  whichever device observes the deletion first (idempotent).
- Full contract: docs/scoped-properties-proposal.md § "Account
  transport"; mirror implementation: internal/spaceimpl/accountmirror.go.
